//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use std::collections::VecDeque;
use std::net::SocketAddr;

use crate::{get_random_u64, get_request_id};
use snafu::ResultExt;
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info, warn};

use pravega_rust_client_retry::retry_async::retry_async;
use pravega_rust_client_retry::retry_policy::RetryWithBackoff;
use pravega_rust_client_retry::retry_result::RetryResult;
use pravega_rust_client_shared::*;
use pravega_wire_protocol::client_connection::*;
use pravega_wire_protocol::commands::{AppendBlockEndCommand, SetupAppendCommand};
use pravega_wire_protocol::wire_commands::{Replies, Requests};

use crate::client_factory::ClientFactoryInternal;
use crate::error::*;
use crate::metric::ClientMetrics;
use crate::raw_client::RawClient;
use crate::reactor::event::{Incoming, PendingEvent, ServerReply};
use std::fmt;

pub(crate) struct SegmentWriter {
    /// unique id for each EventSegmentWriter
    pub(crate) id: WriterId,

    /// the segment that this writer is writing to, it does not change for a EventSegmentWriter instance
    pub(crate) segment: ScopedSegment,

    /// the endpoint for segment, it might change if the segment is moved to another host
    endpoint: SocketAddr,

    /// client connection that writes to the segmentstore
    writer: Option<WritingClientConnection>,

    /// events that are sent but yet acknowledged
    inflight: VecDeque<Append>,

    /// events that are waiting to be sent
    pending: VecDeque<Append>,

    /// incremental event id
    event_num: i64,

    /// the sender that sends back reply to processor
    sender: Sender<Incoming>,

    /// The client retry policy
    retry_policy: RetryWithBackoff,
}

impl SegmentWriter {
    /// maximum data size in one append block
    const MAX_WRITE_SIZE: i32 = 8 * 1024 * 1024 + 8;
    const MAX_EVENTS: i32 = 500;

    pub(crate) fn new(
        segment: ScopedSegment,
        sender: Sender<Incoming>,
        retry_policy: RetryWithBackoff,
    ) -> Self {
        SegmentWriter {
            endpoint: "127.0.0.1:0".parse::<SocketAddr>().expect("get socketaddr"), //Dummy address
            id: WriterId::from(get_random_u64()),
            writer: None,
            segment,
            inflight: VecDeque::new(),
            pending: VecDeque::new(),
            event_num: 0,
            sender,
            retry_policy,
        }
    }

    pub(crate) fn get_uuid(&self) -> WriterId {
        self.id
    }

    pub(crate) fn get_segment_name(&self) -> String {
        self.segment.to_string()
    }

    pub(crate) async fn setup_connection(
        &mut self,
        factory: &ClientFactoryInternal,
    ) -> Result<(), SegmentWriterError> {
        info!("setting up connection for segment writer {:?}", self.id);
        let uri = match factory
            .get_controller_client()
            .get_endpoint_for_segment(&self.segment) // retries are internal to the controller client.
            .await
        {
            Ok(uri) => uri,
            Err(e) => return Err(SegmentWriterError::RetryControllerWriting { err: e }),
        };

        self.endpoint = uri.0.parse::<SocketAddr>().expect("should parse to socketaddr");

        let request = Requests::SetupAppend(SetupAppendCommand {
            request_id: get_request_id(),
            writer_id: self.id.0 as u128,
            segment: self.segment.to_string(),
            delegation_token: "".to_string(),
        });

        let raw_client = factory.create_raw_client(self.endpoint);
        let result = retry_async(self.retry_policy, || async {
            debug!(
                "setting up append for writer:{:?}/segment:{}",
                self.id,
                self.segment.to_string()
            );
            match raw_client.send_setup_request(&request).await {
                Ok((reply, connection)) => RetryResult::Success((reply, connection)),
                Err(e) => {
                    warn!("failed to setup append using rawclient due to {:?}", e);
                    RetryResult::Retry(e)
                }
            }
        })
        .await;

        let mut connection = match result {
            Ok((reply, connection)) => match reply {
                Replies::AppendSetup(cmd) => {
                    debug!(
                        "append setup completed for writer:{:?}/segment:{:?}",
                        cmd.writer_id, cmd.segment
                    );
                    self.ack(cmd.last_event_number);
                    connection
                }
                Replies::WrongHost(cmd) => {
                    warn!("wrong host when setting up append: {:?}", cmd);
                    return Err(SegmentWriterError::WrongHost {
                        error_msg: format!("{:?}", cmd),
                    });
                }
                _ => {
                    warn!(
                        "append setup failed for writer:{:?}/segment:{:?} due to {:?}",
                        self.id, self.segment, reply
                    );
                    return Err(SegmentWriterError::WrongReply {
                        expected: String::from("AppendSetup"),
                        actual: reply,
                    });
                }
            },
            Err(e) => return Err(SegmentWriterError::RetryRawClient { err: e }),
        };

        let (mut r, w) = connection.split();
        self.writer = Some(w);

        let segment = self.segment.clone();
        let mut sender = self.sender.clone();

        // spin up connection listener that keeps listening on the connection
        tokio::spawn(async move {
            loop {
                // listen to the receiver channel
                let reply = match r.read().await {
                    Ok(reply) => reply,
                    Err(e) => {
                        warn!("connection {:?} failed to read data back from segmentstore due to {:?}, closing the listener task", r.get_id(), e);
                        return;
                    }
                };

                let result = sender
                    .send(Incoming::ServerReply(ServerReply {
                        segment: segment.clone(),
                        reply,
                    }))
                    .await;

                if let Err(e) = result {
                    error!("connection {:?} read data from segmentstore but failed to send reply back to processor due to {:?}", r.get_id() ,e);
                    return;
                }
            }
        });
        info!("finished setting up connection");
        Ok(())
    }

    /// first add the event to the pending list
    /// then write the pending list if the inflight list is empty
    pub(crate) async fn write(&mut self, event: PendingEvent) -> Result<(), SegmentWriterError> {
        self.add_pending(event);
        self.write_pending_events().await
    }

    /// add the event to the pending list
    pub(crate) fn add_pending(&mut self, event: PendingEvent) {
        self.event_num += 1;
        self.pending.push_back(Append {
            event_id: self.event_num,
            event,
        });
    }

    /// write the pending events to the server. It will grab at most MAX_WRITE_SIZE of data
    /// from the pending list and send them to the server. Those events will be moved to inflight list waiting to be acked.
    pub(crate) async fn write_pending_events(&mut self) -> Result<(), SegmentWriterError> {
        if !self.inflight.is_empty() || self.pending.is_empty() {
            return Ok(());
        }

        let mut total_size = 0;
        let mut to_send = vec![];
        let mut event_count = 0;

        while let Some(append) = self.pending.pop_front() {
            assert!(
                append.event.data.len() <= SegmentWriter::MAX_WRITE_SIZE as usize,
                "event size {} must be under {}",
                append.event.data.len(),
                SegmentWriter::MAX_WRITE_SIZE
            );
            if append.event.data.len() + to_send.len() <= SegmentWriter::MAX_WRITE_SIZE as usize
                || event_count == SegmentWriter::MAX_EVENTS as usize
            {
                event_count += 1;
                total_size += append.event.data.len();
                to_send.extend(append.event.data.clone());
                self.inflight.push_back(append);
            } else {
                self.pending.push_front(append);
                break;
            }
        }

        debug!(
            "flushing {} events of total size {} to segment {:?}; event segment writer id {:?}/connection id: {:?}",
            event_count,
            to_send.len(),
            self.segment.to_string(),
            self.id,
            self.writer.as_ref().expect("must have writer").get_id(),

        );

        let request = Requests::AppendBlockEnd(AppendBlockEndCommand {
            writer_id: self.id.0 as u128,
            size_of_whole_events: total_size as i32,
            data: to_send,
            num_event: self.inflight.len() as i32,
            last_event_number: self.inflight.back().expect("last event").event_id,
            request_id: get_request_id(),
        });

        let writer = self.writer.as_mut().expect("must have writer");
        writer.write(&request).await.context(SegmentWriting {})?;

        update!(ClientMetrics::ClientAppendBlockSize, total_size as u64, "Segment Writer Id" => self.id.to_string());
        update!(
            ClientMetrics::ClientOutstandingAppendCount,
            self.pending.len() as u64,
            "Segment Writer Id" => self.id.to_string()
        );
        Ok(())
    }

    /// ack inflight events. It will send the reply from server back to the caller using oneshot.
    pub(crate) fn ack(&mut self, event_id: i64) {
        // no events need to ack
        if self.inflight.is_empty() {
            return;
        }

        // event id is -9223372036854775808 if no event acked for appendsetup
        if event_id < 0 {
            return;
        }

        loop {
            let acked = self.inflight.front().expect("must not be empty");

            // event has been acked before, it must be caused by reconnection
            if event_id < acked.event_id {
                return;
            }

            let acked = self.inflight.pop_front().expect("must have");
            if acked.event.oneshot_sender.send(Result::Ok(())).is_err() {
                debug!(
                    "failed to send ack back to caller using oneshot due to Receiver dropped: event id {:?}",
                    acked.event_id
                );
            }

            // ack up to event id
            if acked.event_id == event_id {
                break;
            }
        }
    }

    /// get the unacked events. Notice that it will pass the ownership
    /// of the unacked events to the caller, which means this method can only be called once.
    pub(crate) fn get_unacked_events(&mut self) -> Vec<PendingEvent> {
        let mut ret = vec![];
        while let Some(append) = self.inflight.pop_front() {
            ret.push(append.event);
        }
        while let Some(append) = self.pending.pop_front() {
            ret.push(append.event);
        }
        ret
    }

    pub(crate) async fn reconnect(&mut self, factory: &ClientFactoryInternal) {
        loop {
            debug!("Reconnecting event segment writer {:?}", self.id);
            // setup the connection
            let setup_res = self.setup_connection(factory).await;
            if setup_res.is_err() {
                continue;
            }

            while self.inflight.back().is_some() {
                self.pending
                    .push_front(self.inflight.pop_back().expect("must have event"));
            }

            // flush any pending events
            let flush_res = self.write_pending_events().await;
            if flush_res.is_err() {
                continue;
            }

            return;
        }
    }
}

impl fmt::Debug for SegmentWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegmentWriter")
            .field("segment writer id", &self.id)
            .field("segment", &self.segment)
            .field("endpoint", &self.endpoint)
            .field(
                "writer",
                &match &self.writer {
                    Some(w) => format!("WritingClientConnection id is {}", w.get_id()),
                    None => "doesn't have writer".to_owned(),
                },
            )
            .field(
                "inflight",
                &format!("number of inflight events is {}", &self.inflight.len()),
            )
            .field(
                "pending",
                &format!("number of pending events is {}", &self.pending.len()),
            )
            .field("current event_number", &self.event_num)
            .finish()
    }
}

struct Append {
    event_id: i64,
    event: PendingEvent,
}
