use crate::error::{clear_error, set_error};
use crate::memory::{ackOperationDone, Buffer};
use derive_new::new;
use pravega_client::error::Error;
use pravega_client::event::writer::EventWriter;
use pravega_client_shared::ScopedStream;
use std::panic::{catch_unwind, AssertUnwindSafe};
use tokio::runtime::Handle;
use std::sync::mpsc::{Receiver, SyncSender};
use tokio::sync::oneshot;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub enum Operation {
    WriteEvent{routing_key: Option<String>, event: Vec<u8>},
    Flush,
    Close,
}

#[derive(new)]
pub struct Incoming {
    id: i64,
    operation: Operation,
}

#[derive(new, Debug)]
pub struct EventMetadata {
    future: oneshot::Receiver<Result<(), Error>>,
    id: i64,
}

pub struct StreamWriter {
    writer: EventWriter,
    sender: UnboundedSender<EventMetadata>,
    runtime_handle: Handle,
    stream: ScopedStream,
}

impl StreamWriter {
    pub fn new(
        writer: EventWriter,
        sender: UnboundedSender<EventMetadata>,
        runtime_handle: Handle,
        stream: ScopedStream,
    ) -> Self {
        StreamWriter {
            writer,
            sender,
            runtime_handle,
            stream,
        }
    }

    pub async fn run_reactor(
        mut receiver: UnboundedReceiver<EventMetadata>,
    ) {
        while StreamWriter::run_once(&mut receiver)
            .await
            .is_ok()
        {}
    }

    async fn run_once(
        receiver: &mut UnboundedReceiver<EventMetadata>,
    ) -> Result<(), String> {
        let metadata = receiver.recv().await.expect("sender closed, processor exit");
        metadata.future.await.unwrap().unwrap();
        unsafe {
            ackOperationDone(metadata.id, 0);
        };
        Ok(())
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_writer_write_event(
    writer: *mut StreamWriter,
    event: Buffer,
    routing_key: Buffer,
    id: i64,
    err: Option<&mut Buffer>,
) {
    let stream_writer = &mut *writer;
    if let Err(_) = catch_unwind(AssertUnwindSafe(move || {
        let event = event.read().expect("read event").to_vec();
        let routing_key = routing_key
            .read()
            .map(|v| std::str::from_utf8(v).expect("read routing key").to_string());

        let runtime = &stream_writer.runtime_handle;
        let writer = &mut stream_writer.writer;
        let sender = &mut stream_writer.sender;
        runtime.block_on(async {
            let future = match routing_key {
                Option::None => writer.write_event(event).await,
                Option::Some(key) => writer.write_event_by_routing_key(key, event).await,
            };
            sender.send(EventMetadata::new(future, id)).expect("send event metadata");
        });
        clear_error();
    })) {
        set_error("caught panic".to_string(), err);
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_writer_flush(writer: *mut StreamWriter, id: i64, err: Option<&mut Buffer>) {
    let stream_writer = &mut *writer;
    
    if let Err(_) = catch_unwind(AssertUnwindSafe(move || {
        stream_writer.runtime_handle.block_on(stream_writer.writer.flush()).unwrap();
        clear_error();
    })) {
        set_error("caught panic".to_string(), err);
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_writer_destroy(writer: *mut StreamWriter) {
    if !writer.is_null() {
        Box::from_raw(writer);
    }
}
