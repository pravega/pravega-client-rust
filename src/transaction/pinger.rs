//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactory;
use crate::error::*;
use futures::FutureExt;
use pravega_client_shared::{PingStatus, ScopedStream, TxId};
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::time::sleep;
use tracing::{debug, error, info};

#[derive(Debug)]
enum PingerEvent {
    Add(TxId),
    Remove(TxId),
}

/// Pinger is used to ping transactions periodically. It spawns a task that runs in the background
/// to ping transactions without blocking the current thread. The spawned task uses a loop to run
/// the ping logic and sleeps(does not block other tasks) for a given time before next iteration.
pub(crate) struct Pinger {
    stream: ScopedStream,
    txn_lease_millis: u64,
    ping_interval_millis: u64,
    factory: ClientFactory,
    receiver: UnboundedReceiver<PingerEvent>,
    shutdown: oneshot::Receiver<()>,
}

/// PingerHandle is just a wrapped channel sender which is used to communicate with the Pinger.
/// It can be used to add or remove a transaction from Pinger's ping list.
#[derive(Clone)]
pub(crate) struct PingerHandle(UnboundedSender<PingerEvent>);

impl PingerHandle {
    pub(crate) fn add(&mut self, txn_id: TxId) -> Result<(), TransactionalEventStreamWriterError> {
        if let Err(e) = self.0.send(PingerEvent::Add(txn_id)) {
            error!("pinger failed to add transaction: {:?}", e);
            Err(TransactionalEventStreamWriterError::PingerError {
                msg: format!("failed to add transaction due to: {:?}", e),
            })
        } else {
            Ok(())
        }
    }

    pub(crate) fn remove(&mut self, txn_id: TxId) -> Result<(), TransactionalEventStreamWriterError> {
        if let Err(e) = self.0.send(PingerEvent::Remove(txn_id)) {
            error!("pinger failed to remove transaction: {:?}", e);
            Err(TransactionalEventStreamWriterError::PingerError {
                msg: format!("failed to remove transaction due to: {:?}", e),
            })
        } else {
            Ok(())
        }
    }
}

impl Pinger {
    pub(crate) fn new(
        stream: ScopedStream,
        txn_lease_millis: u64,
        factory: ClientFactory,
    ) -> (Self, PingerHandle, oneshot::Sender<()>) {
        let (tx, rx) = unbounded_channel();
        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        let pinger = Pinger {
            stream,
            txn_lease_millis,
            ping_interval_millis: Pinger::get_ping_interval(txn_lease_millis),
            factory,
            receiver: rx,
            shutdown: oneshot_rx,
        };
        let handle = PingerHandle(tx);
        (pinger, handle, oneshot_tx)
    }

    pub(crate) async fn start_ping(&mut self) {
        // this set is used to store the transactions that are alive and
        // needs to be pinged periodically.
        let mut txn_list: HashSet<TxId> = HashSet::new();

        // this set is used to store the transaction that are aborted or committed
        let mut completed_txns: HashSet<TxId> = HashSet::new();

        loop {
            // try receive any incoming events
            if let Some(option) = self.receiver.recv().now_or_never() {
                if let Some(event) = option {
                    match event {
                        PingerEvent::Add(id) => {
                            txn_list.insert(id);
                        }
                        PingerEvent::Remove(id) => {
                            txn_list.remove(&id);
                        }
                    }
                } else {
                    panic!("pinger sender gone");
                }
            }

            // remove completed transactions from the ping list
            txn_list.retain(|i| !completed_txns.contains(i));
            completed_txns.clear();

            info!("start sending pings to {} transactions.", txn_list.len());
            // for each transaction in the ping list, send ping to the server
            for txn_id in txn_list.iter() {
                debug!(
                    "sending ping request for txn ID: {:?} with lease: {:?}",
                    txn_id, self.txn_lease_millis
                );
                let status = self
                    .factory
                    .get_controller_client()
                    .ping_transaction(
                        &self.stream,
                        txn_id.to_owned(),
                        Duration::from_millis(self.txn_lease_millis),
                    )
                    .await
                    .expect("ping transaction");

                if let PingStatus::Ok = status {
                    debug!("successfully pinged transaction {:?}", txn_id);
                } else {
                    debug!("transaction {:?} is committed/aborted", txn_id);
                    completed_txns.insert(txn_id.to_owned());
                }
            }
            info!("sending transaction pings complete.");

            // delay for transaction lease milliseconds.
            tokio::select! {
                _ = sleep(Duration::from_millis(self.txn_lease_millis)) => {
                    debug!("pinger wake up after {}ms", self.txn_lease_millis);
                }
                _ = &mut self.shutdown => {
                    info!("shut down pinger");
                    return;
                }
            }
        }
    }

    fn get_ping_interval(txn_lease_millis: u64) -> u64 {
        //Provides a good number of attempts: 1 for <4s, 2 for <9s, 3 for <16s, 4 for <25s, ... 10 for <100s
        //while at the same time allowing the interval to grow as the timeout gets larger.
        let target_num_pings = if txn_lease_millis > 1000u64 {
            f64::sqrt(txn_lease_millis as f64 / 1000f64)
        } else {
            1f64
        };
        (txn_lease_millis as f64 / target_num_pings).round() as u64
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_ping_interval() {
        assert_eq!(Pinger::get_ping_interval(1000u64), 1000u64);
        assert_eq!(Pinger::get_ping_interval(4000u64), 2000u64);
        assert_eq!(Pinger::get_ping_interval(9000u64), 3000u64);
        assert_eq!(Pinger::get_ping_interval(16000u64), 4000u64);
        assert_eq!(Pinger::get_ping_interval(25000u64), 5000u64);
        println!("{}", Pinger::get_ping_interval(90 * 1000u64))
    }
}
