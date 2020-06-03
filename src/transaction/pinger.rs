//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use crate::client_factory::ClientFactoryInternal;
use crate::error::*;
use log::{debug, error, info};
use pravega_rust_client_shared::{PingStatus, ScopedStream, TxId};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::delay_for;

#[derive(Debug)]
enum PingerEvent {
    Add(TxId),
    Remove(TxId),
    Terminate,
}

/// Pinger is used to ping transactions periodically. It spawns a task that runs in the background
/// to ping transactions without blocking the current thread. The spawned task uses a loop to run
/// the ping logic and sleeps(does not block other tasks) for a given time before next iteration.
pub(crate) struct Pinger {
    stream: ScopedStream,
    txn_lease_millis: u64,
    ping_interval_millis: u64,
    factory: Arc<ClientFactoryInternal>,
    receiver: Receiver<PingerEvent>,
}

/// PingerHandle is just a wrapped channel sender which is used to communicate with the Pinger.
/// It can be used to add or remove a transaction from Pinger's ping list.
#[derive(Clone)]
pub(crate) struct PingerHandle(Sender<PingerEvent>);

impl PingerHandle {
    pub(crate) async fn add(&mut self, txn_id: TxId) -> Result<(), TransactionalEventStreamWriterError> {
        if let Err(e) = self.0.send(PingerEvent::Add(txn_id)).await {
            error!("pinger failed to add transaction: {:?}", e);
            Err(TransactionalEventStreamWriterError::PingerError {
                msg: String::from("add transaction"),
            })
        } else {
            Ok(())
        }
    }

    pub(crate) async fn remove(&mut self, txn_id: TxId) -> Result<(), TransactionalEventStreamWriterError> {
        if let Err(e) = self.0.send(PingerEvent::Remove(txn_id)).await {
            error!("pinger failed to remove transaction: {:?}", e);
            Err(TransactionalEventStreamWriterError::PingerError {
                msg: String::from("remove transaction"),
            })
        } else {
            Ok(())
        }
    }

    pub(crate) async fn shutdown(&mut self) -> Result<(), TransactionalEventStreamWriterError> {
        if let Err(e) = self.0.send(PingerEvent::Terminate).await {
            error!("pinger failed to shutdown: {:?}", e);
            Err(TransactionalEventStreamWriterError::PingerError {
                msg: String::from("shutdown"),
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
        factory: Arc<ClientFactoryInternal>,
    ) -> (Self, PingerHandle) {
        let (tx, rx) = channel(100);
        let pinger = Pinger {
            stream,
            txn_lease_millis,
            ping_interval_millis: Pinger::get_ping_interval(txn_lease_millis),
            factory,
            receiver: rx,
        };
        let handle = PingerHandle(tx);
        (pinger, handle)
    }

    pub(crate) async fn start_ping(&mut self) {
        // this set is used to store the transactions that are alive and
        // needs to be pinged periodically.
        let mut txn_list: HashSet<TxId> = HashSet::new();

        // this set is used to store the transaction that are aborted or committed
        let mut completed_txns: HashSet<TxId> = HashSet::new();

        loop {
            // try receive any incoming events
            match self.receiver.try_recv() {
                Ok(event) => match event {
                    PingerEvent::Add(id) => {
                        txn_list.insert(id);
                    }
                    PingerEvent::Remove(id) => {
                        txn_list.remove(&id);
                    }
                    PingerEvent::Terminate => {
                        return;
                    }
                },
                Err(e) => {
                    if e != TryRecvError::Empty {
                        error!("pinger exit with error: {:?}", e);
                    }
                }
            }

            info!("start sending transaction pings.");

            // remove completed transactions from the ping list
            txn_list.retain(|i| !completed_txns.contains(i));
            completed_txns.clear();

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
            info!("completed sending transaction pings.");

            // delay for transaction lease milliseconds.
            delay_for(Duration::from_millis(self.txn_lease_millis)).await;
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
    }
}
