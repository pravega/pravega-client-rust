//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use std::collections::vec_deque::Drain;
use std::collections::VecDeque;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::oneshot::Receiver;

pub struct OneShotHolder<E> {
    size: usize,
    inflight: VecDeque<Receiver<Result<(), E>>>,
}

impl<E> OneShotHolder<E> {
    pub fn new(size: usize) -> OneShotHolder<E> {
        OneShotHolder {
            size,
            inflight: VecDeque::with_capacity(size),
        }
    }

    ///
    /// Method to add oneShot Receivers. The method will await on the oneShot receivers
    /// only if the size of queue is greater than configured size.
    ///
    pub async fn add(&mut self, item: Receiver<Result<(), E>>) -> Result<Result<(), E>, RecvError> {
        if self.size == 0 {
            // size is zero await on oneshot receiver directly.
            return item.await;
        }
        let result = if self.inflight.len() >= self.size {
            // await until the first receiver in the list has completed.
            let fut = self.inflight.pop_front().unwrap();
            fut.await
        } else {
            Ok(Ok(()))
        };
        // Append the Receiver to the queue.
        self.inflight.push_back(item);
        result
    }

    ///
    /// Creates a draining iterator that removes all the oneShot receivers
    /// from the OneShotHolder and yields the removed items.
    ///
    pub fn drain(&mut self) -> Drain<'_, Receiver<Result<(), E>>> {
        self.inflight.drain(..)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot::channel;

    #[derive(Debug)]
    struct CustomError;

    #[tokio::test]
    async fn test_oneshot_holder() {
        let mut holder: OneShotHolder<CustomError> = OneShotHolder::new(1);

        let (tx1, rx1) = channel::<Result<(), CustomError>>();
        let (tx2, rx2) = channel::<Result<(), CustomError>>();
        let r = holder.add(rx1).await.unwrap();
        assert!(r.is_ok());
        tokio::spawn(async move {
            if let Err(_) = tx1.send(Ok(())) {
                panic!("error is not expected");
            }
        });

        //wait until rx1 is completed.
        let r = holder.add(rx2).await.unwrap();
        assert!(r.is_ok());

        tokio::spawn(async move {
            if let Err(_) = tx2.send(Err(CustomError)) {
                panic!("error is not expected");
            }
        });
        let mut iter = holder.drain();
        match iter.next() {
            Some(r) => {
                if let Ok(_) = r.await.unwrap() {
                    panic!("Error expected");
                }
            }
            None => panic!("Expected an entry."),
        };
        assert!(iter.next().is_none());
    }

    #[tokio::test]
    async fn test_zero_size_oneshot_holder() {
        let mut holder: OneShotHolder<CustomError> = OneShotHolder::new(0);

        let (tx1, rx1) = channel::<Result<(), CustomError>>();
        tokio::spawn(async move {
            if let Err(_) = tx1.send(Ok(())) {
                panic!("error is not expected");
            }
        });
        let r = holder.add(rx1).await.unwrap();
        assert!(r.is_ok());

        let mut iter = holder.drain();
        assert!(iter.next().is_none());
    }

    #[tokio::test]
    async fn test_receiver_error() {
        let mut holder: OneShotHolder<CustomError> = OneShotHolder::new(1);

        let (tx1, rx1) = channel::<Result<(), CustomError>>();
        let (tx2, rx2) = channel::<Result<(), CustomError>>();
        let r = holder.add(rx1).await.unwrap();
        assert!(r.is_ok());

        tokio::spawn(async move {
            drop(tx1);
        });

        //wait until rx1 is completed.
        let r = holder.add(rx2).await;
        assert!(r.is_err()); //since tx1 is dropped the result is of type Error.

        tokio::spawn(async move {
            if let Err(_) = tx2.send(Err(CustomError)) {
                panic!("error is not expected");
            }
        });
        let mut iter = holder.drain();
        match iter.next() {
            Some(r) => {
                if let Ok(_) = r.await.unwrap() {
                    panic!("Error expected");
                }
            }
            None => panic!("Expected an entry."),
        };
        assert!(iter.next().is_none());
    }
}
