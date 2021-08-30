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
    inflight: VecDeque<tokio::sync::oneshot::Receiver<Result<(), E>>>,
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
    pub async fn add(
        &mut self,
        item: tokio::sync::oneshot::Receiver<Result<(), E>>,
    ) -> Result<Result<(), E>, RecvError> {
        let result;
        if self.inflight.len() > self.size {
            // await until the first receiver in the list has completed.
            let fut = self.inflight.pop_front().unwrap();
            result = fut.await;
        } else {
            result = Ok(Ok(()));
        }
        // Append the Receiver to the queue.
        self.inflight.push_back(item);
        result
    }

    pub fn drain(&mut self) -> Drain<'_, Receiver<Result<(), E>>> {
        self.inflight.drain(..)
    }
}
