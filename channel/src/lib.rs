/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

use futures_intrusive::sync::{GenericSemaphoreReleaser, Semaphore};
use std::cmp::min;
use std::sync::Arc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub struct ChannelSender<T> {
    sender: UnboundedSender<(T, usize)>,
    semaphore: Arc<Semaphore>,
    capacity: usize,
}

impl<T> ChannelSender<T> {
    pub async fn send(&self, message: (T, usize)) -> Result<(), SendError<(T, usize)>> {
        let size = message.1;
        let n_permits = min(size, self.capacity);
        let mut result = self.semaphore.acquire(n_permits).await;
        //disable the automatically drop
        GenericSemaphoreReleaser::disarm(&mut result);
        self.sender.send(message)?;
        Ok(())
    }
}

impl<T> Clone for ChannelSender<T> {
    fn clone(&self) -> ChannelSender<T> {
        ChannelSender {
            sender: self.sender.clone(),
            semaphore: self.semaphore.clone(),
            capacity: self.capacity,
        }
    }
}

pub struct ChannelReceiver<T> {
    receiver: UnboundedReceiver<(T, usize)>,
    semaphore: Arc<Semaphore>,
    capacity: usize,
}

impl<T> ChannelReceiver<T> {
    pub async fn recv(&mut self) -> Option<(T, usize)> {
        let message = self.receiver.recv().await;
        if let Some(msg) = message {
            let size = msg.1;
            let n_permits = min(size, self.capacity);
            self.semaphore.release(n_permits);
            Some(msg)
        } else {
            message
        }
    }
}

pub fn create_channel<U>(capacity: usize) -> (ChannelSender<U>, ChannelReceiver<U>) {
    let (tx, rx) = unbounded_channel();
    let semaphore = Semaphore::new(true, capacity);
    let semaphore_arc = Arc::new(semaphore);
    let sender = ChannelSender {
        sender: tx,
        semaphore: semaphore_arc.clone(),
        capacity,
    };
    let receiver = ChannelReceiver {
        receiver: rx,
        semaphore: semaphore_arc,
        capacity,
    };
    (sender, receiver)
}

#[cfg(test)]
mod tests {
    use super::create_channel;
    use std::{thread, time};
    use tokio::runtime::Runtime;

    #[test]
    fn test_wrapper() {
        let mut runtime = Runtime::new().unwrap();
        runtime.block_on(test_simple_test());
        runtime.block_on(test_send_order());
        runtime.block_on(test_sender_block());
        runtime.block_on(test_sender_close_first());
        runtime.block_on(test_receiver_close_first());
    }

    async fn test_simple_test() {
        // can only hold 4 bytes
        let (tx, mut rx) = create_channel(4);

        tokio::spawn(async move {
            if let Err(_) = tx.send((1, 4)).await {
                println!("receiver dropped");
            }
        });

        if let Some(i) = rx.recv().await {
            assert_eq!(i, (1, 4));
        } else {
            panic!("Test failed");
        }
    }

    async fn test_send_order() {
        // can only hold 4 bytes
        let (tx, mut rx) = create_channel(4);

        let tx1 = tx.clone();
        tokio::spawn(async move {
            thread::sleep(time::Duration::from_secs(1));
            if let Err(_) = tx1.send((1, 4)).await {
                println!("receiver dropped");
            }
        });

        let tx2 = tx.clone();
        tokio::spawn(async move {
            if let Err(_) = tx2.send((2, 4)).await {
                println!("receiver dropped");
            }
        });

        // 2 should come first.
        if let Some(i) = rx.recv().await {
            assert_eq!(i, (2, 4));
        } else {
            panic!("test failed");
        }

        if let Some(i) = rx.recv().await {
            assert_eq!(i, (1, 4));
        } else {
            panic!("test failed");
        }
    }

    async fn test_sender_block() {
        // can only hold 7 bytes
        let (tx, mut rx) = create_channel(7);

        let tx1 = tx.clone();
        // need 4 bytes.
        tokio::spawn(async move {
            if let Err(_) = tx1.send((1, 4)).await {
                println!("receiver dropped");
            }
        });

        // need another 4 bytes. (will block)
        let tx2 = tx.clone();
        tokio::spawn(async move {
            tokio::time::delay_for(time::Duration::from_secs(1)).await;
            if let Err(_) = tx2.send((2, 4)).await {
                println!("receiver dropped");
            }
        });


        if let Some(message) = rx.recv().await {
            assert_eq!(message, (1, 4));
        } else {
            panic!("test failed");
        }

        if let Some(message) = rx.recv().await {
            assert_eq!(message, (2, 4));
        } else {
            panic!("test failed");
        }
    }

    async fn test_sender_close_first() {
        let (tx, mut rx) = create_channel(100);

        // tx would drop in this thread
        tokio::spawn(async move {
            for i in 0..10 {
                if let Err(_) = tx.send((i, 4)).await {
                    println!("receiver dropped");
                    return;
                }
            }
        });

        for i in 0..10 {
            if let Some(j) = rx.recv().await {
                assert_eq!((i, 4), j);
            }
        }

        // `None` is returned when all `Sender` halves have dropped, indicating
        // that no further values can be sent on the channel.
        if let None = rx.recv().await {
            println!("Test passed");
        } else {
            panic!("Test failed");
        }
    }

    async fn test_receiver_close_first() {
        let (tx, mut rx) = create_channel(100);
        tx.send((1, 4)).await.expect("send message to channel");

        tokio::spawn(async move {
            if let Some(i) = rx.recv().await {
                assert_eq!(i, (1, 4));
                return;
            }
        });
        thread::sleep(time::Duration::from_secs(1));
        let result = tx.send((2, 4)).await;
        assert!(result.is_err());
    }
}
