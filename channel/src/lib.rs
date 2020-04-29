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
use std::mem::size_of_val;
use std::sync::Arc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub struct ChannelSender<T> {
    sender: UnboundedSender<T>,
    semaphore: Arc<Semaphore>,
}

impl<T> ChannelSender<T> {
    pub async fn send(&self, message: T) -> Result<(), SendError<T>> {
        let size = size_of_val(&message);
        println!("size is {}", size);
        let mut result = self.semaphore.acquire(size).await;
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
        }
    }
}

pub struct ChannelReceiver<T> {
    receiver: UnboundedReceiver<T>,
    semaphore: Arc<Semaphore>,
}

impl<T> ChannelReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        let message = self.receiver.recv().await;
        if let Some(msg) = message {
            let size = size_of_val(&msg);
            self.semaphore.release(size);
            Some(msg)
        } else {
            message
        }
    }
}

pub fn create_channel<T>(capacity: usize) -> (ChannelSender<T>, ChannelReceiver<T>) {
    let (tx, rx) = unbounded_channel();
    let semaphore = Semaphore::new(true, capacity);
    let semaphore_arc = Arc::new(semaphore);
    let sender = ChannelSender {
        sender: tx,
        semaphore: semaphore_arc.clone(),
    };
    let receiver = ChannelReceiver {
        receiver: rx,
        semaphore: semaphore_arc,
    };
    (sender, receiver)
}

#[cfg(test)]
mod tests {
    use super::create_channel;
    use std::{thread, time};
    use tokio::runtime::Runtime;
    use std::thread::Thread;
    use std::mem::size_of_val;

    #[test]
    fn test_wrapper() {
        let mut runtime = Runtime::new().unwrap();
        runtime.block_on(test_simple_test());
        runtime.block_on(test_send_order());
        runtime.block_on(test_sender_block());
        runtime.block_on(test_sender_close_first());
        runtime.block_on(test_receive_close_first());
    }

    async fn test_simple_test() {
        // can only hold 4 bytes
        let (tx, mut rx) = create_channel(4);

        tokio::spawn(async move {
            if let Err(_) = tx.send(1).await {
                println!("receiver dropped");
            }
        });

        if let Some(i) = rx.recv().await {
            assert_eq!(i, 1);
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
            if let Err(_) = tx1.send(1).await {
                println!("receiver dropped");
            }
        });

        let tx2 = tx.clone();
        tokio::spawn(async move {
            if let Err(_) = tx2.send(2).await {
                println!("receiver dropped");
            }
        });

        // 2 should come first.
        if let Some(i) = rx.recv().await {
            assert_eq!(i, 2);
        } else {
            panic!("test failed");
        }

        if let Some(i) = rx.recv().await {
            assert_eq!(i, 1);
        } else {
            panic!("test failed");
        }
    }

    async fn test_sender_block() {
        // can only hold 7 bytes
        let (tx, mut rx) = create_channel(7);

        let tx1 = tx.clone();
        // need 2 bytes.
        tokio::spawn(async move {
            if let Err(_) = tx1.send(1).await {
                println!("receiver dropped");
            }
        });

        // need 4 bytes. (will block)
        let tx2 = tx.clone();
        tokio::spawn(async move {
            if let Err(_) = tx2.send(2).await {
                println!("receiver dropped");
            }
        });

        let tx3 = tx.clone();
        tokio::spawn(async move {
            if let Err(_) = tx3.send(3).await {
                println!("receiver dropped");
            }
        });


        if let Some(message) = rx.recv().await {
            assert_eq!(message, 1);
        } else {
            panic!("test failed");
        }

        if let Some(message) = rx.recv().await {
            assert_eq!(message, 2);
        } else {
            panic!("test failed");
        }

        if let Some(message) = rx.recv().await {
            assert_eq!(message, 3);
        } else {
            panic!("test failed");
        }
    }

    //
    async fn test_sender_close_first() {
        let (tx, mut rx) = create_channel(100);

        // tx would drop in this thread
        tokio::spawn(async move {
           for i in 0..10 {
               if let Err(_) = tx.send(i).await {
                   println!("receiver dropped");
                   return;
               }
           }
        });

        for i in 0..10 {
            if let Some(j) = rx.recv().await {
                assert_eq!(i, j);
            }
        }

        if let None = rx.recv().await {
            println!("Test passed");
        } else {
            panic!("Test failed");
        }
    }

    async fn test_receive_close_first() {
        let (tx, mut rx) = create_channel(100);
        tx.send(1).await;

        tokio::spawn(async move {
            if let Some(i) = rx.recv().await {
                assert_eq!(i, 1);
                return;
            }
        });
        thread::sleep(time::Duration::from_secs(1));
        let result = tx.send(2).await;

        
    }
}