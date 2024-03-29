use crate::error::{clear_error, set_error};
use crate::memory::{ackOperationDone, Buffer};
use derive_new::new;
use pravega_client::error::Error;
use pravega_client::event::writer::EventWriter;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr::null;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum Operation {
    WriteEvent(oneshot::Receiver<Result<(), Error>>),
    Flush,
    Close,
}

#[derive(new, Debug)]
pub struct Incoming {
    id: i64,
    operation: Operation,
}

pub struct StreamWriter {
    writer: EventWriter,
    sender: UnboundedSender<Incoming>,
    runtime_handle: Handle,
}

impl StreamWriter {
    pub fn new(writer: EventWriter, sender: UnboundedSender<Incoming>, runtime_handle: Handle) -> Self {
        StreamWriter {
            writer,
            sender,
            runtime_handle,
        }
    }

    pub async fn run_reactor(mut receiver: UnboundedReceiver<Incoming>) {
        while StreamWriter::run_once(&mut receiver).await.is_ok() {}
    }

    async fn run_once(receiver: &mut UnboundedReceiver<Incoming>) -> Result<(), String> {
        let incoming = receiver.recv().await.ok_or("sender closed, processor exit")?;
        match incoming.operation {
            Operation::WriteEvent(future) => {
                future.await.expect("event persisted").unwrap();
            }
            Operation::Flush => {}
            Operation::Close => {
                return Err("close".to_string());
            }
        }
        unsafe {
            ackOperationDone(incoming.id, 0, null(), 0);
        };
        Ok(())
    }

    pub fn write_event(&mut self, event: Vec<u8>, routing_key: Option<String>, id: i64) {
        let runtime = &self.runtime_handle;
        let writer = &mut self.writer;
        let sender = &mut self.sender;
        runtime.block_on(async {
            let future = match routing_key {
                Option::None => writer.write_event(event).await,
                Option::Some(key) => writer.write_event_by_routing_key(key, event).await,
            };
            sender
                .send(Incoming::new(id, Operation::WriteEvent(future)))
                .expect("write event");
        });
    }

    pub fn flush(&mut self, id: i64) {
        self.sender
            .send(Incoming::new(id, Operation::Flush))
            .expect("flush event");
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
    let result = catch_unwind(AssertUnwindSafe(move || {
        let event = event.read().expect("read event").to_vec();
        let routing_key = routing_key
            .read()
            .map(|v| std::str::from_utf8(v).expect("read routing key").to_string());
        stream_writer.write_event(event, routing_key, id);
    }));
    if result.is_err() {
        set_error("caught panic".to_string(), err);
    } else {
        clear_error();
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_writer_flush(writer: *mut StreamWriter, id: i64, err: Option<&mut Buffer>) {
    let stream_writer = &mut *writer;

    if catch_unwind(AssertUnwindSafe(move || stream_writer.flush(id))).is_err() {
        set_error("caught panic".to_string(), err);
    } else {
        clear_error();
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_writer_destroy(writer: *mut StreamWriter) {
    let stream_writer = &mut *writer;
    stream_writer
        .sender
        .send(Incoming::new(0, Operation::Close))
        .expect("close writer");

    if !writer.is_null() {
        drop(Box::from_raw(writer));
    }
}
