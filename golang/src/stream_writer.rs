use crate::error::{clear_error, set_error};
use crate::memory::{ackOperationDone, Buffer};
use derive_new::new;
use pravega_client::event::writer::EventWriter;
use pravega_client_shared::ScopedStream;
use std::panic::{catch_unwind, AssertUnwindSafe};
use tokio::runtime::Handle;
use std::sync::mpsc::{Receiver, SyncSender};

pub struct StreamWriter {
    sender: SyncSender<Incoming>,
    runtime_handle: Handle,
    stream: ScopedStream,
}

impl StreamWriter {
    pub fn new(
        sender: SyncSender<Incoming>,
        runtime_handle: Handle,
        stream: ScopedStream,
    ) -> Self {
        StreamWriter {
            sender,
            runtime_handle,
            stream,
        }
    }
}

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

#[derive(new)]
pub struct WriterReactor {}

impl WriterReactor {
    pub async fn run(
        mut writer: EventWriter,
        mut receiver: Receiver<Incoming>,
    ) {
        while WriterReactor::run_once(&mut writer, &mut receiver)
            .await
            .is_ok()
        {}
    }

    async fn run_once(
        writer: &mut EventWriter,
        receiver: &mut Receiver<Incoming>,
    ) -> Result<(), String> {
        let incoming = receiver.recv().expect("sender closed, processor exit");
        match incoming.operation {
            Operation::WriteEvent{routing_key, event} => {
                match routing_key {
                    Option::None => writer.write_event(event).await,
                    Option::Some(key) => writer.write_event_by_routing_key(key, event).await,
                };
            },
            Operation::Flush => {
                writer.flush().await.map_err(|e| format!("Error caught while flushing events {:?}", e))?
            }
            Operation::Close => {
                return Err("close".to_string());
            }
        }
        unsafe {
            ackOperationDone(incoming.id, 0 as usize);
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
        let event = event.read().unwrap().to_vec();
        let routing_key = routing_key
            .read()
            .map(|v| std::str::from_utf8(v).unwrap().to_string());
        stream_writer.sender.send(Incoming::new(
            id,
            Operation::WriteEvent{routing_key, event}
        )).ok();
        clear_error();
    })) {
        set_error("caught panic".to_string(), err);
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_writer_flush(writer: *mut StreamWriter, id: i64, err: Option<&mut Buffer>) {
    let stream_writer = &mut *writer;
    
    if let Err(_) = catch_unwind(AssertUnwindSafe(move || {
        stream_writer.sender.send(Incoming::new(
            id,
            Operation::Flush
        )).ok();
        clear_error();
    })) {
        set_error("caught panic".to_string(), err);
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_writer_destroy(writer: *mut StreamWriter) {
    let stream_writer = &mut *writer;
    
    stream_writer.sender.send(Incoming::new(
        0,
        Operation::Close
    )).ok();

    if !writer.is_null() {
        Box::from_raw(writer);
    }
}
