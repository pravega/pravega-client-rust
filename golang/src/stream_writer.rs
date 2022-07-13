use crate::error::set_error;
use crate::memory::{ackOperationDone, Buffer};
use pravega_client::event::writer::EventWriter;
use pravega_client_shared::ScopedStream;
use std::panic::{catch_unwind, AssertUnwindSafe};
use tokio::runtime::Handle;

pub struct StreamWriter {
    writer: EventWriter,
    runtime_handle: Handle,
    stream: ScopedStream,
}

impl StreamWriter {
    pub fn new(
        writer: EventWriter,
        runtime_handle: Handle,
        stream: ScopedStream,
    ) -> Self {
        StreamWriter {
            writer,
            runtime_handle,
            stream,
        }
    }

    pub async fn write_event_bytes(&mut self, event: Vec<u8>, routing_key: Option<String>) {
        match routing_key {
            Option::None => self.writer.write_event(event).await,
            Option::Some(key) => self.writer.write_event_by_routing_key(key, event).await,
        };
    }

    pub async fn flush(&mut self) -> Result<(), String> {
        self.writer.flush().await.map_err(|e| format!("Error caught while flushing events {:?}", e))
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
        
        let handle = stream_writer.runtime_handle.clone();
        handle.spawn(async move {
            stream_writer.write_event_bytes(event, routing_key).await;
            ackOperationDone(id, 0 as usize);
        });
    })) {
        set_error("caught panic".to_string(), err);
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_writer_flush(writer: *mut StreamWriter, id: i64, err: Option<&mut Buffer>) {
    let stream_writer = &mut *writer;
    
    if let Err(_) = catch_unwind(AssertUnwindSafe(move || {
        let handle = stream_writer.runtime_handle.clone();
        handle.spawn(async move {
            match stream_writer.flush().await {
                Ok(_) => {
                    unsafe {
                        ackOperationDone(id, 0 as usize);
                    };
                }
                Err(err) => {
                    // TODO: send error msg through the channel
                    println!("{}", err);
                }
            }
        });
    })) {
        set_error("caught panic".to_string(), err);
    }
}
