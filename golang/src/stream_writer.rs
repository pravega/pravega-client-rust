use pravega_client::error::Error as WriterError;
use pravega_client::event::writer::EventWriter;
use pravega_client::util::oneshot_holder::OneShotHolder;
use pravega_client_shared::ScopedStream;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::oneshot::error::RecvError;
use tokio::time::timeout;
use crate::error::set_error;
use crate::memory::Buffer;

// The amount of time the python api will wait for the underlying write to be completed.
const TIMEOUT_IN_SECONDS: u64 = 120;
pub struct StreamWriter {
    writer: EventWriter,
    runtime_handle: Handle,
    stream: ScopedStream,
    inflight: OneShotHolder<WriterError>,
}

impl StreamWriter {
    pub fn new(
        writer: EventWriter,
        runtime_handle: Handle,
        stream: ScopedStream,
        max_inflight_count: usize,
    ) -> Self {
        StreamWriter {
            writer,
            runtime_handle,
            stream,
            inflight: OneShotHolder::new(max_inflight_count),
        }
    }

    pub fn write_event_bytes(&mut self, event: &[u8], routing_key: Option<String>) -> Result<(), String> {
        // to_vec creates an owned copy of the byte array object.
        let write_future: tokio::sync::oneshot::Receiver<Result<(), WriterError>> = match routing_key {
            Option::None => {
                self.runtime_handle
                    .block_on(self.writer.write_event(event.to_vec()))
            }
            Option::Some(key) => {
                self.runtime_handle
                    .block_on(self.writer.write_event_by_routing_key(key, event.to_vec()))
            }
        };
        let _guard = self.runtime_handle.enter();
        let timeout_fut = timeout(
            Duration::from_secs(TIMEOUT_IN_SECONDS),
            self.inflight.add(write_future),
        );

        let result: Result<Result<Result<(), WriterError>, RecvError>, _> =
            self.runtime_handle.block_on(timeout_fut);
        match result {
            Ok(t) => match t {
                Ok(t1) => match t1 {
                    Ok(_) => Ok(()),
                    Err(e) => Err(format!(
                        "Error observed while writing an event {:?}",
                        e
                    )),
                },
                Err(e) => Err(format!(
                    "Error observed while writing an event {:?}",
                    e
                )),
            },
            Err(_) => Err("Write timed out, please check connectivity with Pravega.".to_string()),
        }
    }

    pub fn flush(&mut self) -> Result<(), String> {
        for x in self.inflight.drain() {
            let res = self.runtime_handle.block_on(x);
            // fail fast on error.
            if let Err(e) = res {
                return Err(format!(
                    "RecvError observed while writing an event {:?}",
                    e
                ));
            } else if let Err(e) = res.unwrap() {
                return Err(format!(
                    "Error observed while writing an event {:?}",
                    e
                ));
            }
        }
        Ok(())
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_writer_write_event(writer: *mut StreamWriter, event: Buffer, routing_key: Buffer, err: Option<&mut Buffer>) {
    let stream_writer = &mut *writer;
    match catch_unwind(AssertUnwindSafe(move || {
        let event = event.read().unwrap();
        let routing_key = routing_key.read().map(|v|std::str::from_utf8(v).unwrap().to_string());
        stream_writer.write_event_bytes(event, routing_key)
    })) {
        Ok(result) => {
            if let Err(e) = result {
                set_error(e, err);
            }
        },
        Err(_) => {
            set_error("caught panic".to_string(), err);
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_writer_flush(writer: *mut StreamWriter, err: Option<&mut Buffer>) {
    let stream_writer = &mut *writer;
    match catch_unwind(AssertUnwindSafe(move || stream_writer.flush())) {
        Ok(result) => {
            if let Err(e) = result {
                set_error(e, err);
            }
        },
        Err(_) => {
            set_error("caught panic".to_string(), err);
        }
    }
}
