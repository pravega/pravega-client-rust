use derive_new::new;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use crate::stream_reader::*;
use std::panic::{catch_unwind, AssertUnwindSafe};
use libc::c_char;
use std::ffi::{CString};

pub enum Operation {
    GetSegmentSlice(* const StreamReader)
}
pub struct Incoming {
    id: i32,
    operation: Operation
}

#[derive(new)]
pub struct Reactor {}

impl Reactor {
    pub async fn run(
        mut receiver: UnboundedReceiver<Incoming>,
    ) {
        while Reactor::run_once( &mut receiver)
            .await
            .is_ok()
        {}
    }

    async fn  run_once(
        receiver: &mut UnboundedReceiver<Incoming>,
    ) -> Result<(), String> {
        let incoming = receiver.recv().await;
        match incoming {
            Some(request) => {
                let channelId = request.id;
                match request.operation {
                    Operation::GetSegmentSlice(reader) => {
                        let mut stream_reader =  reader;
                        match catch_unwind(AssertUnwindSafe(move || { (*stream_reader).get_segment_slice()})) {
                            Ok(result) => {
                                match result {
                                    Ok(seg_slice) => {
                                        let ptr = Box::into_raw(Box::new(Slice{ seg_slice }));
                                        unsafe {
                                            let empty_error = CString::new("").expect("CString::new failed").as_ptr();
                                            publishBridge(channelId,ptr as usize,empty_error)
                                        };
                                    },
                                    Err(e) => {
                                        unsafe {
                                            let msg = format!("Error while attempting to acquire segment {:?}", e);
                                            let error = CString::new(msg).expect("CString::new failed").as_ptr();
                                            publishBridge(channelId,0,error);
                                        }
                                    }
                                }
                            }

                            Err(x) => {
                                unsafe {
                                    let msg = format!("panic while attempting to acquire segment {:?}", x);
                                    let error = CString::new(msg).expect("CString::new failed").as_ptr();
                                    publishBridge(channelId,0,error);
                                }
                            }
            }},
            }
        },
            None => {println!("unexpect incoming");},
        }
        return Ok(())
    }
}

extern "C" {
    pub fn publishBridge(chan_id:i32, obj_ptr:usize,errorMessage: *const c_char);
}
