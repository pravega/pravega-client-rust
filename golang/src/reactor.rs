use derive_new::new;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub enum Operation {
    GetSegmentSlice(StreamReader)
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
        while Reactor::run_once(&mut selector, &mut receiver, &factory)
            .await
            .is_ok()
        {}
    }

    async fn run_once(
        receiver: &mut ChannelReceiver<Incoming>,
    ) -> Result<(), String> {
        let result = receiver.recv().await;
        match result {
            case Incoming::GetSegmentSlice(StreamReader):
            match catch_unwind(AssertUnwindSafe(move || { stream_reader.get_segment_slice()})) {
                Ok(result) => {
                    match result {
                        Ok(seg_slice) => {
                            clear_error();
                            Box::into_raw(Box::new(Slice{ seg_slice }))
                        },
                        Err(e) => {
                            set_error(format!("Error while attempting to acquire segment {:?}", e), err);
                            ptr::null_mut()
                        }
                    }
                }
                Err(_) => {
                    set_error("caught panic".to_string(), err);
                    ptr::null_mut()
                }
            }
            let slice = get_segment_slice.await;
            publishBridge(chan_id, slice as usize);
        }

    }
}

extern "C" {
    pub fn publishBridge(chan_id:i32, obj_ptr:usize);
}
