use derive_new::new;
use tokio::sync::mpsc::UnboundedReceiver;
use crate::stream_reader::*;

pub enum Operation {
    GetSegmentSlice(SafeStreamReader)
}

#[derive(new)]
pub struct Incoming {
    chan_id: i64,
    operation: Operation
}

#[derive(new)]
pub struct Reactor {}

impl Reactor {
    pub async fn run(
        mut receiver: UnboundedReceiver<Incoming>,
    ) {
        while Reactor::run_once(&mut receiver)
            .await
            .is_ok()
        {}
    }

    async fn run_once(
        receiver: &mut UnboundedReceiver<Incoming>,
    ) -> Result<(), String> {
        let incoming = receiver.recv().await.expect("sender closed, processor exit");
        let chan_id = incoming.chan_id;
        match incoming.operation {
            Operation::GetSegmentSlice(reader) => {
                let stream_reader = reader.get();
                match stream_reader.get_segment_slice().await {
                    Ok(seg_slice) => {
                        let ptr = Box::into_raw(Box::new(Slice::new(seg_slice)));
                        unsafe {
                            publishBridge(chan_id, ptr as usize);
                        };
                    },
                    Err(err) => {
                        // TODO: send error msg through the bridge channel
                        println!("Error while getting segment slice {:?}", err);
                    }
                }
            },
        };
        Ok(())
    }
}

extern "C" {
    pub fn publishBridge(chan_id: i64, obj_ptr: usize);
}
