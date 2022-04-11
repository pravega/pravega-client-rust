// Copyright Pravega Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::stream_reader::StreamReader;
use neon::prelude::*;
use pravega_client::event::reader_group::ReaderGroup;
use pravega_client::event::reader_group::StreamCutVersioned;
use pravega_client::event::reader_group_state::ReaderGroupStateError;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;

impl Finalize for StreamCut {}

pub(crate) struct StreamCut {
    pub stream_cut: StreamCutVersioned,
}

///
/// Represent a consistent position in the stream.
///
impl StreamCut {
    pub fn js_head(mut cx: FunctionContext) -> JsResult<JsBox<StreamCut>> {
        Ok(cx.boxed(StreamCut {
            stream_cut: StreamCutVersioned::Unbounded,
        }))
    }

    pub fn js_tail(mut cx: FunctionContext) -> JsResult<JsBox<StreamCut>> {
        Ok(cx.boxed(StreamCut {
            stream_cut: StreamCutVersioned::Tail,
        }))
    }
}

impl Finalize for StreamReaderGroup {}

///
/// A reader group is a collection of readers that collectively read all the events in the
/// stream. The events are distributed among the readers in the group such that each event goes
/// to only one reader.
///
/// Note: A StreamReaderGroup cannot be created directly without using the StreamManager.
///
#[derive(new)]
pub(crate) struct StreamReaderGroup {
    reader_group: ReaderGroup,
    runtime_handle: Handle,
}

impl StreamReaderGroup {
    ///
    /// This method is used to create a reader under a ReaderGroup.
    ///
    fn create_reader(&self, reader_name: &str) -> StreamReader {
        info!(
            "Creating reader {:?} under reader group {:?}",
            reader_name, self.reader_group.name
        );

        let reader = self
            .runtime_handle
            .block_on(self.reader_group.create_reader(reader_name.to_string()));
        StreamReader::new(
            Arc::new(Mutex::new(reader)),
            self.runtime_handle.clone(),
            self.reader_group.get_managed_streams(),
        )
    }

    ///
    /// This method is used to manually mark a reader as offline under a ReaderGroup.
    ///
    fn reader_offline(&self, reader_name: &str) -> Result<(), ReaderGroupStateError> {
        info!(
            "Marking reader {:?} under reader group {:?} as offline",
            reader_name, self.reader_group.name
        );

        let res = self
            .runtime_handle
            .block_on(self.reader_group.reader_offline(reader_name.to_string(), None));
        match res {
            Ok(_) => Ok(()),
            Err(e) => match e {
                ReaderGroupStateError::SyncError { .. } => {
                    error!("Failed to mark the reader {:?} offline {:?} ", reader_name, e);
                    Err(e)
                }
                ReaderGroupStateError::ReaderAlreadyOfflineError { .. } => {
                    info!("Reader {:?} is already offline", reader_name);
                    Ok(())
                }
            },
        }
    }
}

impl StreamReaderGroup {
    pub fn js_create_reader(mut cx: FunctionContext) -> JsResult<JsBox<StreamReader>> {
        let stream_reader_group = cx
            .this()
            .downcast_or_throw::<JsBox<StreamReaderGroup>, _>(&mut cx)?;
        let reader_name = cx.argument::<JsString>(0)?.value(&mut cx);

        Ok(cx.boxed(stream_reader_group.create_reader(&reader_name)))
    }

    pub fn js_reader_offline(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let stream_reader_group = cx
            .this()
            .downcast_or_throw::<JsBox<StreamReaderGroup>, _>(&mut cx)?;
        let reader_name = cx.argument::<JsString>(0)?.value(&mut cx);

        let res = stream_reader_group.reader_offline(&reader_name);
        match res {
            Ok(()) => Ok(cx.undefined()),
            Err(e) => cx.throw_error(e.to_string()),
        }
    }

    pub fn js_to_str(mut cx: FunctionContext) -> JsResult<JsString> {
        let stream_manager = cx
            .this()
            .downcast_or_throw::<JsBox<StreamReaderGroup>, _>(&mut cx)?;

        Ok(cx.string(format!(
            "ReaderGroup: {:?}, ReaderGroup config : {:?}",
            stream_manager.reader_group.name, stream_manager.reader_group.config
        )))
    }
}
