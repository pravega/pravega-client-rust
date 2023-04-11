use crate::config::*;
use crate::error::{clear_error, set_error};
use crate::memory::Buffer;
use crate::stream_reader_group::StreamReaderGroup;
use crate::stream_writer::StreamWriter;
use libc::c_char;
use pravega_client::client_factory::ClientFactory;
use pravega_client::event::reader_group::ReaderGroupConfigBuilder;
use pravega_client_config::*;
use pravega_client_shared::*;
use std::ffi::CStr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use tokio::sync::mpsc::unbounded_channel;

pub struct StreamManager {
    cf: ClientFactory,
}

impl StreamManager {
    fn new(client_config: ClientConfig) -> Self {
        let client_factory = ClientFactory::new(client_config);

        StreamManager { cf: client_factory }
    }

    pub fn create_scope(&self, scope_name: &str) -> Result<bool, String> {
        let handle = self.cf.runtime_handle();

        let controller = self.cf.controller_client();
        let scope_name = Scope::from(scope_name.to_string());

        handle
            .block_on(controller.create_scope(&scope_name))
            .map_err(|e| format!("{:?}", e))
    }

    pub fn create_stream(&self, stream_config: StreamConfiguration) -> Result<bool, String> {
        let handle = self.cf.runtime_handle();
        let controller = self.cf.controller_client();
        handle
            .block_on(controller.create_stream(&stream_config))
            .map_err(|e| format!("{:?}", e))
    }

    pub fn create_writer(&self, scope_name: &str, stream_name: &str) -> StreamWriter {
        let scoped_stream = ScopedStream {
            scope: Scope::from(scope_name.to_string()),
            stream: Stream::from(stream_name.to_string()),
        };

        let writer = self.cf.create_event_writer(scoped_stream);
        let handle = self.cf.runtime_handle();
        let (tx, rx) = unbounded_channel();
        handle.spawn(StreamWriter::run_reactor(rx));

        StreamWriter::new(writer, tx, self.cf.runtime_handle())
    }

    pub fn create_reader_group(
        &self,
        reader_group_name: &str,
        scope_name: &str,
        stream_name: &str,
        read_from_tail: bool,
    ) -> StreamReaderGroup {
        let scope = Scope::from(scope_name.to_string());
        let scoped_stream = ScopedStream {
            scope: scope.clone(),
            stream: Stream::from(stream_name.to_string()),
        };
        let handle = self.cf.runtime_handle();
        let rg_config = if read_from_tail {
            // Create a reader group to read from the current TAIL/end of the Stream.
            ReaderGroupConfigBuilder::default()
                .read_from_tail_of_stream(scoped_stream)
                .build()
        } else {
            // Create a reader group to read from current HEAD/start of the Stream.
            ReaderGroupConfigBuilder::default()
                .read_from_head_of_stream(scoped_stream)
                .build()
        };
        let rg = handle.block_on(self.cf.create_reader_group_with_config(
            reader_group_name.to_string(),
            rg_config,
            scope,
        ));
        StreamReaderGroup::new(rg, self.cf.runtime_handle())
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_manager_new(
    client_config: ClientConfigMapping,
    err: Option<&mut Buffer>,
) -> *mut StreamManager {
    match catch_unwind(|| {
        let config = client_config.to_client_config();
        StreamManager::new(config)
    }) {
        Ok(manager) => {
            clear_error();
            Box::into_raw(Box::new(manager))
        }
        Err(_) => {
            set_error("caught panic".to_string(), err);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn stream_manager_destroy(manager: *mut StreamManager) {
    if !manager.is_null() {
        unsafe {
            drop(Box::from_raw(manager));
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_manager_create_scope(
    manager: *const StreamManager,
    scope: *const c_char,
    err: Option<&mut Buffer>,
) -> bool {
    let raw = CStr::from_ptr(scope);
    let scope_name = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("failed to parse scope".to_string(), err);
            return false;
        }
    };

    let stream_manager = &*manager;
    match catch_unwind(AssertUnwindSafe(move || stream_manager.create_scope(scope_name))) {
        Ok(result) => match result {
            Ok(val) => val,
            Err(e) => {
                set_error(e, err);
                false
            }
        },
        Err(_) => {
            set_error("caught panic".to_string(), err);
            false
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_manager_create_stream(
    manager: *const StreamManager,
    stream_config: StreamConfigurationMapping,
    err: Option<&mut Buffer>,
) -> bool {
    let stream_manager = &*manager;
    match catch_unwind(AssertUnwindSafe(move || {
        let stream_cfg = stream_config.to_stream_configuration();
        stream_manager.create_stream(stream_cfg)
    })) {
        Ok(result) => match result {
            Ok(val) => val,
            Err(e) => {
                set_error(e, err);
                false
            }
        },
        Err(_) => {
            set_error("caught panic".to_string(), err);
            false
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_writer_new(
    manager: *const StreamManager,
    scope: *const c_char,
    stream: *const c_char,
    err: Option<&mut Buffer>,
) -> *mut StreamWriter {
    let raw = CStr::from_ptr(scope);
    let scope_name = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("failed to parse scope".to_string(), err);
            return ptr::null_mut();
        }
    };

    let raw = CStr::from_ptr(stream);
    let stream_name = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("failed to parse stream".to_string(), err);
            return ptr::null_mut();
        }
    };

    let stream_manager = &*manager;
    match catch_unwind(AssertUnwindSafe(move || {
        stream_manager.create_writer(scope_name, stream_name)
    })) {
        Ok(writer) => Box::into_raw(Box::new(writer)),
        Err(_) => {
            set_error("caught panic".to_string(), err);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_reader_group_new(
    manager: *const StreamManager,
    reader_group: *const c_char,
    scope: *const c_char,
    stream: *const c_char,
    read_from_tail: bool,
    err: Option<&mut Buffer>,
) -> *mut StreamReaderGroup {
    let raw = CStr::from_ptr(reader_group);
    let reader_group_name = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("failed to parse reader group".to_string(), err);
            return ptr::null_mut();
        }
    };

    let raw = CStr::from_ptr(scope);
    let scope_name = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("failed to parse scope".to_string(), err);
            return ptr::null_mut();
        }
    };

    let raw = CStr::from_ptr(stream);
    let stream_name = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("failed to parse stream".to_string(), err);
            return ptr::null_mut();
        }
    };

    let stream_manager = &*manager;
    match catch_unwind(AssertUnwindSafe(move || {
        stream_manager.create_reader_group(reader_group_name, scope_name, stream_name, read_from_tail)
    })) {
        Ok(rg) => {
            // TODO: to check what cause the error
            clear_error();
            Box::into_raw(Box::new(rg))
        }
        Err(_) => {
            set_error("caught panic".to_string(), err);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn stream_reader_group_destroy(rg: *mut StreamReaderGroup) {
    if !rg.is_null() {
        unsafe {
            drop(Box::from_raw(rg));
        }
    }
}
