use pravega_client::client_factory::ClientFactory;
use pravega_client_config::ClientConfigBuilder;
use pravega_client_shared::*;
use std::ptr;
use libc::c_char;
use std::ffi::CStr;

pub struct StreamManager {
    cf: ClientFactory,
}

impl StreamManager {
    fn new(
        controller_uri: &str,
    ) -> Self {
        let mut builder = ClientConfigBuilder::default();

        builder
            .controller_uri(controller_uri)
            .is_auth_enabled(false)
            .is_tls_enabled(false);
        let config = builder.build().expect("creating config");
        let client_factory = ClientFactory::new(config.clone());

        StreamManager {
            cf: client_factory,
        }
    }

    pub fn create_scope(&self, scope_name: &str) -> bool {
        let handle = self.cf.runtime_handle();
    
        let controller = self.cf.controller_client();
        let scope_name = Scope::from(scope_name.to_string());
    
        let result = handle.block_on(controller.create_scope(&scope_name));
        match result {
            Ok(t) => t,
            Err(_) => false,
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_manager_new(uri: *const c_char) -> *mut StreamManager {
    if uri.is_null() {
        return ptr::null_mut();
    }

    let raw = CStr::from_ptr(uri);

    let uri_as_str = match raw.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let manager = StreamManager::new(uri_as_str);
    Box::into_raw(Box::new(manager))
}


#[no_mangle]
pub unsafe extern "C" fn stream_manager_destroy(manager: *mut StreamManager) {
    if !manager.is_null() {
        let manager = Box::from_raw(manager);
        drop(manager);
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_manager_create_scope(manager: *const StreamManager, scope: *const c_char) -> bool {
    if scope.is_null() {
        return false;
    }

    let raw = CStr::from_ptr(scope);

    let scope_as_str = match raw.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };
    let stream_manager = &*manager;
    stream_manager.create_scope(scope_as_str)
}
