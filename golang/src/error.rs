use crate::memory::Buffer;
use errno::{set_errno, Errno};

pub fn clear_error() {
    set_errno(Errno(0));
}

pub fn set_error(msg: String, errout: Option<&mut Buffer>) {
    if let Some(mb) = errout {
        *mb = Buffer::from_vec(msg.into_bytes());
    }
    // TODO: should we set errno to something besides generic 1 always?
    set_errno(Errno(1));
}
