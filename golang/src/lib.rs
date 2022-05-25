mod error;
mod memory;
pub use memory::{free_buffer, Buffer};

mod stream_manager;
pub use stream_manager::StreamManager;
pub use stream_manager::StreamScalingPolicy;
