//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

//! Pravega synchronization primitives.
//!
//! Pravega uses [synchronizer] to synchronize the shared state in different readers.
//! Synchronizer is essentially an optimistic lock. It compares the client state with the server state
//! and only updates the server if the comparison is true. If server state doesn't match the client state,
//! synchronizer will call a user defined callback function to update the client state and do the
//! CAS again. The atomicity of CAS is guaranteed on the server side.
//!
//! More [details].
//!
//! [synchronizer]: crate::sync::synchronizer::Synchronizer
//! [details]: https://pravega.io/docs/nightly/state-synchronizer-design/
pub mod synchronizer;
pub mod table;

#[doc(inline)]
pub use synchronizer::Synchronizer;
#[doc(inline)]
pub use table::Table;
