//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use std::cell::RefCell;
use std::sync::atomic::{AtomicI64, Ordering};

use pcg_rand::Pcg32;
use rand::{Rng, SeedableRng};
use tracing::span;

use pravega_client_shared::{
    Retention, RetentionType, ScaleType, Scaling, Scope, ScopedStream, Stream, StreamConfiguration,
};

use crate::client_factory::ClientFactory;

#[macro_use]
pub(crate) mod metric;
pub mod oneshot_holder;

thread_local! {
    pub(crate) static RNG: RefCell<Pcg32> = RefCell::new(Pcg32::from_entropy());
}

pub(crate) static REQUEST_ID_GENERATOR: AtomicI64 = AtomicI64::new(0);

/// Function used to generate request ids for all the modules.
pub(crate) fn get_request_id() -> i64 {
    REQUEST_ID_GENERATOR.fetch_add(1, Ordering::SeqCst) + 1
}

/// Function used to generate random u64.
pub(crate) fn get_random_u64() -> u64 {
    RNG.with(|rng| rng.borrow_mut().gen())
}

/// Function used to generate random u128.
pub(crate) fn get_random_u128() -> u128 {
    RNG.with(|rng| rng.borrow_mut().gen())
}

/// Function used to generate random i64.
pub(crate) fn get_random_f64() -> f64 {
    RNG.with(|rng| rng.borrow_mut().gen())
}

/// Return the current span.
pub(crate) fn current_span() -> span::Span {
    span::Span::current()
}

pub(crate) async fn create_stream(factory: &ClientFactory, scope: &str, stream: &str, num_segments: i32) {
    factory
        .controller_client()
        .create_scope(&Scope {
            name: scope.to_string(),
        })
        .await
        .unwrap();
    factory
        .controller_client()
        .create_stream(&StreamConfiguration {
            scoped_stream: ScopedStream {
                scope: Scope {
                    name: scope.to_string(),
                },
                stream: Stream {
                    name: stream.to_string(),
                },
            },
            scaling: Scaling {
                scale_type: ScaleType::FixedNumSegments,
                target_rate: 0,
                scale_factor: 0,
                min_num_segments: num_segments,
            },
            retention: Retention {
                retention_type: RetentionType::None,
                retention_param: 0,
            },
            tags: None,
        })
        .await
        .unwrap();
}
