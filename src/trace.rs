//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//

use tracing::{dispatcher, Dispatch, Level};
use tracing_subscriber::fmt::format::{FmtSpan, Full};
use tracing_subscriber::FmtSubscriber;

pub fn init() {
    let subscriber = FmtSubscriber::builder().with_ansi(true).finish();

    let my_dispatch = Dispatch::new(subscriber);
    // this function can only be called once.
    dispatcher::set_global_default(my_dispatch).expect("set global dispatch");
}
