//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use futures::stream::TryStream;
use futures::Sink;
use snafu::Snafu;

use async_trait::async_trait;
use pravega_rust_client_shared::ScopedSegment;

use crate::client_factory::ClientFactory;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum WriteError {
    //TODO ...
}

#[async_trait]
trait ByteStreamWriter: Sink<Vec<u8>, Error = WriteError> {
    async fn open(segment: ScopedSegment, factory: &ClientFactory) -> Self;
}

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum ReadError {
    //TODO ...
}

#[async_trait]
trait ByteStreamReader: TryStream<Ok = Vec<u8>, Error = ReadError> {
    async fn open(segment: ScopedSegment, factory: &ClientFactory) -> Self;
}
