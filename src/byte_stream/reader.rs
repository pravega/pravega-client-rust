use futures::stream::TryStream;
use snafu::Snafu;

use async_trait::async_trait;
use pravega_rust_client_shared::ScopedSegment;

use crate::client_factory::ClientFactory;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum ReadError {
    //TODO ...
}

#[async_trait]
trait ByteStreamReader : TryStream<Ok = Vec<u8>, Error = ReadError> {
    async fn open(segment: ScopedSegment, factory: &dyn ClientFactory) -> Self;
}