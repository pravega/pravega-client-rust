use crate::wire_protocol::commands::*;
use crate::wire_protocol::connection_factory::ConnectionFactory;
use crate::wire_protocol::error::ConnectionError;
use async_trait::async_trait;
use std::net::SocketAddr;

#[derive(PartialEq, Debug)]
pub enum Reply {
    Hello(HelloCommand),
    WrongHost(WrongHostCommand),
    SegmentIsSealed(SegmentIsSealedCommand),
    SegmentAlreadyExists(SegmentAlreadyExistsCommand),
    SegmentIsTruncated(SegmentIsTruncatedCommand),
    NoSuchSegment(NoSuchSegmentCommand),
    TableSegmentNotEmpty(TableSegmentNotEmptyCommand),
    InvalidEventNumber(InvalidEventNumberCommand),
    OperationUnsupported(OperationUnsupportedCommand),
    AppendSetup(AppendSetupCommand),
    DataAppended(DataAppendedCommand),
    ConditionalCheckFailed(ConditionalCheckFailedCommand),
    SegmentRead(SegmentReadCommand),
    SegmentAttribute(SegmentAttributeCommand),
    SegmentAttributeUpdated(SegmentAttributeUpdatedCommand),
    StreamSegmentInfo(StreamSegmentInfoCommand),
    SegmentCreated(SegmentCreatedCommand),
    SegmentPolicyUpdated(SegmentPolicyUpdatedCommand),
    SegmentsMerged(SegmentsMergedCommand),
    SegmentSealed(SegmentSealedCommand),
    SegmentTruncated(SegmentTruncatedCommand),
    SegmentDeleted(SegmentDeletedCommand),
    KeepAlive(KeepAliveCommand),
    AuthTokenCheckFailed(AuthTokenCheckFailedCommand),
    TableEntriesUpdated(TableEntriesUpdatedCommand),
    TableKeysRemoved(TableKeysRemovedCommand),
    TableRead(TableReadCommand),
    TableKeysRead(TableKeysReadCommand),
    TableEntriesRead(TableEntriesReadCommand),
    TableKeyDoesNotExist(TableKeyDoesNotExistCommand),
    TableKeyBadVersion(TableKeyBadVersionCommand),
}

#[async_trait]
trait RawClient {
    ///Given a connection factory create a raw client.
    async fn new(factory: &dyn ConnectionFactory, endpoint: SocketAddr) -> Self;

    ///Asynchronously send a request to the server and receive a response.
    async fn send_request(&self, request: &dyn Request) -> Result<Reply, ConnectionError>;

    ///True if this client currently has an open connection to the server.
    fn is_connected(&self) -> bool;
}
