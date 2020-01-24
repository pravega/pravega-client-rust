use pravega_wire_protocol::connection_factory::ConnectionFactory;
use pravega_controller_client::ControllerClient;

pub(crate) trait ClientFactory {
    fn get_connection_factory(&self) -> &dyn ConnectionFactory;

    fn get_controller_client(&self) -> &dyn ControllerClient;
}
