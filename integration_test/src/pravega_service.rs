use log::info;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::process::{Child, Command};

const PATH: &str = "./pravega/bin/pravega-standalone";
/**
 * Pravega Service abstraction for the test framework.
 */
pub trait PravegaService {
    /**
     * Create and start a PravegaService
     */
    fn start() -> Self;

    /**
     * Stop a given service. If the service is already stopped,return an invalid input
     */
    fn stop(&mut self) -> Result<(), std::io::Error>;

    /**
     * Check if the service is up and running.
     */
    fn check_status(&mut self) -> Result<bool, std::io::Error>;

    /**
     * Get Host:port URI where the service is running.
     */
    fn get_grpc_details(&self) -> SocketAddr;

    fn get_rest_details(&self) -> SocketAddr;
}

/**
 * Create a PravegaStandalone Service, where path is underlying path to Pravega directory.
 */
pub struct PravegaStandaloneService {
    pravega: Child,
}

impl PravegaStandaloneService {
    const CONTROLLER_PORT: u16 = 9098;
    const REST_PORT: u16 = 10080;
    const ADDRESS: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);
}

impl PravegaService for PravegaStandaloneService {
    /**
     * start the pravega standalone. the path should point to the pravega-standalone
     */
    fn start() -> Self {
        info!("start running pravega under path {}", PATH);
        let pravega = Command::new(PATH)
            .spawn()
            .expect("failed to start pravega standalone");
        println!("child pid: {}", pravega.id());
        PravegaStandaloneService { pravega }
    }

    fn stop(&mut self) -> Result<(), std::io::Error> {
        self.pravega.kill()
    }

    fn check_status(&mut self) -> Result<bool, std::io::Error> {
        let status = self.pravega.try_wait();
        match status {
            Ok(Some(_status)) => Ok(false),
            Ok(None) => Ok(true),
            Err(e) => Err(e),
        }
    }

    fn get_grpc_details(&self) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Self::ADDRESS), Self::CONTROLLER_PORT)
    }

    fn get_rest_details(&self) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Self::ADDRESS), Self::REST_PORT)
    }
}
