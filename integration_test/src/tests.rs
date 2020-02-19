use super::pravega_service::PravegaStandaloneService;
use crate::pravega_service::PravegaService;
use std::{thread, time};

#[test]
fn test_start_pravega_standalone() {
    let mut pravega = PravegaStandaloneService::start();
    let two_secs = time::Duration::from_secs(2);
    thread::sleep(two_secs);
    assert_eq!(true, pravega.check_status().unwrap());
    pravega.stop().unwrap();
    thread::sleep(two_secs);
    assert_eq!(false, pravega.check_status().unwrap());
}
