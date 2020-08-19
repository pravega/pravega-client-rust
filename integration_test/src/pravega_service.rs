//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use std::fs::{create_dir, File};
use std::io::{Read, Write};
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use tracing::info;

const PATH: &str = "./pravega/bin/pravega-standalone";
const LOG: &str = "./pravega/conf/logback.xml";
const PROPERTY: &str = "./pravega/conf/standalone-config.properties";
/**
 * Pravega Service abstraction for the test framework.
 */
pub trait PravegaService {
    /**
     * Create and start a PravegaService
     */
    fn start(config: PravegaStandaloneServiceConfig) -> Self;

    /**
     * Stop a given service. If the service is already stopped,nothing would happen.
     */
    fn stop(&mut self) -> Result<(), std::io::Error>;

    /**
     * Enable DEBUG level log of Pravega standalone
     */
    fn enable_debug_log(enable: bool);

    /**
     * Enable Auth for Pravega standalone
     */
    fn enable_auth(enable: bool);

    /**
     * Enable Auth for Pravega standalone
     */
    fn enable_tls(enable: bool);

    /**
     * Check if the service is up and running.
     */
    fn check_status(&mut self) -> Result<bool, std::io::Error>;

    /**
     * Get grpc host:port URI where the service is running.
     */
    fn get_grpc_details(&self) -> SocketAddr;

    /**
     * Get rest host:port URI where the service is running.
     */
    fn get_rest_details(&self) -> SocketAddr;
}

/**
 * Create a PravegaStandalone Service, where path is underlying path to Pravega directory.
 */
pub struct PravegaStandaloneService {
    pravega: Child,
}

impl PravegaStandaloneService {
    const CONTROLLER_PORT: u16 = 9090;
    const REST_PORT: u16 = 10080;
    const ADDRESS: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);
}

impl PravegaService for PravegaStandaloneService {
    /**
     * start the pravega standalone. the path should point to the pravega-standalone
     */
    fn start(config: PravegaStandaloneServiceConfig) -> Self {
        PravegaStandaloneService::enable_debug_log(config.debug);
        PravegaStandaloneService::enable_auth(config.auth);
        PravegaStandaloneService::enable_tls(config.tls);
        let _ = create_dir("./log");
        let output = File::create("./log/output.log").expect("creating file for standalone log");
        info!("start running pravega under path {}", PATH);
        let pravega = Command::new(PATH)
            .stdout(Stdio::from(output))
            .spawn()
            .expect("failed to start pravega standalone");
        info!("child pid: {}", pravega.id());
        PravegaStandaloneService { pravega }
    }

    fn stop(&mut self) -> Result<(), std::io::Error> {
        if self.check_status()? {
            return self.pravega.kill();
        }
        Ok(())
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

    fn enable_debug_log(enable: bool) {
        let file_path = Path::new(&LOG);
        // Open and read the file entirely
        let mut src = File::open(&file_path).expect("open logback.xml file");
        let mut data = String::new();
        src.read_to_string(&mut data).expect("read data");
        drop(src); // Close the file early

        // Run the replace operation in memory
        let new_data: String;
        if enable {
            new_data = data.replace("INFO", "DEBUG");
        } else {
            new_data = data.replace("DEBUG", "INFO");
        };

        // Recreate the file and dump the processed contents to it
        let mut dst = File::create(&file_path).expect("create file");
        dst.write_all(new_data.as_bytes()).expect("write file");
    }

    fn enable_auth(enable: bool) {
        let file_path = Path::new(&PROPERTY);
        // Open and read the file entirely
        let mut src = File::open(&file_path).expect("open standalone property file");
        let mut data = String::new();
        src.read_to_string(&mut data).expect("read data");
        drop(src); // Close the file early

        // Run the replace operation in memory
        let mut new_data: String;
        if enable {
            new_data = data.replace(
                "#singlenode.security.auth.enable=false",
                "singlenode.security.auth.enable=true",
            );
            new_data = new_data.replace(
                "#singlenode.security.auth.pwdAuthHandler.accountsDb.location=../config/passwd",
                "singlenode.security.auth.pwdAuthHandler.accountsDb.location=./pravega/conf/passwd",
            )
        } else {
            new_data = data.replace(
                "singlenode.security.auth.enable=true",
                "#singlenode.security.auth.enable=false",
            )
        };

        // Recreate the file and dump the processed contents to it
        let mut dst = File::create(&file_path).expect("create file");
        dst.write_all(new_data.as_bytes()).expect("write file");
    }

    fn enable_tls(enable: bool) {
        let file_path = Path::new(&PROPERTY);
        // Open and read the file entirely
        let mut src = File::open(&file_path).expect("open standalone property file");
        let mut data = String::new();
        src.read_to_string(&mut data).expect("read data");
        drop(src); // Close the file early

        // Run the replace operation in memory
        let new_data: String;
        if enable {
            new_data = data.replace(
                "#singlenode.security.tls.enable=false",
                "singlenode.security.tls.enable=true",
            )
        } else {
            new_data = data.replace(
                "singlenode.security.tls.enable=true",
                "#singlenode.security.tls.enable=false",
            )
        };

        // Recreate the file and dump the processed contents to it
        let mut dst = File::create(&file_path).expect("create file");
        dst.write_all(new_data.as_bytes()).expect("write file");
    }
}

impl Drop for PravegaStandaloneService {
    fn drop(&mut self) {
        self.stop().expect("Failed to stop pravega");
    }
}

#[derive(Clone, Debug)]
pub struct PravegaStandaloneServiceConfig {
    debug: bool,
    auth: bool,
    tls: bool,
}

impl PravegaStandaloneServiceConfig {
    pub fn new() -> Self {
        PravegaStandaloneServiceConfig {
            debug: false,
            auth: false,
            tls: false,
        }
    }

    pub fn set_debug(mut self) -> Self {
        self.debug = true;
        self
    }

    pub fn set_auth(mut self) -> Self {
        self.auth = true;
        self
    }

    pub fn set_tls(mut self) -> Self {
        self.tls = true;
        self
    }

    pub fn debug(&self) -> bool {
        self.debug
    }

    pub fn auth(&self) -> bool {
        self.auth
    }

    pub fn tls(&self) -> bool {
        self.tls
    }
}
