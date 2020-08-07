//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
use metrics::{register_gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;

pub enum ClientMetrics {
    ClientAppendLatency,
    ClientAppendBlockSize,
    ClientOutstandingAppendCount,
}

pub fn metric_init(scrape_port: SocketAddr) {
    PrometheusBuilder::new()
        .listen_address(scrape_port)
        .install()
        .expect("install scraper");
    register_gauge!(
        "pravega.client.segment.append_latency_ms",
        "The latency for a single append."
    );
    register_gauge!(
        "pravega.client.segment.append_block_size",
        "The block size for a single append wirecommand."
    );
    register_gauge!(
        "pravega.client.segment.outstanding_append_count",
        "The current outstanding appends from caller."
    );
}

macro_rules! update {
    ($metric:expr, $value:expr, $($tags:tt)*) => {
        match $metric {
            ClientMetrics::ClientAppendLatency => {
                metrics::gauge!("pravega.client.segment.append_latency_ms", $value as f64, $($tags)*);
            }
            ClientMetrics::ClientAppendBlockSize => {
                metrics::gauge!("pravega.client.segment.append_block_size", $value as f64, $($tags)*);
            }
            ClientMetrics::ClientOutstandingAppendCount => {
                metrics::gauge!("pravega.client.segment.outstanding_append_count", $value as f64, $($tags)*);
            }
        }
    };
}
