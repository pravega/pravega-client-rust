//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
use enum_iterator::IntoEnumIterator;
use metrics::register_gauge;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;

struct Metric(ClientMetrics, String, String);

#[derive(Clone, IntoEnumIterator, PartialEq)]
pub enum ClientMetrics {
    ClientAppendLatency,
    ClientAppendBlockSize,
    ClientOutstandingAppendCount,
}

impl ClientMetrics {
    fn register(&self) {
        match self {
            ClientMetrics::ClientAppendLatency => {
                register_gauge!(
                    "pravega.client.segment.append_latency_ms",
                    "The latency for a single append."
                );
            }
            ClientMetrics::ClientAppendBlockSize => {
                register_gauge!(
                    "pravega.client.segment.append_block_size",
                    "The block size for a single append wirecommand."
                );
            }
            ClientMetrics::ClientOutstandingAppendCount => {
                register_gauge!(
                    "pravega.client.segment.outstanding_append_count",
                    "The current outstanding appends from caller."
                );
            }
        }
    }
}

pub fn metric_init(scrape_port: SocketAddr) {
    PrometheusBuilder::new()
        .listen_address(scrape_port)
        .install()
        .expect("install scraper");

    for metric in ClientMetrics::into_enum_iter() {
        metric.register();
    }
}

#[macro_export]
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
