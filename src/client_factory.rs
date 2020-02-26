//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use pravega_controller_client::ControllerClient;
use pravega_wire_protocol::connection_factory::ConnectionFactory;

pub(crate) trait ClientFactory {
    fn get_connection_factory(&self) -> &dyn ConnectionFactory;

    fn get_controller_client(&self) -> &dyn ControllerClient;
}
