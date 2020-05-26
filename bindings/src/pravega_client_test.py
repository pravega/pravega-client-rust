#
# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#

import unittest
import pravega_client;

class PravegaTest(unittest.TestCase):
    def test_writeEvent(self):
        print("Creating a Stream Manager, ensure pravega is running")
        stream_manager=pravega_client.StreamManager("127.0.0.1:9090")

        print("Creating a scope")
        scope_result=stream_manager.create_scope("testScope")
        self.assertEqual(True, scope_result, "Scope creation status")

        print("Creating a stream")
        stream_result=stream_manager.create_stream("testScope", "testStream", 1)
        self.assertEqual(True, stream_result, "Stream creation status")

        print("Creating a writer for Stream")
        w1=stream_manager.create_writer("testScope","testStream")

        print("Write events")
        w1.write_event("test event1")
        w1.write_event("test event2")


if __name__ == '__main__':
    unittest.main()
