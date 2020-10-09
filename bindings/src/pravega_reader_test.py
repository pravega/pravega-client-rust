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
import secrets
import string
import pravega_client;
import asyncio


async def test_writeEventAndRead():
    scope = ''.join(secrets.choice(string.ascii_lowercase + string.digits)
                     for i in range(10))
    print("Creating a Stream Manager, ensure Pravega is running")
    stream_manager = pravega_client.StreamManager("127.0.0.1:9090")

    print("Creating a scope")
    scope_result = stream_manager.create_scope(scope)
    print(scope_result)
    print("Creating a stream")
    stream_result = stream_manager.create_stream(scope, "testStream", 1)
    print(stream_result)

    print("Creating a writer for Stream")
    w1 = stream_manager.create_writer(scope, "testStream")

    print("Write events")
    w1.write_event("test event1")
    w1.write_event("test event2")

    r1 = stream_manager.create_reader(scope, "testStream")
    r2 = await r1.get_segment_slice_async()
    print("completed invoked")
    print(r2)

asyncio.run(test_writeEventAndRead())