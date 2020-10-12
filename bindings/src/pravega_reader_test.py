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
import random


# Helper method to invoke an coroutine inside a test.
def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


class PravegaReaderTest(unittest.TestCase):
    def test_writeEventAndRead(self):
        scope = "testScope"+str(random.randint(0, 100))
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
        w1.write_event("test event")
        w1.write_event("test event")

        r1 = stream_manager.create_reader(scope, "testStream")
        segment_slice = _run(self.get_segment_slice(r1))
        print(segment_slice)
        # consume the segment slice for events.
        count=0
        for event in segment_slice:
            count+=1
            print(event.data())
            self.assertEqual(b'test event', event.data(), "Invalid event data")
        self.assertEqual(count, 2, "Two events are expected")

    # wrapper function to ensure we pass a co-routine to run method, since we cannot directly invoke
    # await reader.get_segment_slice_async() inside the test.
    async def get_segment_slice(self, reader):
        return await reader.get_segment_slice_async()
