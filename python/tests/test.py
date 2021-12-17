#!/usr/bin/env python3

import pravega_client
import random
import asyncio

async def test_writeEventAndRead():
    suffix = str(random.randint(0, 100))
    scope = "testRead"
    stream = "testStream" + suffix
    print("Creating a Stream Manager, ensure Pravega is running")
    stream_manager = pravega_client.StreamManager("127.0.0.1:9090")

    print("Creating a scope")
    scope_result = stream_manager.create_scope(scope)
    print(scope_result)
    print("Creating a stream ", stream)
    stream_result = stream_manager.create_stream(scope, stream, 1)
    print(stream_result)

    print("Creating a writer for Stream")
    w1 = stream_manager.create_writer(scope, stream)

    print("Write events")
    w1.write_event("test event")
    w1.write_event("test event")
    reader_group = stream_manager.create_reader_group("rg" + suffix, scope, stream);
    r1 = reader_group.create_reader("reader-1")
    segment_slice = await r1.get_segment_slice_async()
    print(segment_slice)
    # consume the segment slice for events.
    count=0
    for event in segment_slice:
        count+=1
        print(event.data())
        self.assertEqual(b'test event', event.data(), "Invalid event data")
    self.assertEqual(count, 2, "Two events are expected")

asyncio.run(test_writeEventAndRead())