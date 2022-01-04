#
# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#

import pravega_client
import random
import asyncio
import aiounittest



# Helper method to invoke an coroutine inside a test.
def _run(coro):
    loop = asyncio.get_event_loop()
    loop.is_closed()
    result = loop.run_until_complete(coro)
    loop.close()
    return result

class PravegaReaderTest(aiounittest.AsyncTestCase):
    async def test_writeEventAndRead(self):
        suffix = str(random.randint(0, 100))
        scope = "testRead"
        stream = "testStream" + suffix
        print("Creating a Stream Manager, ensure Pravega is running")
        stream_manager = pravega_client.StreamManager("tcp://127.0.0.1:9090")

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
        w1.flush()
        # Create a reader Group Configuration to read from HEAD of stream.
        rg_config = pravega_client.StreamReaderGroupConfig(False, scope, stream)
        reader_group=stream_manager.create_reader_group_with_config("rg" + suffix, scope, rg_config)
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

    async def test_writeEventWithInflightAndRead(self):
        suffix = str(random.randint(0, 100))
        scope = "testRead"
        stream = "testStream" + suffix
        print("Creating a Stream Manager, ensure Pravega is running")
        stream_manager = pravega_client.StreamManager("tcp://127.0.0.1:9090")

        print("Creating a scope")
        scope_result = stream_manager.create_scope(scope)
        print(scope_result)
        print("Creating a stream ", stream)
        stream_result = stream_manager.create_stream(scope, stream, 1)
        print(stream_result)

        print("Creating a writer for Stream")
        w1 = stream_manager.create_writer(scope, stream, 10)

        print("Write 10 events and flush")
        for _ in range(10):
            w1.write_event("test event")
        w1.flush()

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
        self.assertEqual(count, 10, "Ten events are expected")

    # This test verifies data reading a Pravega stream with multiple readers.
    # It also invokes release Segment after consuming the first segment slice and marks the first
    # reader as offline. This test verifies if we are able to read all the 100 elements.
    async def test_multipleReader(self):
        suffix = str(random.randint(0, 100))
        scope = "testRead"
        stream = "testMulti" + suffix
        print("Creating a Stream Manager, ensure Pravega is running")
        stream_manager = pravega_client.StreamManager("tcp://127.0.0.1:9090")

        print("Creating a scope")
        scope_result = stream_manager.create_scope(scope)
        print(scope_result)
        print("Creating a stream ", stream)
        stream_result = stream_manager.create_stream(scope, stream, 2)
        print(stream_result)

        print("Creating a writer for Stream")
        w1 = stream_manager.create_writer(scope, stream)

        print("Write events 100 events")
        for i in range(100):
            w1.write_event("data")
        w1.flush()

        reader_group = stream_manager.create_reader_group("rg-multi" + suffix, scope, stream)
        r1 = reader_group.create_reader("r1")
        slice1 = await r1.get_segment_slice_async()
        print(slice1)
        # consume the segment slice for events.
        count=0
        for event in slice1:
            count+=1
            self.assertEqual(b'data', event.data(), "Invalid event data")
        print("Number of events read after consuming slice1 ", count)
        #release the segment.
        r1.release_segment(slice1)
        #mark the reader as offline.
        r1.reader_offline()

        r2 = reader_group.create_reader("r2")
        slice2 = await r2.get_segment_slice_async()
        for event in slice2:
            count+=1
            self.assertEqual(b'data', event.data(), "Invalid event data")

        print("Number of events read after consuming slice2 ", count)
        self.assertEqual(count, 100, "100 events are expected")

    # This test verifies data reading a Pravega stream with multiple readers.
    # It also invokes release Segment after consuming part of the first segment slice and marks the first
    # reader as offline. This test verifies if we are able to read all the 100 elements.
    async def test_multipleReaderPartialRead(self):
        suffix = str(random.randint(0, 100))
        scope = "testRead"
        stream = "testPartial" + suffix
        print("Creating a Stream Manager, ensure Pravega is running")
        stream_manager = pravega_client.StreamManager("tcp://127.0.0.1:9090")

        print("Creating a scope")
        scope_result = stream_manager.create_scope(scope)
        print(scope_result)
        print("Creating a stream ", stream)
        stream_result = stream_manager.create_stream(scope, stream, 2)
        print(stream_result)

        print("Creating a writer for Stream ", stream)
        w1 = stream_manager.create_writer(scope, stream)

        print("Write events 100 events")
        for i in range(100):
            w1.write_event("data")
        w1.flush()

        reader_group = stream_manager.create_reader_group("rg-partial" + suffix, scope, stream)
        r1 = reader_group.create_reader("r1")
        slice1 = await r1.get_segment_slice_async()
        print(slice1)
        # consume the just 1 event from the first segment slice.
        count=0
        event = next(slice1)
        self.assertEqual(b'data', event.data(), "Invalid event data")
        count+=1

        print("Number of events read after consuming slice1 ", count)
        #release the partially read segment slice.
        r1.release_segment(slice1)
        #mark the reader as offline.
        r1.reader_offline()

        r2 = reader_group.create_reader("r2")
        slice2 = await r2.get_segment_slice_async()
        for event in slice2:
            count+=1
            self.assertEqual(b'data', event.data(), "Invalid event data")
        print("Number of events read after consuming slice2 ", count)
        if count != 100:
            slice3 = await r2.get_segment_slice_async()
            for event in slice3:
                count+=1
                self.assertEqual(b'data', event.data(), "Invalid event data")
            print("Number of events read after consuming slice3 ", count)

        self.assertEqual(count, 100, "100 events are expected")

    # This test verifies reading large events from a Pravega stream
    async def test_largeEvents(self):
        suffix = str(random.randint(0, 100))
        scope = "testRead"
        stream = "testLargeEvent" + suffix
        print("Creating a Stream Manager, ensure Pravega is running")
        stream_manager = pravega_client.StreamManager("tcp://127.0.0.1:9090")

        print("Creating a scope")
        scope_result = stream_manager.create_scope(scope)
        print(scope_result)
        print("Creating a stream ", stream)
        stream_result = stream_manager.create_stream(scope, stream, 1)
        print(stream_result)

        print("Creating a writer for Stream")
        w1 = stream_manager.create_writer(scope, stream)

        print("Write events")
        for x in range(0, 1000):
            payload = 'a' * 100000
            w1.write_event(payload)
        w1.flush()
        reader_group = stream_manager.create_reader_group("rg" + suffix, scope, stream);
        r1 = reader_group.create_reader("reader-1")
        # consume the segment slice for events.
        count = 0
        while count != 1000:
            segment_slice = await r1.get_segment_slice_async()
            for event in segment_slice:
                count+=1
                self.assertEqual(b'a'*100000, event.data(), "Invalid event data")
            r1.release_segment(segment_slice)
