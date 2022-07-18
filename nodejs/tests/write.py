import asyncio

import pravega_client

SCOPE = 'scope1'
STREAM = 'stream1'

manager = pravega_client.StreamManager("127.0.0.1:9090")

manager.create_scope(SCOPE)
manager.create_stream(SCOPE, STREAM, 1)

writer = manager.create_writer(SCOPE, STREAM)
writer.write_event("e1")
writer.write_event("e2")
writer.write_event("e3")
writer.write_event("e4")
writer.flush()

reader_group = manager.create_reader_group('rg2', SCOPE, STREAM)
reader = reader_group.create_reader("r2")

try:
    async def read():
        slice = await reader.get_segment_slice_async()
        for event in slice:
            print(event.data())

    asyncio.run(read())
finally:
    reader.reader_offline()
