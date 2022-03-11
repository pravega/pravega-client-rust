import { StreamManager } from '../stream_manager.js';
import { StreamCut } from '../stream_reader_group.js';

const stream_manager = StreamManager('tcp://127.0.0.1:9090', false, false, true);

const stream_reader_group = stream_manager.create_reader_group(StreamCut.head(), 'rg1', 'scope1', 'stream1');
const stream_reader = stream_reader_group.create_reader('r1');

try {
    const seg_slice = await stream_reader.get_segment_slice();
    const enc = new TextDecoder('utf-8');
    for (const event of seg_slice) {
        const raw_value = event.data();
        console.log(`Event at ${event.offset()} reads ${enc.decode(raw_value)}`);
    }
} catch (e) {
    console.log(e);
} finally {
    stream_reader_group.reader_offline('r1');
}
