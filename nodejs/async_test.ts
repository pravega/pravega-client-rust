import { StreamManager } from './stream_manager.js';

const SCOPE = 'scope1';
const STREAM = 'stream1';
const DATA = 'Hello World!';

const stream_manager = StreamManager('tcp://127.0.0.1:9090', false, false, true);
// [...Array(1000).keys()].map(i => stream_manager.create_scope(`scope${i}`));

// const num = await stream_manager.async_sleep();
// console.log(num);
stream_manager
    .async_sleep()
    .then(num => console.log(num))
    .catch(() => console.log('timeout'));
// const scopes = await stream_manager.list_scopes();
// console.log(scopes);
