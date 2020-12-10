# Transaction

Transactions provides a mechanism for writing many events atomically.
A Transaction is unbounded in size but is bounded in time. If it has not been committed within a time window
specified  at the time of its creation it will be automatically aborted.

For more details please refer [API docs](./doc/pravega_client_rust/transaction/transactional_event_stream_writer/index.html).
