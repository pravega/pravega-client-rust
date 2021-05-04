//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

//! The Event API for writing and reading data.
//!
//! An Event is represented as a set of bytes within a Stream.
//! For example, an Event could be as simple as a small number of bytes containing
//! a temperature reading from an IoT sensor composed of a timestamp, a metric identifier and a value.
//! An Event could also be a web log data associated with a user click on a website.
//! Applications make sense of Events using their own serializers and deserializers,
//! allowing them to read and write objects in Pravega similarly to reading and writing objects from files.
//! # APIs
//! ## [EventWriter]
//! [EventWriter] writes events to the tail of Pravega streams.
//! It has exactly once guarantee that an Event will not missing or duplicating in Pravega
//! in case of a connection failure.
//! Each async write call will return a Result type containing an [oneshot].
//! Application can await on this [oneshot] to make sure that an Event has been persisted in Pravega.
//! If an Event is confirmed to be successfully persisted, any previous Events
//! are also guaranteed to be persisted.
//!
//! ## [TransactionalEventWriter]
//! [TransactionalEventWriter] provides a way to execute [Transaction] in Pravega.
//! The idea of a Transaction is that a Writer can "batch" up a bunch of Events and commit them as a unit into a Stream.
//! This is useful, for example, in Flink jobs, using Pravega as a sink.
//! The Flink job can continuously produce results for some data processing and use the Transaction
//! to durably accumulate the results of the processing.
//! For example, at the end of some sort of time window, the Flink job can commit the Transaction
//! and therefore make the results of the processing available for downstream processing,
//! or in the case of an error, the Transaction is aborted and the results disappear.
//! See more [details].
//!
//! ## [EventReader]
//! [EventReader] reads Events from a Pravega Stream.
//! An [EventReader] must belong to a [ReaderGroup]. The [EventReader] read call returns a slice of segment data
//! and application can call its iterator API to get the next Event. After finishing the slice, reader
//! drops the slice and will try to get another segment slice from the stream.
//!
//! ## [ReaderGroup]
//! [ReaderGroup] a named collection of [EventReader].
//! A Reader Group perform parallel reads from a given Stream.
//! It is guaranteed that each Event published to a Stream is sent to exactly one Reader within the Reader Group.
//! There could be one or more [EventReader] in the Reader Group and there could be many different
//! Reader Groups simultaneously reading from any given Stream.
//! A Reader Group can be considered as a "composite Reader" or "distributed Reader",
//! that allows a distributed application to read and process Stream data in parallel.
//! A large amount of Stream data can be consumed by a coordinated group of Readers in a Reader Group.
//! For example, a collection of Flink tasks processing Stream data in parallel using Reader Group.
//!
//! [EventWriter]: crate::event::writer::EventWriter
//! [oneshot]: https://docs.rs/tokio/1.5.0/tokio/sync/oneshot/index.html
//! [TransactionalEventWriter]: crate::event::transactional_writer::TransactionalEventWriter
//! [Transaction]: crate::event::transactional_writer::Transaction
//! [details]: https://pravega.io/docs/nightly/pravega-concepts/#transactions
//! [EventReader]: crate::event::reader::EventReader
//! [ReaderGroup]: crate::event::reader_group::ReaderGroup
//!
pub mod reader;
pub mod reader_group;
pub(crate) mod reader_group_state;
pub mod transactional_writer;
pub mod writer;
