/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

use crate::{ControllerClient, ControllerError, ResultRetry};
use futures::prelude::*;
use futures::stream::{self};
use pravega_client_retry::retry_result::RetryError;
use pravega_client_shared::Scope;
use std::vec::IntoIter;
use tracing::info;

///
///Helper method to iterated over the all the Pravega streams under the provided Scope.
///This method returns a stream of values,Pravega streams, produced asynchronously.
///
/// The below snippets show case the example uses.
/// Sample 1:
///```
/// use pravega_client_shared::Scope;
/// use pravega_controller_client::paginator::list_streams;
///     let stream = list_streams(
///         Scope {
///             name: "testScope".to_string(),
///         },
///         controller_client,
///     );
///     // collect all the Streams in a single vector
///     let stream_list:Vec<String> = stream.map(|str| str.unwrap()).collect::<Vec<String>>().await;
/// ```
///
/// Sample 2:
/// ```
/// use pravega_client_shared::Scope;
/// use pravega_controller_client::paginator::list_streams;
///     let mut stream = list_streams(
///         Scope {
///             name: "testScope".to_string(),
///         },
///         controller_client,
///     );
/// let pravega_stream_1 = stream.next().await;
/// let pravega_stream_2 = stream.next().await;
/// // A None is returned at the end of the stream.
/// ```
///
pub fn list_streams(
    scope: Scope,
    client: &dyn ControllerClient,
) -> impl Stream<Item = Result<String, RetryError<ControllerError>>> + '_ {
    struct State {
        streams: IntoIter<String>,
        scope: Scope,
        token: String,
    };

    // Initial state with an empty Continuation token.
    stream::unfold(
        State {
            streams: Vec::new().into_iter(),
            scope: scope.clone(),
            token: String::from(""),
        },
        move |mut state| async move {
            if let Some(element) = state.streams.next() {
                Some((Ok(element), state))
            } else {
                // execute a request to the controller.
                info!(
                    "Fetch the next set of streams under scope {} using the provided token",
                    state.scope
                );
                let res: ResultRetry<Option<(Vec<String>, String)>> =
                    client.list_streams(&state.scope, &state.token).await;
                match res {
                    Ok(None) => None,
                    Ok(Some((list, ct))) => {
                        // create a consuming iterator
                        let mut stream_iter = list.into_iter();
                        Some((
                            Ok(stream_iter.next()?),
                            State {
                                streams: stream_iter,
                                scope: state.scope.clone(),
                                token: ct,
                            },
                        ))
                    }
                    Err(e) => Some((Err(e), state)),
                }
            }
        },
    )
}
