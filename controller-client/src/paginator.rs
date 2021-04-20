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
use tracing::{debug, error, info, warn};

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
// impl Paginator<'_> {
//     // pub fn list_streams(&self, scope: &Scope) -> impl Stream<Item = String> {
//     //     struct State {
//     //         scope: Scope,
//     //         token: String,
//     //     };
//     //
//     //     // Initial state with an empty Continuation token.
//     //     let stream_result = stream::unfold(
//     //         State {
//     //             scope: scope.clone(),
//     //             token: String::from(""),
//     //         },
//     //         |state| async move {
//     //             let res: ResultRetry<Option<(Vec<String>, String)>> =
//     //                 self.client.list_streams(scope, &state.token).await;
//     //             match res {
//     //                 Ok(None) => None,
//     //                 Ok(Some((list, ct))) => Some((
//     //                     stream::iter(list),
//     //                     State {
//     //                         scope: state.scope,
//     //                         token: ct,
//     //                     },
//     //                 )),
//     //                 _ => None,
//     //             }
//     //         },
//     //     )
//     //     .flatten();
//     //     stream_result
//     // }
//     // pub fn list_streams(&self, scope: &Scope) -> impl Stream<Item = Result<String, ControllerError>> {
//     //     struct State {
//     //         scope: Scope,
//     //         list: Vec<String>,
//     //         token: String,
//     //     };
//     //
//     //     // Initial state with an empty Continuation token.
//     //
//     //     let stream_result = stream::unfold(
//     //         State {
//     //             scope: scope.clone(),
//     //             list: vec![],
//     //             token: String::from(""),
//     //         },
//     //         |mut state| async move {
//     //             if !state.list.is_empty() {
//     //                 // Return from already fetched stream list.
//     //                 Some((Ok(state.list.pop().unwrap()), state))
//     //             } else {
//     //                 // The list is empty, try fetching it from the controller using the previous continuation token.
//     //
//     //                 debug!(
//     //                     "Triggering a request to the controller to list streams for scope {}",
//     //                     &state.scope
//     //                 );
//     //                 let res: ResultRetry<Option<(Vec<String>, String)>> =
//     //                     self.client.list_streams(&state.scope, &state.token).await;
//     //
//     //                 match res {
//     //                     Ok(None) => None,
//     //                     Ok(Some((list, token))) => {
//     //                         if list.is_empty() {
//     //                             // Empty result from the controller implies no further streams present.
//     //                             None
//     //                         } else {
//     //                             // update state with the new set of streams.
//     //                             state.list.extend_from_slice(list.as_slice());
//     //                             state.token = token;
//     //                             Some((Ok(state.list.pop().unwrap()), state))
//     //                         }
//     //                     }
//     //                     Err(err) => {
//     //                         error!(
//     //                             "Error while listing streams under scope {}, {:?}",
//     //                             &state.scope, err
//     //                         );
//     //                         None
//     //                     }
//     //                 }
//     //             }
//     //         },
//     //     );
//     //     stream_result
//     // }
// }
