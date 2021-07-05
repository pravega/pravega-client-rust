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
use pravega_client_shared::{CToken, Scope, ScopedStream};
use std::vec::IntoIter;
use tracing::error;
use tracing::info;

///
///Helper method to iterate over the all the Pravega Scopes.
///This method returns a stream of values, Pravega scopes, produced asynchronously.
///
/// The below snippets show case the example uses.
/// Sample 1:
///```
/// # use tonic::transport::Channel;
/// # use pravega_controller_client::controller::controller_service_client::ControllerServiceClient;
/// use pravega_controller_client::ControllerClient;
/// # async fn call_list_scope(controller_client: &dyn ControllerClient) {
/// use pravega_client_shared::Scope;
/// use pravega_client_shared::ScopedStream;
/// use futures::future;
/// use futures::stream::StreamExt;
/// use pravega_controller_client::paginator::list_scopes;
/// let stream = list_scopes(
///     controller_client,
/// );
/// // collect all the Scopes in a single vector
/// let scope_list:Vec<Scope> = stream.map(|str| str.unwrap()).collect::<Vec<Scope>>().await;
/// # }
/// ```
///
/// Sample 2:
/// ```
/// # use tonic::transport::Channel;
/// # use pravega_controller_client::controller::controller_service_client::ControllerServiceClient;
/// use pravega_controller_client::ControllerClient;
/// # async fn call_list_scope(controller_client: &dyn ControllerClient) {
/// use pravega_client_shared::Scope;
/// use pravega_client_shared::ScopedStream;
/// use futures::future;
/// use futures::stream::StreamExt;
/// use pravega_controller_client::paginator::list_scopes;
/// let stream = list_scopes(
///     controller_client,
/// );
/// futures::pin_mut!(stream);
/// let pravega_scope_1 = stream.next().await;
/// let pravega_scope_1 = stream.next().await;
/// // A None is returned at the end of the stream.
/// # }
/// ```
///
pub fn list_scopes(
    client: &dyn ControllerClient,
) -> impl Stream<Item = Result<Scope, RetryError<ControllerError>>> + '_ {
    struct State {
        scopes: IntoIter<Scope>,
        token: CToken,
    }

    // Initial state with an empty Continuation token.
    let get_next_stream_async = move |mut state: State| async move {
        if let Some(element) = state.scopes.next() {
            Some((Ok(element), state))
        } else {
            // execute a request to the controller.
            info!("Fetch the next set of scopes  using the provided token",);
            let res: ResultRetry<Option<(Vec<Scope>, CToken)>> = client.list_scopes(&state.token).await;
            match res {
                Ok(None) => None,
                Ok(Some((list, ct))) => {
                    // create a consuming iterator
                    let mut scope_iter = list.into_iter();
                    Some((
                        Ok(scope_iter.next()?),
                        State {
                            scopes: scope_iter,
                            token: ct,
                        },
                    ))
                }
                Err(e) => {
                    //log an error and return None to indicate end of stream.
                    error!("Error while attempting to list scopes. Error: {:?}", e);
                    None
                }
            }
        }
    };
    stream::unfold(
        State {
            scopes: Vec::new().into_iter(),
            token: CToken::empty(),
        },
        get_next_stream_async,
    )
}

///
///Helper method to iterated over the all the Pravega streams under the provided Scope.
///This method returns a stream of values,Pravega streams, produced asynchronously.
///
/// The below snippets show case the example uses.
/// Sample 1:
///```
/// # use tonic::transport::Channel;
/// # use pravega_controller_client::controller::controller_service_client::ControllerServiceClient;
/// use pravega_controller_client::ControllerClient;
/// # async fn call_list_stream(controller_client: &dyn ControllerClient) {
/// use pravega_client_shared::Scope;
/// use pravega_client_shared::ScopedStream;
/// use futures::future;
/// use futures::stream::StreamExt;
/// use pravega_controller_client::paginator::list_streams;
/// let stream = list_streams(
///     Scope {
///         name: "testScope".to_string(),
///     },
///     controller_client,
/// );
/// // collect all the Streams in a single vector
/// let stream_list:Vec<ScopedStream> = stream.map(|str| str.unwrap()).collect::<Vec<ScopedStream>>().await;
/// # }
/// ```
///
/// Sample 2:
/// ```
/// # use tonic::transport::Channel;
/// # use pravega_controller_client::controller::controller_service_client::ControllerServiceClient;
/// use pravega_controller_client::ControllerClient;
/// # async fn call_list_stream(controller_client: &dyn ControllerClient) {
/// use pravega_client_shared::Scope;
/// use pravega_client_shared::ScopedStream;
/// use futures::future;
/// use futures::stream::StreamExt;
/// use pravega_controller_client::paginator::list_streams;
/// let stream = list_streams(
///     Scope {
///         name: "testScope".to_string(),
///     },
///     controller_client,
/// );
/// futures::pin_mut!(stream);
/// let pravega_stream_1 = stream.next().await;
/// let pravega_stream_2 = stream.next().await;
/// // A None is returned at the end of the stream.
/// # }
/// ```
///
pub fn list_streams(
    scope: Scope,
    client: &dyn ControllerClient,
) -> impl Stream<Item = Result<ScopedStream, RetryError<ControllerError>>> + '_ {
    struct State {
        streams: IntoIter<ScopedStream>,
        scope: Scope,
        token: CToken,
    }

    // Initial state with an empty Continuation token.
    let get_next_stream_async = move |mut state: State| async move {
        if let Some(element) = state.streams.next() {
            Some((Ok(element), state))
        } else {
            // execute a request to the controller.
            info!(
                "Fetch the next set of streams under scope {} using the provided token",
                state.scope
            );
            let res: ResultRetry<Option<(Vec<ScopedStream>, CToken)>> =
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
                Err(e) => {
                    //log an error and return None to indicate end of stream.
                    error!(
                        "Error while attempting to list streams for scope {}. Error: {:?}",
                        state.scope, e
                    );
                    None
                }
            }
        }
    };
    stream::unfold(
        State {
            streams: Vec::new().into_iter(),
            scope,
            token: CToken::empty(),
        },
        get_next_stream_async,
    )
}

///
///Helper method to iterated over the all the Pravega streams under the provided Scope.
///This method returns a stream of values,Pravega streams, produced asynchronously.
///
/// The below snippets show case the example uses.
///
/// Sample 1:
/// ```
/// # use tonic::transport::Channel;
/// # use pravega_controller_client::controller::controller_service_client::ControllerServiceClient;
/// use pravega_controller_client::ControllerClient;
/// # async fn call_list_stream(controller_client: &dyn ControllerClient) {
/// use pravega_client_shared::Scope;
/// use pravega_client_shared::ScopedStream;
/// use futures::future;
/// use futures::stream::StreamExt;
/// use pravega_controller_client::paginator::list_streams_for_tag;
/// let stream = list_streams_for_tag(
///     Scope {
///         name: "testScope".to_string(),
///     },
///     "tagx".to_string(),
///     controller_client,
/// );
/// // collect all the Streams in a single vector
/// let stream_list:Vec<ScopedStream> = stream.map(|str| str.unwrap()).collect::<Vec<ScopedStream>>().await;
/// # }
/// ```
///
/// Sample 2:
/// ```
/// # use tonic::transport::Channel;
/// # use pravega_controller_client::controller::controller_service_client::ControllerServiceClient;
/// use pravega_controller_client::ControllerClient;
/// # async fn call_list_stream(controller_client: &dyn ControllerClient) {
/// use pravega_client_shared::Scope;
/// use pravega_client_shared::ScopedStream;
/// use futures::future;
/// use futures::stream::StreamExt;
/// use pravega_controller_client::paginator::list_streams_for_tag;
/// let stream = list_streams_for_tag(
///      Scope {
///         name: "testScope".to_string(),
///      },
///     "tagx".to_string(),
///     controller_client,
/// );
/// futures::pin_mut!(stream);
/// let pravega_stream_1 = stream.next().await;
/// let pravega_stream_2 = stream.next().await;
/// // A None is returned at the end of the stream.
/// # }
/// ```
///
pub fn list_streams_for_tag(
    scope: Scope,
    tag: String,
    client: &dyn ControllerClient,
) -> impl Stream<Item = Result<ScopedStream, RetryError<ControllerError>>> + '_ {
    struct State {
        streams: IntoIter<ScopedStream>,
        scope: Scope,
        tag: String,
        token: CToken,
    }

    // Initial state with an empty Continuation token.
    let get_next_stream_async = move |mut state: State| async move {
        if let Some(element) = state.streams.next() {
            Some((Ok(element), state))
        } else {
            // execute a request to the controller.
            info!(
                "Fetch the next set of streams with tag {} under scope {} using the provided token",
                state.tag, state.scope
            );
            let res: ResultRetry<Option<(Vec<ScopedStream>, CToken)>> = client
                .list_streams_for_tag(&state.scope, &state.tag, &state.token)
                .await;
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
                            tag: state.tag.clone(),
                            token: ct,
                        },
                    ))
                }
                Err(e) => {
                    //log an error and return None to indicate end of stream.
                    error!(
                        "Error while attempting to list streams with tag {} under scope {}. Error: {:?}",
                        state.tag, state.scope, e
                    );
                    None
                }
            }
        }
    };
    stream::unfold(
        State {
            streams: Vec::new().into_iter(),
            scope,
            tag,
            token: CToken::empty(),
        },
        get_next_stream_async,
    )
}
