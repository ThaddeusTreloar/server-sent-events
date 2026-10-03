//! Streaming adapters for event streams.

mod event;
#[cfg(feature = "json")]
mod json_event;

use std::convert::Infallible;

use futures::{Stream, StreamExt};

pub use event::EventStream;
#[cfg(feature = "json")]
pub use json_event::{JsonEventStream, JsonEventStreamError};

use crate::event::Event;
#[cfg(feature = "json")]
use crate::event::EventParsingError;

/// Adapter stream extensions.
pub trait EventStreamExt: Stream + Sized {
    /// Yields a stream of server sent events from raw bytes.
    /// The data fields are plain [`String`].
    fn event_stream<B, E>(self) -> EventStream<Self>
    where
        Self: Stream<Item = Result<B, E>>,
        B: AsRef<[u8]>,
    {
        EventStream::new(self)
    }

    /// Like [`event_stream`](Self::event_stream), but for a source that
    /// can't fail. Since there is no possible error left at all, this
    /// yields bare [`Event<String>`]s rather than [`Result`]s.
    fn event_stream_infallible<B>(self) -> impl Stream<Item = Event<String>>
    where
        Self: Stream<Item = B> + Unpin,
        B: AsRef<[u8]>,
    {
        EventStream::new(StreamExt::map(self, Ok as fn(B) -> Result<B, Infallible>)).map(|result| {
            match result {
                Ok(event) => event,
                Err(infallible) => match infallible {},
            }
        })
    }

    /// Yields a stream of server sent events from raw bytes.
    /// The data fields are parsed as the provided type from JSON,
    /// returning `JsonEventStreamError<E>` on failure.
    #[cfg(feature = "json")]
    fn json_event_stream<Data, B, E>(self) -> JsonEventStream<Self, Data>
    where
        Self: Stream<Item = Result<B, E>>,
        B: AsRef<[u8]>,
        Data: serde::de::DeserializeOwned,
    {
        JsonEventStream::new(self)
    }

    /// Like [`json_event_stream`](Self::json_event_stream), but for a
    /// source that can't fail. The only error left possible is a JSON
    /// deserialisation failure, so this yields `Result<Event<Data>,
    /// EventParsingError>` rather than the full `JsonEventStreamError<E>`.
    #[cfg(feature = "json")]
    fn json_event_stream_infallible<Data, B>(
        self,
    ) -> impl Stream<Item = Result<Event<Data>, EventParsingError>>
    where
        Self: Stream<Item = B> + Unpin,
        B: AsRef<[u8]>,
        Data: serde::de::DeserializeOwned,
    {
        JsonEventStream::new(StreamExt::map(self, Ok as fn(B) -> Result<B, Infallible>)).map(
            |result| match result {
                Ok(event) => Ok(event),
                Err(JsonEventStreamError::Source(infallible)) => match infallible {},
                Err(JsonEventStreamError::Json(err)) => Err(err),
            },
        )
    }
}

impl<S: Stream> EventStreamExt for S {}
