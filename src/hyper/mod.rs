//! Hyper client response extensions.

use bytes::Bytes;
use futures::{Stream, StreamExt};
use http_body_util::BodyStream;
use hyper::{Error, Response, body::Incoming};

#[cfg(feature = "json")]
use crate::stream::JsonEventStream;
use crate::stream::{EventStream, EventStreamExt};

/// `hyper::body::Incoming` only implements `http_body::Body` (a
/// poll-one-frame-at-a-time trait), not `futures::Stream`. This turns it
/// into a plain byte stream, discarding trailer frames and passing frame-
/// level errors straight through.
fn into_byte_stream(incoming: Incoming) -> impl Stream<Item = Result<Bytes, Error>> {
    BodyStream::new(incoming).filter_map(|frame| {
        std::future::ready(match frame {
            Ok(frame) => frame.into_data().ok().map(Ok),
            Err(e) => Some(Err(e)),
        })
    })
}

/// Extension to adapt hyper body responses to an event stream.
pub trait ResponseExt {
    /// Parses this response's body as a stream of server-sent events.
    fn event_stream(self) -> EventStream<impl Stream<Item = Result<Bytes, Error>>>;

    /// Parses this response's body as a stream of server-sent events,
    /// deserialising each event's `data` as JSON into the provided type.
    #[cfg(feature = "json")]
    fn json_event_stream<Data>(
        self,
    ) -> JsonEventStream<impl Stream<Item = Result<Bytes, Error>>, Data>
    where
        Data: serde::de::DeserializeOwned;
}

impl ResponseExt for Response<Incoming> {
    fn event_stream(self) -> EventStream<impl Stream<Item = Result<Bytes, Error>>> {
        into_byte_stream(self.into_body()).event_stream()
    }

    #[cfg(feature = "json")]
    fn json_event_stream<Data>(
        self,
    ) -> JsonEventStream<impl Stream<Item = Result<Bytes, Error>>, Data>
    where
        Data: serde::de::DeserializeOwned,
    {
        into_byte_stream(self.into_body()).json_event_stream::<Data, _, _>()
    }
}
