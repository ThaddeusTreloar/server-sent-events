use bytes::Bytes;
use futures::Stream;
use reqwest::{Error, Response};

#[cfg(feature = "json")]
use crate::stream::JsonEventStream;
use crate::stream::{EventStream, EventStreamExt};

pub trait ResponseExt {
    /// Parses this response's body as a stream of server-sent events.
    fn event_stream(self) -> EventStream<impl Stream<Item = Result<Bytes, Error>>>;

    /// Parses this response's body as a stream of server-sent events,
    /// deserialising each event's `data` as JSON.
    #[cfg(feature = "json")]
    fn json_event_stream<Data>(
        self,
    ) -> JsonEventStream<impl Stream<Item = Result<Bytes, Error>>, Data>
    where
        Data: serde::de::DeserializeOwned;
}

impl ResponseExt for Response {
    fn event_stream(self) -> EventStream<impl Stream<Item = Result<Bytes, Error>>> {
        self.bytes_stream().event_stream()
    }

    #[cfg(feature = "json")]
    fn json_event_stream<Data>(
        self,
    ) -> JsonEventStream<impl Stream<Item = Result<Bytes, Error>>, Data>
    where
        Data: serde::de::DeserializeOwned,
    {
        self.bytes_stream().json_event_stream::<Data, _, _>()
    }
}
