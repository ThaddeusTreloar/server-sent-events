use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use serde::de::DeserializeOwned;

use crate::event::{Event, EventParsingError};

use super::event::EventStream;

/// A wrapper adapter that deserialises `data` as JSON for every event produced
/// from an inner [`EventStream`].
pub struct JsonEventStream<S, Data> {
    inner: EventStream<S>,
    // `Data` is never actually stored, so a plain `PhantomData<Data>` would
    // needlessly tie this struct's auto traits (e.g. `Unpin`) to `Data`'s.
    // The fn-pointer is a work around for simplicity.
    _data: PhantomData<fn() -> Data>,
}

impl<S, Data> JsonEventStream<S, Data> {
    pub fn new(inner: S) -> Self {
        Self {
            inner: EventStream::new(inner),
            _data: PhantomData,
        }
    }
}

/// An error returned from a [`JsonEventStream`].
#[derive(Debug, thiserror::Error)]
pub enum JsonEventStreamError<E> {
    /// An error returned transparently from the inner stream.
    #[error(transparent)]
    Source(E),
    /// The stream failed to deserialise the data field as JSON, yielding an [`EventParsingError`].
    #[error(transparent)]
    Json(#[from] EventParsingError),
}

impl<S, B, E, Data> Stream for JsonEventStream<S, Data>
where
    S: Stream<Item = Result<B, E>> + Unpin,
    B: AsRef<[u8]>,
    Data: DeserializeOwned,
{
    type Item = Result<Event<Data>, JsonEventStreamError<E>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        Pin::new(&mut this.inner).poll_next(cx).map(|opt| {
            opt.map(|result| match result {
                Err(e) => Err(JsonEventStreamError::Source(e)),
                Ok(event) => event
                    .into_parsed_json_data::<Data>()
                    .map_err(JsonEventStreamError::Json),
            })
        })
    }
}

#[cfg(test)]
mod test {
    use std::convert::Infallible;

    use futures::executor::block_on;
    use futures::stream::{self, StreamExt};

    use crate::stream::EventStreamExt;

    #[derive(Debug, serde::Deserialize)]
    struct Datum {
        field: String,
    }

    #[test]
    fn test_json_event_stream() {
        let chunks: Vec<Result<&[u8], Infallible>> =
            vec![Ok(b"data: {\"field\":\"hi\"}\n\n".as_slice())];

        let mut es = stream::iter(chunks).json_event_stream::<Datum, _, _>();
        let event = block_on(es.next()).unwrap().unwrap();
        assert_eq!(event.data.field, "hi");
    }

    #[test]
    fn test_json_event_stream_bad_data_then_good_event() {
        let chunks: Vec<Result<&[u8], Infallible>> = vec![Ok(b"data: not-json\n\n\
            data: {\"field\":\"hi\"}\n\n"
            .as_slice())];

        let mut es = stream::iter(chunks).json_event_stream::<Datum, _, _>();

        let first = block_on(es.next()).unwrap();
        assert!(matches!(first, Err(super::JsonEventStreamError::Json(_))));

        let second = block_on(es.next()).unwrap().unwrap();
        assert_eq!(second.data.field, "hi");
    }
}
