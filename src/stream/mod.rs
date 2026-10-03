pub mod event;
#[cfg(feature = "json")]
pub mod json_event;

use futures::Stream;

pub use event::{EventStream, EventStreamError};
#[cfg(feature = "json")]
pub use json_event::{JsonEventStream, JsonEventStreamError};

pub trait EventStreamExt: Stream + Sized {
    fn event_stream<B, E>(self) -> EventStream<Self>
    where
        Self: Stream<Item = Result<B, E>>,
        B: AsRef<[u8]>,
    {
        EventStream::new(self)
    }

    #[cfg(feature = "json")]
    fn json_event_stream<Data, B, E>(self) -> JsonEventStream<Self, Data>
    where
        Self: Stream<Item = Result<B, E>>,
        B: AsRef<[u8]>,
        Data: serde::de::DeserializeOwned,
    {
        JsonEventStream::new(self)
    }
}

impl<S: Stream> EventStreamExt for S {}
