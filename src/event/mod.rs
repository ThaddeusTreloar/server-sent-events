//! Event type and utilities.
//!
//! Written against HTML Server-Sent Events Spec.
//!
//! link: <https://html.spec.whatwg.org/multipage/server-sent-events.html#processField>
use std::time::Duration;

#[cfg(feature = "json")]
use serde::de::DeserializeOwned;
#[cfg(feature = "json")]
use serde_json::error::Category;

mod buffer;
pub(crate) mod state;

/// Holds the data of a server sent event.
#[derive(Debug)]
pub struct Event<Data> {
    /// If the server provides no event type, the default is `message`.
    pub event: String,
    /// The event data.
    pub data: Data,
    /// Derived from the current values in the state machine, which is only updated
    /// when the server sends an updated ID field. As such, if the target server does not
    /// return IDs or does not update them, this will remain static. Defaults to an
    /// empty string.
    pub id: String,
    /// Derived from the current values in the state machine, which is only updated
    /// when the server sends an updated retry field. As such, if the target server does not
    /// return retry or does not update it, this will remain static. Defaults to None.
    pub retry: Option<Duration>,
}

impl Event<String> {
    /// Parses the plaintext data in this event as json and returns
    /// a new instance of event, with the provided `Data` type.
    #[cfg(feature = "json")]
    pub fn into_parsed_json_data<Data>(self) -> Result<Event<Data>, EventParsingError>
    where
        Data: DeserializeOwned,
    {
        let Event {
            event,
            id,
            retry,
            data: string_data,
        } = self;

        match serde_json::from_str::<Data>(&string_data) {
            Ok(data) => Ok(Event {
                event,
                id,
                retry,
                data,
            }),
            Err(e) => Err(EventParsingError::JsonDeserialisation {
                line: e.line(),
                column: e.column(),
                cause: match e.classify() {
                    Category::Data => "semantic-error",
                    Category::Io => "io-failure",
                    Category::Syntax => "invalid-syntax",
                    Category::Eof => "unexpected-eof",
                },
            }),
        }
    }
}

/// A JSON deserialisation error returned while trying to decode the event data.
#[cfg(feature = "json")]
#[derive(Debug, thiserror::Error)]
pub enum EventParsingError {
    /// JSON Deserialisation has encountered an unrecoverable error.
    #[error("Failed to deserialise json data, line: {line}, column: {column}, cause: {cause}")]
    JsonDeserialisation {
        /// The line the error ocurred on.
        line: usize,
        /// The column the error ocurred on.
        column: usize,
        /// An indicative cause of the error.
        cause: &'static str,
    },
}

#[cfg(test)]
mod test {
    #[cfg(feature = "json")]
    use crate::event::Event;
    #[cfg(feature = "json")]
    use serde::Deserialize;

    #[cfg(feature = "json")]
    #[derive(Debug, Deserialize)]
    struct Datum {
        field: String,
        count: usize,
    }

    #[cfg(feature = "json")]
    #[test]
    fn test_json_dispatch() {
        let event = Event {
            event: String::from("message"),
            data: String::from("{\"field\":\"hello\",\"count\":10}"),
            id: String::default(),
            retry: None,
        };

        let data_event_result = event.into_parsed_json_data::<Datum>();

        assert!(data_event_result.is_ok());

        let data_event = data_event_result.unwrap();

        assert!(data_event.data.field == "hello");
        assert!(data_event.data.count == 10);
    }

    #[cfg(feature = "json")]
    #[test]
    fn test_json_serialisation_errors_on_utf8_replacement() {
        // Per spec, malformed byte sequences are replaced with U+FFFD
        // rather than ending the stream with an error. Here that lands
        // right after the opening brace, breaking JSON syntax, so it
        // surfaces as a JSON deserialisation error rather than a utf8 one.

        let event = Event {
            event: String::from("message"),
            data: String::from("{\u{FFFD}\"field\":\"hi\",\"count\":10}"),
            id: String::default(),
            retry: None,
        };

        let data_event_result = event.into_parsed_json_data::<Datum>();

        assert!(data_event_result.is_err());
    }
}
