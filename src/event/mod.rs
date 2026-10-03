/// Written against HTML Server-Sent Events Spec @ 2/10/2026
/// link: https://html.spec.whatwg.org/multipage/server-sent-events.html#processField
use std::time::Duration;

#[cfg(feature = "json")]
use serde::de::DeserializeOwned;
#[cfg(feature = "json")]
use serde_json::error::Category;

mod buffer;
pub mod state;

#[derive(Debug)]
pub struct Event<Data> {
    pub event: String,
    pub data: Data,
    pub id: String,
    pub retry: Option<Duration>,
}

impl Event<String> {
    #[cfg(feature = "json")]
    pub(super) fn into_parsed_json_data<Data>(self) -> Result<Event<Data>, EventParsingError>
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

#[cfg(feature = "json")]
#[derive(Debug, thiserror::Error)]
pub enum EventParsingError {
    #[error("Failed to deserialise json data, line: {line}, column: {column}, cause: {cause}")]
    JsonDeserialisation {
        line: usize,
        column: usize,
        cause: &'static str,
    },
}

#[cfg(test)]
mod test {
    use serde::Deserialize;

    use crate::event::Event;

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
}
