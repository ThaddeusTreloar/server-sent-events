use std::time::Duration;

use crate::event::{Event, buffer::EventBuffer};

#[derive(Debug, Default)]
pub(crate) struct EventStreamState {
    event_buffer: EventBuffer,
    line_buffer: String,
    line_buffer_position: usize,
}

impl EventStreamState {
    pub fn new() -> Self {
        Self::default()
    }

    fn extract_next_line(&mut self, new_string: &str) -> Option<String> {
        self.line_buffer.push_str(new_string);

        // We will only check the unread portion of the buffer
        // so we don't continually re-read on long messages.
        let unread = self
            .line_buffer
            .get(self.line_buffer_position..)
            .expect("Excpeted valid utf8 slice, this is a an obvious logic error.");

        // Attempt to find the next line break BYTE
        let (string_end_pos, is_crlf) = match unread.find(['\r', '\n']) {
            None => {
                // We can set the newline position for the next invocation
                self.line_buffer_position += new_string.len();
                return None;
            }
            // The BYTE position of the next line break
            Some(pos) => (
                self.line_buffer_position + pos,
                unread
                    .as_bytes()
                    .get(pos)
                    .filter(|c| **c == b'\r')
                    .is_some()
                    && unread
                        .as_bytes()
                        .get(pos + 1)
                        .filter(|c| **c == b'\n')
                        .is_some(),
            ),
        };

        let line = self
            .line_buffer
            .drain(0..string_end_pos)
            .collect::<String>();

        if is_crlf {
            let _ = self.line_buffer.drain(0..2);
        } else {
            let _ = self.line_buffer.drain(0..1);
        }

        // reset our position
        self.line_buffer_position = 0;

        Some(line)
    }

    fn extract_lines(&mut self, new_string: &str) -> Vec<String> {
        let mut lines = Vec::<String>::new();

        match self.extract_next_line(new_string) {
            None => return lines,
            Some(line) => {
                lines.push(line);

                // We will continue to append nothing until the buffer lines
                // are exhausted.
                while let Some(line) = self.extract_next_line("") {
                    lines.push(line);
                }
            }
        }

        lines
    }

    pub(crate) fn parse(&mut self, bytes: &[u8]) -> Vec<Event<String>> {
        // Per spec, decoding never fails outright - malformed byte
        // sequences are replaced with U+FFFD and decoding continues.
        let new_string = String::from_utf8_lossy(bytes);

        self.extract_lines(&new_string)
            .into_iter()
            .flat_map(|line| self.parse_line(line))
            .collect()
    }

    fn parse_line(&mut self, mut content: String) -> Option<Event<String>> {
        if content.is_empty() {
            return self.event_buffer.dispatch();
        }

        let (field, mut value) = match content.find(':') {
            Some(0) => return None,
            None => (content, None),
            Some(pos) => (
                content.drain(0..pos).collect(),
                // Drain panics on out of bounds so we need to
                // ensure there are chars to drain beyond the
                // ':' delimiter
                Some(content)
                    .filter(|s| s.len() > 1)
                    .map(|mut s| s.drain(1..).collect::<String>()),
            ),
        };

        // Must remove the first character if it is U+0020

        if let Some(val) = value.as_mut()
            && val.starts_with(' ')
        {
            val.remove(0);
        }

        match field.as_str() {
            "event" => self.event_buffer.event = value,
            "data" => {
                // The spec dicates we always append a line feed
                // to the end of a data string before appending
                // to the existing state.
                let mut val = value.unwrap_or(String::new());
                val.push('\n');
                match self.event_buffer.data.as_mut() {
                    None => {
                        let _ = self.event_buffer.data.replace(val);
                    }
                    Some(current) => {
                        current.push_str(&val);
                    }
                }
            }
            "id" => {
                if let Some(val) = value
                    && !val.contains('\0')
                {
                    let _ = self.event_buffer.last_event_id.replace(val);
                }
            }
            "retry" => {
                if let Some(val) = value
                    && val.chars().all(|c| c.is_ascii_digit())
                {
                    // If there is a parsing error we will just ignore the line.
                    // This will only catch if the number is larger than u64 as the
                    // source is only digits.
                    let millis = val.parse::<u64>().ok()?;

                    let _ = self
                        .event_buffer
                        .retry
                        .replace(Duration::from_millis(millis));
                }
            }
            _ => return None,
        }

        None
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::EventStreamState;

    #[test]
    fn test_line_extraction_line_feed() {
        let mut state = EventStreamState::new();

        let mut maybe_line = state.extract_next_line("Hello World\n");
        assert!(maybe_line.is_some());
        let line = maybe_line.unwrap();
        assert!(state.line_buffer.is_empty());
        assert!(state.line_buffer_position == 0);
        assert!(line == "Hello World");

        maybe_line = state.extract_next_line("Hello");
        assert!(maybe_line.is_none());
        assert!(state.line_buffer == "Hello");
        assert!(state.line_buffer_position == 5);

        maybe_line = state.extract_next_line("\n");
        assert!(maybe_line.is_some());
        let line = maybe_line.unwrap();
        dbg!(&state.line_buffer);
        assert!(state.line_buffer.is_empty());
        assert!(state.line_buffer_position == 0);
        assert!(line == "Hello");
    }

    #[test]
    fn test_line_extraction_carriage_return() {
        let mut state = EventStreamState::new();

        let mut maybe_line = state.extract_next_line("Hello World\r");
        assert!(maybe_line.is_some());
        let line = maybe_line.unwrap();
        assert!(state.line_buffer.is_empty());
        assert!(state.line_buffer_position == 0);
        assert!(line == "Hello World");

        maybe_line = state.extract_next_line("Hello");
        assert!(maybe_line.is_none());
        assert!(state.line_buffer == "Hello");
        assert!(state.line_buffer_position == 5);

        maybe_line = state.extract_next_line("\r");
        assert!(maybe_line.is_some());
        let line = maybe_line.unwrap();
        dbg!(&state.line_buffer);
        assert!(state.line_buffer.is_empty());
        assert!(state.line_buffer_position == 0);
        assert!(line == "Hello");
    }

    #[test]
    fn test_line_extraction_crlf() {
        let mut state = EventStreamState::new();

        let mut maybe_line = state.extract_next_line("Hello World\r\n");
        assert!(maybe_line.is_some());
        let line = maybe_line.unwrap();
        assert!(state.line_buffer.is_empty());
        assert!(state.line_buffer_position == 0);
        assert!(line == "Hello World");

        maybe_line = state.extract_next_line("Hello");
        assert!(maybe_line.is_none());
        assert!(state.line_buffer == "Hello");
        assert!(state.line_buffer_position == 5);

        maybe_line = state.extract_next_line("\r\n");
        assert!(maybe_line.is_some());
        let line = maybe_line.unwrap();
        dbg!(&state.line_buffer);
        assert!(state.line_buffer.is_empty());
        assert!(state.line_buffer_position == 0);
        assert!(line == "Hello");
    }

    #[test]
    fn test_line_extraction_empty() {
        let mut state = EventStreamState::new();

        let mut maybe_line = state.extract_next_line("\n");
        assert!(maybe_line.is_some());
        let line = maybe_line.unwrap();
        assert!(state.line_buffer.is_empty());
        assert!(state.line_buffer_position == 0);
        assert!(line.is_empty());
        maybe_line = state.extract_next_line("\r");
        assert!(maybe_line.is_some());
        let line = maybe_line.unwrap();
        assert!(state.line_buffer.is_empty());
        assert!(state.line_buffer_position == 0);
        assert!(line.is_empty());
        maybe_line = state.extract_next_line("\r\n");
        assert!(maybe_line.is_some());
        let line = maybe_line.unwrap();
        assert!(state.line_buffer.is_empty());
        assert!(state.line_buffer_position == 0);
        assert!(line.is_empty());
    }

    #[test]
    fn test_line_continues_extraction_with_multi_line_buffer() {
        let mut state = EventStreamState::new();

        let mut maybe_line = state.extract_next_line("event: myevent\ndata: somedata\n\n");
        assert!(maybe_line.is_some());
        let mut line = maybe_line.unwrap();
        assert!(line == "event: myevent");

        maybe_line = state.extract_next_line("");
        assert!(maybe_line.is_some());
        line = maybe_line.unwrap();
        assert!(line == "data: somedata");

        maybe_line = state.extract_next_line("");
        assert!(maybe_line.is_some());
        line = maybe_line.unwrap();
        assert!(line.is_empty());

        maybe_line = state.extract_next_line("");
        assert!(maybe_line.is_none());
    }

    #[test]
    fn test_all_lines_extracted() {
        let mut state = EventStreamState::new();

        let mut lines = state.extract_lines("event: myevent\ndata: somedata\n\n");
        assert!(lines.len() == 3);
        assert!(lines.pop().unwrap().is_empty());
        assert!(lines.pop().unwrap() == "data: somedata");
        assert!(lines.pop().unwrap() == "event: myevent");
    }

    #[test]
    fn test_line_parsing_no_vals() {
        let mut state = EventStreamState::new();

        let maybe_empty_event = state.parse_line(String::new());

        assert!(maybe_empty_event.is_none());
    }

    #[test]
    fn test_line_parsing_no_data() {
        let mut state = EventStreamState::new();

        let maybe_empty_event = state.parse_line(String::from("event: myevent"));
        assert!(maybe_empty_event.is_none());
        let maybe_empty_event = state.parse_line(String::from(""));
        assert!(maybe_empty_event.is_none());
        assert!(state.event_buffer.event.is_none());
        assert!(state.event_buffer.data.is_none());
    }

    #[test]
    fn test_line_parsing_with_data() {
        let mut state = EventStreamState::new();

        let mut maybe_event = state.parse_line(String::from("event: myevent"));
        assert!(maybe_event.is_none());
        maybe_event = state.parse_line(String::from("data: hello world"));
        assert!(maybe_event.is_none());
        maybe_event = state.parse_line(String::from("id: some-id"));
        assert!(maybe_event.is_none());
        maybe_event = state.parse_line(String::from("retry: 1000"));
        assert!(maybe_event.is_none());
        maybe_event = state.parse_line(String::from(""));
        assert!(maybe_event.is_some());

        let event = maybe_event.unwrap();
        assert!(event.event == "myevent");
        assert!(event.data == "hello world");
        assert!(event.id == "some-id");
        assert!(event.retry.unwrap() == Duration::from_millis(1000));
        assert!(state.event_buffer.event.is_none());
        assert!(state.event_buffer.data.is_none());
    }

    #[test]
    fn test_line_parsing_with_default_event_name() {
        let mut state = EventStreamState::new();

        let mut maybe_event = state.parse_line(String::from("data: hello world"));
        assert!(maybe_event.is_none());
        maybe_event = state.parse_line(String::from(""));
        assert!(maybe_event.is_some());

        let event = maybe_event.unwrap();
        assert!(event.event == "message");
        assert!(event.data == "hello world");
        assert!(event.id.is_empty());
        assert!(event.retry.is_none());
        assert!(state.event_buffer.event.is_none());
        assert!(state.event_buffer.data.is_none());
    }

    #[test]
    fn test_line_parsing_id_persists() {
        let mut state = EventStreamState::new();

        let mut maybe_event = state.parse_line(String::from("data: hello world"));
        assert!(maybe_event.is_none());
        maybe_event = state.parse_line(String::from("id: some-id"));
        assert!(maybe_event.is_none());
        maybe_event = state.parse_line(String::from(""));
        assert!(maybe_event.is_some());

        let event = maybe_event.unwrap();
        assert!(event.id == "some-id");

        let mut maybe_event = state.parse_line(String::from("data: hello world"));
        assert!(maybe_event.is_none());
        maybe_event = state.parse_line(String::from(""));
        assert!(maybe_event.is_some());

        let event = maybe_event.unwrap();
        assert!(event.id == "some-id");
    }

    #[test]
    fn test_line_parsing_retry_persists() {
        let mut state = EventStreamState::new();

        let mut maybe_event = state.parse_line(String::from("data: hello world"));
        assert!(maybe_event.is_none());
        maybe_event = state.parse_line(String::from("retry: 1000"));
        assert!(maybe_event.is_none());
        maybe_event = state.parse_line(String::from(""));
        assert!(maybe_event.is_some());

        let event = maybe_event.unwrap();
        assert!(event.retry.unwrap() == Duration::from_millis(1000));

        let mut maybe_event = state.parse_line(String::from("data: hello world"));
        assert!(maybe_event.is_none());
        maybe_event = state.parse_line(String::from(""));
        assert!(maybe_event.is_some());

        let event = maybe_event.unwrap();
        assert!(event.retry.unwrap() == Duration::from_millis(1000));
    }

    #[test]
    fn test_line_parsing_with_multiline_data() {
        let mut state = EventStreamState::new();

        let mut maybe_event = state.parse_line(String::from("data: hello world"));
        assert!(maybe_event.is_none());
        maybe_event = state.parse_line(String::from("data: and friends"));
        assert!(maybe_event.is_none());
        maybe_event = state.parse_line(String::from(""));
        assert!(maybe_event.is_some());

        let event = maybe_event.unwrap();
        assert!(event.event == "message");
        assert!(event.data == "hello world\nand friends");
        assert!(state.event_buffer.event.is_none());
        assert!(state.event_buffer.data.is_none());

        maybe_event = state.parse_line(String::from("data: hello world\nand friends"));
        assert!(maybe_event.is_none());
        maybe_event = state.parse_line(String::from(""));
        assert!(maybe_event.is_some());

        let event = maybe_event.unwrap();
        assert!(event.event == "message");
        assert!(event.data == "hello world\nand friends");
        assert!(state.event_buffer.event.is_none());
        assert!(state.event_buffer.data.is_none());
    }

    #[test]
    fn test_parsing_no_event() {
        let mut state = EventStreamState::new();

        let buf = String::from("event: noop\n");

        let parse_output = state.parse(buf.as_bytes());

        assert!(parse_output.is_empty());
    }

    #[test]
    fn test_parsing_event() {
        let mut state = EventStreamState::new();

        let buf = String::from("data: hello\n");
        let _ = state.parse(buf.as_bytes());
        let parse_output = state.parse(String::from("\n").as_bytes());

        assert!(parse_output.len() == 1);

        let parse_event = parse_output.first().unwrap();

        assert!(parse_event.data == "hello");
    }

    #[test]
    fn test_parsing_multiline_event() {
        let mut state = EventStreamState::new();

        let buf = String::from("event: myevent\ndata: somedata\n\n");
        let parse_output = state.parse(buf.as_bytes());

        assert!(parse_output.len() == 1);

        let parse_event = parse_output.first().unwrap();

        assert!(parse_event.event == "myevent");
        assert!(parse_event.data == "somedata");
    }
}
