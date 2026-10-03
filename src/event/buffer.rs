use super::Event;
use std::time::Duration;

#[derive(Debug, Default)]
pub(super) struct EventBuffer {
    pub(super) event: Option<String>,
    pub(super) data: Option<String>,
    pub(super) last_event_id: Option<String>,
    pub(super) retry: Option<Duration>,
}

impl EventBuffer {
    pub(super) fn dispatch(&mut self) -> Option<Event<String>> {
        // If data is none then we discard the event
        // and reset the event and data buffers.
        if self.data.is_none() {
            let _ = self.event.take();
            let _ = self.data.take();
            return None;
        }

        Some(Event {
            event: self
                .event
                .take()
                // If event is empty, then we use the default 'message'
                .unwrap_or(String::from("message")),
            data: self
                .data
                .take()
                // We must remove the last charact of data if it is a line feed
                .map(|mut s| {
                    if s.ends_with("\n") {
                        let _ = s.pop();
                    }

                    s
                })
                .unwrap_or_default(),
            // last_event_id is preserved until updated
            id: self.last_event_id.clone().unwrap_or_default(),
            // The spec doesn't say to unset this so we will clone it
            // every time. Might be worth optimisation in future.
            retry: self.retry,
        })
    }
}
