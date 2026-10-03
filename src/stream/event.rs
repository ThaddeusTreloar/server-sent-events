use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;

use crate::event::Event;
use crate::event::state::EventStreamState;

pub struct EventStream<S> {
    inner: S,
    state: EventStreamState,
    queue: VecDeque<Event<String>>,
}

impl<S> EventStream<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            state: EventStreamState::new(),
            queue: VecDeque::new(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EventStreamError<E> {
    #[error("received invalid utf8 while parsing SSE stream")]
    InvalidUtf8,
    #[error(transparent)]
    Source(E),
}

impl<S, B, E> Stream for EventStream<S>
where
    S: Stream<Item = Result<B, E>> + Unpin,
    B: AsRef<[u8]>,
{
    type Item = Result<Event<String>, EventStreamError<E>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if let Some(event) = this.queue.pop_front() {
                return Poll::Ready(Some(Ok(event)));
            }

            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(EventStreamError::Source(e))));
                }
                Poll::Ready(Some(Ok(bytes))) => match this.state.parse(bytes.as_ref()) {
                    Ok(events) => this.queue.extend(events),
                    Err(_) => return Poll::Ready(Some(Err(EventStreamError::InvalidUtf8))),
                },
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::convert::Infallible;

    use futures::executor::block_on;
    use futures::stream::{self, StreamExt};

    use crate::stream::EventStreamExt;

    #[test]
    fn test_event_stream_single_chunk() {
        let chunks: Vec<Result<&[u8], Infallible>> =
            vec![Ok(b"event: myevent\ndata: hello\n\n".as_slice())];

        let mut es = stream::iter(chunks).event_stream();

        let event = block_on(es.next()).unwrap().unwrap();
        assert_eq!(event.event, "myevent");
        assert_eq!(event.data, "hello");

        assert!(block_on(es.next()).is_none());
    }

    #[test]
    fn test_event_stream_split_across_chunks() {
        let chunks: Vec<Result<&[u8], Infallible>> =
            vec![Ok(b"data: hel".as_slice()), Ok(b"lo\n\n".as_slice())];

        let mut es = stream::iter(chunks).event_stream();
        let event = block_on(es.next()).unwrap().unwrap();
        assert_eq!(event.data, "hello");
    }

    #[test]
    fn test_event_stream_propagates_source_error() {
        #[derive(Debug, PartialEq)]
        struct MyErr;

        let chunks: Vec<Result<&[u8], MyErr>> = vec![Err(MyErr)];
        let mut es = stream::iter(chunks).event_stream();
        let result = block_on(es.next()).unwrap();
        assert!(matches!(
            result,
            Err(super::EventStreamError::Source(MyErr))
        ));
    }

    #[test]
    fn test_event_stream_invalid_utf8_errors() {
        let chunks: Vec<Result<&[u8], Infallible>> = vec![Ok(b"\xff\xfe".as_slice())];

        let mut es = stream::iter(chunks).event_stream();
        let result = block_on(es.next()).unwrap();
        assert!(matches!(result, Err(super::EventStreamError::InvalidUtf8)));
    }
}
