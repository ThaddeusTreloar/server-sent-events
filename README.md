# server-sent-events

A small, spec-driven [Server-Sent Events](https://html.spec.whatwg.org/multipage/server-sent-events.html)
parser for Rust. It is an adapter from raw byte streams to stream of `Event`s, with
optional JSON deserialisation of event data and `reqwest`/`hyper` integration.

## Features

- `json` — deserialise each event's `data` field into your own type via `serde`.
- `reqwest` — adds `.event_stream()` / `.json_event_stream::<T>()` directly on
  `reqwest::Response`.
- `hyper` — adds `.event_stream()` / `.json_event_stream::<T>()` directly on
  `hyper::Response<hyper::body::Incoming>`, for users of the raw `hyper` 1.x client.

## Basic usage

Anything implementing `futures::Stream<Item = Result<B, E>>` where `B: AsRef<[u8]>`
(e.g. the body stream of an HTTP client) gets `.event_stream()` via the
`EventStreamExt` trait (see [`examples/raw_plain.rs`](./examples/raw_plain.rs),
runnable via `cargo run --example raw_plain`):

```rust
use futures::StreamExt;
use server_sent_events::stream::EventStreamExt;

fn main() {
    let raw = b"event: greeting\ndata: hello world\n\n".to_vec();
    let chunks: Vec<Result<Vec<u8>, std::io::Error>> = vec![Ok(raw)];

    futures::executor::block_on(async {
        let mut events = futures::stream::iter(chunks).event_stream();

        while let Some(event) = events.next().await {
            match event {
                Ok(event) => println!("{}: {}", event.event, event.data),
                Err(err) => eprintln!("stream error: {err}"),
            }
        }
    });
}
```

```text
greeting: hello world
```

## Deserialising JSON data (`json` feature)

When the `json` feature is enabled, streams also get `.json_event_stream::<T>()`
via the `EventStreamExt` trait (see [`examples/raw_json.rs`](./examples/raw_json.rs),
runnable via `cargo run --example raw_json --features json`):

```rust
use futures::StreamExt;
use serde::Deserialize;
use server_sent_events::stream::EventStreamExt;

#[derive(Deserialize)]
struct Message {
    text: String,
}

fn main() {
    let raw = b"data: {\"text\":\"hello world\"}\n\n".to_vec();
    let chunks: Vec<Result<Vec<u8>, std::io::Error>> = vec![Ok(raw)];

    futures::executor::block_on(async {
        let mut events = futures::stream::iter(chunks).json_event_stream::<Message, _, _>();

        while let Some(event) = events.next().await {
            match event {
                Ok(event) => println!("{}", event.data.text),
                Err(err) => eprintln!("error: {err}"),
            }
        }
    });
}
```

```text
hello world
```

Each event's JSON deserialisation is independent — one malformed event yields
an `Err` for that item without ending the stream.

## `reqwest` integration (`reqwest` feature)

`reqwest::Response` gets `.event_stream()` and `.json_event_stream::<T>()` via
`ResponseExt`. See [`examples/reqwest_plain.rs`](./examples/reqwest_plain.rs) (and
[`examples/reqwest_json.rs`](./examples/reqwest_json.rs) for the JSON variant):

```rust,no_run
use futures::StreamExt;
use server_sent_events::reqwest::ResponseExt;

#[tokio::main]
async fn main() -> Result<(), reqwest::Error> {
    let client = reqwest::Client::builder()
        .user_agent("my-app/0.1 (https://example.com)")
        .build()?;

    let response = client
        .get("https://stream.wikimedia.org/v2/stream/recentchange")
        .send()
        .await?;

    let mut events = response.event_stream();

    while let Some(event) = events.next().await {
        match event {
            Ok(event) => println!("{}: {}", event.event, event.data),
            Err(err) => eprintln!("stream error: {err}"),
        }
    }

    Ok(())
}
```

## `hyper` integration (`hyper` feature)

`hyper::Response<hyper::body::Incoming>` gets `.event_stream()` and
`.json_event_stream::<T>()` via `ResponseExt`. See [`examples/hyper_plain.rs`](./examples/hyper_plain.rs)
(and [`examples/hyper_json.rs`](./examples/hyper_json.rs) for the JSON variant):

```rust,no_run
use bytes::Bytes;
use futures::StreamExt;
use http_body_util::Empty;
use hyper::Request;
use hyper::header::USER_AGENT;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use server_sent_events::hyper::ResponseExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let https = HttpsConnectorBuilder::new()
        .with_native_roots()?
        .https_only()
        .enable_http1()
        .build();

    let client: Client<_, Empty<Bytes>> = Client::builder(TokioExecutor::new()).build(https);

    let request = Request::builder()
        .uri("https://stream.wikimedia.org/v2/stream/recentchange")
        .header(USER_AGENT, "my-app/0.1 (https://example.com)")
        .body(Empty::<Bytes>::new())?;

    let response = client.request(request).await?;
    let mut events = response.event_stream();

    while let Some(event) = events.next().await {
        match event {
            Ok(event) => println!("{}: {}", event.event, event.data),
            Err(err) => eprintln!("stream error: {err}"),
        }
    }

    Ok(())
}
```
