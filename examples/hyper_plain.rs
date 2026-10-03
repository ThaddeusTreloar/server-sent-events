/// Streams events from Wikimedia's public recent-changes feed and
/// prints the first few as raw `Event<String>`s.
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
        // Wikimedia's robot policy (https://w.wiki/4wJS) rejects requests without a
        // descriptive `User-Agent`, so we set one explicitly on the request.
        .header(
            USER_AGENT,
            "server-sent-events-rs-example/0.1 (https://crates.io/crates/server-sent-events)",
        )
        .body(Empty::<Bytes>::new())?;

    let response = client.request(request).await?;
    let mut events = response.event_stream().take(5);

    while let Some(event) = events.next().await {
        match event {
            Ok(event) => println!("{}: {}", event.event, event.data),
            Err(err) => eprintln!("stream error: {err}"),
        }
    }

    Ok(())
}
