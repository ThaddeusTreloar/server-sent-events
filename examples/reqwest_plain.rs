/// Streams events from Wikimedia's public recent-changes feed and
/// prints the first few as raw `Event<String>`s.
use futures::StreamExt;
use server_sent_events::reqwest::ResponseExt;

#[tokio::main]
async fn main() -> Result<(), reqwest::Error> {
    let client = reqwest::Client::builder()
        // Wikimedia's robot policy (https://w.wiki/4wJS) rejects requests without a
        // descriptive `User-Agent`, so we build a client with one instead of using
        .user_agent(
            "server-sent-events-rs-example/0.1 (https://crates.io/crates/server-sent-events)",
        )
        .build()?;

    let response = client
        .get("https://stream.wikimedia.org/v2/stream/recentchange")
        .send()
        .await?;

    let mut events = response.event_stream().take(5);

    while let Some(event) = events.next().await {
        match event {
            Ok(event) => println!("{}: {}", event.event, event.data),
            Err(err) => eprintln!("stream error: {err}"),
        }
    }

    Ok(())
}
