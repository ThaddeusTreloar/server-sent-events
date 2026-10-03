/// Streams events from Wikimedia's public recent-changes feed and
/// deserialises each one's `data` into `RecentChange`.
use futures::StreamExt;
use serde::Deserialize;
use server_sent_events::reqwest::ResponseExt;

#[derive(Debug, Deserialize)]
struct RecentChange {
    title: String,
    user: String,
}

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

    let mut events = response.json_event_stream::<RecentChange>().take(5);

    while let Some(event) = events.next().await {
        match event {
            Ok(event) => println!("{} edited by {}", event.data.title, event.data.user),
            Err(err) => eprintln!("error: {err}"),
        }
    }

    Ok(())
}
