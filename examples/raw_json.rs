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
