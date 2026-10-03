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
