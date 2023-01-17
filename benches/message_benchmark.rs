use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use hauspost::broker::MessageBroker;
use tokio::runtime::Runtime;

async fn bench_send_receive_subsequent(count: usize) {
    let connection = MessageBroker::new().run_in_background();
    for _ in 1..count {
        connection
            .send_message_nonblocking(
                "message".to_string(),
                "topic".to_string(),
                Some(Duration::from_secs(3)),
            )
            .await;
    }

    // Receive must be in-order within topic
    for _ in 1..count {
        connection.receive_message("topic".to_string(), None).await;
    }
}

async fn bench_send_receive_blocking(count: usize) {
    let connection = MessageBroker::new().run_in_background();
    let send_connection = connection.clone();
    tokio::spawn(async move {
        for _ in 1..count {
            send_connection
                .send_message_nonblocking(
                    "message".to_string(),
                    "topic".to_string(),
                    Some(Duration::from_secs(3)),
                )
                .await;
        }
    });

    // Receive must be in-order within topic
    for _ in 1..count {
        connection.receive_message("topic".to_string(), None).await;
    }
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let size: usize = 10_000;
    let rt = Runtime::new().unwrap();
    c.bench_with_input(
        BenchmarkId::new("nonblocking send then receive", size),
        &size,
        |b, &s| {
            b.to_async(&rt)
                .iter(|| bench_send_receive_subsequent(black_box(s)));
        },
    );
    c.bench_with_input(
        BenchmarkId::new("blocking send in separate thread", size),
        &size,
        |b, &s| {
            b.to_async(&rt)
                .iter(|| bench_send_receive_blocking(black_box(s)));
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
