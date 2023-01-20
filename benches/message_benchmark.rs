use core::panic;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use hauspost::broker::{MessageBroker, ReceiveStatus};
use tokio::runtime::Runtime;

async fn bench_send_receive_subsequent(count: usize) {
    let connection = MessageBroker::new().run_in_background();
    for _ in 1..count {
        connection
            .send_message_nonblocking(
                "message".to_string(),
                "topic".to_string(),
                Some(Duration::from_secs(10)),
            )
            .await;
    }

    // Since all messages have been sent, peek must return all messages
    for _ in 1..count {
        match connection.peek_message("topic".to_string()).await {
            ReceiveStatus::Received(_) => {}
            _ => panic!("expected to receive"),
        };
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

    // All messages must be received eventually
    for _ in 1..count {
        match connection.receive_message("topic".to_string(), None).await {
            ReceiveStatus::Received(_) => {}
            _ => panic!("expected to receive"),
        };
    }
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let size: usize = 10_000;
    let rt = Runtime::new().unwrap();
    let mut g = c.benchmark_group("throughput");
    g.throughput(criterion::Throughput::Elements(size as u64));
    g.bench_with_input(
        BenchmarkId::new("nonblocking send then receive", size),
        &size,
        |b, &s| {
            b.to_async(&rt)
                .iter(|| bench_send_receive_subsequent(black_box(s)));
        },
    );
    g.bench_with_input(
        BenchmarkId::new("blocking send in separate thread", size),
        &size,
        |b, &s| {
            b.to_async(&rt)
                .iter(|| bench_send_receive_blocking(black_box(s)));
        },
    );
    g.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
