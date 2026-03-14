use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use hornetmq::queue::{AddJobOptions, Queue};
use redis::Commands;
use serde::Serialize;
use uuid::Uuid;

#[derive(Serialize)]
struct BenchPayload {
    message: String,
    count: u64,
}

fn unique_queue() -> String {
    format!("bench-{}", Uuid::new_v4())
}

fn cleanup(queue_name: &str) {
    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut conn = client.get_connection().unwrap();
    let prefix = format!("bull:{}:", queue_name);
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{}*", prefix))
        .query(&mut conn)
        .unwrap_or_default();
    for key in keys {
        let _: () = conn.del(&key).unwrap_or(());
    }
}

fn bench_enqueue_single(c: &mut Criterion) {
    let queue_name = unique_queue();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379".into());

    c.bench_function("queue/enqueue_single", |b| {
        b.iter(|| {
            queue
                .add(
                    "bench-job",
                    BenchPayload {
                        message: "hello".into(),
                        count: 1,
                    },
                    AddJobOptions::default(),
                )
                .unwrap();
        })
    });

    cleanup(&queue_name);
}

fn bench_enqueue_with_options(c: &mut Criterion) {
    use hornetmq::core::backoff::BackoffStrategy;

    let queue_name = unique_queue();
    let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379".into());

    c.bench_function("queue/enqueue_with_options", |b| {
        b.iter(|| {
            queue
                .add(
                    "bench-job",
                    BenchPayload {
                        message: "hello".into(),
                        count: 1,
                    },
                    AddJobOptions {
                        attempts: Some(5),
                        priority: Some(3),
                        delay: Some(0),
                        backoff: Some(BackoffStrategy::Exponential {
                            base: 1000,
                            max: 30_000,
                        }),
                    },
                )
                .unwrap();
        })
    });

    cleanup(&queue_name);
}

fn bench_enqueue_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue/enqueue_batch");

    for batch_size in [10, 50, 100] {
        let queue_name = unique_queue();
        let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379".into());

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                b.iter(|| {
                    for i in 0..size {
                        queue
                            .add(
                                "batch-job",
                                BenchPayload {
                                    message: format!("item-{i}"),
                                    count: i,
                                },
                                AddJobOptions::default(),
                            )
                            .unwrap();
                    }
                });
            },
        );

        cleanup(&queue_name);
    }

    group.finish();
}

fn bench_enqueue_payload_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue/payload_size");

    for size in [100, 1_000, 10_000] {
        let queue_name = unique_queue();
        let mut queue = Queue::new(queue_name.clone(), "redis://localhost:6379".into());
        let payload = BenchPayload {
            message: "x".repeat(size),
            count: 0,
        };

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{size}_bytes")),
            &payload,
            |b, payload| {
                b.iter(|| {
                    queue
                        .add("payload-job", payload, AddJobOptions::default())
                        .unwrap();
                });
            },
        );

        cleanup(&queue_name);
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_enqueue_single,
    bench_enqueue_with_options,
    bench_enqueue_batch,
    bench_enqueue_payload_sizes,
);
criterion_main!(benches);
