use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use hornetmq::{
    core::job::Job,
    queue::{AddJobOptions, Queue},
    worker::Worker,
};
use redis::Commands;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
struct BenchData {
    value: u64,
}

fn noop_processor(_job: &Job<BenchData>) -> anyhow::Result<String> {
    Ok("ok".into())
}

fn unique_queue() -> String {
    format!("bench-w-{}", Uuid::new_v4())
}

fn cleanup(queue_name: &str) {
    let client = redis::Client::open("redis://localhost:6379").unwrap();
    let mut conn = client.get_connection().unwrap();
    let prefix = format!("bull:{queue_name}:");
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{prefix}*"))
        .query(&mut conn)
        .unwrap_or_default();
    for key in keys {
        let _: () = conn.del(&key).unwrap_or(());
    }
}

fn enqueue_n_jobs(queue_name: &str, n: u64) {
    let mut queue = Queue::new(queue_name.into(), "redis://localhost:6379".into());
    for i in 0..n {
        queue
            .add("bench", BenchData { value: i }, AddJobOptions::default())
            .unwrap();
    }
}

fn bench_worker_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("worker/throughput");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    for (job_count, concurrency) in [(50, 1), (50, 4), (100, 4), (100, 8)] {
        let id = format!("{job_count}_jobs_c{concurrency}");

        group.bench_function(BenchmarkId::from_parameter(&id), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;

                for _ in 0..iters {
                    let queue_name = unique_queue();
                    enqueue_n_jobs(&queue_name, job_count);

                    let start = Instant::now();

                    rt.block_on(async {
                        let mut worker = Worker::new(
                            queue_name.clone(),
                            "redis://localhost:6379".into(),
                            concurrency,
                            noop_processor,
                        );

                        let shutdown = worker.shutdown_flag();
                        let qn = queue_name.clone();

                        tokio::spawn(async move {
                            loop {
                                let client = redis::Client::open("redis://localhost:6379").unwrap();
                                let mut conn = client.get_connection().unwrap();
                                let key = format!("bull:{qn}:completed");
                                let count: u64 = conn.zcard(&key).unwrap_or(0);
                                if count >= job_count {
                                    shutdown.store(true, Ordering::SeqCst);
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                        });

                        worker.run().await.unwrap();
                    });

                    total += start.elapsed();
                    cleanup(&queue_name);
                }

                total
            });
        });
    }

    group.finish();
}

fn bench_worker_startup_shutdown(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("worker/startup_shutdown", |b| {
        b.iter(|| {
            let queue_name = unique_queue();

            rt.block_on(async {
                let mut worker = Worker::new(
                    queue_name.clone(),
                    "redis://localhost:6379".into(),
                    1,
                    noop_processor,
                );

                let shutdown = worker.shutdown_flag();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    shutdown.store(true, Ordering::SeqCst);
                });

                worker.run().await.unwrap();
            });

            cleanup(&queue_name);
        })
    });
}

criterion_group!(
    benches,
    bench_worker_throughput,
    bench_worker_startup_shutdown
);
criterion_main!(benches);
