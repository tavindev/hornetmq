use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use hornetmq::core::{
    backoff::{self, BackoffStrategy},
    retry,
};

fn bench_compute_delay_fixed(c: &mut Criterion) {
    let strategy = BackoffStrategy::Fixed(1000);
    c.bench_function("backoff/fixed", |b| {
        b.iter(|| backoff::compute_delay(black_box(&strategy), black_box(5)))
    });
}

fn bench_compute_delay_exponential(c: &mut Criterion) {
    let strategy = BackoffStrategy::Exponential {
        base: 1000,
        max: 30_000,
    };

    let mut group = c.benchmark_group("backoff/exponential");
    for attempt in [1, 5, 10, 20] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("attempt_{attempt}")),
            &attempt,
            |b, &attempt| {
                b.iter(|| backoff::compute_delay(black_box(&strategy), black_box(attempt)))
            },
        );
    }
    group.finish();
}

fn bench_should_retry(c: &mut Criterion) {
    let mut group = c.benchmark_group("retry/should_retry");
    for (attempts_made, max) in [(0, 3), (2, 3), (3, 3), (10, 100)] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{attempts_made}_of_{max}")),
            &(attempts_made, max),
            |b, &(made, max)| b.iter(|| retry::should_retry(black_box(made), black_box(max))),
        );
    }
    group.finish();
}

fn bench_next_retry_delay(c: &mut Criterion) {
    let mut group = c.benchmark_group("retry/next_delay");

    let none_strategy: Option<BackoffStrategy> = None;
    group.bench_function("no_strategy", |b| {
        b.iter(|| retry::next_retry_delay(black_box(none_strategy.as_ref()), black_box(3)))
    });

    let fixed = Some(BackoffStrategy::Fixed(2000));
    group.bench_function("fixed", |b| {
        b.iter(|| retry::next_retry_delay(black_box(fixed.as_ref()), black_box(3)))
    });

    let exponential = Some(BackoffStrategy::Exponential {
        base: 1000,
        max: 30_000,
    });
    group.bench_function("exponential", |b| {
        b.iter(|| retry::next_retry_delay(black_box(exponential.as_ref()), black_box(3)))
    });

    group.finish();
}

fn bench_backoff_strategy_serde(c: &mut Criterion) {
    let mut group = c.benchmark_group("backoff/serde");

    let fixed = BackoffStrategy::Fixed(1000);
    group.bench_function("serialize_fixed", |b| {
        b.iter(|| serde_json::to_string(black_box(&fixed)).unwrap())
    });

    let exp = BackoffStrategy::Exponential {
        base: 1000,
        max: 30_000,
    };
    group.bench_function("serialize_exponential", |b| {
        b.iter(|| serde_json::to_string(black_box(&exp)).unwrap())
    });

    let fixed_json = r#"{"Fixed":1000}"#;
    group.bench_function("deserialize_fixed", |b| {
        b.iter(|| serde_json::from_str::<BackoffStrategy>(black_box(fixed_json)).unwrap())
    });

    let exp_json = r#"{"Exponential":{"base":1000,"max":30000}}"#;
    group.bench_function("deserialize_exponential", |b| {
        b.iter(|| serde_json::from_str::<BackoffStrategy>(black_box(exp_json)).unwrap())
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_compute_delay_fixed,
    bench_compute_delay_exponential,
    bench_should_retry,
    bench_next_retry_delay,
    bench_backoff_strategy_serde,
);
criterion_main!(benches);
