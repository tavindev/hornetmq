#[test]
fn worker_macro_tests() {
    let t = trybuild::TestCases::new();
    t.pass("tests/macro_cases/basic_worker.rs");
    t.pass("tests/macro_cases/all_options.rs");
    t.pass("tests/macro_cases/defaults_only.rs");
    t.compile_fail("tests/macro_cases/missing_queue.rs");
    t.compile_fail("tests/macro_cases/invalid_backoff.rs");
}
