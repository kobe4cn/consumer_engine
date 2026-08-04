//! Query-latency calibration harness for the B/F/J/P capability types
//! (specs/71, issue #10 AC1 — lock guardrail numbers from a bench).
//!
//! Seeds a synthetic `erp.orders` corpus (scale via `CE_SCALE_ROWS`, default
//! 50 000), then runs a warm batch of each query type through the real
//! `QueryEngine` (guardrails ON, catalogue enforced) and reports P50/P99
//! latencies. Run:
//!
//! ```sh
//! cargo run --release -p consumer_engine-query --example query_latency
//! CE_SCALE_ROWS=100000 cargo run --release -p consumer_engine-query --example query_latency
//! ```
//!
//! The ≤50M-user target corpus calibrates on a file-backed DuckLake attach
//! (the in-memory dev attach ingests too slowly at that scale — see
//! `docs/research/perf-calibration.md`); the harness is scale-agnostic via the
//! env var.

use std::{
    env, process,
    sync::Arc,
    time::{Duration, Instant},
};

use consumer_engine_core::{
    CatalogRow, FeatureRow, FreshnessRegistry, GuardrailConfig, SemanticType, SuppressionRules,
};
use consumer_engine_execution::{Reader, ReaderLimits};
use consumer_engine_ingestion::{IngestionHandle, ProducerRegistry};
use consumer_engine_query::QueryEngine;
use consumer_engine_storage::Writer;

fn main() {
    let scale = env_u64("CE_SCALE_ROWS", 50_000) as usize;
    // Distinct-user divisor; guard tiny scales so the bench never divides by
    // zero (and always has at least one distinct user).
    let users = (scale / 10).max(1);
    println!("seeding corpus of {scale} rows ({users} users)...");

    let tmp = tempfile::tempdir().expect("tmp");
    // The bench measures the production read path (issue #20 / P1-1): the
    // writer bumps a shared generation after every committed write, and the
    // pooled readers re-attach only when it advances — no per-query attach.
    let write_gen = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let writer = Writer::attach_with_gen(
        &tmp.path().join("cat.db"),
        &tmp.path().join("data"),
        &consumer_engine_core::CompactionConfig::default(),
        Some(std::sync::Arc::clone(&write_gen)),
    )
    .expect("attach writer");

    // A corpus of orders: `user_id` (pseudonymous), `ts` (ISO-8601), `amount`
    // (numeric as VARCHAR — raw tables are VARCHAR), `category`.
    let mut rows: Vec<Vec<Option<String>>> = Vec::with_capacity(scale);
    for i in 0..scale {
        let user = format!("u{}", i % users);
        let ts = format!("2025-01-{:02}T00:00:00Z", (i % 28) + 1);
        let amount = (i % 1000).to_string();
        let cat = ["A", "B", "C"][i % 3].to_string();
        rows.push(vec![Some(user), Some(ts), Some(amount), Some(cat)]);
    }
    writer
        .ingest_raw(
            "erp",
            "orders",
            &[
                "user_id".into(),
                "ts".into(),
                "amount".into(),
                "category".into(),
            ],
            &rows,
        )
        .expect("ingest");

    // Catalogue the four columns (the query path enforces catalogue presence).
    let catalog: Vec<CatalogRow> = ["user_id", "ts", "amount", "category"]
        .iter()
        .map(|c| CatalogRow {
            entity_type: "column".into(),
            system: "erp".into(),
            table_name: "orders".into(),
            column_name: Some((*c).into()),
            semantic_type: SemanticType::Identifier,
            data_type: "VARCHAR".into(),
            description: format!("column {c}"),
            pii_flag: false,
            sample_values: serde_json::json!([]),
            embedding: vec![0.0; 4],
            source_epoch: 0,
        })
        .collect();
    writer.write_catalog_rows(&catalog).expect("catalog");

    // A feature + wide view for F queries.
    writer.ensure_feature_store_table().expect("feature store");
    let features: Vec<FeatureRow> = (0..users)
        .map(|u| FeatureRow {
            user_id: format!("u{u}"),
            feature_name: "cadence.regularity".into(),
            num_value: 0.5,
            as_of_ts: "2025-01-01T00:00:00Z".into(),
            producer_id: "cadence_sql".into(),
        })
        .collect();
    writer.write_feature_rows(&features).expect("features");
    writer
        .refresh_feature_wide_view("cadence", &["regularity".into()])
        .expect("wide view");

    let ingestion =
        IngestionHandle::start(writer, Arc::new(ProducerRegistry::new())).expect("ingestion");
    let attach_sql = consumer_engine_storage::read_only_attach_sql(
        &tmp.path().join("cat.db"),
        &tmp.path().join("data"),
    );
    // Read pool: one read-only worker per physical core, refreshed on the
    // writer's generation bump (the production wiring, specs/11 §2a).
    let workers = ReaderLimits::default().threads.max(1);
    let conns: Vec<duckdb::Connection> = (0..workers)
        .map(|_| {
            consumer_engine_storage::open_reader(
                &tmp.path().join("cat.db"),
                &tmp.path().join("data"),
            )
            .expect("read attach")
        })
        .collect();
    let reader = Reader::start_pooled(
        conns,
        attach_sql,
        ReaderLimits::default(),
        Some(write_gen),
        std::time::Duration::from_secs(5),
    )
    .expect("reader");
    let engine = QueryEngine::new(
        reader,
        ingestion.clone(),
        GuardrailConfig::default(),
        Arc::new(FreshnessRegistry::new()),
        SuppressionRules::default(),
    );

    // One query per capability, run in a tokio runtime.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let results = runtime.block_on(async {
        let b = serde_json::json!({
            "source": {"system":"erp","entity":"orders"}, "key": "user_id",
            "ops": [{"kind":"filter","predicate":{"column":"category","op":"eq","value":"A"}}]
        });
        let f = serde_json::json!({
            "source": {"system":"erp","entity":"orders"}, "key": "user_id",
            "ops": [{"kind":"feature","name":"cadence.regularity","op":"gt","value":0.4}]
        });
        let j = serde_json::json!({
            "source": {"system":"erp","entity":"orders"}, "key": "user_id",
            "ops": [
                {"kind":"filter","predicate":{"column":"category","op":"eq","value":"A"}},
                {"kind":"derive","name":"revenue_a",
                 "metric":{"kind":"sum","event":{"system":"erp","entity":"orders"},"column":"amount"}}
            ]
        });
        let p = serde_json::json!({
            "source": {"system":"erp","entity":"orders"}, "key": "user_id",
            "ops": [
                {"kind":"filter","predicate":{"column":"category","op":"eq","value":"A"}},
                {"kind":"characterize","event":{"system":"erp","entity":"orders"},
                 "tsColumn":"ts","monetaryColumn":"amount","categoryColumn":"category"}
            ]
        });
        let specs = vec![("B", b), ("F", f), ("J", j), ("P", p)];
        let mut results = Vec::with_capacity(specs.len());
        for (name, dsl) in specs {
            let samples = sample(&engine, &dsl, 100).await;
            results.push((name, samples));
        }
        results
    });

    println!(
        "\n{:>4} {:>10} {:>10} {:>10}",
        "type", "p50(ms)", "p99(ms)", "mean(ms)"
    );
    // The gate thresholds are the LOCKED budgets (specs/71 §3: P50 < 1s,
    // P99 < 5s), overridable via env so the gate itself is testable
    // (issue #25: the perf budget is a CI-enforced exit criterion, not a soft
    // target).
    let max_p50_ms = env_u64("CE_MAX_P50_MS", 1000);
    let max_p99_ms = env_u64("CE_MAX_P99_MS", 5000);
    let mut failed = false;
    for (name, samples) in &results {
        let p50 = percentile(samples, 0.50);
        let p99 = percentile(samples, 0.99);
        let mean = samples.iter().sum::<Duration>().as_secs_f64() / samples.len() as f64 * 1000.0;
        let p50_ms = p50.as_secs_f64() * 1000.0;
        let p99_ms = p99.as_secs_f64() * 1000.0;
        let ok = p50_ms < max_p50_ms as f64 && p99_ms < max_p99_ms as f64;
        failed |= !ok;
        println!(
            "{name:>4} {:>10.2} {:>10.2} {:>10.2}  {}",
            p50_ms,
            p99_ms,
            mean,
            if ok { "PASS" } else { "FAIL" }
        );
    }
    println!(
        "\nbudgets: P50 < {max_p50_ms} ms, P99 < {max_p99_ms} ms (specs/71 §3 locked budgets; \
         overridable via CE_MAX_P50_MS / CE_MAX_P99_MS)"
    );
    println!(
        "calibration corpus: {scale} rows (target ≤50M needs the file-backed DuckLake attach; see \
         docs/research/perf-calibration.md)"
    );
    ingestion.shutdown();
    if failed {
        // The gate: non-zero exit so `make bench-queries` / CI fails red.
        eprintln!("BENCH GATE FAILED — P50/P99 budgets exceeded");
        process::exit(1);
    }
}

/// Read an env var as `u64` with a fallback.
fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Run `dsl` `n` times synchronously, recording each wall-clock latency. A
/// warm-up run precedes the samples so first-query cold costs (DuckLake attach,
/// EXPLAIN, catalogue probes) don't skew P50/P99; every result must return at
/// least one row or the sample is invalid (an empty segment is not a query
/// latency worth calibrating).
async fn sample(engine: &QueryEngine, dsl: &serde_json::Value, n: usize) -> Vec<Duration> {
    // Warm-up: one run, assert a non-empty result.
    let warm = engine
        .run(dsl.clone(), "default")
        .await
        .expect("warm-up query");
    assert!(
        !warm.rows.is_empty(),
        "warm-up query returned no rows — the calibration would be meaningless"
    );
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        let res = engine.run(dsl.clone(), "default").await.expect("query");
        assert!(
            !res.rows.is_empty(),
            "query returned no rows during sampling"
        );
        out.push(t.elapsed());
    }
    out
}

/// The nearest-rank percentile of `samples`.
fn percentile(samples: &[Duration], q: f64) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    let mut sorted: Vec<Duration> = samples.to_vec();
    sorted.sort_unstable();
    let idx = ((q * (sorted.len() as f64 - 1.0)).round() as usize).min(sorted.len() - 1);
    sorted[idx]
}
