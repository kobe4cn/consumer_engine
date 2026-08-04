//! P1-1 read-path spike (issue #12 / GC-P0) — measure the read-path refresh
//! options and the EXPLAIN double-execution cost before locking the T2
//! read-pool design (specs/92 Phase 1).
//!
//! Phases:
//!   A. Freshness: is a long-lived read-only DuckLake attach pinned at attach
//!      time (P1-1), i.e. does a re-attach become mandatory for visibility?
//!   B. Dirty-check: does `ducklake_snapshots` change after a commit, cheaply
//!      enough to serve as a "needs refresh" signal for a lazy re-attach?
//!   C. Attach-only cost: `SELECT 1` through the per-query-refresh `Reader`
//!      vs a raw `DETACH/ATTACH` on a raw connection.
//!   D. Per-capability decomposition (B/F/J/P): full `engine.run()` path vs
//!      execute-only (long-lived attach, no refresh) vs EXPLAIN-only.
//!   E. Pool floor: the long-lived-attach execute cost is what a cadence-
//!      refreshed pool pays per query; the residual is what the pool removes.
//!
//! Run (release, corpus scale via `CE_SCALE_ROWS`, samples via `CE_SAMPLES`):
//! ```sh
//! cargo run --release -p consumer_engine-query --example read_path_spike
//! ```

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use consumer_engine_core::{
    CatalogRow, FeatureRow, FreshnessRegistry, GuardrailConfig, READ_ONLY_CATALOG_ALIAS,
    SemanticType, SuppressionRules,
};
use consumer_engine_execution::{Reader, ReaderLimits};
use consumer_engine_ingestion::{IngestionHandle, ProducerRegistry};
use consumer_engine_query::{QueryEngine, compile_with_alias, parse};
use consumer_engine_storage::{self as storage, Writer};
use duckdb::Connection;

fn main() {
    let scale: usize = env_usize("CE_SCALE_ROWS", 50_000);
    let samples_full: usize = env_usize("CE_SAMPLES_FULL", 5);
    let samples_fast: usize = env_usize("CE_SAMPLES_FAST", 9);
    let users = (scale / 10).max(1);
    println!("=== read-path spike: {scale} rows, {users} users ===");

    // ---- seed corpus + catalog + feature wide view (same as query_latency) ----
    let tmp = tempfile::tempdir().expect("tmp");
    let writer = Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data"))
        .expect("attach writer");
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
    let attach_sql =
        storage::read_only_attach_sql(&tmp.path().join("cat.db"), &tmp.path().join("data"));
    let attach = attach_sql.clone();

    // The single ingestion writer must stay alive for the freshness phase.
    let ingestion =
        IngestionHandle::start(writer, Arc::new(ProducerRegistry::new())).expect("ingestion");

    // ---- Phase A + B: long-lived read-only connection (attached once) ----
    let long = storage::open_reader(&tmp.path().join("cat.db"), &tmp.path().join("data"))
        .expect("long-lived read attach");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let count_rows = |conn: &Connection| -> i64 {
        conn.query_row("SELECT count(*) FROM dro.raw_erp_orders", [], |r| r.get(0))
            .expect("count")
    };
    let n_before = count_rows(&long);
    println!("\n--- A. freshness pinning (P1-1) ---");
    // Commit new rows through the writer, then read on the SAME attach.
    runtime
        .block_on(async {
            ingestion
                .ingest_raw(
                    "erp",
                    "orders",
                    vec![
                        "user_id".into(),
                        "ts".into(),
                        "amount".into(),
                        "category".into(),
                    ],
                    vec![
                        vec![
                            Some("fresh_a".into()),
                            Some("2025-06-01T00:00:00Z".into()),
                            Some("1".into()),
                            Some("A".into()),
                        ],
                        vec![
                            Some("fresh_b".into()),
                            Some("2025-06-02T00:00:00Z".into()),
                            Some("2".into()),
                            Some("B".into()),
                        ],
                    ],
                )
                .await
        })
        .expect("ingest more");
    let n_pinned = count_rows(&long);
    println!(
        "long-lived attach sees post-commit rows? before={n_before} after-commit={n_pinned} -> \
         {}{}",
        if n_pinned == n_before {
            "NO (pinned at attach time)"
        } else {
            "YES"
        },
        if n_pinned == n_before {
            " — re-attach is REQUIRED for visibility"
        } else {
            ""
        }
    );
    long.execute_batch(&format!("DETACH {READ_ONLY_CATALOG_ALIAS}; {attach}"))
        .expect("refresh attach");
    let n_refreshed = count_rows(&long);
    println!(
        "after DETACH+ATTACH: {n_refreshed} (delta={})",
        n_refreshed - n_before
    );

    // ---- B. dirty-check viability: is there a cheap refresh signal? ----
    println!("\n--- B. dirty-check: cheap refresh signals ---");
    // B1. ducklake_snapshots from INSIDE the pinned attach (reads the attach's
    // own pinned view → trivially unchanged).
    let snap_before = ducklake_snapshots_count(&long);
    let snap_cost = time_ns(10, || {
        let _ = ducklake_snapshots_count(&long);
    });
    // B2. catalog-file mtime (an external, DuckDB-free signal the read pool can
    // stat without re-attaching).
    let catalog_path = tmp.path().join("cat.db");
    let mtime_before = fs_modified(&catalog_path);
    let stat_cost = time_ns(100, || {
        let _ = fs_modified(&catalog_path);
    });
    // Commit another batch, then re-measure both WITHOUT re-attaching.
    runtime
        .block_on(async {
            ingestion
                .ingest_raw(
                    "erp",
                    "orders",
                    vec![
                        "user_id".into(),
                        "ts".into(),
                        "amount".into(),
                        "category".into(),
                    ],
                    vec![vec![
                        Some("fresh_c".into()),
                        Some("2025-06-03T00:00:00Z".into()),
                        Some("3".into()),
                        Some("C".into()),
                    ]],
                )
                .await
        })
        .expect("ingest more");
    let snap_after = ducklake_snapshots_count(&long);
    let mtime_after = fs_modified(&catalog_path);
    println!(
        "snapshots-from-pinned-attach: before={snap_before} after-commit={snap_after} changed={} \
         (cost ~{} us/check) — NOT a viable signal",
        snap_before != snap_after,
        snap_cost.as_micros()
    );
    println!(
        "catalog-file mtime: before={mtime_before:?} after-commit={mtime_after:?} changed={} \
         (cost ~{} us/check) — viable if it advances on commit",
        mtime_before != mtime_after,
        stat_cost.as_micros()
    );

    // ---- C. attach-only cost ----
    println!("\n--- C. attach-only cost ---");
    let read_conn = storage::open_reader(&tmp.path().join("cat.db"), &tmp.path().join("data"))
        .expect("read attach");
    let reader = Reader::start(read_conn, attach_sql, ReaderLimits::default()).expect("reader");
    let attach_via_reader = runtime.block_on(async {
        time_ns_async(samples_fast, || async {
            reader.query("SELECT 1").await.expect("select 1");
        })
        .await
    });
    let attach_raw = time_ns(15, || {
        long.execute_batch(&format!("DETACH {READ_ONLY_CATALOG_ALIAS}; {attach}"))
            .expect("raw detach/attach");
    });
    println!(
        "SELECT 1 via per-query-refresh Reader: p50 {:.2} ms (incl. DETACH+ATTACH)",
        attach_via_reader.as_secs_f64() * 1000.0
    );
    println!(
        "raw DETACH+ATTACH on long-lived conn: p50 {:.2} ms",
        attach_raw.as_secs_f64() * 1000.0
    );

    // ---- D + E. per-capability decomposition ----
    println!("\n--- D/E. per-capability decomposition (ms, p50) ---");
    let engine = QueryEngine::new(
        reader.clone(),
        ingestion.clone(),
        GuardrailConfig::default(),
        Arc::new(FreshnessRegistry::new()),
        SuppressionRules::default(),
    );
    println!(
        "{:>4} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "cap", "full", "exec", "explain", "ex/ef", "residual"
    );
    for (name, dsl) in capability_dsls() {
        let q = parse::parse(dsl.clone()).expect("parse");
        let full = runtime.block_on(async {
            time_ns_async(samples_full, || async {
                let _ = engine.run(dsl.clone()).await.expect("engine.run");
            })
            .await
        });
        let full_ms = full.as_secs_f64() * 1000.0;
        // B/F/J compile to a single parameterised SQL → full exec/explain
        // decomposition on a long-lived attach (J's survivor-CTE gets the
        // measured survivor count as the inner LIMIT, exactly as the engine
        // plans it). P has no single-SQL compile and no EXPLAIN pre-flight in
        // the real path (three profile queries) — reported full-path only.
        let compiled = match name {
            "B" | "F" => Some(compile_with_alias(&q, READ_ONLY_CATALOG_ALIAS).expect("compile")),
            "J" => Some(
                consumer_engine_query::compiler::compile_with_opts(
                    &q,
                    &consumer_engine_query::compiler::CompileOptions::new(
                        READ_ONLY_CATALOG_ALIAS,
                        &SuppressionRules::default(),
                        Some(users as u64),
                    ),
                )
                .expect("compile J"),
            ),
            _ => None,
        };
        match compiled {
            None => println!(
                "{name:>4} {full_ms:>10.1} {:>10} {:>10} {:>10} {:>10}",
                "-", "-", "-", "-"
            ),
            Some(c) => {
                let exec = time_ns(samples_fast, || exec_compiled(&long, &c.sql, &c.params));
                let explain = time_ns(samples_fast, || {
                    exec_compiled(
                        &long,
                        &format!("EXPLAIN (FORMAT JSON) {}", c.sql),
                        &c.params,
                    )
                });
                let exec_ms = exec.as_secs_f64() * 1000.0;
                let explain_ms = explain.as_secs_f64() * 1000.0;
                let ratio = if exec_ms > 0.0 {
                    explain_ms / exec_ms
                } else {
                    0.0
                };
                let residual_ms = (full_ms - exec_ms - explain_ms).max(0.0);
                println!(
                    "{name:>4} {full_ms:>10.1} {exec_ms:>10.1} {explain_ms:>10.1} {ratio:>10.2}x \
                     {residual_ms:>10.1}"
                );
            }
        }
    }

    println!(
        "\nnote: full = engine.run() (catalogue probes + EXPLAIN pre-flight + execute, each via \
         per-query re-attach); exec = execute on a long-lived attach (pool floor); explain = \
         EXPLAIN (FORMAT JSON) on the long-lived attach; residual = attach costs + catalogue \
         probes + engine overhead."
    );
    ingestion.shutdown();
    reader.shutdown();
}

/// The four capability queries, identical to `query_latency`.
fn capability_dsls() -> Vec<(&'static str, serde_json::Value)> {
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
    vec![("B", b), ("F", f), ("J", j), ("P", p)]
}

/// Execute a compiled query (or EXPLAIN wrapper) on a long-lived connection.
fn exec_compiled(conn: &Connection, sql: &str, params: &[duckdb::types::Value]) {
    let mut stmt = conn.prepare(sql).expect("prepare");
    let mut rs = stmt
        .query(duckdb::params_from_iter(params.iter()))
        .expect("query");
    while rs.next().expect("next").is_some() {}
}

/// `SELECT count(*) FROM ducklake_snapshots('<alias>')`.
fn ducklake_snapshots_count(conn: &Connection) -> i64 {
    conn.query_row(
        &format!("SELECT count(*) FROM ducklake_snapshots('{READ_ONLY_CATALOG_ALIAS}')"),
        [],
        |r| r.get(0),
    )
    .expect("ducklake_snapshots count")
}

/// The catalog file's last modification time (a DuckDB-free dirty signal).
///
/// The stat is intentionally synchronous: the read pool's background refresh
/// runs on a `std::thread` (DuckDB `Connection` is not `Sync`), and this spike
/// measures exactly that sync path's cost. `tokio::fs` would add an executor
/// dependency to a µs-scale measurement for no benefit.
#[allow(
    clippy::disallowed_methods,
    reason = "sync stat on a std::thread; tokio::fs adds nothing at µs scale"
)]
fn fs_modified(path: &std::path::Path) -> std::time::SystemTime {
    std::fs::metadata(path)
        .expect("catalog metadata")
        .modified()
        .expect("catalog mtime")
}

/// Median wall-clock of `f` over `n` runs (sync).
fn time_ns(n: usize, mut f: impl FnMut()) -> Duration {
    f(); // warm
    let mut samples = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        f();
        samples.push(t.elapsed());
    }
    median(samples)
}

/// Median wall-clock of `f` over `n` runs (async).
async fn time_ns_async<F, Fut>(n: usize, mut f: F) -> Duration
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    f().await; // warm
    let mut samples = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        f().await;
        samples.push(t.elapsed());
    }
    median(samples)
}

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort_unstable();
    v.get(v.len() / 2).copied().unwrap_or_default()
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
