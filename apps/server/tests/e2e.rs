//! T1/T2 end-to-end integration tests at the REST seam.
//!
//! User-facing behaviours (onboard, DSL query + freshness, escape-hatch
//! rejection, boundary validation) go through HTTP. Engine invariants not
//! observable through REST (single-writer refusal, restart durability,
//! compaction) are covered by `consumer_engine-storage` unit tests.

#![forbid(unsafe_code)]

use consumer_engine_core::EngineConfig;
use consumer_engine_server::Engine;
use serde_json::Value;

/// Build an engine on a temp DuckLake and serve it on an ephemeral port.
///
/// The `Engine` and tempdir are intentionally leaked (`mem::forget`) so they
/// outlive the test's requests — a test's HTTP traffic is bounded and the
/// process exits when the test binary does.
async fn spawn() -> String {
    spawn_guardrails(consumer_engine_core::GuardrailConfig::default()).await
}

/// Like [`spawn`] but with custom guardrail budgets.
async fn spawn_guardrails(guardrails: consumer_engine_core::GuardrailConfig) -> String {
    let tmp = tempfile::tempdir().expect("tmp");
    let cfg = EngineConfig {
        catalog_path: tmp.path().join("cat.db"),
        data_path: tmp.path().join("data"),
        compaction_interval_secs: 0, // disable periodic compaction in tests
        guardrails,
        ..EngineConfig::default()
    };
    let (router, engine) = Engine::build(&cfg).expect("build engine");
    std::mem::forget(engine);
    std::mem::forget(tmp);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn test_should_run_dsl_filter_query_over_rest() {
    let base = spawn().await;
    let client = reqwest::Client::new();

    let onb = client
        .post(format!("{base}/sources/onboard"))
        .json(&serde_json::json!({
            "system": "erp", "entity": "users",
            "columns": ["id", "name"],
            "rows": [["u1", "alice"], ["u2", "bob"]]
        }))
        .send()
        .await
        .expect("onboard");
    assert!(onb.status().is_success(), "onboard failed");

    // DSL: filter id = 'u1' (value bound, not interpolated).
    let q = client
        .post(format!("{base}/query"))
        .json(&serde_json::json!({
            "dsl": {
                "source": {"system":"erp","entity":"users"},
                "key": "id",
                "ops": [
                    {"kind":"filter","predicate":{"column":"id","op":"eq","value":"u1"}}
                ]
            }
        }))
        .send()
        .await
        .expect("query");
    assert!(q.status().is_success(), "dsl query failed: {}", q.status());
    let q = q.json::<Value>().await.expect("query json");
    assert_eq!(q["columns"][0], "id");
    assert_eq!(q["rows"].as_array().map(Vec::len), Some(1));
    assert_eq!(q["rows"][0][0], "u1");
    assert_eq!(q["count"], 1);
    assert_eq!(q["freshness"]["worstSource"], "batch");
    assert!(q["queryId"].as_str().is_some_and(|s| s.starts_with("q_")));
}

#[tokio::test]
async fn test_should_reject_raw_sql_escape_hatch() {
    let base = spawn().await;
    let client = reqwest::Client::new();
    // M1: the raw-SQL escape hatch is closed regardless of token.
    let resp = client
        .post(format!("{base}/query"))
        .json(&serde_json::json!({ "sql": "SELECT 1", "approvalToken": "t" }))
        .send()
        .await
        .expect("query");
    assert!(
        !resp.status().is_success(),
        "raw-SQL escape hatch must be rejected in M1"
    );
}

#[tokio::test]
async fn test_should_reject_invalid_dsl() {
    let base = spawn().await;
    let client = reqwest::Client::new();
    // Bad source.system identifier.
    let resp = client
        .post(format!("{base}/query"))
        .json(&serde_json::json!({
            "dsl": {
                "source": {"system":"erp; DROP","entity":"users"},
                "key": "id", "ops": []
            }
        }))
        .send()
        .await
        .expect("query");
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_should_reject_over_budget_query_pre_execution() {
    // Tiny sync_row_cap: a query whose EXPLAIN estimate exceeds it is rejected
    // BEFORE it executes (AC#3 pre-flight).
    let base = spawn_guardrails(consumer_engine_core::GuardrailConfig {
        sync_row_cap: 5,
        ..consumer_engine_core::GuardrailConfig::default()
    })
    .await;
    let client = reqwest::Client::new();

    // 200 rows (~50 distinct users) so EXPLAIN estimates well above 5.
    let rows: Vec<serde_json::Value> = (0..200)
        .map(|i| serde_json::json!([format!("u{}", i % 50)]))
        .collect();
    let onb = client
        .post(format!("{base}/sources/onboard"))
        .json(&serde_json::json!({
            "system": "erp", "entity": "users",
            "columns": ["user_id"], "rows": rows
        }))
        .send()
        .await
        .expect("onboard");
    assert!(onb.status().is_success(), "onboard failed");

    // DSL: distinct user_id (no filter) — EXPLAIN estimates tens/hundreds.
    let resp = client
        .post(format!("{base}/query"))
        .json(&serde_json::json!({
            "dsl": {
                "source": {"system":"erp","entity":"users"},
                "key": "user_id", "ops": []
            }
        }))
        .send()
        .await
        .expect("query");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::PAYLOAD_TOO_LARGE,
        "over-budget query must be rejected pre-execution (AC#3): {}",
        resp.status()
    );
}

#[tokio::test]
async fn test_should_reject_invalid_onboard_input() {
    let base = spawn().await;
    let client = reqwest::Client::new();

    // Bad system identifier (attempted injection).
    let r = client
        .post(format!("{base}/sources/onboard"))
        .json(&serde_json::json!({
            "system": "erp; DROP", "entity": "users",
            "columns": ["id"], "rows": [["u1"]]
        }))
        .send()
        .await
        .expect("req");
    assert_eq!(r.status(), reqwest::StatusCode::BAD_REQUEST);

    // Row width mismatch.
    let r = client
        .post(format!("{base}/sources/onboard"))
        .json(&serde_json::json!({
            "system": "erp", "entity": "orders",
            "columns": ["a", "b"], "rows": [["x"]]
        }))
        .send()
        .await
        .expect("req");
    assert_eq!(r.status(), reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_should_reject_too_many_columns() {
    let base = spawn().await;
    let client = reqwest::Client::new();
    // 1025 columns > MAX_COLUMNS (1024) — bounds the CREATE TABLE width.
    let cols: Vec<String> = (0..1025).map(|i| format!("c{i}")).collect();
    let r = client
        .post(format!("{base}/sources/onboard"))
        .json(&serde_json::json!({
            "system": "erp", "entity": "wide",
            "columns": cols, "rows": []
        }))
        .send()
        .await
        .expect("req");
    assert_eq!(r.status(), reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_should_reject_oversized_sql() {
    let base = spawn().await;
    let client = reqwest::Client::new();
    // 8193 bytes > MAX_SQL_BYTES (8192); the byte cap fires before the reader.
    let oversize = format!("SELECT {}", "x".repeat(8_193));
    let r = client
        .post(format!("{base}/query"))
        .json(&serde_json::json!({ "sql": oversize }))
        .send()
        .await
        .expect("req");
    assert_eq!(r.status(), reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_should_reject_oversized_cell() {
    let base = spawn().await;
    let client = reqwest::Client::new();
    // 4097 bytes > MAX_CELL_BYTES (4096).
    let big = "y".repeat(4_097);
    let r = client
        .post(format!("{base}/sources/onboard"))
        .json(&serde_json::json!({
            "system": "erp", "entity": "users",
            "columns": ["id"], "rows": [[big]]
        }))
        .send()
        .await
        .expect("req");
    assert_eq!(r.status(), reqwest::StatusCode::BAD_REQUEST);
}
