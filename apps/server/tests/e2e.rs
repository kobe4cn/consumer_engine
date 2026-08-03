//! T1 end-to-end integration tests at the REST seam.
//!
//! User-facing behaviours (onboard, read-only query + freshness, read-only
//! write rejection, boundary validation) go through HTTP. Engine invariants
//! not observable through REST (single-writer refusal, restart durability,
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
    let tmp = tempfile::tempdir().expect("tmp");
    let cfg = EngineConfig {
        catalog_path: tmp.path().join("cat.db"),
        data_path: tmp.path().join("data"),
        compaction_interval_secs: 0, // disable periodic compaction in tests
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
async fn test_should_onboard_then_query_with_freshness() {
    let base = spawn().await;
    let client = reqwest::Client::new();

    let onb_resp = client
        .post(format!("{base}/sources/onboard"))
        .json(&serde_json::json!({
            "system": "erp", "entity": "users",
            "columns": ["id", "name"],
            "rows": [["u1", "alice"], ["u2", "bob"]]
        }))
        .send()
        .await
        .expect("onboard");
    let body = onb_resp.text().await.unwrap_or_default();
    let onb: Value =
        serde_json::from_str(&body).unwrap_or_else(|_| panic!("onboard non-json: {body}"));
    assert_eq!(onb["rowsInserted"], 2);

    let q = client
        .post(format!("{base}/query"))
        .json(&serde_json::json!({ "sql": "SELECT count(*) AS c FROM dro.raw_erp_users" }))
        .send()
        .await
        .expect("query")
        .json::<Value>()
        .await
        .expect("query json");
    assert_eq!(q["columns"][0], "c");
    assert_eq!(q["rows"][0][0], 2);
    assert_eq!(q["freshness"]["worstSource"], "batch");
    assert!(q["freshness"]["lagSeconds"].as_i64().is_some());
}

#[tokio::test]
async fn test_should_reject_write_on_readonly_path() {
    let base = spawn().await;
    let client = reqwest::Client::new();

    client
        .post(format!("{base}/sources/onboard"))
        .json(&serde_json::json!({
            "system": "erp", "entity": "users",
            "columns": ["id"], "rows": [["u1"]]
        }))
        .send()
        .await
        .expect("onboard");

    // An INSERT submitted to the read-only query path must fail.
    let resp = client
        .post(format!("{base}/query"))
        .json(&serde_json::json!({ "sql": "INSERT INTO dro.raw_erp_users (id) VALUES ('evil')" }))
        .send()
        .await
        .expect("query");
    assert!(
        !resp.status().is_success(),
        "read-only path must reject writes"
    );

    // And the table is unchanged.
    let q = client
        .post(format!("{base}/query"))
        .json(&serde_json::json!({ "sql": "SELECT count(*) AS c FROM dro.raw_erp_users" }))
        .send()
        .await
        .expect("query")
        .json::<Value>()
        .await
        .expect("json");
    assert_eq!(q["rows"][0][0], 1, "no row was inserted");
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
