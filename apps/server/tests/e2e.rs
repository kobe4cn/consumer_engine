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

/// Maximum wall-clock budget for polling a job to completion.
const JOB_POLL_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

/// Build an engine on a temp DuckLake and serve it on an ephemeral port.
///
/// The `Engine` and tempdir are intentionally leaked (`mem::forget`) so they
/// outlive the test's requests — a test's HTTP traffic is bounded and the
/// process exits when the test binary does.
async fn spawn() -> String {
    spawn_guardrails(consumer_engine_core::GuardrailConfig::default()).await
}

/// Onboard a source table; asserts the ingest succeeds.
async fn onboard(
    client: &reqwest::Client,
    base: &str,
    system: &str,
    entity: &str,
    columns: &[&str],
    rows: Vec<Vec<&str>>,
) {
    let resp = client
        .post(format!("{base}/sources/onboard"))
        .json(&serde_json::json!({
            "system": system, "entity": entity,
            "columns": columns, "rows": rows
        }))
        .send()
        .await
        .expect("onboard");
    assert!(
        resp.status().is_success(),
        "onboard failed: {}",
        resp.status()
    );
}

/// Poll `GET /jobs/:id` until `done == true`, returning the final body.
async fn poll_until_done(client: &reqwest::Client, base: &str, job_id: &str) -> Value {
    let deadline = std::time::Instant::now() + JOB_POLL_BUDGET;
    loop {
        let resp = client
            .get(format!("{base}/jobs/{job_id}"))
            .send()
            .await
            .expect("poll");
        let v: Value = resp.json().await.expect("job json");
        if v["done"] == serde_json::Value::Bool(true) {
            return v;
        }
        if std::time::Instant::now() > deadline {
            panic!("job {job_id} did not finish within {JOB_POLL_BUDGET:?}: {v}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// Decode a snapshot Parquet body: returns (row_count, hit_reason_nulls,
/// features_nulls), asserting every `hit_reason`/`features` value is non-empty.
/// Read straight from in-memory `bytes::Bytes` — the parquet crate implements
/// `ChunkReader` for `Bytes` — so no temp file and no blocking `std::fs` (the
/// project's `clippy.toml` bans blocking fs even in tests).
fn decode_parquet_snapshot(bytes: bytes::Bytes) -> (usize, usize, usize) {
    use arrow::array::Array;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).expect("open parquet reader");
    let reader = builder.build().expect("build record-batch reader");

    let mut rows = 0usize;
    let mut hr_nulls = 0usize;
    let mut feat_nulls = 0usize;
    for batch in reader {
        let batch = batch.expect("read batch");
        rows += batch.num_rows();
        for (i, field) in batch.schema_ref().fields().iter().enumerate() {
            let col = batch.column(i);
            match field.name().as_str() {
                "hit_reason" => {
                    hr_nulls += col.null_count();
                    assert_nonempty(col);
                }
                "features" => {
                    feat_nulls += col.null_count();
                    assert_nonempty(col);
                }
                _ => {}
            }
        }
    }
    (rows, hr_nulls, feat_nulls)
}

/// Assert a column's values are non-empty (no nulls and no empty strings).
/// JSON columns may arrive as `Utf8`/`LargeUtf8` or `Binary`/`LargeBinary`;
/// handle both; for any other type, only the null check applies.
fn assert_nonempty(col: &dyn arrow::array::Array) {
    use arrow::array::AsArray;
    assert_eq!(col.null_count(), 0, "column must have zero nulls");
    if let Some(arr) = col.as_string_opt::<i32>() {
        for v in arr.iter() {
            let s = v.expect("string value must be non-null");
            assert!(!s.is_empty(), "string value must be non-empty");
        }
    } else if let Some(arr) = col.as_binary_opt::<i32>() {
        for v in arr.iter() {
            let b = v.expect("binary value must be non-null");
            assert!(!b.is_empty(), "binary value must be non-empty");
        }
    }
    // LargeUtf8/LargeBinary and other layouts: null-count check above suffices.
}

/// Post a `sku = eq A` filter segment for materialisation, returning the job id.
async fn post_filter_job(client: &reqwest::Client, base: &str, campaign_id: &str) -> String {
    let resp = client
        .post(format!("{base}/jobs"))
        .json(&serde_json::json!({
            "dsl": {
                "source": {"system":"erp","entity":"orders"},
                "key": "user_id",
                "ops": [
                    {"kind":"filter","predicate":{"column":"sku","op":"eq","value":"A"}}
                ]
            },
            "materialize": { "campaignId": campaign_id }
        }))
        .send()
        .await
        .expect("post jobs");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::ACCEPTED,
        "POST /jobs must return 202: {}",
        resp.status()
    );
    let v: Value = resp.json().await.expect("jobs json");
    v["jobId"].as_str().expect("jobId present").to_string()
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

// --- M2: materialisation + delivery pull (Phase 3) ---

#[tokio::test]
async fn test_should_post_jobs_returns_202_with_jobid() {
    let base = spawn().await;
    let client = reqwest::Client::new();
    onboard(
        &client,
        &base,
        "erp",
        "orders",
        &["user_id", "sku"],
        vec![vec!["u1", "A"]],
    )
    .await;

    let resp = client
        .post(format!("{base}/jobs"))
        .json(&serde_json::json!({
            "dsl": {
                "source": {"system":"erp","entity":"orders"},
                "key": "user_id",
                "ops": [
                    {"kind":"filter","predicate":{"column":"sku","op":"eq","value":"A"}}
                ]
            },
            "materialize": { "campaignId": "c1" }
        }))
        .send()
        .await
        .expect("post jobs");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::ACCEPTED,
        "POST /jobs must return 202"
    );
    let v: Value = resp.json().await.expect("jobs json");
    let job_id = v["jobId"].as_str().expect("jobId present");
    assert!(
        job_id.starts_with("j_"),
        "jobId must be j_-prefixed: {job_id}"
    );
}

#[tokio::test]
async fn test_should_poll_job_until_done_or_failed() {
    let base = spawn().await;
    let client = reqwest::Client::new();
    onboard(
        &client,
        &base,
        "erp",
        "orders",
        &["user_id", "sku"],
        vec![vec!["u1", "A"], vec!["u2", "A"]],
    )
    .await;

    let job_id = post_filter_job(&client, &base, "c1").await;
    let status = poll_until_done(&client, &base, &job_id).await;
    let snap = status["snapshotId"]
        .as_str()
        .expect("snapshotId present on done")
        .to_string();
    assert!(
        snap.starts_with("snap_"),
        "snapshotId must be snap_-prefixed: {snap}"
    );
    assert!(
        status["error"].is_null(),
        "successful job must carry no error: {status}"
    );
}

#[tokio::test]
async fn test_should_reject_jobs_with_bad_campaign_id() {
    let base = spawn().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/jobs"))
        .json(&serde_json::json!({
            "dsl": {
                "source": {"system":"erp","entity":"orders"},
                "key": "user_id",
                "ops": [
                    {"kind":"filter","predicate":{"column":"sku","op":"eq","value":"A"}}
                ]
            },
            "materialize": { "campaignId": "bad id!" }
        }))
        .send()
        .await
        .expect("post jobs");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "bad campaignId must be rejected: {}",
        resp.status()
    );
}

#[tokio::test]
async fn test_should_materialise_snapshot_atomically_with_hit_reason() {
    let base = spawn().await;
    let client = reqwest::Client::new();
    // Three distinct users match sku=A; a fourth (sku=B) is noise.
    onboard(
        &client,
        &base,
        "erp",
        "orders",
        &["user_id", "sku"],
        vec![
            vec!["u1", "A"],
            vec!["u2", "A"],
            vec!["u3", "A"],
            vec!["u4", "B"],
        ],
    )
    .await;

    // Atomicity: a snapshot that was never materialised is not observable.
    let unknown = format!("snap_{}", uuid::Uuid::now_v7());
    let resp = client
        .get(format!("{base}/audience/{unknown}"))
        .send()
        .await
        .expect("get unknown audience");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "unmaterialised snapshot must be 404"
    );

    let job_id = post_filter_job(&client, &base, "c1").await;
    let status = poll_until_done(&client, &base, &job_id).await;
    let snap = status["snapshotId"]
        .as_str()
        .expect("snapshotId on done")
        .to_string();

    // Once Done, the snapshot is observable with the FULL row set (atomicity
    // I4: never a partial count).
    let resp = client
        .get(format!("{base}/audience/{snap}"))
        .send()
        .await
        .expect("get audience");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let meta: Value = resp.json().await.expect("audience meta");
    assert_eq!(
        meta["rowCount"], 3,
        "rowCount must equal distinct sku=A users"
    );
    assert!(
        meta["downloadUrl"].as_str().is_some_and(|s| !s.is_empty()),
        "downloadUrl must be present"
    );

    // Fetch the presigned export and decode the Parquet.
    let dl = meta["downloadUrl"].as_str().expect("downloadUrl");
    let resp = client
        .get(format!("{base}{dl}"))
        .send()
        .await
        .expect("export");
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "export must be 200");
    assert_eq!(
        resp.headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/vnd.apache.parquet"),
        "content-type must be parquet"
    );
    let bytes = resp.bytes().await.expect("export bytes");
    let (rows, hr_nulls, feat_nulls) = decode_parquet_snapshot(bytes);
    assert_eq!(rows, 3, "parquet row count must equal materialised rows");
    assert_eq!(hr_nulls, 0, "hit_reason must have zero nulls");
    assert_eq!(feat_nulls, 0, "features must have zero nulls");
}

#[tokio::test]
async fn test_should_stream_parquet_export() {
    let base = spawn().await;
    let client = reqwest::Client::new();
    onboard(
        &client,
        &base,
        "erp",
        "orders",
        &["user_id", "sku"],
        vec![vec!["u1", "A"], vec!["u2", "A"]],
    )
    .await;

    let job_id = post_filter_job(&client, &base, "c1").await;
    let status = poll_until_done(&client, &base, &job_id).await;
    let snap = status["snapshotId"]
        .as_str()
        .expect("snapshotId on done")
        .to_string();

    // Resolve the presigned download URL (relative) from the audience metadata.
    let resp = client
        .get(format!("{base}/audience/{snap}"))
        .send()
        .await
        .expect("get audience");
    let meta: Value = resp.json().await.expect("meta");
    let dl = meta["downloadUrl"].as_str().expect("downloadUrl");

    // Valid token + format=parquet → 200, non-empty, Parquet magic.
    let resp = client
        .get(format!("{base}{dl}"))
        .send()
        .await
        .expect("export");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let bytes = resp.bytes().await.expect("bytes");
    assert!(!bytes.is_empty(), "export body must be non-empty");
    assert!(
        bytes.starts_with(b"PAR1"),
        "parquet must start with PAR1 magic"
    );

    // Tampered token → 401. Corrupt ONLY the token (the snapshot id is hex and
    // also contains 'a'), so split at the token boundary rather than mutating
    // the whole URL (which would corrupt the snapshot id).
    let token_idx = dl.find("token=").expect("token in url") + "token=".len();
    let (prefix, token) = dl.split_at(token_idx);
    let tampered_token = token.replace('a', "b");
    let tampered_dl = format!("{prefix}{tampered_token}");
    let resp = client
        .get(format!("{base}{tampered_dl}"))
        .send()
        .await
        .expect("tampered export");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "tampered token must be 401"
    );

    // Unsupported format → 415 (format checked before the token).
    let csv_url = format!("{base}/audience/{snap}/export?format=csv&token=x.{snap}");
    let resp = client.get(&csv_url).send().await.expect("csv export");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "csv format must be 415"
    );
}
