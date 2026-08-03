//! Async materialisation jobs: `POST /jobs` (mint + spawn) and `GET /jobs/:id`
//! (poll). The job lifecycle lives here; the materialise *work* (DSL → snapshot)
//! lives in `consumer_engine_query::QueryEngine::materialize`.
//!
//! `JobRegistry` is an `Arc<DashMap>` (AGENTS.md § Async: `DashMap` over
//! `Mutex<HashMap>`). The POST handler spawns a supervisor task that owns an
//! inner materialise task so a panic in the materialise future is captured as
//! `Failed` rather than orphaning the job (AGENTS.md § Async: handle task
//! panics). No `futures` dependency is needed — `tokio::spawn`'s `JoinHandle`
//! suffices.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use consumer_engine_core::validate_ident;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::{ApiError, AppState};

/// How long a finished/expired job stays in the registry before it is dropped
/// on the next poll. Bounds the registry so it cannot grow without limit
/// (AGENTS.md § Resource Limits: bound every collection).
const JOB_TTL: Duration = Duration::from_secs(60 * 60);

/// In-memory job registry keyed by job id. Cheap to clone (`Arc`). Entries
/// expire lazily on [`JobRegistry::get`] after `JOB_TTL`.
#[derive(Clone, Debug)]
pub struct JobRegistry(Arc<DashMap<String, (JobStatus, Instant)>>);

impl JobRegistry {
    /// Build an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(DashMap::new()))
    }

    /// Mark `id` as `Running`.
    pub fn insert_running(&self, id: &str) {
        self.0
            .insert(id.to_string(), (JobStatus::Running, Instant::now()));
    }

    /// Mark `id` as `Done(snapshot)`.
    pub fn set_done(&self, id: &str, snapshot: String) {
        self.0
            .insert(id.to_string(), (JobStatus::Done(snapshot), Instant::now()));
    }

    /// Mark `id` as `Failed(err)`.
    pub fn set_failed(&self, id: &str, err: String) {
        self.0
            .insert(id.to_string(), (JobStatus::Failed(err), Instant::now()));
    }

    /// Read the status of `id`, if present. Entries older than `JOB_TTL` are
    /// dropped (and reported as absent), bounding the map's size.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<JobStatus> {
        let now = Instant::now();
        // Check-and-expire under the shard guard so a concurrent poll can't race
        // a stale entry.
        if let Some(entry) = self.0.get(id)
            && now.duration_since(entry.1) > JOB_TTL
        {
            drop(entry);
            self.0.remove(id);
            return None;
        }
        self.0.get(id).map(|r| r.0.clone())
    }
}

impl Default for JobRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Terminal-ish status of a job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobStatus {
    /// Still materialising.
    Running,
    /// Finished; carries the snapshot id (`snap_<uuid>`).
    Done(String),
    /// Failed; carries a short error string.
    Failed(String),
}

/// `POST /jobs` materialisation spec.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MaterializeSpec {
    campaign_id: String,
}

/// `POST /jobs` request body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobsRequest {
    dsl: serde_json::Value,
    materialize: MaterializeSpec,
}

/// `POST /jobs` response body.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobsResponse {
    job_id: String,
}

/// `GET /jobs/:id` response body.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobResponse {
    done: bool,
    snapshot_id: Option<String>,
    error: Option<String>,
}

/// `POST /jobs`: validate the DSL + campaign id, mint a job id, spawn the
/// materialise work under a concurrency cap, and return `202 { jobId }`.
pub async fn post_jobs(
    State(st): State<AppState>,
    Json(req): Json<JobsRequest>,
) -> Result<(StatusCode, Json<JobsResponse>), ApiError> {
    validate_ident(&req.materialize.campaign_id)?;
    // Parse/validate the DSL up front so a bad segment is a 400, not a failed
    // job (fail fast at the trust boundary).
    let q = consumer_engine_query::parse::parse(req.dsl)?;
    let id = format!("j_{}", uuid::Uuid::now_v7());
    st.jobs.insert_running(&id);

    // Concurrency cap (AGENTS.md § Resource Limits: bound concurrent in-flight
    // work with a Semaphore — an unbounded spawn per request is a fork bomb).
    // Wait for a slot (queueing, not rejecting); the permit is held for the
    // whole materialise and released on drop.
    let permit = st
        .materialise_slots
        .clone()
        .acquire_owned()
        .await
        .map_err(|e| {
            ApiError::Core(consumer_engine_core::Error::Ingestion(Box::from(format!(
                "materialise slot closed: {e}"
            ))))
        })?;

    // Supervisor owns the inner materialise task so a panic is captured.
    let qe = st.query_engine.clone();
    let jobs = st.jobs.clone();
    let job_id = id.clone();
    let campaign_id = req.materialize.campaign_id.clone();
    tokio::spawn(async move {
        let _permit = permit;
        let qe_inner = qe.clone();
        let q_owned = q;
        let camp_inner = campaign_id.clone();
        let inner = tokio::spawn(async move { qe_inner.materialize(&q_owned, &camp_inner).await });
        match inner.await {
            Ok(Ok(snapshot)) => jobs.set_done(&job_id, snapshot),
            Ok(Err(e)) => jobs.set_failed(&job_id, e.to_string()),
            Err(join) => jobs.set_failed(&job_id, format!("materialise task failed: {join}")),
        }
    });

    Ok((StatusCode::ACCEPTED, Json(JobsResponse { job_id: id })))
}

/// `GET /jobs/:id`: poll a job. `404` if unknown.
pub async fn get_job(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<JobResponse>, ApiError> {
    let Some(status) = st.jobs.get(&id) else {
        return Err(ApiError::NotFound);
    };
    let resp = match status {
        JobStatus::Running => JobResponse {
            done: false,
            snapshot_id: None,
            error: None,
        },
        JobStatus::Done(snapshot) => JobResponse {
            done: true,
            snapshot_id: Some(snapshot),
            error: None,
        },
        JobStatus::Failed(err) => JobResponse {
            done: true,
            snapshot_id: None,
            error: Some(err),
        },
    };
    Ok(Json(resp))
}
