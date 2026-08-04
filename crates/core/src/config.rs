//! Engine configuration.
//!
//! Per `AGENTS.md`, configuration is YAML-loaded via the `config` crate and
//! data that may be tuned at runtime lives here (compile-time constants are
//! reserved for truly fixed values).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::Result;

/// Top-level engine configuration. Loaded from YAML at startup.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EngineConfig {
    /// Filesystem path to the `DuckLake` catalogue database (a `DuckDB` file for
    /// single-process operation; `Postgres` DSN when multi-writer is needed).
    pub catalog_path: PathBuf,
    /// Filesystem path (or object-storage prefix) where `DuckLake` writes `Parquet`.
    pub data_path: PathBuf,
    /// How often the compaction task runs, in seconds.
    #[serde(default = "default_compaction_interval")]
    pub compaction_interval_secs: u64,
    /// Micro-batch flush threshold: flush once this many rows are queued
    /// per `(system, entity)` (specs/71 §4). `0` disables buffering — every
    /// ingest flushes immediately.
    #[serde(default = "default_micro_batch_rows")]
    pub micro_batch_flush_rows: u64,
    /// Micro-batch flush age: flush a queued batch once it has been buffered
    /// for this many seconds, even if the row threshold is not reached
    /// (specs/71 §4). `0` disables age-based flush.
    #[serde(default = "default_micro_batch_flush_age")]
    pub micro_batch_flush_age_secs: u64,
    /// Bind address for the REST ingress.
    #[serde(default = "default_bind")]
    pub bind: String,
    /// Query guardrail budgets (see `specs/71-performance-budgets.md`).
    #[serde(default)]
    pub guardrails: GuardrailConfig,
    /// Suppression rules consumed by the `Exclude` capability (specs/20 §5).
    #[serde(default)]
    pub suppression: SuppressionRules,
    /// DuckLake compaction tuning (specs/71 §4, spike-microbatch-compaction).
    #[serde(default)]
    pub compaction: CompactionConfig,
    /// Bearer auth token protecting every route except /healthz and /readyz
    /// (specs/21 I1). `None` leaves the engine unauthenticated — development
    /// convenience only; production deployments MUST set it (a tokenless engine
    /// lets any caller mint presigned exports, IDOR).
    #[serde(default)]
    pub auth_token: Option<String>,
    /// Token that authorises the raw-SQL escape hatch (specs/21 §4 E2):
    /// `POST /query { sql, approvalToken }` runs only with a matching token,
    /// under the same guardrails, always audit-logged. `None` disables the
    /// hatch entirely.
    #[serde(default)]
    pub sql_approval_token: Option<String>,
    /// HTTP LLM/embedding service config (spec 13 §4). `None` uses the
    /// deterministic stubs (M3 default, no network).
    #[serde(default)]
    pub llm: Option<LlmConfig>,
}

const fn default_compaction_interval() -> u64 {
    3600
}

const fn default_micro_batch_rows() -> u64 {
    50_000
}

const fn default_micro_batch_flush_age() -> u64 {
    30
}

fn default_bind() -> String {
    "127.0.0.1:8080".to_string()
}

/// Query guardrail budgets. Defaults from `specs/71-performance-budgets.md`;
/// calibrate against a real corpus (the `max_bytes_scanned` cap especially).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GuardrailConfig {
    /// Per-query `DuckDB` memory limit, e.g. `"8GB"`.
    #[serde(default = "default_memory_limit")]
    pub memory_limit: String,
    /// `DuckDB` thread count (default: physical cores).
    #[serde(default = "default_threads")]
    pub threads: usize,
    /// Hard per-statement timeout in seconds.
    #[serde(default = "default_statement_timeout")]
    pub statement_timeout_secs: u64,
    /// Row count above which a sync query becomes async.
    #[serde(default = "default_sync_row_cap")]
    pub sync_row_cap: u64,
    /// Maximum rows ever returned inline.
    #[serde(default = "default_max_output_rows")]
    pub max_output_rows: u64,
    /// JIT (`Derive`) survivor-set cap above which a derive is rejected.
    #[serde(default = "default_j_survivor_cap")]
    pub j_survivor_cap: u64,
    /// Whether the query path rejects DSL referencing columns not present in the
    /// `semantic_catalog` (spec 13 §1: the agent may only query catalogued
    /// columns). Defaults on so production is safe; unit tests that onboard
    /// directly (bypassing the Profiler) opt out.
    #[serde(default = "default_enforce_catalogue")]
    pub enforce_catalogue: bool,
}

fn default_memory_limit() -> String {
    "8GB".to_string()
}

fn default_threads() -> usize {
    match std::thread::available_parallelism() {
        Ok(n) => n.get(),
        Err(_) => 8,
    }
}

const fn default_statement_timeout() -> u64 {
    30
}

const fn default_sync_row_cap() -> u64 {
    100_000
}

const fn default_max_output_rows() -> u64 {
    1_000_000
}

const fn default_j_survivor_cap() -> u64 {
    200_000
}

/// Catalogue enforcement defaults ON (production-safe; spec 13 §1).
const fn default_enforce_catalogue() -> bool {
    true
}

impl Default for GuardrailConfig {
    fn default() -> Self {
        Self {
            memory_limit: default_memory_limit(),
            threads: default_threads(),
            statement_timeout_secs: default_statement_timeout(),
            sync_row_cap: default_sync_row_cap(),
            max_output_rows: default_max_output_rows(),
            j_survivor_cap: default_j_survivor_cap(),
            enforce_catalogue: default_enforce_catalogue(),
        }
    }
}

/// Suppression rules consumed by the `Exclude` capability (specs/20 §5). Rules
/// are config, not code; both toggles below are runtime-tunable YAML.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SuppressionRules {
    /// Exclude a user from a campaign if they have any `targeted`/`delivered`
    /// writeback for it (default on).
    #[serde(default = "default_per_campaign_no_repeat")]
    pub per_campaign_no_repeat: bool,
    /// Global frequency cap: exclude a user with `>= max_contacts`
    /// `targeted`/`delivered` writebacks in the last `window_days` days across
    /// campaigns. `None` disables the cap (default).
    #[serde(default)]
    pub frequency_cap: Option<FrequencyCap>,
}

/// The global frequency cap: `N` contacts in `D` days (specs/20 §5). Both
/// fields are non-zero (`NonZeroU32` — a zero cap would exclude everyone and a
/// zero window is meaningless; AGENTS.md § Type Design).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FrequencyCap {
    /// Maximum contacts (`N`).
    pub max_contacts: std::num::NonZeroU32,
    /// The window, in days (`D`).
    pub window_days: std::num::NonZeroU32,
}

const fn default_per_campaign_no_repeat() -> bool {
    true
}

impl Default for SuppressionRules {
    fn default() -> Self {
        Self {
            per_campaign_no_repeat: default_per_campaign_no_repeat(),
            frequency_cap: None,
        }
    }
}

/// DuckLake compaction tuning (specs/71 §4; spike-microbatch-compaction.md).
/// Runtime-tunable YAML: `inlining_row_limit = 0` makes every micro-batch a
/// data file (so compaction has small files to merge); `target_file_size` is
/// the merge target. Calibrate on target storage (spike R1).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactionConfig {
    /// Rows below which a write is inlined into the catalog instead of a data
    /// file; `0` disables inlining entirely.
    #[serde(default = "default_inlining_row_limit")]
    pub inlining_row_limit: u64,
    /// The target merged file size (DuckLake setting `ducklake_target_file_size`,
    /// e.g. `"1MB"`).
    #[serde(default = "default_target_file_size")]
    pub target_file_size: String,
}

const fn default_inlining_row_limit() -> u64 {
    0
}

fn default_target_file_size() -> String {
    "1MB".to_string()
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            inlining_row_limit: default_inlining_row_limit(),
            target_file_size: default_target_file_size(),
        }
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            catalog_path: PathBuf::from("catalog.db"),
            data_path: PathBuf::from("./data"),
            compaction_interval_secs: default_compaction_interval(),
            micro_batch_flush_rows: default_micro_batch_rows(),
            micro_batch_flush_age_secs: default_micro_batch_flush_age(),
            bind: default_bind(),
            guardrails: GuardrailConfig::default(),
            suppression: SuppressionRules::default(),
            compaction: CompactionConfig::default(),
            auth_token: None,
            sql_approval_token: None,
            llm: None,
        }
    }
}

impl EngineConfig {
    /// Load configuration from a YAML file. Missing file is an error; the
    /// caller must supply a real path.
    ///
    /// # Errors
    /// - [`crate::Error::InvalidInput`] if the file cannot be read or parsed.
    pub fn from_yaml_file(path: &Path) -> Result<Self> {
        let source = config::File::from(path).format(config::FileFormat::Yaml);
        let cfg = config::Config::builder()
            .add_source(source)
            .build()
            .map_err(|e| crate::Error::InvalidInput(format!("read config {path:?}: {e}")))?;
        cfg.try_deserialize()
            .map_err(|e| crate::Error::InvalidInput(format!("parse config: {e}")))
    }

    /// Same as [`Self::from_yaml_file`] but returns the typed error directly,
    /// useful for tests that build a temp config.
    ///
    /// # Errors
    /// Propagates parse/read failures as [`crate::Error::InvalidInput`].
    pub fn from_yaml_str(yaml: &str) -> Result<Self> {
        let cfg = config::Config::builder()
            .add_source(config::File::from_str(yaml, config::FileFormat::Yaml))
            .build()
            .map_err(|e| crate::Error::InvalidInput(format!("build config: {e}")))?;
        cfg.try_deserialize()
            .map_err(|e| crate::Error::InvalidInput(format!("parse config: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_parse_minimal_yaml() {
        let yaml = "\
catalog_path: /tmp/cat.db
data_path: /tmp/data
";
        let cfg = EngineConfig::from_yaml_str(yaml).expect("parse");
        assert_eq!(cfg.catalog_path, PathBuf::from("/tmp/cat.db"));
        assert_eq!(cfg.compaction_interval_secs, 3600);
        assert_eq!(cfg.micro_batch_flush_rows, 50_000);
        assert_eq!(cfg.micro_batch_flush_age_secs, 30);
        assert_eq!(cfg.bind, "127.0.0.1:8080");
        assert_eq!(cfg.guardrails.statement_timeout_secs, 30);
        assert_eq!(cfg.guardrails.sync_row_cap, 100_000);
    }

    #[test]
    fn test_should_reject_unknown_field() {
        let yaml = "catalog_path: /tmp/cat.db\ndata_path: /tmp/data\nbogus: 1\n";
        let res = EngineConfig::from_yaml_str(yaml);
        assert!(res.is_err(), "deny_unknown_fields must reject bogus");
    }
}

/// HTTP LLM/embedding service configuration (spec 13 §4). When set, the server
/// builds real HTTP clients (with timeout + retry) instead of the deterministic
/// stubs; the embedding dimension must match the service's output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LlmConfig {
    /// Base URL of the OpenAI-compatible service (e.g. `http://llm:8080`).
    pub base_url: String,
    /// API key sent as `Authorization: Bearer <key>`.
    pub api_key: String,
    /// Fixed embedding dimension returned by the service (must match the
    /// `semantic_catalog` embeddings the Profiler writes).
    pub embedding_dim: usize,
}
