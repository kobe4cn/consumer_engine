//! Engine configuration.
//!
//! Per `AGENTS.md`, configuration is YAML-loaded via the `config` crate and
//! data that may be tuned at runtime lives here (compile-time constants are
//! reserved for truly fixed values).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{BoxError, Result};

/// Top-level engine configuration. Loaded from YAML at startup.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EngineConfig {
    /// Filesystem path to the DuckLake catalogue database (a DuckDB file for
    /// single-process operation; Postgres DSN when multi-writer is needed).
    pub catalog_path: PathBuf,
    /// Filesystem path (or object-storage prefix) where DuckLake writes Parquet.
    pub data_path: PathBuf,
    /// How often the compaction task runs, in seconds.
    #[serde(default = "default_compaction_interval")]
    pub compaction_interval_secs: u64,
    /// Micro-batch flush threshold: flush once this many rows are queued.
    #[serde(default = "default_micro_batch_rows")]
    pub micro_batch_flush_rows: usize,
    /// Bind address for the REST ingress.
    #[serde(default = "default_bind")]
    pub bind: String,
}

const fn default_compaction_interval() -> u64 {
    3600
}

const fn default_micro_batch_rows() -> usize {
    50_000
}

fn default_bind() -> String {
    "127.0.0.1:8080".to_string()
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            catalog_path: PathBuf::from("catalog.db"),
            data_path: PathBuf::from("./data"),
            compaction_interval_secs: default_compaction_interval(),
            micro_batch_flush_rows: default_micro_batch_rows(),
            bind: default_bind(),
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
            .map_err(BoxError::from)
            .map_err(crate::Error::Ingestion)?;
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
        assert_eq!(cfg.bind, "127.0.0.1:8080");
    }

    #[test]
    fn test_should_reject_unknown_field() {
        let yaml = "catalog_path: /tmp/cat.db\ndata_path: /tmp/data\nbogus: 1\n";
        let res = EngineConfig::from_yaml_str(yaml);
        assert!(res.is_err(), "deny_unknown_fields must reject bogus");
    }
}
