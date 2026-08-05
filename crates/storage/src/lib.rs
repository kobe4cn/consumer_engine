//! DuckLake storage layer.
//!
//! Owns the **single writable handle** to a DuckLake catalog ([`Writer`]) and a
//! factory for **read-only** reader connections ([`open_reader`]). Per decision
//! D3, at most one [`Writer`] exists per catalog; the ingestion actor enforces
//! singleness, and the query path only ever reads.
//!
//! All identifiers (`system`, `entity`, column names) crossing this boundary are
//! validated against a strict allowlist (`^[a-zA-Z0-9_-]{1,64}$`) — per
//! `AGENTS.md` § Injection Prevention, identifiers are never interpolated from
//! raw caller input, and row values are always bound parameters, never literals.

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]

use std::{
    cell::Cell,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use consumer_engine_core::{
    BoxError, CatalogRow, Error, FeatureRow, READ_ONLY_CATALOG_ALIAS, Result, SnapshotSpec,
    SuppressionRow, WRITE_CATALOG_ALIAS, validate_feature_name, validate_ident,
};
use duckdb::{Connection, types::Value};
use fs2::FileExt;

/// Build the qualified raw table name `raw_<system>_<entity>` (validated).
fn raw_table_name(system: &str, entity: &str) -> Result<String> {
    validate_ident(system)?;
    validate_ident(entity)?;
    Ok(format!("raw_{system}_{entity}"))
}

/// Load the `ducklake` extension on a connection (install is idempotent).
fn load_ducklake(conn: &Connection) -> Result<()> {
    conn.execute_batch("INSTALL ducklake; LOAD ducklake;")
        .map_err(|e| Error::Storage(BoxError::from(e)))?;
    Ok(())
}

/// The single writable DuckLake handle.
///
/// Construct via [`Writer::attach`]. Move-only: it cannot be cloned, ensuring a
/// single owner. The ingestion actor takes it into a dedicated thread. An
/// exclusive file lock on a sibling of the catalog path enforces the
/// single-writer invariant **across processes** (decision D3): a second
/// [`Writer::attach`] against the same catalog returns [`Error::WriterAlreadyHeld`].
pub struct Writer {
    conn: Connection,
    /// Held until drop; releases the exclusive catalog lock.
    #[allow(clippy::disallowed_types)]
    _lock: std::fs::File,
    /// Advanced after every successful write (issue #20 / P1-1): the read
    /// pool's `OnWrite` refresh policy re-attaches a worker only when this
    /// changes — the single writer (D3) is the reliable commit signal the
    /// spike found no reader-side proxy for.
    write_gen: Option<Arc<AtomicU64>>,
    /// Whether the DuckLake build can bind the timestamp-parameterized
    /// maintenance procedures (`ducklake_expire_snapshots` / orphan cleanup).
    /// Probed once at attach: duckdb 1.10505.0 has a TIMESTAMPTZ binder defect
    /// that rejects them entirely (issue #17 — see specs/93 GC-MAINT-BINDER).
    maintenance_available: Cell<bool>,
    /// The engine's tenant, stamped on every committed row (issue #14 / AC6
    /// foundation). Set from config at attach; single-tenant today.
    tenant: String,
}

impl std::fmt::Debug for Writer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer").finish_non_exhaustive()
    }
}

impl Writer {
    /// Attach a writable DuckLake catalog at `catalog_path` with Parquet data
    /// under `data_path`, with the default compaction tuning. Acquires an
    /// exclusive lock; a second attach to the same catalog fails with
    /// [`Error::WriterAlreadyHeld`].
    ///
    /// # Errors
    /// - [`Error::WriterAlreadyHeld`] if the catalog is already locked.
    /// - [`Error::Storage`] if DuckDB/DuckLake attach fails.
    pub fn attach(catalog_path: &Path, data_path: &Path) -> Result<Self> {
        Self::attach_with_compaction(
            catalog_path,
            data_path,
            &consumer_engine_core::CompactionConfig::default(),
        )
    }

    /// Like [`Self::attach`] but with explicit DuckLake compaction tuning
    /// (runtime-tunable config — AGENTS.md: tunable data lives in the config
    /// file, not in SQL literals).
    ///
    /// # Errors
    /// - [`Error::WriterAlreadyHeld`] if the catalog is already locked.
    /// - [`Error::Storage`] if DuckDB/DuckLake attach or the compaction SET fails.
    pub fn attach_with_compaction(
        catalog_path: &Path,
        data_path: &Path,
        compaction: &consumer_engine_core::CompactionConfig,
    ) -> Result<Self> {
        // Single-writer enforcement (D3): exclusive lock on a sibling file.
        let lock_path = catalog_path.with_extension("db.writelock");
        // Advisory file locking (fs2::FileExt) is only impl'd for std::fs::File,
        // not tokio::fs::File, and this is a synchronous startup operation that
        // runs outside the async runtime — so std::fs::OpenOptions is correct
        // here despite the workspace's tokio::fs preference (AGENTS.md § Async).
        #[allow(clippy::disallowed_types)]
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&lock_path)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        lock_file
            .try_lock_exclusive()
            .map_err(|_| Error::WriterAlreadyHeld)?;

        let conn = Connection::open_in_memory().map_err(|e| Error::Storage(BoxError::from(e)))?;
        load_ducklake(&conn)?;
        let sql = format!(
            "ATTACH 'ducklake:{}' AS {WRITE_CATALOG_ALIAS} (DATA_PATH '{}');",
            escape_for_sql_literal(&catalog_path.to_string_lossy()),
            escape_for_sql_literal(&data_path.to_string_lossy()),
        );
        conn.execute_batch(&sql)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        // Compaction tuning (specs/71 §4, spike-microbatch-compaction.md):
        // write every batch as a data file (no inlining into the catalog) so
        // micro-batches accumulate small files that compaction can merge, and
        // merge up to the configured target size. Without this DuckLake inlines
        // small writes and `ducklake_list_files` stays empty. Values are
        // runtime-tunable config, not SQL literals (AGENTS.md).
        let target = escape_for_sql_literal(&compaction.target_file_size);
        conn.execute_batch(&format!(
            "SET ducklake_default_data_inlining_row_limit = {}; SET ducklake_target_file_size = \
             '{target}';",
            compaction.inlining_row_limit,
        ))
        .map_err(|e| Error::Storage(BoxError::from(e)))?;
        Ok(Self {
            conn,
            _lock: lock_file,
            write_gen: None,
            maintenance_available: Cell::new(false),
            tenant: "default".to_string(),
        })
    }

    /// Set the tenant stamped on every committed row (issue #14). The single
    /// writer owns the engine's tenant; per-caller tenant from auth claims
    /// lands with the isolation ticket (#22).
    pub fn set_tenant(&mut self, tenant: String) {
        self.tenant = tenant;
    }

    /// Like [`Self::attach_with_compaction`] but wired to a shared write
    /// generation counter: every successful write bumps it, so a read pool
    /// running the `OnWrite` refresh policy (issue #20 / P1-1) re-attaches
    /// exactly when the catalog changed. Pass the same `Arc` to
    /// `consumer_engine_execution::Reader::start_pooled`.
    ///
    /// # Errors
    /// - [`Error::WriterAlreadyHeld`] if the catalog is already locked.
    /// - [`Error::Storage`] if DuckDB/DuckLake attach or the compaction SET fails.
    pub fn attach_with_gen(
        catalog_path: &Path,
        data_path: &Path,
        compaction: &consumer_engine_core::CompactionConfig,
        write_gen: Option<Arc<AtomicU64>>,
    ) -> Result<Self> {
        let mut writer = Self::attach_with_compaction(catalog_path, data_path, compaction)?;
        writer.write_gen = write_gen;
        Ok(writer)
    }

    /// Record a committed write: advance the shared generation so pooled
    /// readers re-attach on their next query. Called after every successful
    /// write; harmless if no counter is wired.
    fn bump_write(&self) {
        if let Some(g) = &self.write_gen {
            g.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Create the `feature_store` table if absent. No PRIMARY KEY — DuckLake
    /// rejects constraints (`specs/10`, `specs/20 §4`); the store is append-only
    /// by `as_of_ts` (a newer value supersedes, never overwrites).
    pub fn ensure_feature_store_table(&self) -> Result<()> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {WRITE_CATALOG_ALIAS}.feature_store (user_id VARCHAR, \
             feature_name VARCHAR, num_value DOUBLE, as_of_ts TIMESTAMPTZ, producer_id VARCHAR, \
             tenant_id VARCHAR)"
        );
        self.conn
            .execute_batch(&sql)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        // Migration for pre-#14 catalogs: add the tenant column if absent.
        self.conn
            .execute_batch(&format!(
                "ALTER TABLE {WRITE_CATALOG_ALIAS}.feature_store ADD COLUMN IF NOT EXISTS \
                 tenant_id VARCHAR"
            ))
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        Ok(())
    }

    /// Create the `semantic_catalog` table if absent. M3 uses a variable-length
    /// `FLOAT[]` embedding so brute-force cosine works without a fixed-size HNSW
    /// index; a phase-2 migration to fixed `FLOAT[dim]` + HNSW is flagged in
    /// `specs/93`.
    pub fn ensure_semantic_catalog_table(&self) -> Result<()> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {WRITE_CATALOG_ALIAS}.semantic_catalog (entity_type \
             VARCHAR, system VARCHAR, table_name VARCHAR, column_name VARCHAR, semantic_type \
             VARCHAR, data_type VARCHAR, description VARCHAR, pii_flag BOOLEAN, sample_values \
             JSON, embedding FLOAT[], source_epoch BIGINT, tenant_id VARCHAR)"
        );
        self.conn
            .execute_batch(&sql)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        // Migration for pre-#14 catalogs: add the tenant column if absent.
        self.conn
            .execute_batch(&format!(
                "ALTER TABLE {WRITE_CATALOG_ALIAS}.semantic_catalog ADD COLUMN IF NOT EXISTS \
                 tenant_id VARCHAR"
            ))
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        Ok(())
    }

    /// Create the `suppression` table if absent. No PRIMARY KEY — DuckLake
    /// rejects constraints (specs/10 §2); idempotency is enforced by the write
    /// path on the `suppression_id` logical key (specs/20 §5, E1).
    pub fn ensure_suppression_table(&self) -> Result<()> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {WRITE_CATALOG_ALIAS}.suppression (suppression_id UUID, \
             campaign_id VARCHAR, user_id VARCHAR, channel VARCHAR, action VARCHAR, occurred_ts \
             TIMESTAMPTZ, received_ts TIMESTAMPTZ, tenant_id VARCHAR)"
        );
        self.conn
            .execute_batch(&sql)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        // Migration for pre-#14 catalogs: add the tenant column if absent.
        self.conn
            .execute_batch(&format!(
                "ALTER TABLE {WRITE_CATALOG_ALIAS}.suppression ADD COLUMN IF NOT EXISTS tenant_id \
                 VARCHAR"
            ))
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        Ok(())
    }

    /// Append suppression rows **idempotently**: a row whose `suppression_id`
    /// already exists is skipped (the delivery system supplies the id for
    /// dedup — re-POSTing the same outcome writes nothing new, specs/21 §4 E1).
    /// Returns the number of rows actually inserted.
    ///
    /// # Errors
    /// [`Error::Storage`] on insert failure.
    pub fn write_suppression_idempotent(&self, rows: &[SuppressionRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        self.ensure_suppression_table()?;
        let insert = format!(
            "INSERT INTO {WRITE_CATALOG_ALIAS}.suppression (suppression_id, campaign_id, user_id, \
             channel, action, occurred_ts, received_ts, tenant_id) SELECT CAST(? AS UUID), ?, ?, \
             ?, ?, CAST(? AS TIMESTAMPTZ), CAST(? AS TIMESTAMPTZ), ? WHERE NOT EXISTS (SELECT 1 \
             FROM {WRITE_CATALOG_ALIAS}.suppression WHERE suppression_id = CAST(? AS UUID))"
        );
        let mut stmt = self
            .conn
            .prepare(&insert)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        let tenant = self.tenant.clone();
        let mut written = 0usize;
        for r in rows {
            written += stmt
                .execute(duckdb::params![
                    r.suppression_id,
                    r.campaign_id,
                    r.user_id,
                    r.channel.as_str(),
                    r.action.as_str(),
                    r.occurred_ts,
                    r.received_ts,
                    tenant.as_str(),
                    r.suppression_id,
                ])
                .map_err(|e| Error::Storage(BoxError::from(e)))?;
        }
        self.bump_write();
        Ok(written)
    }

    /// Append feature rows to `feature_store`. Append-only: a newer `as_of_ts`
    /// supersedes via the wide view, never overwrites (I4). Validates each
    /// `feature_name`/`producer_id` at the boundary. Returns rows inserted.
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] on a bad feature name/producer id.
    /// - [`Error::Storage`] on insert failure.
    pub fn write_feature_rows(&self, rows: &[FeatureRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        for r in rows {
            validate_feature_name(&r.feature_name)
                .map_err(|e| Error::InvalidInput(format!("feature_name: {e}")))?;
            validate_feature_name(&r.producer_id)
                .map_err(|e| Error::InvalidInput(format!("producer_id: {e}")))?;
        }
        self.ensure_feature_store_table()?;
        let tenant = self.tenant.clone();
        let n = insert_chunked(
            &self.conn,
            &format!(
                "INSERT INTO {WRITE_CATALOG_ALIAS}.feature_store (user_id, feature_name, \
                 num_value, as_of_ts, producer_id, tenant_id) VALUES "
            ),
            6,
            rows,
            |r| {
                vec![
                    Value::Text(r.user_id.clone()),
                    Value::Text(r.feature_name.clone()),
                    Value::Double(r.num_value),
                    Value::Text(r.as_of_ts.clone()),
                    Value::Text(r.producer_id.clone()),
                    Value::Text(tenant.clone()),
                ]
            },
        )?;
        Ok(n)
    }

    /// Append catalog rows to `semantic_catalog`. The embedding is written via a
    /// `list_value(?, ?, …)` constructor with one scalar placeholder per
    /// dimension (DuckDB's Rust binding cannot bind a `List` parameter directly,
    /// so each float is bound individually). All rows must share the same
    /// embedding dimension. Returns rows inserted.
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] if the rows' embedding dimensions disagree.
    /// - [`Error::Storage`] on insert failure.
    pub fn write_catalog_rows(&self, rows: &[CatalogRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        self.ensure_semantic_catalog_table()?;
        // `rows` is non-empty (checked above); `first()` keeps the lint set
        // (no indexing) clean while the dimension is taken from row 0.
        let dim = rows.first().map(|r| r.embedding.len()).unwrap_or(0);
        for r in rows {
            if r.embedding.len() != dim {
                return Err(Error::InvalidInput(format!(
                    "catalog embedding dimension mismatch: expected {dim}, got {}",
                    r.embedding.len()
                )));
            }
        }
        let emb_expr = if dim == 0 {
            "list_value()".to_string()
        } else {
            format!("list_value({})", vec!["?"; dim].join(", "))
        };
        let insert = format!(
            "INSERT INTO {WRITE_CATALOG_ALIAS}.semantic_catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, \
             CAST(? AS JSON), {emb_expr}, ?, ?)"
        );
        let mut stmt = self
            .conn
            .prepare(&insert)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        let mut written = 0usize;
        for r in rows {
            let sample = serde_json::to_string(&r.sample_values)
                .map_err(|e| Error::Storage(BoxError::from(e)))?;
            let mut params: Vec<Value> = vec![
                Value::Text(r.entity_type.clone()),
                Value::Text(r.system.clone()),
                Value::Text(r.table_name.clone()),
                r.column_name
                    .clone()
                    .map(Value::Text)
                    .unwrap_or(Value::Null),
                Value::Text(r.semantic_type.as_str().to_string()),
                Value::Text(r.data_type.clone()),
                Value::Text(r.description.clone()),
                Value::Boolean(r.pii_flag),
                Value::Text(sample),
            ];
            for f in &r.embedding {
                params.push(Value::Float(*f));
            }
            // The trailing `?`s in the INSERT (after `{emb_expr}`) are
            // `source_epoch` then `tenant_id` — bind after the embedding floats
            // so columns align.
            params.push(Value::BigInt(r.source_epoch));
            params.push(Value::Text(self.tenant.clone()));
            written += stmt
                .execute(duckdb::params_from_iter(params.iter()))
                .map_err(|e| Error::Storage(BoxError::from(e)))?;
        }
        self.bump_write();
        Ok(written)
    }

    /// Append feature rows and refresh the wide pivot views for every distinct
    /// family in **one catalog transaction** (specs/20 I4: a partial write is
    /// never observable). Each view is rebuilt with the **union** of the batch's
    /// short names and those already stored (`specs/10 §2`: the wide pivot
    /// covers all stored features — a partial batch never drops columns). On any
    /// failure the whole transaction rolls back: no rows written, no view
    /// changed. This is the write path the ingestion actor uses for `Feature`
    /// producer output.
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] on a bad feature name/producer id or family.
    /// - [`Error::Storage`] on insert/view/transaction failure.
    pub fn write_features_and_refresh(&self, rows: &[FeatureRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        self.conn
            .execute_batch("BEGIN TRANSACTION")
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        let outcome = (|| -> Result<usize> {
            let n = self.write_feature_rows(rows)?;
            let mut families: std::collections::BTreeMap<
                String,
                std::collections::BTreeSet<String>,
            > = std::collections::BTreeMap::new();
            for r in rows {
                let (family, short) = consumer_engine_core::split_feature_name(&r.feature_name)?;
                families.entry(family).or_default().insert(short);
            }
            for (family, batch_shorts) in families {
                let mut all: std::collections::BTreeSet<String> =
                    self.feature_short_names(&family)?.into_iter().collect();
                all.extend(batch_shorts);
                self.refresh_feature_wide_view(&family, &all.into_iter().collect::<Vec<_>>())?;
            }
            Ok(n)
        })();
        match outcome {
            Ok(n) => {
                self.conn
                    .execute_batch("COMMIT")
                    .map_err(|e| Error::Storage(BoxError::from(e)))?;
                self.bump_write();
                Ok(n)
            }
            Err(e) => {
                // Best-effort rollback; the original error is what surfaces.
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Read the distinct feature short names already stored for `family`
    /// (`feature_name` values `family.<short>`). Used by the ingestion writer to
    /// rebuild a wide view that unions the current batch with everything
    /// previously written (so a partial batch never drops columns from the
    /// view — `specs/10 §2`). `family` is validated; the prefix is bound as a
    /// parameter and matched with `starts_with` (no LIKE wildcard pitfalls).
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] on a bad `family`.
    /// - [`Error::Storage`] on read failure.
    pub fn feature_short_names(&self, family: &str) -> Result<Vec<String>> {
        validate_ident(family)?;
        self.ensure_feature_store_table()?;
        let prefix = format!("{family}.");
        let mut stmt = self
            .conn
            .prepare(&format!(
                "SELECT DISTINCT feature_name FROM {WRITE_CATALOG_ALIAS}.feature_store WHERE \
                 starts_with(feature_name, ?)"
            ))
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        let rows = stmt
            .query_map([&prefix], |r| r.get::<_, String>(0))
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        let mut shorts: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for name in rows {
            let name = name.map_err(|e| Error::Storage(BoxError::from(e)))?;
            if let Some((_, short)) = name.split_once('.') {
                shorts.insert(short.to_string());
            }
        }
        Ok(shorts.into_iter().collect())
    }

    /// Create or replace the wide pivot view `feature_wide_{family}` with one
    /// `arg_max(num_value, as_of_ts)` column per short name (latest value wins
    /// by `as_of_ts`). `family` and each short name are validated identifiers
    /// (interpolated into the view name/columns — defense-in-depth).
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] on a bad `family`/short name.
    /// - [`Error::Storage`] on view failure.
    pub fn refresh_feature_wide_view(
        &self,
        family: &str,
        feature_short_names: &[String],
    ) -> Result<()> {
        validate_ident(family)?;
        if feature_short_names.is_empty() {
            return Ok(());
        }
        self.ensure_feature_store_table()?;
        let mut cols = Vec::with_capacity(feature_short_names.len() + 2);
        cols.push("user_id".to_string());
        cols.push("tenant_id".to_string());
        for short in feature_short_names {
            validate_ident(short)?;
            // Constant string literal: validated identifier parts only, so the
            // FILTER literal cannot inject (no quotes/semicolons possible).
            cols.push(format!(
                "arg_max(num_value, as_of_ts) FILTER (WHERE feature_name = '{family}.{short}') AS \
                 {short}"
            ));
        }
        let select = cols.join(", ");
        // `starts_with(feature_name, '{family}.')` instead of `LIKE '{family}.%'` —
        // `_` in a family is a LIKE single-char wildcard that would match other
        // families' rows. `starts_with` compares literally, and `family` is a
        // validated identifier (no quotes) so the literal is injection-safe.
        // (A bound parameter cannot be used here: DuckDB rejects prepared
        // parameters inside a CREATE VIEW statement.)
        let sql = format!(
            "CREATE OR REPLACE VIEW {WRITE_CATALOG_ALIAS}.feature_wide_{family} AS SELECT \
             {select} FROM {WRITE_CATALOG_ALIAS}.feature_store WHERE starts_with(feature_name, \
             '{family}.') GROUP BY user_id, tenant_id"
        );
        self.conn
            .execute_batch(&sql)
            .map_err(|e| Error::Storage(BoxError::from(e)))
    }

    /// Create `raw_<system>_<entity>` with the given VARCHAR columns if absent,
    /// then insert all rows. Returns the number of rows inserted.
    ///
    /// Column names are validated; row values are bound parameters.
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] on a bad identifier.
    /// - [`Error::Storage`] on DDL/insert failure.
    pub fn ingest_raw(
        &self,
        system: &str,
        entity: &str,
        columns: &[String],
        rows: &[Vec<Option<String>>],
    ) -> Result<usize> {
        if columns.is_empty() {
            return Err(Error::InvalidInput("columns must not be empty".into()));
        }
        for c in columns {
            validate_ident(c)?;
        }
        let table = raw_table_name(system, entity)?;
        let cols_names = columns.join(", ");
        let mut cols_typed = columns
            .iter()
            .map(|c| format!("{c} VARCHAR"))
            .collect::<Vec<_>>();
        // Tenant stamping (issue #14): every raw table carries a `tenant_id`
        // column, filled from the writer's configured tenant.
        cols_typed.push("tenant_id VARCHAR".to_string());
        let create = format!(
            "CREATE TABLE IF NOT EXISTS {WRITE_CATALOG_ALIAS}.{table} ({})",
            cols_typed.join(", ")
        );
        self.conn
            .execute_batch(&create)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        // Migration for tables created before #14: add the column if absent.
        self.conn
            .execute_batch(&format!(
                "ALTER TABLE {WRITE_CATALOG_ALIAS}.{table} ADD COLUMN IF NOT EXISTS tenant_id \
                 VARCHAR"
            ))
            .map_err(|e| Error::Storage(BoxError::from(e)))?;

        if rows.is_empty() {
            return Ok(0);
        }
        let tenant = self.tenant.clone();
        let n = insert_chunked(
            &self.conn,
            &format!("INSERT INTO {WRITE_CATALOG_ALIAS}.{table} ({cols_names}, tenant_id) VALUES "),
            columns.len() + 1,
            rows,
            |row| {
                let mut values: Vec<Value> = row
                    .iter()
                    .map(|v| match v {
                        Some(s) => Value::Text(s.clone()),
                        None => Value::Null,
                    })
                    .collect();
                values.push(Value::Text(tenant.clone()));
                values
            },
        )?;
        self.bump_write();
        Ok(n)
    }

    /// Upsert a dimension batch by `key` (specs/20 §4): rows are **deduplicated
    /// by key within the batch** (last row wins — DuckLake does not dedup), then
    /// merged with a single `WHEN MATCHED THEN UPDATE / WHEN NOT MATCHED THEN
    /// INSERT` statement per chunk, one catalog transaction per chunk (one
    /// DuckLake snapshot per chunk, keeping the read-attach cost low — issue
    /// #12). Events should use [`Self::ingest_raw`] (append); only dims upsert.
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] on a bad identifier or a `key` absent from `columns`.
    /// - [`Error::Storage`] on DDL/MERGE failure.
    pub fn upsert_raw(
        &self,
        system: &str,
        entity: &str,
        columns: &[String],
        key: &str,
        rows: &[Vec<Option<String>>],
    ) -> Result<usize> {
        if columns.is_empty() {
            return Err(Error::InvalidInput("columns must not be empty".into()));
        }
        for c in columns {
            validate_ident(c)?;
        }
        validate_ident(key)?;
        let key_idx = columns
            .iter()
            .position(|c| c == key)
            .ok_or_else(|| Error::InvalidInput(format!("merge key {key:?} must be a column")))?;
        let table = raw_table_name(system, entity)?;
        let mut cols_typed = columns
            .iter()
            .map(|c| format!("{c} VARCHAR"))
            .collect::<Vec<_>>();
        cols_typed.push("tenant_id VARCHAR".to_string());
        let create = format!(
            "CREATE TABLE IF NOT EXISTS {WRITE_CATALOG_ALIAS}.{table} ({})",
            cols_typed.join(", ")
        );
        self.conn
            .execute_batch(&create)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        self.conn
            .execute_batch(&format!(
                "ALTER TABLE {WRITE_CATALOG_ALIAS}.{table} ADD COLUMN IF NOT EXISTS tenant_id \
                 VARCHAR"
            ))
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        if rows.is_empty() {
            return Ok(0);
        }
        // Adapter-boundary dedup: last row per key wins; NULL-key rows skipped
        // (specs/20 §4 — DuckLake accumulates duplicates; the writer must not).
        let deduped = Self::dedup_by_key(rows, key_idx);
        let affected = self.merge_upserts(&table, columns, key, &deduped)?;
        self.bump_write();
        Ok(affected)
    }

    /// Dedup rows by `key_idx`, LAST row wins (specs/20 §4 — DuckLake
    /// accumulates duplicates; the writer must not). Rows with a NULL/missing
    /// key are **skipped** (they cannot be merged and collapsing them would
    /// lose distinct rows) and logged.
    fn dedup_by_key(rows: &[Vec<Option<String>>], key_idx: usize) -> Vec<&Vec<Option<String>>> {
        let mut deduped: Vec<&Vec<Option<String>>> = Vec::new();
        let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for row in rows {
            let Some(k) = row.get(key_idx).and_then(|v| v.as_deref()) else {
                tracing::warn!("row with a NULL merge key skipped (cannot merge)");
                continue;
            };
            match seen.get(k) {
                Some(&i) => {
                    // `i` was pushed as `deduped.len()`, so it is always in
                    // bounds; get_mut keeps the boundary lint set clean.
                    if let Some(slot) = deduped.get_mut(i) {
                        *slot = row;
                    }
                }
                None => {
                    seen.insert(k.to_string(), deduped.len());
                    deduped.push(row);
                }
            }
        }
        deduped
    }

    /// Chunked upsert MERGE for `deduped` rows (assumes an open catalog
    /// transaction — callers own BEGIN/COMMIT so data and other writes commit
    /// atomically, e.g. the CDC offset in [`Self::apply_cdc_batch`]). One
    /// multi-row `VALUES` MERGE per chunk (a single statement per chunk ⇒ one
    /// commit; per-row MERGE would create one snapshot per row — the issue #12
    /// defect class).
    fn merge_upserts(
        &self,
        table: &str,
        columns: &[String],
        key: &str,
        deduped: &[&Vec<Option<String>>],
    ) -> Result<usize> {
        const MERGE_CHUNK_ROWS: usize = 500;
        let tenant = self.tenant.clone();
        let mut cols_all: Vec<String> = columns.to_vec();
        cols_all.push("tenant_id".to_string());
        let cols_names = cols_all.join(", ");
        let set_clause = cols_all
            .iter()
            .map(|c| format!("{c} = s.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_vals = cols_all
            .iter()
            .map(|c| format!("s.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut affected = 0usize;
        for chunk in deduped.chunks(MERGE_CHUNK_ROWS) {
            let row_sql = format!("({})", vec!["?"; cols_all.len()].join(", "));
            let multi = vec![row_sql.as_str(); chunk.len()].join(", ");
            let merge = format!(
                "MERGE INTO {WRITE_CATALOG_ALIAS}.{table} AS t USING (VALUES {multi}) AS \
                 s({cols_names}) ON t.{key} = s.{key} WHEN MATCHED THEN UPDATE SET {set_clause} \
                 WHEN NOT MATCHED THEN INSERT ({cols_names}) VALUES ({insert_vals})"
            );
            let mut stmt = self
                .conn
                .prepare(&merge)
                .map_err(|e| Error::Storage(BoxError::from(e)))?;
            let params: Vec<Option<&str>> = chunk
                .iter()
                .flat_map(|row| {
                    row.iter()
                        .map(|v| v.as_deref())
                        .chain(std::iter::once(Some(tenant.as_str())))
                })
                .collect();
            affected += stmt
                .execute(duckdb::params_from_iter(params))
                .map_err(|e| Error::Storage(BoxError::from(e)))?;
        }
        Ok(affected)
    }

    /// Logically delete rows by `key` (specs/20 §4): a `MERGE ... WHEN MATCHED
    /// THEN DELETE` writes a delete-file in the catalog — the rows disappear
    /// from reads while the Parquet data files are untouched. One catalog
    /// transaction per chunk (one snapshot per chunk).
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] on a bad identifier.
    /// - [`Error::Storage`] on DDL/MERGE failure.
    pub fn delete_raw(
        &self,
        system: &str,
        entity: &str,
        key: &str,
        keys: &[String],
    ) -> Result<usize> {
        validate_ident(key)?;
        let table = raw_table_name(system, entity)?;
        if keys.is_empty() {
            return Ok(0);
        }
        let affected = self.merge_deletes(&table, key, keys)?;
        self.bump_write();
        Ok(affected)
    }

    /// Chunked logical-delete MERGE for `keys` (assumes an open catalog
    /// transaction; tenant-scoped so a delete never touches another tenant's
    /// rows). See [`Self::merge_upserts`] for the one-commit-per-chunk note.
    fn merge_deletes(&self, table: &str, key: &str, keys: &[String]) -> Result<usize> {
        const MERGE_CHUNK_KEYS: usize = 500;
        let tenant = self.tenant.clone();
        let mut affected = 0usize;
        for chunk in keys.chunks(MERGE_CHUNK_KEYS) {
            let multi = vec!["(?)"; chunk.len()].join(", ");
            let merge = format!(
                "MERGE INTO {WRITE_CATALOG_ALIAS}.{table} AS t USING (VALUES {multi}) AS s({key}) \
                 ON t.{key} = s.{key} AND t.tenant_id = ? WHEN MATCHED THEN DELETE"
            );
            let mut stmt = self
                .conn
                .prepare(&merge)
                .map_err(|e| Error::Storage(BoxError::from(e)))?;
            let mut params: Vec<&str> = chunk.iter().map(String::as_str).collect();
            params.push(&tenant);
            affected += stmt
                .execute(duckdb::params_from_iter(params))
                .map_err(|e| Error::Storage(BoxError::from(e)))?;
        }
        Ok(affected)
    }

    /// Apply one CDC batch atomically (issue #24 / specs/20 I2): the data
    /// writes (upserts + logical deletes) and the source's **offset commit**
    /// land in ONE catalog transaction — a crash or failure leaves neither
    /// data nor a half-advanced offset, so restart resumes exactly from the
    /// last committed position. `columns`/`key` are validated; upserts are
    /// deduped by key (at-least-once source → effectively-once catalog);
    /// deletes are logical. Rows are stamped with the writer's tenant.
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] on a bad identifier or a `key` absent from `columns`.
    /// - [`Error::Storage`] on DDL/MERGE failure (the whole batch rolls back).
    #[allow(
        clippy::too_many_arguments,
        reason = "mirrors ingestion::SourceBatch's fields; storage must not depend on ingestion"
    )]
    pub fn apply_cdc_batch(
        &self,
        system: &str,
        entity: &str,
        columns: &[String],
        key: &str,
        upserts: &[Vec<Option<String>>],
        deletes: &[String],
        offsets: &[(i32, i64)],
    ) -> Result<usize> {
        if columns.is_empty() {
            return Err(Error::InvalidInput("columns must not be empty".into()));
        }
        for c in columns {
            validate_ident(c)?;
        }
        validate_ident(key)?;
        if !columns.iter().any(|c| c == key) {
            return Err(Error::InvalidInput(format!(
                "merge key {key:?} must be a column"
            )));
        }
        let table = raw_table_name(system, entity)?;
        let source = format!("{system}.{entity}");
        // EVERYTHING — schema DDL included — happens inside the transaction so
        // a failed batch rolls back the table creation too (issue #24 / I2:
        // the whole batch is atomic, not just data+offset).
        self.conn
            .execute_batch("BEGIN TRANSACTION")
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        let outcome = (|| -> Result<usize> {
            let mut cols_typed = columns
                .iter()
                .map(|c| format!("{c} VARCHAR"))
                .collect::<Vec<_>>();
            cols_typed.push("tenant_id VARCHAR".to_string());
            self.conn
                .execute_batch(&format!(
                    "CREATE TABLE IF NOT EXISTS {WRITE_CATALOG_ALIAS}.{table} ({})",
                    cols_typed.join(", ")
                ))
                .map_err(|e| Error::Storage(BoxError::from(e)))?;
            self.conn
                .execute_batch(&format!(
                    "ALTER TABLE {WRITE_CATALOG_ALIAS}.{table} ADD COLUMN IF NOT EXISTS tenant_id \
                     VARCHAR"
                ))
                .map_err(|e| Error::Storage(BoxError::from(e)))?;
            self.conn
                .execute_batch(&format!(
                    "CREATE TABLE IF NOT EXISTS {WRITE_CATALOG_ALIAS}.cdc_offsets (source \
                     VARCHAR, partition BIGINT, cdc_offset BIGINT, updated_at TIMESTAMPTZ)"
                ))
                .map_err(|e| Error::Storage(BoxError::from(e)))?;
            let key_idx = columns.iter().position(|c| c == key).ok_or_else(|| {
                Error::InvalidInput(format!("merge key {key:?} must be a column"))
            })?;
            let deduped = Self::dedup_by_key(upserts, key_idx);
            let written = self.merge_upserts(&table, columns, key, &deduped)?;
            self.merge_deletes(&table, key, deletes)?;
            // Offset commit, same transaction as the data (I2) — one row per
            // (source, partition) so a multi-partition topic resumes exactly.
            for (partition, offset) in offsets {
                self.upsert_cdc_offset(
                    &source,
                    *partition,
                    *offset,
                    &chrono::Utc::now().to_rfc3339(),
                )?;
            }
            Ok(written)
        })();
        match outcome {
            Ok(n) => {
                self.conn
                    .execute_batch("COMMIT")
                    .map_err(|e| Error::Storage(BoxError::from(e)))?;
                self.bump_write();
                Ok(n)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Upsert a source+partition's committed CDC offset (assumes an open
    /// transaction; one row per partition so a multi-partition topic resumes
    /// exactly, issue #24).
    fn upsert_cdc_offset(
        &self,
        source: &str,
        partition: i32,
        cdc_offset: i64,
        updated_at: &str,
    ) -> Result<()> {
        let merge = format!(
            "MERGE INTO {WRITE_CATALOG_ALIAS}.cdc_offsets AS t USING (VALUES (?, ?, ?, ?)) AS \
             s(source, partition, cdc_offset, updated_at) ON t.source = s.source AND t.partition \
             = s.partition WHEN MATCHED THEN UPDATE SET cdc_offset = s.cdc_offset, updated_at = \
             s.updated_at WHEN NOT MATCHED THEN INSERT (source, partition, cdc_offset, \
             updated_at) VALUES (s.source, s.partition, s.cdc_offset, s.updated_at)"
        );
        let mut stmt = self
            .conn
            .prepare(&merge)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        stmt.execute(duckdb::params![source, partition, cdc_offset, updated_at])
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        Ok(())
    }

    /// Read a source's committed CDC offsets per partition (restart recovery,
    /// I2). `None` when the source has never committed.
    ///
    /// # Errors
    /// [`Error::Storage`] on read failure.
    pub fn read_cdc_offsets(&self, source: &str) -> Result<Vec<(i32, i64)>> {
        self.conn
            .execute_batch(&format!(
                "CREATE TABLE IF NOT EXISTS {WRITE_CATALOG_ALIAS}.cdc_offsets (source VARCHAR, \
                 partition BIGINT, cdc_offset BIGINT, updated_at TIMESTAMPTZ)"
            ))
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        let sql = format!(
            "SELECT partition, cdc_offset FROM {WRITE_CATALOG_ALIAS}.cdc_offsets WHERE source = ?"
        );
        let mut stmt = self
            .conn
            .prepare(&sql)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        let rows = stmt
            .query_map([source], |r| {
                Ok((r.get::<_, i64>(0)? as i32, r.get::<_, i64>(1)?))
            })
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(|e| Error::Storage(BoxError::from(e)))?);
        }
        Ok(out)
    }

    /// Run DuckLake compaction on a table. Uses `ducklake_merge_adjacent_files`
    /// — the procedure that actually merges in this DuckLake build
    /// (`ducklake_rewrite_data_files` is threshold-gated and returns 0
    /// processed; see docs/research/perf-calibration.md).
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] on a bad identifier.
    /// - [`Error::Storage`] on failure.
    pub fn compact(&self, system: &str, entity: &str) -> Result<()> {
        let table = raw_table_name(system, entity)?;
        // `ducklake_merge_adjacent_files` is the compaction that actually fires
        // in this DuckLake build (`ducklake_rewrite_data_files` is threshold-
        // gated and returns 0 processed); it merges adjacent small files toward
        // the configured target size. Old snapshots are retained (time-travel).
        let sql =
            format!("CALL ducklake_merge_adjacent_files('{WRITE_CATALOG_ALIAS}', '{table}');");
        self.conn
            .execute_batch(&sql)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        self.bump_write();
        Ok(())
    }

    /// Expire snapshots older than `retention_days` (specs/71 §4, issue #17):
    /// `ducklake_expire_snapshots` drops old time-travel versions, bounding the
    /// catalog size that drives the read-attach cost (issue #12). The latest
    /// snapshot is never older than `now`, so current data always survives.
    ///
    /// # Errors
    /// [`Error::Storage`] on expiry failure.
    /// Probe once (at first maintenance use) whether the DuckLake build can
    /// bind the timestamp-parameterized maintenance procedures. A no-op dry-run
    /// CALL with a far-future timestamp expires nothing and deletes nothing;
    /// on duckdb 1.10505.0 the TIMESTAMPTZ binder defect makes it fail, so
    /// expiry/cleanup degrade to a documented no-op (issue #17, specs/93
    /// GC-MAINT-BINDER) rather than failing the whole maintenance sweep.
    /// Probe once (at first maintenance use) whether the DuckLake build can
    /// bind the timestamp-parameterized maintenance procedures. The probe is a
    /// **dry-run** CALL with a **far-past** `older_than` (1970) so it expires
    /// nothing and deletes nothing even on a build where binding works — a
    /// far-future timestamp would be a data-destroying time bomb the moment the
    /// binder defect is fixed (it would expire every snapshot). On duckdb
    /// 1.10505.0 the TIMESTAMPTZ binder defect rejects the call entirely, so
    /// expiry/cleanup degrade to a documented no-op (issue #17, specs/93
    /// GC-MAINT-BINDER) rather than failing the whole maintenance sweep.
    fn probe_maintenance(&self) {
        if self.maintenance_available.get() {
            return;
        }
        let probe = format!(
            "CALL ducklake_expire_snapshots('{WRITE_CATALOG_ALIAS}', CAST(? AS TIMESTAMPTZ), \
             CAST([] AS UBIGINT[]), true)"
        );
        let ok = self
            .conn
            .prepare(&probe)
            .and_then(|mut stmt| stmt.execute(duckdb::params!["1970-01-01T00:00:00Z"]))
            .is_ok();
        self.maintenance_available.set(ok);
        if !ok {
            tracing::warn!(
                "DuckLake build cannot bind timestamp maintenance procedures; snapshot expiry and \
                 orphan cleanup are disabled until a DuckLake upgrade (issue #17)"
            );
        }
    }

    /// Expire snapshots older than `retention_days` (specs/71 §4, issue #17):
    /// `ducklake_expire_snapshots` drops old time-travel versions, bounding the
    /// catalog size that drives the read-attach cost (issue #12). The latest
    /// snapshot is never older than `now`, so current data always survives.
    /// Degrades to a no-op on builds that cannot bind the procedure
    /// (see [`Self::probe_maintenance`]).
    ///
    /// # Errors
    /// [`Error::Storage`] on expiry failure (when the procedure is callable).
    pub fn expire_snapshots(&self, retention_days: u64) -> Result<()> {
        self.probe_maintenance();
        if !self.maintenance_available.get() {
            return Ok(());
        }
        let sql = format!(
            "CALL ducklake_expire_snapshots('{WRITE_CATALOG_ALIAS}', CAST(CAST(now() AS \
             TIMESTAMP) - INTERVAL '{retention_days}' DAY AS TIMESTAMPTZ), CAST([] AS UBIGINT[]), \
             false)"
        );
        self.conn
            .execute_batch(&sql)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        self.bump_write();
        Ok(())
    }

    /// Reclaim orphaned files (specs/71 §4, issue #17): `ducklake_delete_orphaned_files`
    /// removes catalog-unreferenced data files, so a long-lived catalog does not
    /// leak space.
    ///
    /// # Errors
    /// [`Error::Storage`] on cleanup failure.
    /// Reclaim orphaned files (specs/71 §4, issue #17):
    /// `ducklake_delete_orphaned_files` removes catalog-unreferenced data files.
    /// Degrades to a no-op on builds that cannot bind the procedure
    /// (see [`Self::probe_maintenance`]).
    ///
    /// # Errors
    /// [`Error::Storage`] on cleanup failure (when the procedure is callable).
    pub fn delete_orphaned_files(&self, retention_days: u64) -> Result<()> {
        self.probe_maintenance();
        if !self.maintenance_available.get() {
            return Ok(());
        }
        let sql = format!(
            "CALL ducklake_delete_orphaned_files('{WRITE_CATALOG_ALIAS}', CAST(CAST(now() AS \
             TIMESTAMP) - INTERVAL '{retention_days}' DAY AS TIMESTAMPTZ), true, false)"
        );
        self.conn
            .execute_batch(&sql)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        self.bump_write();
        Ok(())
    }
    /// Create the `audience_snapshot` table if absent. No PRIMARY KEY/UNIQUE —
    /// DuckLake rejects them (`specs/10`, `specs/20 §4`). Called at materialise
    /// time (defensive) and at engine startup so reads never hit a missing
    /// table.
    pub fn ensure_audience_snapshot_table(&self) -> Result<()> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {WRITE_CATALOG_ALIAS}.audience_snapshot (snapshot_id \
             UUID, campaign_id VARCHAR, as_of_ts TIMESTAMPTZ, user_id VARCHAR, features JSON, \
             hit_reason JSON, tenant_id VARCHAR)"
        );
        self.conn
            .execute_batch(&sql)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        // Migration for pre-#14 catalogs: add the tenant column if absent.
        self.conn
            .execute_batch(&format!(
                "ALTER TABLE {WRITE_CATALOG_ALIAS}.audience_snapshot ADD COLUMN IF NOT EXISTS \
                 tenant_id VARCHAR"
            ))
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        Ok(())
    }

    /// Atomically materialise a DSL segment's distinct keys into
    /// `audience_snapshot` via a single `INSERT … SELECT` (one catalog
    /// transaction ⇒ a partial snapshot is never observable, `specs/20 I4`).
    ///
    /// `subquery_sql` must reference the **write** alias (`dl.raw_*`) and emit
    /// three columns: `<key_column>`, `features` (JSON), `hit_reason` (JSON) —
    /// the frozen per-row feature values and the predicate chain (D11, issue
    /// #13). Its `?` placeholders are bound by `subquery_params`.
    ///
    /// # Errors
    /// - [`Error::InvalidInput`] on a bad key column.
    /// - [`Error::Storage`] on table/insert failure.
    pub fn materialize_snapshot(
        &self,
        subquery_sql: &str,
        subquery_params: &[Value],
        key_column: &str,
        spec: &SnapshotSpec,
    ) -> Result<u64> {
        validate_ident(key_column)?;
        self.ensure_audience_snapshot_table()?;
        let sql = format!(
            "INSERT INTO {WRITE_CATALOG_ALIAS}.audience_snapshot (snapshot_id, campaign_id, \
             as_of_ts, user_id, features, hit_reason, tenant_id) SELECT CAST(? AS UUID), ?, \
             CAST(? AS TIMESTAMPTZ), sub.{key_column}, sub.features, sub.hit_reason, ? FROM \
             ({subquery_sql}) sub"
        );
        let mut params: Vec<Value> = vec![
            Value::Text(spec.snapshot_id.clone()),
            Value::Text(spec.campaign_id.clone()),
            Value::Text(spec.as_of_ts.clone()),
            Value::Text(self.tenant.clone()),
        ];
        params.extend_from_slice(subquery_params);
        let n = self
            .conn
            .execute(&sql, duckdb::params_from_iter(params.iter()))
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        self.bump_write();
        Ok(n as u64)
    }

    /// Export a snapshot to a Parquet file at `dest` (server-controlled path).
    ///
    /// # Errors
    /// - [`Error::Storage`] on failure.
    pub fn export_snapshot_parquet(&self, snapshot_id: &str, dest: &Path) -> Result<()> {
        let dest = escape_for_sql_literal(&dest.to_string_lossy());
        let sql = format!(
            "COPY (SELECT snapshot_id, campaign_id, as_of_ts, user_id, features, hit_reason FROM \
             {WRITE_CATALOG_ALIAS}.audience_snapshot WHERE snapshot_id = CAST(? AS UUID)) TO \
             '{dest}' (FORMAT 'PARQUET')"
        );
        self.conn
            .execute(&sql, duckdb::params![snapshot_id])
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        Ok(())
    }
}

/// Chunked multi-row INSERT helper shared by [`Writer::ingest_raw`] and
/// [`Writer::write_feature_rows`].
///
/// Why chunked: a per-row autocommit path creates **one DuckLake snapshot per
/// row** (a 50k-row seed produced 55k snapshots), which ballooned the catalog
/// and made every read-only attach cost ~500 ms — the real root of the P1-1
/// latency gap (read_path_spike, issue #12). A multi-row `VALUES` statement is
/// one commit, so a chunk keeps the catalog at one file/snapshot per chunk.
///
/// `cols` is the row width; the per-chunk row count is capped so the bound
/// parameters never exceed DuckDB's per-statement limit (`MAX_PARAMS`),
/// independent of how wide `cols` is. `bind` maps one row to its bound values.
/// Returns the total rows affected (DuckDB reports a multi-row `VALUES`
/// insert's row count).
fn insert_chunked<R>(
    conn: &Connection,
    insert_prefix: &str,
    cols: usize,
    rows: &[R],
    bind: impl Fn(&R) -> Vec<Value>,
) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }
    // DuckDB's default maximum bound parameters per prepared statement.
    const MAX_PARAMS: usize = 65_535;
    // 500 rows per chunk at typical widths; narrower automatically when wide
    // (e.g. a 1024-column table → 500 × 1024 = 512k params would exceed the
    // limit, so the chunk shrinks to 63 rows).
    const CHUNK_ROWS: usize = 500;
    let chunk_rows = CHUNK_ROWS.min(MAX_PARAMS / cols.max(1));
    let mut affected = 0usize;
    for chunk in rows.chunks(chunk_rows) {
        let row_sql = format!("({})", vec!["?"; cols].join(", "));
        let multi = vec![row_sql.as_str(); chunk.len()].join(", ");
        let sql = format!("{insert_prefix}{multi}");
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
        let params: Vec<Value> = chunk.iter().flat_map(&bind).collect();
        affected += stmt
            .execute(duckdb::params_from_iter(params.iter()))
            .map_err(|e| Error::Storage(BoxError::from(e)))?;
    }
    Ok(affected)
}

/// Build the `ATTACH ... AS dro (READ_ONLY)` SQL for the catalog. Centralised
/// so the execution reader can re-issue it to refresh its snapshot (a
/// long-lived read-only attach does not see commits made after attach).
#[must_use]
pub fn read_only_attach_sql(catalog_path: &Path, data_path: &Path) -> String {
    format!(
        "ATTACH 'ducklake:{}' AS {READ_ONLY_CATALOG_ALIAS} (DATA_PATH '{}', READ_ONLY);",
        escape_for_sql_literal(&catalog_path.to_string_lossy()),
        escape_for_sql_literal(&data_path.to_string_lossy()),
    )
}

/// Open a **read-only** connection attached to the same catalog. Inserts on this
/// connection are rejected by DuckLake. Note: a connection attached this way is
/// pinned to the snapshot at attach time; callers needing to see later commits
/// must re-attach (see [`read_only_attach_sql`]).
///
/// # Errors
/// - [`Error::Execution`] if attach fails.
pub fn open_reader(catalog_path: &Path, data_path: &Path) -> Result<Connection> {
    let conn = Connection::open_in_memory().map_err(|e| Error::Execution(BoxError::from(e)))?;
    load_ducklake(&conn).map_err(|e| match e {
        Error::Storage(b) => Error::Execution(b),
        other => other,
    })?;
    let sql = read_only_attach_sql(catalog_path, data_path);
    conn.execute_batch(&sql)
        .map_err(|e| Error::Execution(BoxError::from(e)))?;
    Ok(conn)
}

/// Escape backticks/quotes/backslashes in a filesystem path before embedding it
/// in an `ATTACH` literal. This is for the **path** (operator-supplied config),
/// not for row data; row data is always parameterized.
fn escape_for_sql_literal(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_writer() -> (tempfile::TempDir, Writer) {
        let tmp = tempfile::tempdir().expect("tmp");
        let w =
            Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("attach");
        (tmp, w)
    }

    #[test]
    fn test_should_ingest_and_count_raw_table() {
        let (_tmp, w) = tmp_writer();
        let n = w
            .ingest_raw(
                "erp",
                "users",
                &["id".into(), "name".into()],
                &[
                    vec![Some("u1".into()), Some("alice".into())],
                    vec![Some("u2".into()), Some("bob".into())],
                ],
            )
            .expect("ingest");
        assert_eq!(n, 2);
        let count: i64 = w
            .conn
            .query_row("SELECT count(*) FROM dl.raw_erp_users", [], |r| r.get(0))
            .expect("count");
        assert_eq!(count, 2);
    }

    #[test]
    fn test_should_ingest_multi_chunk_returns_total_and_keeps_catalog_small() {
        // Regression for the per-row-commit defect (issue #12): a multi-chunk
        // ingest must return the TOTAL row count (not per-statement), and must
        // not create one DuckLake snapshot per row — the catalog stays at a
        // handful of snapshots, which is what keeps the read attach cheap.
        let (_tmp, w) = tmp_writer();
        let rows: Vec<Vec<Option<String>>> = (0..1200)
            .map(|i| vec![Some(format!("u{i}")), Some("A".into())])
            .collect();
        let n = w
            .ingest_raw("erp", "orders", &["user_id".into(), "sku".into()], &rows)
            .expect("ingest");
        assert_eq!(n, 1200, "returned count must be the total rows inserted");
        let count: i64 = w
            .conn
            .query_row("SELECT count(*) FROM dl.raw_erp_orders", [], |r| r.get(0))
            .expect("count");
        assert_eq!(count, 1200);
        let snaps: i64 = w
            .conn
            .query_row("SELECT count(*) FROM ducklake_snapshots('dl')", [], |r| {
                r.get(0)
            })
            .expect("snapshots");
        assert!(
            snaps < 10,
            "1200 rows over 500-row chunks must stay at a handful of snapshots, got {snaps}"
        );
    }

    #[test]
    fn test_should_write_features_multi_chunk_returns_total() {
        let (_tmp, w) = tmp_writer();
        let rows: Vec<FeatureRow> = (0..1200)
            .map(|i| FeatureRow {
                user_id: format!("u{i}"),
                feature_name: "cadence.regularity".into(),
                num_value: 0.5,
                as_of_ts: "2025-01-01T00:00:00Z".into(),
                producer_id: "cadence_sql".into(),
            })
            .collect();
        let n = w.write_feature_rows(&rows).expect("write features");
        assert_eq!(n, 1200, "returned count must be the total rows inserted");
        let count: i64 = w
            .conn
            .query_row("SELECT count(*) FROM dl.feature_store", [], |r| r.get(0))
            .expect("count");
        assert_eq!(count, 1200);
    }

    #[test]
    fn test_should_reject_invalid_identifier() {
        let (_tmp, w) = tmp_writer();
        let res = w.ingest_raw("erp", "users; DROP", &["id".into()], &[]);
        assert!(matches!(res, Err(Error::InvalidInput(_))));
    }

    #[test]
    fn test_should_upsert_dedup_and_update_by_key() {
        // Dim upsert (specs/20 §4): same-key rows within a batch are deduplicated
        // (last wins) and the merged result never grows duplicate rows.
        let (_tmp, w) = tmp_writer();
        w.ingest_raw(
            "erp",
            "users",
            &["id".into(), "tier".into()],
            &[
                vec![Some("u1".into()), Some("gold".into())],
                vec![Some("u2".into()), Some("silver".into())],
            ],
        )
        .expect("seed");
        // One batch, u1 twice: u1 must end at the LAST value (tier=platinum);
        // u2 unchanged; no duplicate rows anywhere.
        let n = w
            .upsert_raw(
                "erp",
                "users",
                &["id".into(), "tier".into()],
                "id",
                &[
                    vec![Some("u1".into()), Some("bronze".into())],
                    vec![Some("u3".into()), Some("diamond".into())],
                    vec![Some("u1".into()), Some("platinum".into())],
                ],
            )
            .expect("upsert");
        assert_eq!(n, 2, "one update (u1, deduped) + one insert (u3)");
        let mut stmt = w
            .conn
            .prepare("SELECT id, tier FROM dl.raw_erp_users ORDER BY id")
            .expect("prepare");
        let rows: Vec<(String, String)> = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
            .expect("query")
            .map(|r| r.expect("row"))
            .collect();
        assert_eq!(
            rows,
            vec![
                ("u1".into(), "platinum".into()),
                ("u2".into(), "silver".into()),
                ("u3".into(), "diamond".into()),
            ],
            "last-wins dedup + update, no duplicate keys"
        );
    }

    #[test]
    fn test_should_reject_upsert_without_key_column() {
        let (_tmp, w) = tmp_writer();
        let res = w.upsert_raw(
            "erp",
            "users",
            &["id".into(), "tier".into()],
            "nope",
            &[vec![Some("u1".into()), Some("gold".into())]],
        );
        assert!(
            matches!(res, Err(Error::InvalidInput(_))),
            "merge key must be one of the columns"
        );
    }

    #[test]
    fn test_should_maintenance_pass_keep_rows_and_expire_when_supported() {
        // Issue #17 (specs/71 §4): the maintenance pass runs without failing
        // the sweep, current data always survives, and — on builds where the
        // timestamp-parameterized DuckLake procedures bind (duckdb 1.10505.0
        // does NOT — specs/93 GC-MAINT-BINDER) — old snapshots are actually
        // expired. The assertion adapts to the build capability so a future
        // DuckLake upgrade turns the graceful no-op into a real expiry.
        let (_tmp, w) = tmp_writer();
        // Several batches = several snapshots (one commit per batch).
        for b in 0..20 {
            let rows: Vec<Vec<Option<String>>> =
                (0..10).map(|i| vec![Some(format!("b{b}_r{i}"))]).collect();
            w.ingest_raw("erp", "evt", &["id".into()], &rows)
                .expect("ingest");
        }
        let snaps_before: i64 = w
            .conn
            .query_row(
                &format!("SELECT count(*) FROM ducklake_snapshots('{WRITE_CATALOG_ALIAS}')"),
                [],
                |r| r.get(0),
            )
            .expect("snapshots");
        assert!(
            snaps_before > 5,
            "seeded batches must produce multiple snapshots: {snaps_before}"
        );

        // Expire everything older than now (retention 0) + orphan cleanup must
        // not fail the sweep regardless of build capability.
        w.expire_snapshots(0).expect("expire");
        w.delete_orphaned_files(0).expect("orphans");

        let snaps_after: i64 = w
            .conn
            .query_row(
                &format!("SELECT count(*) FROM ducklake_snapshots('{WRITE_CATALOG_ALIAS}')"),
                [],
                |r| r.get(0),
            )
            .expect("snapshots");
        if w.maintenance_available.get() {
            assert!(
                snaps_after < snaps_before,
                "expiry must shrink the snapshot count: before={snaps_before} after={snaps_after}"
            );
        } else {
            // Build blocker: the procedures cannot bind; the sweep degrades
            // gracefully (documented in specs/93 GC-MAINT-BINDER).
            assert_eq!(
                snaps_after, snaps_before,
                "degraded maintenance must be a no-op on builds without TSTZ binding"
            );
        }
        // Current data survives the retention window either way.
        let rows: i64 = w
            .conn
            .query_row("SELECT count(*) FROM dl.raw_erp_evt", [], |r| r.get(0))
            .expect("rows");
        assert_eq!(rows, 200, "all 20*10 rows must survive maintenance");
    }

    #[test]
    fn test_should_logical_delete_by_key() {
        // Logical delete (specs/20 §4): MERGE ... THEN DELETE writes a catalog
        // delete-file — the row vanishes from reads while the Parquet data
        // files stay untouched (that is what makes it logical, not physical).
        let (_tmp, w) = tmp_writer();
        w.ingest_raw(
            "erp",
            "users",
            &["id".into(), "tier".into()],
            &[
                vec![Some("u1".into()), Some("gold".into())],
                vec![Some("u2".into()), Some("silver".into())],
            ],
        )
        .expect("seed");
        let files_before: i64 = w
            .conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM ducklake_list_files('{WRITE_CATALOG_ALIAS}', \
                     'raw_erp_users')"
                ),
                [],
                |r| r.get(0),
            )
            .expect("files");
        let n = w
            .delete_raw("erp", "users", "id", &["u1".into()])
            .expect("delete");
        assert_eq!(n, 1, "one row logically deleted");
        let count: i64 = w
            .conn
            .query_row("SELECT count(*) FROM dl.raw_erp_users", [], |r| r.get(0))
            .expect("count");
        assert_eq!(count, 1, "deleted row must be gone from reads");
        let files_after: i64 = w
            .conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM ducklake_list_files('{WRITE_CATALOG_ALIAS}', \
                     'raw_erp_users')"
                ),
                [],
                |r| r.get(0),
            )
            .expect("files");
        assert_eq!(
            files_before, files_after,
            "logical delete must not touch the Parquet data files"
        );
    }

    #[test]
    fn test_should_run_compact_without_error() {
        let (_tmp, w) = tmp_writer();
        w.ingest_raw("erp", "orders", &["id".into()], &[vec![Some("o1".into())]])
            .expect("ingest");
        w.compact("erp", "orders")
            .expect("compact is best-effort ok");
    }

    #[test]
    fn test_should_compact_reduce_file_count_and_preserve_rows_and_snapshots() {
        // R2 from spike-microbatch-compaction.md: seed many small batches (each
        // a data file once inlining is off), compact, assert the DuckLake file
        // count drops, all rows remain readable, and the snapshot history
        // (time-travel window) is retained.
        let (_tmp, w) = tmp_writer();
        for b in 0..20 {
            let rows: Vec<Vec<Option<String>>> =
                (0..50).map(|i| vec![Some(format!("b{b}_r{i}"))]).collect();
            w.ingest_raw("erp", "evt", &["id".into()], &rows)
                .expect("ingest");
        }
        let files = |w: &Writer| -> i64 {
            w.conn
                .query_row(
                    &format!(
                        "SELECT count(*) FROM ducklake_list_files('{WRITE_CATALOG_ALIAS}', \
                         'raw_erp_evt')"
                    ),
                    [],
                    |r| r.get(0),
                )
                .expect("list files")
        };
        let before = files(&w);
        let snapshots_before: i64 = w
            .conn
            .query_row(
                &format!("SELECT count(*) FROM ducklake_snapshots('{WRITE_CATALOG_ALIAS}')"),
                [],
                |r| r.get(0),
            )
            .expect("snapshots");
        assert!(
            before >= 20,
            "seeded batches must produce data files: {before}"
        );

        w.compact("erp", "evt").expect("compact");

        let after = files(&w);
        assert!(
            after < before,
            "compaction must reduce the file count: before={before} after={after}"
        );
        // All rows remain readable (no data loss from the merge).
        let rows: i64 = w
            .conn
            .query_row("SELECT count(*) FROM dl.raw_erp_evt", [], |r| r.get(0))
            .expect("rows");
        assert_eq!(rows, 1000, "all 20*50 rows must survive compaction");
        // The snapshot history (time-travel window) is retained: compaction
        // must not expire the pre-merge snapshots. (Reading a table AT a
        // historical snapshot is not resolvable in this DuckLake build — the
        // `AS OF` / `ducklake_scan` version APIs reject — so retention of the
        // snapshot list is the observable time-travel proxy; see
        // docs/research/perf-calibration.md.)
        let snapshots_after: i64 = w
            .conn
            .query_row(
                &format!("SELECT count(*) FROM ducklake_snapshots('{WRITE_CATALOG_ALIAS}')"),
                [],
                |r| r.get(0),
            )
            .expect("snapshots");
        assert!(
            snapshots_after >= snapshots_before,
            "compaction must not shrink the time-travel window: before={snapshots_before} \
             after={snapshots_after}"
        );
    }

    #[test]
    fn test_should_refuse_second_writer() {
        let (tmp, w) = tmp_writer();
        let second = Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data"));
        assert!(
            matches!(second, Err(Error::WriterAlreadyHeld)),
            "a second writer on the same catalog must be refused"
        );
        drop(w);
        // Once the first writer is released, a fresh attach must succeed.
        let again = Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data"));
        assert!(
            again.is_ok(),
            "writer must re-attach after the lock is released"
        );
    }

    #[test]
    fn test_should_persist_across_restart() {
        let (tmp, w) = tmp_writer();
        w.ingest_raw(
            "erp",
            "users",
            &["id".into()],
            &[vec![Some("u1".into())], vec![Some("u2".into())]],
        )
        .expect("ingest");
        drop(w);
        // The engine holds no unflushed state: re-attaching read-only must see
        // the previously committed rows (DuckLake durability).
        let r =
            open_reader(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("read attach");
        let mut stmt = r
            .prepare("SELECT count(*) FROM dro.raw_erp_users")
            .expect("prepare");
        let n: i64 = stmt.query_row([], |row| row.get(0)).expect("count");
        assert_eq!(n, 2);
    }

    fn feature_row(user: &str, value: f64, as_of: &str) -> consumer_engine_core::FeatureRow {
        consumer_engine_core::FeatureRow {
            user_id: user.into(),
            feature_name: "cadence.regularity".into(),
            num_value: value,
            as_of_ts: as_of.into(),
            producer_id: "cadence_sql".into(),
        }
    }

    #[test]
    fn test_should_write_and_read_feature_store() {
        let (_tmp, w) = tmp_writer();
        let rows = vec![
            feature_row("u1", 0.9, "2025-01-01T00:00:00Z"),
            feature_row("u2", 0.2, "2025-01-01T00:00:00Z"),
        ];
        let n = w.write_feature_rows(&rows).expect("write features");
        assert_eq!(n, 2);
        let count: i64 = w
            .conn
            .query_row("SELECT count(*) FROM dl.feature_store", [], |r| r.get(0))
            .expect("count");
        assert_eq!(count, 2);
    }

    #[test]
    fn test_should_leave_no_state_on_failed_write_features_and_refresh() {
        // A failed transactional write must leave the store empty and no view
        // (specs/20 I4: a partial batch is never observable).
        let (_tmp, w) = tmp_writer();
        let mut bad = feature_row("u1", 0.9, "2025-01-01T00:00:00Z");
        bad.feature_name = "cadence; DROP".into();
        let res =
            w.write_features_and_refresh(&[feature_row("u1", 0.9, "2025-01-01T00:00:00Z"), bad]);
        assert!(matches!(res, Err(Error::InvalidInput(_))));
        // Fully atomic: the rollback even undoes the CREATE TABLE — no table,
        // no rows, no view (specs/20 I4: a partial batch is never observable).
        let table_err = w
            .conn
            .query_row("SELECT count(*) FROM dl.feature_store", [], |r| {
                r.get::<_, i64>(0)
            });
        assert!(
            table_err.is_err(),
            "failed transaction must leave no feature_store table"
        );
    }

    #[test]
    fn test_should_reject_bad_feature_name() {
        let (_tmp, w) = tmp_writer();
        let mut r = feature_row("u1", 0.9, "2025-01-01T00:00:00Z");
        r.feature_name = "cadence; DROP".into();
        assert!(matches!(
            w.write_feature_rows(&[r]),
            Err(Error::InvalidInput(_))
        ));
    }

    #[test]
    fn test_should_stamp_tenant_on_every_engine_table() {
        use consumer_engine_core::{SemanticType, SuppressionAction, SuppressionChannel};
        // Issue #14: the writer stamps its configured tenant on every committed
        // row — raw, feature, suppression, snapshot and catalogue alike.
        let tmp = tempfile::tempdir().expect("tmp");
        let mut w =
            Writer::attach(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("attach");
        w.set_tenant("tenant_a".to_string());
        // raw
        w.ingest_raw(
            "erp",
            "orders",
            &["user_id".into(), "sku".into()],
            &[vec![Some("u1".into()), Some("A".into())]],
        )
        .expect("ingest");
        // feature
        w.write_feature_rows(&[FeatureRow {
            user_id: "u1".into(),
            feature_name: "cadence.regularity".into(),
            num_value: 1.0,
            as_of_ts: "2025-01-01T00:00:00Z".into(),
            producer_id: "cadence_sql".into(),
        }])
        .expect("features");
        // suppression
        w.write_suppression_idempotent(&[SuppressionRow {
            suppression_id: "11111111-2222-3333-4444-555555555555".into(),
            campaign_id: "c1".into(),
            user_id: "u1".into(),
            channel: SuppressionChannel::Email,
            action: SuppressionAction::Delivered,
            occurred_ts: "2025-01-01T00:00:00Z".into(),
            received_ts: "2025-01-01T00:00:01Z".into(),
        }])
        .expect("suppression");
        // catalogue
        w.write_catalog_rows(&[CatalogRow {
            entity_type: "column".into(),
            system: "erp".into(),
            table_name: "orders".into(),
            column_name: Some("user_id".into()),
            semantic_type: SemanticType::Identifier,
            data_type: "VARCHAR".into(),
            description: "user id".into(),
            pii_flag: false,
            sample_values: serde_json::json!([]),
            embedding: vec![0.0; 4],
            source_epoch: 1,
        }])
        .expect("catalog");
        // snapshot
        w.materialize_snapshot(
            "SELECT DISTINCT base.user_id, CAST('{}' AS JSON) AS features, CAST('{}' AS JSON) AS \
             hit_reason FROM dl.raw_erp_orders base",
            &[],
            "user_id",
            &SnapshotSpec {
                snapshot_id: "22222222-3333-4444-5555-666666666666".into(),
                campaign_id: "c1".into(),
                as_of_ts: "2025-01-01T00:00:00Z".into(),
            },
        )
        .expect("snapshot");

        for (table, sql) in [
            (
                "raw",
                "SELECT count(*) FROM dl.raw_erp_orders WHERE tenant_id = 'tenant_a'",
            ),
            (
                "feature_store",
                "SELECT count(*) FROM dl.feature_store WHERE tenant_id = 'tenant_a'",
            ),
            (
                "suppression",
                "SELECT count(*) FROM dl.suppression WHERE tenant_id = 'tenant_a'",
            ),
            (
                "semantic_catalog",
                "SELECT count(*) FROM dl.semantic_catalog WHERE tenant_id = 'tenant_a'",
            ),
            (
                "audience_snapshot",
                "SELECT count(*) FROM dl.audience_snapshot WHERE tenant_id = 'tenant_a'",
            ),
        ] {
            let n: i64 = w.conn.query_row(sql, [], |r| r.get(0)).expect("count");
            assert_eq!(
                n, 1,
                "{table} must carry exactly one tenant_a-stamped row (sql: {sql})"
            );
        }
    }

    #[test]
    fn test_should_write_suppression_idempotently() {
        use consumer_engine_core::{SuppressionAction, SuppressionChannel};
        let (_tmp, w) = tmp_writer();
        let row = SuppressionRow {
            suppression_id: "11111111-2222-3333-4444-555555555555".into(),
            campaign_id: "c1".into(),
            user_id: "u1".into(),
            channel: SuppressionChannel::Email,
            action: SuppressionAction::Delivered,
            occurred_ts: "2025-01-01T00:00:00Z".into(),
            received_ts: "2025-01-01T00:00:01Z".into(),
        };
        // First write inserts; re-writing the same suppression_id is a no-op.
        let n1 = w
            .write_suppression_idempotent(std::slice::from_ref(&row))
            .expect("write");
        let n2 = w
            .write_suppression_idempotent(std::slice::from_ref(&row))
            .expect("write");
        assert_eq!(n1, 1, "first write must insert");
        assert_eq!(
            n2, 0,
            "duplicate suppression_id must be skipped (idempotent)"
        );
        let count: i64 = w
            .conn
            .query_row("SELECT count(*) FROM dl.suppression", [], |r| r.get(0))
            .expect("count");
        assert_eq!(count, 1, "only one row despite the re-POST");
    }

    #[test]
    fn test_should_persist_suppression_across_restart() {
        // Write-through durability: an acked writeback survives a restart (the
        // "restart replays queued writeback without loss" AC — the ack is only
        // sent after the DuckLake commit).
        use consumer_engine_core::{SuppressionAction, SuppressionChannel};
        let (tmp, w) = tmp_writer();
        w.write_suppression_idempotent(&[SuppressionRow {
            suppression_id: "11111111-2222-3333-4444-555555555555".into(),
            campaign_id: "c1".into(),
            user_id: "u1".into(),
            channel: SuppressionChannel::Email,
            action: SuppressionAction::Delivered,
            occurred_ts: "2025-01-01T00:00:00Z".into(),
            received_ts: "2025-01-01T00:00:01Z".into(),
        }])
        .expect("write");
        drop(w);
        let r =
            open_reader(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("read attach");
        let count: i64 = r
            .query_row("SELECT count(*) FROM dro.suppression", [], |row| row.get(0))
            .expect("count");
        assert_eq!(count, 1, "suppression must survive restart");
    }

    #[test]
    fn test_should_refresh_feature_wide_view() {
        let (_tmp, w) = tmp_writer();
        // Two users; a newer as_of_ts supersedes for u1.
        let rows = vec![
            feature_row("u1", 0.5, "2025-01-01T00:00:00Z"),
            feature_row("u1", 0.9, "2025-01-02T00:00:00Z"),
            feature_row("u2", 0.2, "2025-01-01T00:00:00Z"),
        ];
        w.write_feature_rows(&rows).expect("write");
        w.refresh_feature_wide_view("cadence", &["regularity".into()])
            .expect("refresh view");
        // Latest value wins for u1 (0.9).
        let mut stmt = w
            .conn
            .prepare("SELECT user_id, regularity FROM dl.feature_wide_cadence ORDER BY user_id")
            .expect("prepare");
        let rows: Vec<(String, f64)> = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, f64>(1)?)))
            .expect("query")
            .map(|r| r.expect("row"))
            .collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "u1");
        assert!((rows[0].1 - 0.9).abs() < 1e-9, "latest value must win");
        assert!((rows[1].1 - 0.2).abs() < 1e-9);
    }

    #[test]
    fn test_should_reader_resolves_writer_wide_view() {
        // The load-bearing cross-alias test: a view created on the writer (`dl`)
        // must resolve when read via a fresh `open_reader` under `dro.*`.
        let (tmp, w) = tmp_writer();
        let rows = vec![
            feature_row("u1", 0.9, "2025-01-01T00:00:00Z"),
            feature_row("u2", 0.2, "2025-01-01T00:00:00Z"),
        ];
        w.write_feature_rows(&rows).expect("write");
        w.refresh_feature_wide_view("cadence", &["regularity".into()])
            .expect("refresh view");
        let r =
            open_reader(&tmp.path().join("cat.db"), &tmp.path().join("data")).expect("read attach");
        let mut stmt = r
            .prepare("SELECT count(*) FROM dro.feature_wide_cadence")
            .expect("prepare");
        let n: i64 = stmt.query_row([], |row| row.get(0)).expect("count");
        assert_eq!(n, 2, "reader must resolve the writer-created wide view");
    }

    #[test]
    fn test_should_append_versioned_catalog_rows() {
        // Issue #18 (spec 13 I5): a re-onboard APPENDS a newer catalogue row
        // (versioned delta) rather than overwriting — both entries stay, the
        // newest (by source_epoch) wins for staleness checks.
        let (_tmp, w) = tmp_writer();
        let mk = |epoch: i64| CatalogRow {
            entity_type: "column".into(),
            system: "erp".into(),
            table_name: "orders".into(),
            column_name: Some("sku".into()),
            semantic_type: consumer_engine_core::SemanticType::Dimension,
            data_type: "VARCHAR".into(),
            description: format!("sku v{epoch}"),
            pii_flag: false,
            sample_values: serde_json::json!([]),
            embedding: vec![0.0; 4],
            source_epoch: epoch,
        };
        w.write_catalog_rows(&[mk(1)]).expect("first onboard");
        w.write_catalog_rows(&[mk(2)]).expect("re-onboard");
        let count: i64 = w
            .conn
            .query_row(
                "SELECT count(*) FROM dl.semantic_catalog WHERE column_name = 'sku'",
                [],
                |r| r.get(0),
            )
            .expect("count");
        assert_eq!(
            count, 2,
            "re-onboard must append a versioned delta, not overwrite"
        );
        let max_epoch: i64 = w
            .conn
            .query_row(
                "SELECT max(source_epoch) FROM dl.semantic_catalog WHERE column_name = 'sku'",
                [],
                |r| r.get(0),
            )
            .expect("max epoch");
        assert_eq!(max_epoch, 2, "newest entry must carry the newer stamp");
    }

    #[test]
    fn test_should_write_catalog_rows() {
        let (_tmp, w) = tmp_writer();
        let row = consumer_engine_core::CatalogRow {
            entity_type: "column".into(),
            system: "erp".into(),
            table_name: "orders".into(),
            column_name: Some("user_id".into()),
            semantic_type: consumer_engine_core::SemanticType::Identifier,
            data_type: "VARCHAR".into(),
            description: "user id".into(),
            pii_flag: false,
            sample_values: serde_json::json!(["u1", "u2"]),
            embedding: vec![0.1, 0.2, 0.3],
            source_epoch: 0,
        };
        let n = w.write_catalog_rows(&[row]).expect("write catalog");
        assert_eq!(n, 1);
        let count: i64 = w
            .conn
            .query_row("SELECT count(*) FROM dl.semantic_catalog", [], |r| r.get(0))
            .expect("count");
        assert_eq!(count, 1);
    }
}
