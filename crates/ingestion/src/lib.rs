//! The single ingestion writer actor.
//!
//! Owns the sole [`Writer`] to DuckLake (decision D3) inside a dedicated OS
//! thread — `duckdb::Connection` is not `Sync`. The async side sends commands
//! over a `flume` channel and awaits typed replies.
//!
//! For T1, an `IngestRaw` command flushes the supplied batch immediately via the
//! writer's multi-row parameterised insert (the batch is the micro-batch at the
//! SQL level). CDC-driven cross-call accumulation on the configured flush
//! threshold lands with the CDC adapter (survey-cdc-adapter.md). A
//! [`IngestionHandle::compact_all`] entry point plus the server's interval task
//! wire compaction (decision D6).

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]

use std::{collections::HashSet, thread};

use consumer_engine_core::{BoxError, Error, Result};
use consumer_engine_storage::Writer;

/// Commands sent to the writer thread.
enum Cmd {
    /// Create/insert into a `raw_*` table.
    IngestRaw {
        /// Source system identifier.
        system: String,
        /// Source entity (table) identifier.
        entity: String,
        /// Column names.
        columns: Vec<String>,
        /// Rows of optional string cells.
        rows: Vec<Vec<Option<String>>>,
        /// Reply channel carrying the inserted row count.
        reply: flume::Sender<Result<usize>>,
    },
    /// Compact one table.
    Compact {
        /// Source system.
        system: String,
        /// Source entity.
        entity: String,
        /// Reply channel.
        reply: flume::Sender<Result<()>>,
    },
    /// Compact every table this actor has ingested.
    CompactAll {
        /// Reply channel.
        reply: flume::Sender<Result<()>>,
    },
    /// Stop the writer thread.
    Shutdown,
}

/// Handle to the single ingestion writer. Cheap to clone.
#[derive(Clone)]
pub struct IngestionHandle {
    tx: flume::Sender<Cmd>,
}

impl std::fmt::Debug for IngestionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestionHandle").finish_non_exhaustive()
    }
}

impl IngestionHandle {
    /// Start the writer thread owning `writer`. Exactly one handle should be
    /// built per catalog (the writer's file lock enforces singleness).
    ///
    /// # Errors
    /// - [`Error::Ingestion`] if the thread cannot be spawned.
    pub fn start(writer: Writer) -> Result<Self> {
        let (tx, rx) = flume::bounded::<Cmd>(64);
        thread::Builder::new()
            .name("ce-ingestion".into())
            .spawn(move || writer_loop(writer, rx))
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        Ok(Self { tx })
    }

    /// Ingest a batch into `raw_<system>_<entity>`. Returns the row count.
    ///
    /// # Errors
    /// Propagates validation/storage errors from the writer.
    pub async fn ingest_raw(
        &self,
        system: impl Into<String>,
        entity: impl Into<String>,
        columns: Vec<String>,
        rows: Vec<Vec<Option<String>>>,
    ) -> Result<usize> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::IngestRaw {
                system: system.into(),
                entity: entity.into(),
                columns,
                rows,
                reply: rtx,
            })
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        rrx.recv_async()
            .await
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?
    }

    /// Compact a single table.
    ///
    /// # Errors
    /// Propagates storage errors from the writer.
    pub async fn compact(
        &self,
        system: impl Into<String>,
        entity: impl Into<String>,
    ) -> Result<()> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::Compact {
                system: system.into(),
                entity: entity.into(),
                reply: rtx,
            })
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        rrx.recv_async()
            .await
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?
    }

    /// Compact every table ingested so far (best-effort; last error wins).
    ///
    /// # Errors
    /// Propagates the first storage error encountered.
    pub async fn compact_all(&self) -> Result<()> {
        let (rtx, rrx) = flume::bounded(1);
        self.tx
            .send(Cmd::CompactAll { reply: rtx })
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?;
        rrx.recv_async()
            .await
            .map_err(|e| Error::Ingestion(BoxError::from(e)))?
    }

    /// Signal the writer thread to stop. Best-effort.
    pub fn shutdown(&self) {
        let _ = self.tx.send(Cmd::Shutdown);
    }
}

/// The writer thread body: own the writer, track known tables, serve commands.
fn writer_loop(writer: Writer, rx: flume::Receiver<Cmd>) {
    let mut known: HashSet<(String, String)> = HashSet::new();
    for cmd in rx.iter() {
        match cmd {
            Cmd::IngestRaw {
                system,
                entity,
                columns,
                rows,
                reply,
            } => {
                let res = writer.ingest_raw(&system, &entity, &columns, &rows);
                if res.is_ok() {
                    known.insert((system, entity));
                }
                let _ = reply.send(res);
            }
            Cmd::Compact {
                system,
                entity,
                reply,
            } => {
                let _ = reply.send(writer.compact(&system, &entity));
            }
            Cmd::CompactAll { reply } => {
                let mut last: Result<()> = Ok(());
                for (s, e) in &known {
                    if let Err(err) = writer.compact(s, e) {
                        last = Err(err);
                    }
                }
                let _ = reply.send(last);
            }
            Cmd::Shutdown => break,
        }
    }
}
