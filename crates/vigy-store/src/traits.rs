//! `VigyStore` — the persistence trait.
//!
//! Every method maps 1:1 to the operations the runtime needs. No
//! query DSL escapes the impl boundary — callers see a small set of
//! typed operations on domain types, never SeaORM internals.

use crate::store::StoreError;
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use vigy_types::{Vigy, VigyId, VigyRun};

pub type Result<T> = std::result::Result<T, StoreError>;

/// Persistence surface for vigy. Hosts pick the impl that matches
/// their durability requirements; tests prefer [`crate::InMemoryStore`].
#[async_trait]
pub trait VigyStore: Send + Sync {
    // ── vigy lifecycle ─────────────────────────────────────────

    async fn upsert_vigy(&self, vigy: &Vigy) -> Result<()>;
    async fn get_vigy(&self, id: &VigyId) -> Result<Vigy>;
    async fn list_vigies(&self, label_selector: Option<&str>) -> Result<Vec<Vigy>>;
    async fn set_enabled(&self, id: &VigyId, enabled: bool) -> Result<()>;
    async fn delete_vigy(&self, id: &VigyId) -> Result<bool>;

    // ── runs ─────────────────────────────────────────────────

    async fn insert_run(&self, run: &VigyRun) -> Result<()>;
    async fn recent_runs(&self, vigy_id: &VigyId, limit: u64) -> Result<Vec<VigyRun>>;

    // ── persistent kv ─────────────────────────────────────────

    async fn load_kv(
        &self,
        vigy_id: &VigyId,
    ) -> Result<BTreeMap<String, serde_json::Value>>;

    async fn save_kv(
        &self,
        vigy_id: &VigyId,
        dirty: &BTreeMap<String, serde_json::Value>,
        deleted: &BTreeSet<String>,
    ) -> Result<()>;
}
