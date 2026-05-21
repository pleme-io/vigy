//! In-memory implementation of [`VigyStore`].
//!
//! Drop-in for tests + ephemeral hosts that don't want a SQLite file.
//! Same semantics as [`crate::SeaormStore`] — same trait, same return
//! types, same error variants. Difference: state lives in `parking_lot`
//! locks behind an `Arc`; the process exit erases everything.

use crate::store::{Result, StoreError};
use crate::traits::VigyStore;
use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use vigy_types::{Vigy, VigyId, VigyRun};

#[derive(Default)]
struct State {
    vigies: HashMap<String, Vigy>,
    runs: Vec<VigyRun>,
    kv: HashMap<(String, String), serde_json::Value>,
}

#[derive(Clone, Default)]
pub struct InMemoryStore {
    state: Arc<RwLock<State>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl VigyStore for InMemoryStore {
    async fn upsert_vigy(&self, vigy: &Vigy) -> Result<()> {
        self.state
            .write()
            .vigies
            .insert(vigy.id.to_string(), vigy.clone());
        Ok(())
    }

    async fn get_vigy(&self, id: &VigyId) -> Result<Vigy> {
        self.state
            .read()
            .vigies
            .get(id.as_str())
            .cloned()
            .ok_or_else(|| StoreError::NotFound {
                kind: "vigy",
                id: id.to_string(),
            })
    }

    async fn list_vigies(&self, label_selector: Option<&str>) -> Result<Vec<Vigy>> {
        let state = self.state.read();
        let mut out = Vec::with_capacity(state.vigies.len());
        for v in state.vigies.values() {
            match label_selector {
                Some(sel) => {
                    if v.labels.matches_selector(sel)? {
                        out.push(v.clone());
                    }
                }
                None => out.push(v.clone()),
            }
        }
        Ok(out)
    }

    async fn set_enabled(&self, id: &VigyId, enabled: bool) -> Result<()> {
        let mut state = self.state.write();
        let v = state
            .vigies
            .get_mut(id.as_str())
            .ok_or_else(|| StoreError::NotFound {
                kind: "vigy",
                id: id.to_string(),
            })?;
        v.enabled = enabled;
        v.updated_at = time::OffsetDateTime::now_utc();
        Ok(())
    }

    async fn delete_vigy(&self, id: &VigyId) -> Result<bool> {
        Ok(self.state.write().vigies.remove(id.as_str()).is_some())
    }

    async fn insert_run(&self, run: &VigyRun) -> Result<()> {
        self.state.write().runs.push(run.clone());
        Ok(())
    }

    async fn recent_runs(&self, vigy_id: &VigyId, limit: u64) -> Result<Vec<VigyRun>> {
        let state = self.state.read();
        let mut filtered: Vec<VigyRun> = state
            .runs
            .iter()
            .filter(|r| r.vigy_id == *vigy_id)
            .cloned()
            .collect();
        filtered.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        filtered.truncate(limit as usize);
        Ok(filtered)
    }

    async fn load_kv(
        &self,
        vigy_id: &VigyId,
    ) -> Result<BTreeMap<String, serde_json::Value>> {
        let state = self.state.read();
        Ok(state
            .kv
            .iter()
            .filter(|((vid, _k), _v)| vid == vigy_id.as_str())
            .map(|((_vid, k), v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn save_kv(
        &self,
        vigy_id: &VigyId,
        dirty: &BTreeMap<String, serde_json::Value>,
        deleted: &BTreeSet<String>,
    ) -> Result<()> {
        let mut state = self.state.write();
        for (k, v) in dirty {
            state.kv.insert((vigy_id.to_string(), k.clone()), v.clone());
        }
        for k in deleted {
            state.kv.remove(&(vigy_id.to_string(), k.clone()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vigy_types::{TickInterval, Vigy};

    #[tokio::test]
    async fn round_trips_a_vigy() {
        let s = InMemoryStore::new();
        let v = Vigy::new("t", "(vigy-noop)", TickInterval::default()).unwrap();
        s.upsert_vigy(&v).await.unwrap();
        let got = s.get_vigy(&v.id).await.unwrap();
        assert_eq!(got.id, v.id);
    }

    #[tokio::test]
    async fn kv_round_trips() {
        let s = InMemoryStore::new();
        let v = Vigy::new("t", "(vigy-noop)", TickInterval::default()).unwrap();
        s.upsert_vigy(&v).await.unwrap();
        let mut dirty = BTreeMap::new();
        dirty.insert("k".into(), serde_json::json!(42));
        s.save_kv(&v.id, &dirty, &BTreeSet::new()).await.unwrap();
        let loaded = s.load_kv(&v.id).await.unwrap();
        assert_eq!(loaded.get("k").and_then(|v| v.as_i64()), Some(42));
    }

    #[tokio::test]
    async fn label_selector_filters() {
        let s = InMemoryStore::new();
        let mut a = Vigy::new("a", "(vigy-noop)", TickInterval::default()).unwrap();
        a.labels.insert("host", "mado").unwrap();
        let mut b = Vigy::new("b", "(vigy-noop)", TickInterval::default()).unwrap();
        b.labels.insert("host", "tear").unwrap();
        s.upsert_vigy(&a).await.unwrap();
        s.upsert_vigy(&b).await.unwrap();
        let mado_only = s.list_vigies(Some("host=mado")).await.unwrap();
        assert_eq!(mado_only.len(), 1);
        assert_eq!(mado_only[0].name, "a");
    }
}
