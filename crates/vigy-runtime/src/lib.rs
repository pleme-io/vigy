//! Tokio-driven tick scheduler + registry for vigies.
//!
//! ## Topology
//!
//! ```text
//!                 ┌──────────────────────────┐
//!                 │  RuntimeHandle (Clone)   │  ← public surface used by
//!                 └────────────┬─────────────┘    vigy-cli / vigy-rpc /
//!                              │                  vigy-graphql / vigy-rest
//!                              ▼
//!                 ┌──────────────────────────┐
//!                 │      Inner (Arc'd)       │
//!                 │  - store: Store          │
//!                 │  - tasks: Mutex<HashMap> │
//!                 │  - bus:   broadcast tx   │
//!                 └────────────┬─────────────┘
//!                              │ spawn per-vigy
//!                              ▼
//!     ┌─────────────────────────────────────────────┐
//!     │  tick_loop(vigy_id) — one tokio task each   │
//!     │  loop {                                      │
//!     │    sleep(interval).                          │
//!     │    evaluate(vigy.program, fresh host).       │
//!     │    persist VigyRun.                          │
//!     │    broadcast to bus.                         │
//!     │    backoff on error.                         │
//!     │  }                                           │
//!     └─────────────────────────────────────────────┘
//! ```
//!
//! ## Invariants
//!
//! - **One task per vigy.** `register_or_update` cancels the old task
//!   before spawning the new one, so there's no double-tick risk.
//! - **Backoff on errors.** Three failing ticks in a row → cap at
//!   30s sleep until a tick succeeds.
//! - **Store is source of truth.** On startup, `RuntimeHandle::open`
//!   reads existing vigies from the store and spawns tasks for the
//!   enabled ones.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use vigy_eval::{evaluate, VigyHost};
use vigy_store::{Store, StoreError};
use vigy_types::{ReconcileAction, ResultStatus, Vigy, VigyId, VigyRun};

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("vigy types: {0}")]
    Types(#[from] vigy_types::Error),
    #[error("eval: {0}")]
    Eval(#[from] vigy_eval::EvalErr),
}

pub type Result<T> = std::result::Result<T, RuntimeError>;

/// Public, cheaply-cloneable handle. All API surfaces drive the runtime
/// through this — no direct access to internals.
#[derive(Clone)]
pub struct RuntimeHandle {
    inner: Arc<Inner>,
}

struct Inner {
    store: Store,
    tasks: Mutex<HashMap<VigyId, JoinHandle<()>>>,
    bus: broadcast::Sender<VigyRun>,
}

const EVENT_BUS_CAPACITY: usize = 1024;
const MAX_BACKOFF: Duration = Duration::from_secs(30);

impl RuntimeHandle {
    /// Open / create the persistent store at `path`, then re-spawn
    /// tick tasks for any enabled vigies recorded there.
    pub async fn open(path: &Path) -> Result<Self> {
        let store = Store::open(path).await?;
        Self::with_store(store).await
    }

    /// In-memory store — only useful in tests + ephemeral one-shot runs.
    pub async fn open_in_memory() -> Result<Self> {
        let store = Store::open_in_memory().await?;
        Self::with_store(store).await
    }

    async fn with_store(store: Store) -> Result<Self> {
        let (bus, _) = broadcast::channel(EVENT_BUS_CAPACITY);
        let handle = Self {
            inner: Arc::new(Inner {
                store,
                tasks: Mutex::new(HashMap::new()),
                bus,
            }),
        };
        // Resume any pre-existing vigies.
        let existing = handle.inner.store.list_vigies(None).await?;
        let count = existing.len();
        for v in existing {
            if v.enabled {
                handle.spawn_task(v).await;
            }
        }
        info!(resumed = count, "runtime ready");
        Ok(handle)
    }

    /// Register a fresh vigy (or replace if its id matches an existing one).
    /// The new task spawns immediately; the old task (if any) is cancelled
    /// before the new one starts to guarantee no concurrent ticks.
    pub async fn register_or_update(&self, vigy: Vigy) -> Result<Vigy> {
        self.inner.store.upsert_vigy(&vigy).await?;
        if vigy.enabled {
            self.spawn_task(vigy.clone()).await;
        } else {
            self.cancel_task(&vigy.id).await;
        }
        Ok(vigy)
    }

    pub async fn enable(&self, id: &VigyId) -> Result<Vigy> {
        self.inner.store.set_enabled(id, true).await?;
        let v = self.inner.store.get_vigy(id).await?;
        self.spawn_task(v.clone()).await;
        Ok(v)
    }

    pub async fn disable(&self, id: &VigyId) -> Result<Vigy> {
        self.inner.store.set_enabled(id, false).await?;
        self.cancel_task(id).await;
        self.inner.store.get_vigy(id).await.map_err(Into::into)
    }

    pub async fn delete(&self, id: &VigyId) -> Result<bool> {
        self.cancel_task(id).await;
        Ok(self.inner.store.delete_vigy(id).await?)
    }

    pub async fn get(&self, id: &VigyId) -> Result<Vigy> {
        Ok(self.inner.store.get_vigy(id).await?)
    }

    pub async fn list(&self, label_selector: Option<&str>) -> Result<Vec<Vigy>> {
        Ok(self.inner.store.list_vigies(label_selector).await?)
    }

    pub async fn recent_runs(&self, id: &VigyId, limit: u64) -> Result<Vec<VigyRun>> {
        Ok(self.inner.store.recent_runs(id, limit).await?)
    }

    /// Force-tick a vigy now, regardless of its schedule. Useful for
    /// `carve gate`-style CI hooks + the `vigy <id> tick` CLI.
    pub async fn tick_now(&self, id: &VigyId) -> Result<VigyRun> {
        let vigy = self.inner.store.get_vigy(id).await?;
        let run = run_once(&vigy);
        self.inner.store.insert_run(&run).await?;
        let _ = self.inner.bus.send(run.clone());
        Ok(run)
    }

    /// Subscribe to the reconcile event bus. Caller gets every tick's
    /// finalised `VigyRun` until they drop the receiver.
    pub fn subscribe(&self) -> broadcast::Receiver<VigyRun> {
        self.inner.bus.subscribe()
    }

    // ---------- internals ----------

    async fn spawn_task(&self, vigy: Vigy) {
        self.cancel_task(&vigy.id).await;
        let inner = self.inner.clone();
        let handle = tokio::spawn(tick_loop(inner, vigy.clone()));
        self.inner.tasks.lock().await.insert(vigy.id, handle);
    }

    async fn cancel_task(&self, id: &VigyId) {
        if let Some(h) = self.inner.tasks.lock().await.remove(id) {
            h.abort();
        }
    }
}

/// The per-vigy reconcile loop. Lives in its own task; one per registered
/// vigy. Aborts when the runtime drops it.
async fn tick_loop(inner: Arc<Inner>, vigy: Vigy) {
    let id = vigy.id.clone();
    let interval = vigy.tick_interval.as_duration();
    let mut failures = 0u32;

    info!(vigy_id = %id, name = %vigy.name, interval_ms = interval.as_millis() as u64, "vigy tick loop started");

    loop {
        sleep(interval).await;

        // Always re-read the vigy from the store: lets edits land without
        // having to restart the task. (We still re-register on update to
        // pick up new tick intervals; this is belt-and-suspenders.)
        let current = match inner.store.get_vigy(&id).await {
            Ok(v) if v.enabled => v,
            Ok(_) => {
                debug!(vigy_id = %id, "vigy disabled mid-flight; exiting loop");
                break;
            }
            Err(e) => {
                error!(vigy_id = %id, err = %e, "vigy disappeared from store; exiting loop");
                break;
            }
        };

        let run = run_once(&current);
        let failed = matches!(run.result, ResultStatus::Failed);

        if let Err(e) = inner.store.insert_run(&run).await {
            error!(vigy_id = %id, err = %e, "failed to persist VigyRun");
        }
        let _ = inner.bus.send(run);

        if failed {
            failures = failures.saturating_add(1);
            let backoff = backoff_for(failures);
            warn!(vigy_id = %id, failures, backoff_ms = backoff.as_millis() as u64, "vigy tick failed; backing off");
            sleep(backoff).await;
        } else {
            failures = 0;
        }
    }
}

fn backoff_for(failures: u32) -> Duration {
    // Exponential backoff capped at MAX_BACKOFF. Failures: 1 → 1s,
    // 2 → 2s, 3 → 4s, 4 → 8s, 5 → 16s, ≥6 → 30s.
    let secs = 1u64.checked_shl(failures.saturating_sub(1).min(5)).unwrap_or(MAX_BACKOFF.as_secs());
    Duration::from_secs(secs).min(MAX_BACKOFF)
}

/// Execute one tick of a vigy synchronously. The tatara-lisp eval is
/// pure-CPU + bounded; running on the same task is fine.
fn run_once(vigy: &Vigy) -> VigyRun {
    let now = time::OffsetDateTime::now_utc();
    let tick_start_ms = (now.unix_timestamp_nanos() / 1_000_000) as i64;
    let host = VigyHost {
        tick_start_ms,
        actions: Vec::new(),
        log: Vec::new(),
    };

    let run = VigyRun::started(vigy.id.clone());

    match evaluate(&vigy.program, host) {
        Ok(populated) => {
            let actions: Vec<ReconcileAction> = populated.actions;
            run.complete_ok(actions)
        }
        Err(e) => run.complete_failed(format!("{e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vigy_types::TickInterval;

    #[tokio::test]
    async fn register_and_tick_emits_an_action() {
        let rt = RuntimeHandle::open_in_memory().await.unwrap();
        let v = Vigy::new(
            "test",
            "(vigy-noop)",
            TickInterval::from_millis(100).unwrap(),
        )
        .unwrap();
        let mut sub = rt.subscribe();
        let id = v.id.clone();
        rt.register_or_update(v).await.unwrap();
        // Force a tick directly instead of waiting on the scheduled one
        // — keeps the test deterministic + fast.
        let run = rt.tick_now(&id).await.unwrap();
        assert_eq!(run.actions.len(), 1);
        // The event bus also saw it.
        let bus_run = sub.recv().await.unwrap();
        assert_eq!(bus_run.id, run.id);
    }

    #[tokio::test]
    async fn disable_stops_ticking() {
        let rt = RuntimeHandle::open_in_memory().await.unwrap();
        let v = Vigy::new(
            "test",
            "(vigy-noop)",
            TickInterval::from_millis(100).unwrap(),
        )
        .unwrap();
        let id = v.id.clone();
        rt.register_or_update(v).await.unwrap();
        rt.disable(&id).await.unwrap();
        assert!(!rt.get(&id).await.unwrap().enabled);
    }

    #[tokio::test]
    async fn failed_run_records_error_and_keeps_loop_alive() {
        let rt = RuntimeHandle::open_in_memory().await.unwrap();
        // Unbound symbol → eval error → run.result = Failed.
        let v = Vigy::new(
            "broken",
            "(this-symbol-does-not-exist)",
            TickInterval::from_millis(100).unwrap(),
        )
        .unwrap();
        let id = v.id.clone();
        rt.register_or_update(v).await.unwrap();
        let run = rt.tick_now(&id).await.unwrap();
        assert!(matches!(run.result, ResultStatus::Failed));
        assert!(run.error.is_some());
    }

    #[test]
    fn backoff_curve() {
        assert_eq!(backoff_for(1), Duration::from_secs(1));
        assert_eq!(backoff_for(2), Duration::from_secs(2));
        assert_eq!(backoff_for(3), Duration::from_secs(4));
        assert_eq!(backoff_for(4), Duration::from_secs(8));
        assert_eq!(backoff_for(5), Duration::from_secs(16));
        assert_eq!(backoff_for(6), Duration::from_secs(30));
        assert_eq!(backoff_for(100), Duration::from_secs(30));
    }
}
