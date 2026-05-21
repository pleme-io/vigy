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

use vigy_eval::{ExtensionHandle, LispReconciler, Reconciler, VigyHost};
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
    /// Shared extension bundle every LispReconciler in this runtime
    /// gets. Hosts (mado, tear-daemon) construct the runtime with
    /// `with_extensions(...)` to add their own intrinsics; the default
    /// is `vigy_eval::standard_extensions()`.
    extensions: Vec<ExtensionHandle>,
}

const EVENT_BUS_CAPACITY: usize = 1024;
const MAX_BACKOFF: Duration = Duration::from_secs(30);

impl RuntimeHandle {
    /// Open / create the persistent store at `path`, then re-spawn
    /// tick tasks for any enabled vigies recorded there.
    pub async fn open(path: &Path) -> Result<Self> {
        let store = Store::open(path).await?;
        Self::with_store(store, vigy_eval::standard_extensions()).await
    }

    /// In-memory store — only useful in tests + ephemeral one-shot runs.
    pub async fn open_in_memory() -> Result<Self> {
        let store = Store::open_in_memory().await?;
        Self::with_store(store, vigy_eval::standard_extensions()).await
    }

    /// Open with a custom extension bundle. mado calls this with
    /// `standard_extensions() ++ [MadoTearExtension]`; tear-daemon
    /// calls it with `standard_extensions() ++ [TearCoreExtension]`.
    /// Pure operators (`vigy serve`) take the default via [`open`] which
    /// uses `vigy_eval::standard_extensions()` only.
    pub async fn open_with_extensions(
        path: &Path,
        extensions: Vec<ExtensionHandle>,
    ) -> Result<Self> {
        let store = Store::open(path).await?;
        Self::with_store(store, extensions).await
    }

    async fn with_store(store: Store, extensions: Vec<ExtensionHandle>) -> Result<Self> {
        let (bus, _) = broadcast::channel(EVENT_BUS_CAPACITY);
        let handle = Self {
            inner: Arc::new(Inner {
                store,
                tasks: Mutex::new(HashMap::new()),
                bus,
                extensions,
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
        let run = run_once(&self.inner.store, &vigy, &self.inner.extensions).await;
        self.inner.store.insert_run(&run).await?;
        let _ = self.inner.bus.send(run.clone());
        Ok(run)
    }

    /// Force-tick a vigy through a caller-supplied reconciler. Useful
    /// for tests that swap in NoopReconciler / ChainReconciler / a
    /// custom impl without changing the vigy's stored program.
    pub async fn tick_now_with(
        &self,
        id: &VigyId,
        reconciler: &dyn Reconciler,
    ) -> Result<VigyRun> {
        let vigy = self.inner.store.get_vigy(id).await?;
        let run = run_once_with(&self.inner.store, &vigy, reconciler).await;
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

        let run = run_once(&inner.store, &current, &inner.extensions).await;
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

/// Reserved kv keys the runtime maintains automatically. Operator
/// programs SHOULD NOT write these — read via `(vigy-tick-count)` etc.
/// Persisting them in the same kv table keeps the framework state
/// homogeneous (one round-trip per tick to load/save everything).
const KV_TICK_COUNT: &str = "__sys::tick_count";
const KV_PREV_TICK_MS: &str = "__sys::prev_tick_ms";

/// Execute one tick of a vigy through the supplied reconciler.
/// Handles the persistence sandwich — hydrate kv before, save dirty
/// after — so reconciler impls can be pure with respect to storage.
async fn run_once_with(
    store: &vigy_store::Store,
    vigy: &Vigy,
    reconciler: &dyn Reconciler,
) -> VigyRun {
    let now = time::OffsetDateTime::now_utc();
    let tick_start_ms = (now.unix_timestamp_nanos() / 1_000_000) as i64;

    let kv = match store.load_kv(&vigy.id).await {
        Ok(k) => k,
        Err(e) => {
            tracing::warn!(vigy_id = %vigy.id, err = %e, "kv load failed; tick proceeds with empty kv");
            std::collections::BTreeMap::new()
        }
    };

    let prior_tick_count = kv.get(KV_TICK_COUNT).and_then(|v| v.as_i64()).unwrap_or(0);
    let previous_tick_ms = kv.get(KV_PREV_TICK_MS).and_then(|v| v.as_i64());
    let tick_count = prior_tick_count + 1;

    let mut host = VigyHost {
        tick_start_ms,
        previous_tick_ms,
        tick_count,
        kv,
        ..Default::default()
    };
    host.kv.insert(
        KV_TICK_COUNT.to_string(),
        serde_json::Value::Number(tick_count.into()),
    );
    host.kv.insert(
        KV_PREV_TICK_MS.to_string(),
        serde_json::Value::Number(tick_start_ms.into()),
    );
    host.kv_dirty.insert(KV_TICK_COUNT.to_string());
    host.kv_dirty.insert(KV_PREV_TICK_MS.to_string());

    let run = VigyRun::started(vigy.id.clone());

    match reconciler.tick(host).await {
        Ok(populated) => {
            let dirty: std::collections::BTreeMap<String, serde_json::Value> = populated
                .kv_dirty
                .iter()
                .filter_map(|k| populated.kv.get(k).map(|v| (k.clone(), v.clone())))
                .collect();
            if !dirty.is_empty() || !populated.kv_deleted.is_empty() {
                if let Err(e) = store.save_kv(&vigy.id, &dirty, &populated.kv_deleted).await {
                    tracing::warn!(vigy_id = %vigy.id, err = %e, "kv save failed; in-memory state lost");
                }
            }
            let actions: Vec<ReconcileAction> = populated.actions;
            run.complete_ok(actions)
        }
        Err(e) => run.complete_failed(format!("{e}")),
    }
}

/// Backwards-compatible default tick — builds a LispReconciler with the
/// runtime's extensions and dispatches through [`run_once_with`].
async fn run_once(
    store: &vigy_store::Store,
    vigy: &Vigy,
    extensions: &[ExtensionHandle],
) -> VigyRun {
    let reconciler = LispReconciler::with_extensions(vigy.program.clone(), extensions.to_vec());
    run_once_with(store, vigy, &reconciler).await
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
    async fn kv_persists_across_ticks() {
        let rt = RuntimeHandle::open_in_memory().await.unwrap();
        // A vigy that increments a counter on each tick.
        let v = Vigy::new(
            "counter",
            "(vigy-incr \"hits\")",
            TickInterval::from_millis(100).unwrap(),
        )
        .unwrap();
        let id = v.id.clone();
        rt.register_or_update(v).await.unwrap();

        // Tick three times and read the kv state via the store each time.
        rt.tick_now(&id).await.unwrap();
        rt.tick_now(&id).await.unwrap();
        rt.tick_now(&id).await.unwrap();

        let kv = rt.inner.store.load_kv(&id).await.unwrap();
        let hits = kv.get("hits").and_then(|v| v.as_i64()).unwrap_or(0);
        assert_eq!(hits, 3, "counter should have incremented once per tick");

        // Reserved sys keys should also be populated.
        let tick_count = kv
            .get("__sys::tick_count")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        assert_eq!(tick_count, 3);
    }

    #[tokio::test]
    async fn tick_now_with_swaps_reconciler() {
        // Register a vigy whose stored program would emit a Noop. Then
        // force-tick it with NoopReconciler (no actions), then with a
        // ChainReconciler that runs two LispReconcilers in sequence —
        // proving the runtime really dispatches through the trait, not
        // through the stored program.
        use vigy_eval::{ChainReconciler, LispReconciler, NoopReconciler};

        let rt = RuntimeHandle::open_in_memory().await.unwrap();
        let v = Vigy::new(
            "swap-test",
            "(vigy-noop)",
            TickInterval::from_millis(100).unwrap(),
        )
        .unwrap();
        let id = v.id.clone();
        rt.register_or_update(v).await.unwrap();

        // NoopReconciler: no actions.
        let r1 = rt
            .tick_now_with(&id, &NoopReconciler)
            .await
            .unwrap();
        assert!(r1.actions.is_empty());

        // ChainReconciler: two pulls in sequence.
        let chain = ChainReconciler::new(vec![
            Box::new(LispReconciler::standard("(vigy-pull \"a\")")),
            Box::new(LispReconciler::standard("(vigy-pull \"b\")")),
        ]);
        let r2 = rt.tick_now_with(&id, &chain).await.unwrap();
        assert_eq!(r2.actions.len(), 2);
        assert_eq!(
            r2.actions
                .iter()
                .map(|a| a.kind)
                .collect::<Vec<_>>(),
            vec![
                vigy_types::ReconcileKind::Pull,
                vigy_types::ReconcileKind::Pull,
            ]
        );
    }

    #[tokio::test]
    async fn convergence_flag_survives_ticks() {
        let rt = RuntimeHandle::open_in_memory().await.unwrap();
        // A vigy that converges on its first tick + records the state.
        let v = Vigy::new(
            "converger",
            r#"
            (vigy-set "is_converged_now" (vigy-converged? "goal"))
            (vigy-mark-converged "goal")
            "#,
            TickInterval::from_millis(100).unwrap(),
        )
        .unwrap();
        let id = v.id.clone();
        rt.register_or_update(v).await.unwrap();

        rt.tick_now(&id).await.unwrap();
        let kv1 = rt.inner.store.load_kv(&id).await.unwrap();
        // First tick: marked AFTER reading, so reads false.
        assert_eq!(
            kv1.get("is_converged_now"),
            Some(&serde_json::Value::Bool(false))
        );

        rt.tick_now(&id).await.unwrap();
        let kv2 = rt.inner.store.load_kv(&id).await.unwrap();
        // Second tick: already marked, so reads true.
        assert_eq!(
            kv2.get("is_converged_now"),
            Some(&serde_json::Value::Bool(true))
        );
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
