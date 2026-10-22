//! Tatara-lisp host bindings + framework intrinsics for vigy.
//!
//! ## The host
//!
//! [`VigyHost`] is the per-tick context handed to a vigy program.
//! It carries five buffers the program populates via intrinsics:
//!
//! - **actions**     — ReconcileActions emitted (`vigy-emit` family)
//! - **desired**     — keyed declarative "what should be true"
//! - **observed**    — keyed recorded "what currently is true"
//! - **conditions**  — kubernetes-style health/readiness signals
//! - **log**         — structured log lines
//! - **trace**       — typed kv traces for the run's audit record
//! - **metrics**     — named numeric metrics for observability
//! - **events**      — kubernetes-style events for tail consumers
//!
//! Plus two state surfaces the *runtime* manages (not the program):
//!
//! - **tick_start_ms** — readable via `(vigy-tick)`
//! - **kv**            — persistent k/v store, hydrated at tick start,
//!                       readable + writable via `(vigy-get/set/incr)`,
//!                       saved back to the store at tick end
//!
//! ## Framework verb surface
//!
//! ### Actions (typed ReconcileKind)
//! ```text
//! (vigy-noop)                        — recorded; "nothing to do"
//! (vigy-defer reason)                — "skip this tick; try next"
//! (vigy-pull payload)                — fetch upstream state locally
//! (vigy-push payload)                — push local state to upstream
//! (vigy-create payload)              — target doesn't exist; create
//! (vigy-update payload)              — target exists; modify
//! (vigy-delete payload)              — target should not exist
//! (vigy-apply payload)               — idempotent "make it match"
//! (vigy-restart payload)             — runtime stuck; restart
//! (vigy-emit kind payload?)          — escape hatch for unusual kinds
//! ```
//!
//! ### Structured state
//! ```text
//! (vigy-desired k v)                 — declare what should be true
//! (vigy-observed k v)                — record what currently is true
//! (vigy-condition name status        — kubernetes-style condition.
//!                  reason? message?)   status: "true"|"false"|"unknown"
//! ```
//!
//! ### Persistent KV (survives across ticks; per-vigy scope)
//! ```text
//! (vigy-get k default?)              — read; default if absent
//! (vigy-set k v)                     — write; saved on tick end
//! (vigy-incr k delta)                — atomic numeric increment
//! (vigy-has? k)                      — presence check
//! (vigy-del k)                       — remove
//! ```
//!
//! ### Convergence / idempotency
//! ```text
//! (vigy-once k)                      — true exactly once per vigy lifetime
//! (vigy-mark-converged k)            — record permanent achievement
//! (vigy-converged? k)                — read convergence flag
//! ```
//!
//! ### Scheduling helpers
//! ```text
//! (vigy-tick)                        — epoch ms of this tick's start
//! (vigy-tick-count)                  — N (Nth tick of this vigy)
//! (vigy-since-last-tick)             — ms since previous tick
//! (vigy-rate-limited? k min-ms)      — token-bucket gate; returns true
//!                                       if k was acted on within min-ms
//! (vigy-backoff-ms attempt)          — capped exponential (2^attempt * 1000ms, max 30s)
//! ```
//!
//! ### Diagnostics
//! ```text
//! (vigy-log level msg)               — text log line
//! (vigy-trace k v)                   — typed kv on the audit record
//! (vigy-metric name f)               — numeric metric
//! (vigy-event kind message)          — kubernetes-style event
//! ```
//!
//! Plus the full tatara-lisp stdlib — arithmetic, comparison, list ops,
//! strings, channels, fibers, higher-order helpers.
//!
//! ## Entry point
//!
//! [`evaluate`] takes a program + a fresh host (with `kv` pre-loaded by
//! the runtime), evaluates, returns the host (now populated). The
//! runtime drains buffers + persists dirty kv keys.

use blue_lang_host::{host, host_effect_names, Budget, Entry, Program};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use tatara_lisp::read_spanned;
use tatara_lisp_eval::{
    install_full_stdlib_with, Arity, EvalError, Interpreter, IntoValue, Value as LispValue,
};
use thiserror::Error;
use vigy_types::{Condition, ConditionStatus, Language, ReconcileAction, ReconcileKind};

pub use blue_lang_host::{Host, HostFn};

// Re-exports — hosts that ship their own HostExtension need access to
// the lisp surface to register intrinsics. Exposing these here keeps
// the dependency story clean ("add vigy-eval"; no tatara-lisp dep
// needed for host code).
pub use tatara_lisp::Span;
pub use tatara_lisp_eval::{
    Arity as ArityRe, EvalError as EvalErrorRe, Interpreter as InterpreterRe, Value as LispValueRe,
};
/// Re-exported `tatara_lisp_eval::Arity`. Host extensions specify the
/// arity of their custom intrinsics with this.
pub type ExtArity = Arity;
/// Re-exported `tatara_lisp_eval::Value`. Host intrinsics return this
/// from their callable.
pub type ExtValue = LispValue;
/// Re-exported `tatara_lisp_eval::Interpreter<VigyHost>` — the second
/// argument to `HostExtension::install`.
pub type ExtInterpreter = Interpreter<VigyHost>;
/// Re-exported `tatara_lisp_eval::EvalError`. Host intrinsics return
/// this for type mismatches / runtime errors.
pub type ExtEvalError = EvalError;

#[derive(Debug, Error)]
pub enum EvalErr {
    #[error("parse: {0}")]
    Parse(String),
    #[error("eval: {0}")]
    Eval(String),
    #[error("blue: {0}")]
    Blue(#[from] blue_lang_host::Error),
}

pub type Result<T> = std::result::Result<T, EvalErr>;

/// Per-tick host. Programs populate buffers via intrinsics; the runtime
/// drains them after the tick + persists dirty kv keys.
#[derive(Debug, Default)]
pub struct VigyHost {
    // ── tick metadata (read-only from program) ───────────────────
    pub tick_start_ms: i64,
    pub previous_tick_ms: Option<i64>,
    pub tick_count: i64,

    // ── action / decision buffers (program writes; runtime drains) ──
    pub actions: Vec<ReconcileAction>,
    pub log: Vec<LogEntry>,
    pub trace: BTreeMap<String, JsonValue>,
    pub metrics: BTreeMap<String, f64>,
    pub events: Vec<HostEvent>,

    // ── structured state buffers ────────────────────────────────
    pub desired: BTreeMap<String, JsonValue>,
    pub observed: BTreeMap<String, JsonValue>,
    pub conditions: Vec<Condition>,

    // ── persistent kv (runtime loads on tick start; saves dirty on end) ──
    pub kv: BTreeMap<String, JsonValue>,
    pub kv_dirty: std::collections::BTreeSet<String>,
    pub kv_deleted: std::collections::BTreeSet<String>,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub level: LogLevel,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "trace" => Self::Trace,
            "debug" => Self::Debug,
            "info" => Self::Info,
            "warn" => Self::Warn,
            "error" => Self::Error,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone)]
pub struct HostEvent {
    pub kind: String,
    pub message: String,
}

/// Evaluate a vigy program against a fresh host. Convenience wrapper
/// over the trait-based path with the standard extension bundle —
/// the entrypoint vigy-runtime uses by default. Embedders that want
/// additional intrinsics call [`evaluate_with_extensions`] directly.
pub fn evaluate(program: &str, host: VigyHost) -> Result<VigyHost> {
    evaluate_with_extensions(program, host, &standard_extensions())
}

/// Evaluate a program with a custom extension bundle. Hosts (mado,
/// tear-daemon) compose `standard_extensions()` with their own
/// [`HostExtension`] impls — e.g. mado adds `MadoTearExtension` that
/// registers `(mado-tear-list-sessions)` / `(mado-tear-attach)`
/// intrinsics, the rest of the framework keeps working unchanged.
pub fn evaluate_with_extensions(
    program: &str,
    mut host: VigyHost,
    extensions: &[ExtensionHandle],
) -> Result<VigyHost> {
    let mut interp: Interpreter<VigyHost> = Interpreter::new();
    install_full_stdlib_with(&mut interp, &mut host);
    for ext in extensions {
        ext.install(&mut interp);
    }
    let forms = read_spanned(program).map_err(|e| EvalErr::Parse(format!("{e}")))?;
    interp
        .eval_program(&forms, &mut host)
        .map_err(|e| EvalErr::Eval(format!("{e}")))?;
    Ok(host)
}

/// Register every vigy framework intrinsic. Kept as a free function for
/// the rare embedder that wants the entire bundle in one call without
/// touching `Box<dyn HostExtension>`. Equivalent to installing the
/// `standard_extensions()` list one at a time.
pub fn install_vigy_intrinsics(interp: &mut Interpreter<VigyHost>) {
    for rows in TATARA {
        install_rows(interp, rows);
    }
}

// ─────────────────────────────────────────────────────────────────
// Traits — the compounding surface
// ─────────────────────────────────────────────────────────────────

/// Pluggable intrinsic pack. The framework ships six standard
/// extensions (`Actions`, `State`, `Kv`, `Convergence`, `Scheduling`,
/// `Diagnostics`) — together they form `standard_extensions()`. Hosts
/// add their own by implementing this trait and passing their impls
/// to `evaluate_with_extensions` alongside the standard set.
///
/// Implementations should be cheap to construct + cheap to clone; the
/// runtime currently instantiates them per-tick (room to cache later
/// once a real benchmark calls for it).
pub trait HostExtension: Send + Sync {
    /// Register every intrinsic this extension owns on the interpreter.
    /// Idempotent within a single evaluation — registering twice on
    /// the same interpreter overrides the prior binding (tatara-lisp's
    /// default behaviour). Across different interpreter instances each
    /// install is independent.
    fn install(&self, interp: &mut Interpreter<VigyHost>);

    fn install_blue(&self, interp: &mut Interpreter<VigyHost>) {
        self.install(interp);
    }
}

/// Shared handle for a HostExtension. `Arc` chosen over `Box` so the
/// reconciler can hand its extension list to spawn_blocking without
/// requiring every extension to be `Clone` (some impls might hold
/// non-cloneable resources like a `tonic::Channel`).
pub type ExtensionHandle = std::sync::Arc<dyn HostExtension>;

/// The six standard extensions, in the order they install on a fresh
/// interpreter. Order doesn't matter today (no two extensions share
/// an intrinsic name) but is documented for stability.
pub fn standard_extensions() -> Vec<ExtensionHandle> {
    use std::sync::Arc;
    vec![
        Arc::new(ActionsExtension),
        Arc::new(StateExtension),
        Arc::new(KvExtension),
        Arc::new(ConvergenceExtension),
        Arc::new(SchedulingExtension),
        Arc::new(DiagnosticsExtension),
    ]
}

/// Typed reconciler — what runs each tick. tatara-lisp evaluation is
/// the default implementation ([`LispReconciler`]) but the trait lets
/// any execution model (Rust-native, Wasm, HTTP webhook) participate
/// in the same runtime loop.
///
/// Reconcilers consume the input `VigyHost` (with kv hydrated by the
/// runtime + tick metadata stamped) and return the host with buffers
/// populated. The runtime drains buffers + persists dirty kv keys
/// after the trait method returns.
#[async_trait::async_trait]
pub trait Reconciler: Send + Sync {
    async fn tick(&self, host: VigyHost) -> Result<VigyHost>;
}

/// The default reconciler — evaluates a tatara-lisp program with the
/// supplied extension bundle. Constructible two ways:
///
///   - `LispReconciler::standard(program)` uses `standard_extensions()`.
///   - `LispReconciler::with_extensions(program, exts)` for hosts
///     that want additional intrinsics.
///
/// Holds an owned program + a shared extension list. Cheap to clone
/// (Arcs all the way down) so the runtime can rebuild it on every
/// vigy edit without paying for interpreter construction up-front.
pub struct LispReconciler {
    pub program: String,
    pub extensions: Vec<ExtensionHandle>,
}

impl LispReconciler {
    pub fn standard(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            extensions: standard_extensions(),
        }
    }

    pub fn with_extensions(
        program: impl Into<String>,
        extensions: Vec<ExtensionHandle>,
    ) -> Self {
        Self {
            program: program.into(),
            extensions,
        }
    }
}

#[async_trait::async_trait]
impl Reconciler for LispReconciler {
    async fn tick(&self, host: VigyHost) -> Result<VigyHost> {
        // Tatara-lisp eval is sync + CPU-bound. spawn_blocking keeps
        // the tokio worker free; the reconciler returns its host
        // verbatim once the lisp program finishes (or errors).
        let program = self.program.clone();
        let extensions = self.extensions.clone();
        tokio::task::spawn_blocking(move || {
            evaluate_with_extensions(&program, host, &extensions)
        })
        .await
        .map_err(|e| EvalErr::Eval(format!("join: {e}")))?
    }
}

/// A reconciler that does nothing. Tests + the "vigy is disabled"
/// path use this to drive the runtime without any program. Returns
/// the host unchanged.
pub struct NoopReconciler;

#[async_trait::async_trait]
impl Reconciler for NoopReconciler {
    async fn tick(&self, host: VigyHost) -> Result<VigyHost> {
        Ok(host)
    }
}

/// Composite reconciler — runs its children in order, threading the
/// host through. Each child sees the buffer state the previous child
/// wrote, so a chain can stack: "first reconciler observes from RPC,
/// second writes a tatara-lisp policy over those observations."
pub struct ChainReconciler {
    pub children: Vec<Box<dyn Reconciler>>,
}

impl ChainReconciler {
    pub fn new(children: Vec<Box<dyn Reconciler>>) -> Self {
        Self { children }
    }
}

#[async_trait::async_trait]
impl Reconciler for ChainReconciler {
    async fn tick(&self, mut host: VigyHost) -> Result<VigyHost> {
        for child in &self.children {
            host = child.tick(host).await?;
        }
        Ok(host)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IntrinsicError {
    #[error("expected {expected}, got {got}")]
    Type {
        expected: &'static str,
        got: &'static str,
    },
    #[error("{0}")]
    Invalid(String),
}

impl IntrinsicError {
    fn at(self, name: &'static str, sp: Span) -> EvalError {
        match self {
            IntrinsicError::Type { expected, got } => EvalError::type_mismatch(expected, got, sp),
            IntrinsicError::Invalid(reason) => EvalError::native_fn(name, reason, sp),
        }
    }
}

type Unit = std::result::Result<(), IntrinsicError>;

#[host(prefix = "vigy_")]
impl VigyHost {
    fn emit(&mut self, kind: LispValue, payload: Option<LispValue>) -> std::result::Result<(), IntrinsicError> {
        let kind = parse_kind(&text(&kind)?)?;
        self.actions.push(ReconcileAction {
            kind,
            payload: payload.as_ref().map(lisp_to_json),
            result: None,
            message: None,
        });
        Ok(())
    }

    fn noop(&mut self) {
        self.actions.push(ReconcileAction::noop());
    }

    fn defer(&mut self, reason: LispValue) {
        self.actions
            .push(ReconcileAction::defer(reason.to_display_string()));
    }

    fn pull(&mut self, payload: LispValue) {
        self.actions.push(ReconcileAction::pull(lisp_to_json(&payload)));
    }

    fn push(&mut self, payload: LispValue) {
        self.actions.push(ReconcileAction::push(lisp_to_json(&payload)));
    }

    fn create(&mut self, payload: LispValue) {
        self.actions
            .push(ReconcileAction::create(lisp_to_json(&payload)));
    }

    fn update(&mut self, payload: LispValue) {
        self.actions
            .push(ReconcileAction::update(lisp_to_json(&payload)));
    }

    fn delete_action(&mut self, payload: LispValue) {
        self.actions
            .push(ReconcileAction::delete(lisp_to_json(&payload)));
    }

    fn apply(&mut self, payload: LispValue) {
        self.actions.push(ReconcileAction::apply(lisp_to_json(&payload)));
    }

    fn restart(&mut self, payload: LispValue) {
        self.actions
            .push(ReconcileAction::restart(lisp_to_json(&payload)));
    }

    fn desired(&mut self, key: LispValue, value: LispValue) -> std::result::Result<(), IntrinsicError> {
        self.desired.insert(text(&key)?, lisp_to_json(&value));
        Ok(())
    }

    fn observed(&mut self, key: LispValue, value: LispValue) -> std::result::Result<(), IntrinsicError> {
        self.observed.insert(text(&key)?, lisp_to_json(&value));
        Ok(())
    }

    fn condition(
        &mut self,
        name: LispValue,
        status: LispValue,
        reason: Option<LispValue>,
        message: Option<LispValue>,
    ) -> std::result::Result<(), IntrinsicError> {
        let name = text(&name)?;
        let status = match text(&status)?.as_str() {
            "true" | "True" => ConditionStatus::True,
            "false" | "False" => ConditionStatus::False,
            "unknown" | "Unknown" => ConditionStatus::Unknown,
            other => {
                return Err(IntrinsicError::Invalid(format!(
                    "status must be true|false|unknown, got {other:?}"
                )))
            }
        };
        let reason = reason.as_ref().map(text).transpose()?;
        let message = message.as_ref().map(text).transpose()?;
        self.conditions.push(Condition {
            name,
            status,
            reason,
            message,
            last_transition: time::OffsetDateTime::now_utc(),
        });
        Ok(())
    }

    fn get(
        &mut self,
        key: LispValue,
        default: Option<LispValue>,
    ) -> std::result::Result<LispValue, IntrinsicError> {
        Ok(match self.kv.get(&text(&key)?) {
            Some(v) => json_to_lisp(v),
            None => default.unwrap_or(LispValue::Nil),
        })
    }

    fn set(&mut self, key: LispValue, value: LispValue) -> std::result::Result<(), IntrinsicError> {
        let key = text(&key)?;
        self.kv.insert(key.clone(), lisp_to_json(&value));
        self.kv_dirty.insert(key.clone());
        self.kv_deleted.remove(&key);
        Ok(())
    }

    fn incr(
        &mut self,
        key: LispValue,
        delta: Option<LispValue>,
    ) -> std::result::Result<i64, IntrinsicError> {
        let key = text(&key)?;
        let delta = match delta {
            Some(d) => int(&d, "integer delta")?,
            None => 1,
        };
        let next = self
            .kv
            .get(&key)
            .and_then(JsonValue::as_i64)
            .unwrap_or(0)
            .saturating_add(delta);
        self.kv.insert(key.clone(), JsonValue::Number(next.into()));
        self.kv_dirty.insert(key.clone());
        self.kv_deleted.remove(&key);
        Ok(next)
    }

    #[blue(name = "vigy_has?")]
    fn has(&mut self, key: LispValue) -> std::result::Result<bool, IntrinsicError> {
        Ok(self.kv.contains_key(&text(&key)?))
    }

    fn del(&mut self, key: LispValue) -> std::result::Result<bool, IntrinsicError> {
        let key = text(&key)?;
        let existed = self.kv.remove(&key).is_some();
        self.kv_dirty.remove(&key);
        self.kv_deleted.insert(key);
        Ok(existed)
    }

    fn once(&mut self, key: LispValue) -> std::result::Result<bool, IntrinsicError> {
        let key = format!("__once::{}", text(&key)?);
        if self.kv.contains_key(&key) {
            return Ok(false);
        }
        self.kv.insert(key.clone(), JsonValue::Bool(true));
        self.kv_dirty.insert(key);
        Ok(true)
    }

    fn mark_converged(&mut self, key: LispValue) -> std::result::Result<(), IntrinsicError> {
        let key = format!("__converged::{}", text(&key)?);
        self.kv.insert(
            key.clone(),
            JsonValue::String(
                time::OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_default(),
            ),
        );
        self.kv_dirty.insert(key);
        Ok(())
    }

    #[blue(name = "vigy_converged?")]
    fn converged(&mut self, key: LispValue) -> std::result::Result<bool, IntrinsicError> {
        Ok(self
            .kv
            .contains_key(&format!("__converged::{}", text(&key)?)))
    }

    fn tick(&mut self) -> i64 {
        self.tick_start_ms
    }

    fn tick_count(&mut self) -> i64 {
        self.tick_count
    }

    fn since_last_tick(&mut self) -> i64 {
        self.previous_tick_ms
            .map(|p| self.tick_start_ms.saturating_sub(p))
            .unwrap_or(-1)
    }

    #[blue(name = "vigy_rate_limited?")]
    fn rate_limited(
        &mut self,
        key: LispValue,
        min_interval_ms: LispValue,
    ) -> std::result::Result<bool, IntrinsicError> {
        let key = format!("__ratelimit::{}", text(&key)?);
        let min_ms = int(&min_interval_ms, "integer min-interval-ms")?;
        let last = self.kv.get(&key).and_then(JsonValue::as_i64).unwrap_or(0);
        if self.tick_start_ms.saturating_sub(last) < min_ms {
            return Ok(true);
        }
        self.kv
            .insert(key.clone(), JsonValue::Number(self.tick_start_ms.into()));
        self.kv_dirty.insert(key);
        Ok(false)
    }

    fn backoff_ms(&mut self, attempt: LispValue) -> std::result::Result<i64, IntrinsicError> {
        let attempt = u32::try_from(int(&attempt, "integer attempt")?.max(0)).unwrap_or(u32::MAX);
        let secs = 1u64.checked_shl(attempt.min(5)).unwrap_or(30);
        Ok(i64::try_from(secs.min(30) * 1000).unwrap_or(30_000))
    }

    fn log(&mut self, level: LispValue, message: LispValue) -> std::result::Result<(), IntrinsicError> {
        let level_str = text(&level)?;
        let level = LogLevel::parse(&level_str).ok_or_else(|| {
            IntrinsicError::Invalid(format!(
                "unknown level {level_str:?}; expected trace|debug|info|warn|error"
            ))
        })?;
        let message = text(&message)?;
        self.log.push(LogEntry { level, message });
        Ok(())
    }

    fn trace(&mut self, key: LispValue, value: LispValue) -> std::result::Result<(), IntrinsicError> {
        self.trace.insert(text(&key)?, lisp_to_json(&value));
        Ok(())
    }

    fn metric(&mut self, name: LispValue, value: LispValue) -> std::result::Result<(), IntrinsicError> {
        let name = text(&name)?;
        let value = match &value {
            LispValue::Int(n) => *n as f64,
            LispValue::Float(n) => *n,
            other => {
                return Err(IntrinsicError::Type {
                    expected: "numeric metric value",
                    got: other.type_name(),
                })
            }
        };
        self.metrics.insert(name, value);
        Ok(())
    }

    fn event(&mut self, kind: LispValue, message: LispValue) -> std::result::Result<(), IntrinsicError> {
        let kind = text(&kind)?;
        let message = text(&message)?;
        self.events.push(HostEvent { kind, message });
        Ok(())
    }
}

type Run = fn(&mut VigyHost, &[LispValue]) -> std::result::Result<LispValue, IntrinsicError>;

#[derive(Clone, Copy)]
struct TataraIntrinsic {
    name: &'static str,
    arity: Arity,
    run: Run,
}

const fn row(name: &'static str, arity: Arity, run: Run) -> TataraIntrinsic {
    TataraIntrinsic { name, arity, run }
}

fn nil(r: Unit) -> std::result::Result<LispValue, IntrinsicError> {
    r.map(|()| LispValue::Nil)
}

fn value<T: IntoValue>(
    r: std::result::Result<T, IntrinsicError>,
) -> std::result::Result<LispValue, IntrinsicError> {
    r.map(IntoValue::into_value)
}

fn at_most(args: &[LispValue], max: usize, shape: &str) -> std::result::Result<(), IntrinsicError> {
    if args.len() > max {
        return Err(IntrinsicError::Invalid(format!(
            "expected {shape}, got {}",
            args.len()
        )));
    }
    Ok(())
}

const ACTIONS: &[TataraIntrinsic] = &[
    row("vigy-emit", Arity::AtLeast(1), |h, a| {
        at_most(a, 2, "1 or 2 args (kind, payload?)")?;
        nil(h.emit(a[0].clone(), a.get(1).cloned()))
    }),
    row("vigy-noop", Arity::Exact(0), |h, _| {
        h.noop();
        Ok(LispValue::Nil)
    }),
    row("vigy-defer", Arity::Exact(1), |h, a| {
        h.defer(a[0].clone());
        Ok(LispValue::Nil)
    }),
    row("vigy-pull", Arity::Exact(1), |h, a| {
        h.pull(a[0].clone());
        Ok(LispValue::Nil)
    }),
    row("vigy-push", Arity::Exact(1), |h, a| {
        h.push(a[0].clone());
        Ok(LispValue::Nil)
    }),
    row("vigy-create", Arity::Exact(1), |h, a| {
        h.create(a[0].clone());
        Ok(LispValue::Nil)
    }),
    row("vigy-update", Arity::Exact(1), |h, a| {
        h.update(a[0].clone());
        Ok(LispValue::Nil)
    }),
    row("vigy-delete-action", Arity::Exact(1), |h, a| {
        h.delete_action(a[0].clone());
        Ok(LispValue::Nil)
    }),
    row("vigy-apply", Arity::Exact(1), |h, a| {
        h.apply(a[0].clone());
        Ok(LispValue::Nil)
    }),
    row("vigy-restart", Arity::Exact(1), |h, a| {
        h.restart(a[0].clone());
        Ok(LispValue::Nil)
    }),
];

const STATE: &[TataraIntrinsic] = &[
    row("vigy-desired", Arity::Exact(2), |h, a| {
        nil(h.desired(a[0].clone(), a[1].clone()))
    }),
    row("vigy-observed", Arity::Exact(2), |h, a| {
        nil(h.observed(a[0].clone(), a[1].clone()))
    }),
    row("vigy-condition", Arity::AtLeast(2), |h, a| {
        at_most(a, 4, "2..4 args (name, status, reason?, message?)")?;
        nil(h.condition(
            a[0].clone(),
            a[1].clone(),
            a.get(2).cloned(),
            a.get(3).cloned(),
        ))
    }),
];

const KV: &[TataraIntrinsic] = &[
    row("vigy-get", Arity::AtLeast(1), |h, a| {
        h.get(a[0].clone(), a.get(1).cloned())
    }),
    row("vigy-set", Arity::Exact(2), |h, a| {
        nil(h.set(a[0].clone(), a[1].clone()))
    }),
    row("vigy-incr", Arity::AtLeast(1), |h, a| {
        value(h.incr(a[0].clone(), a.get(1).cloned()))
    }),
    row("vigy-has?", Arity::Exact(1), |h, a| value(h.has(a[0].clone()))),
    row("vigy-del", Arity::Exact(1), |h, a| value(h.del(a[0].clone()))),
];

const CONVERGENCE: &[TataraIntrinsic] = &[
    row("vigy-once", Arity::Exact(1), |h, a| value(h.once(a[0].clone()))),
    row("vigy-mark-converged", Arity::Exact(1), |h, a| {
        nil(h.mark_converged(a[0].clone()))
    }),
    row("vigy-converged?", Arity::Exact(1), |h, a| {
        value(h.converged(a[0].clone()))
    }),
];

const SCHEDULING: &[TataraIntrinsic] = &[
    row("vigy-tick", Arity::Exact(0), |h, _| Ok(LispValue::Int(h.tick()))),
    row("vigy-tick-count", Arity::Exact(0), |h, _| {
        Ok(LispValue::Int(h.tick_count()))
    }),
    row("vigy-since-last-tick", Arity::Exact(0), |h, _| {
        Ok(LispValue::Int(h.since_last_tick()))
    }),
    row("vigy-rate-limited?", Arity::Exact(2), |h, a| {
        value(h.rate_limited(a[0].clone(), a[1].clone()))
    }),
    row("vigy-backoff-ms", Arity::Exact(1), |h, a| {
        value(h.backoff_ms(a[0].clone()))
    }),
];

const DIAGNOSTICS: &[TataraIntrinsic] = &[
    row("vigy-log", Arity::Exact(2), |h, a| {
        nil(h.log(a[0].clone(), a[1].clone()))
    }),
    row("vigy-trace", Arity::Exact(2), |h, a| {
        nil(h.trace(a[0].clone(), a[1].clone()))
    }),
    row("vigy-metric", Arity::Exact(2), |h, a| {
        nil(h.metric(a[0].clone(), a[1].clone()))
    }),
    row("vigy-event", Arity::Exact(2), |h, a| {
        nil(h.event(a[0].clone(), a[1].clone()))
    }),
];

const TATARA: [&[TataraIntrinsic]; 6] = [ACTIONS, STATE, KV, CONVERGENCE, SCHEDULING, DIAGNOSTICS];

fn install_rows(interp: &mut Interpreter<VigyHost>, rows: &[TataraIntrinsic]) {
    for &TataraIntrinsic { name, arity, run } in rows {
        interp.register_fn(
            name,
            arity,
            move |args: &[LispValue], host: &mut VigyHost, sp: Span| {
                run(host, args).map_err(|e| e.at(name, sp))
            },
        );
    }
}

fn parse_kind(s: &str) -> std::result::Result<ReconcileKind, IntrinsicError> {
    match s {
        "noop" => Ok(ReconcileKind::Noop),
        "defer" => Ok(ReconcileKind::Defer),
        "pull" => Ok(ReconcileKind::Pull),
        "push" => Ok(ReconcileKind::Push),
        "create" => Ok(ReconcileKind::Create),
        "update" => Ok(ReconcileKind::Update),
        "delete" => Ok(ReconcileKind::Delete),
        "apply" => Ok(ReconcileKind::Apply),
        "restart" => Ok(ReconcileKind::Restart),
        "custom" => Ok(ReconcileKind::Custom),
        other => Err(IntrinsicError::Invalid(format!(
            "unknown kind {other:?}; expected noop|defer|pull|push|create|update|delete|apply|restart|custom"
        ))),
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ActionsExtension;
impl HostExtension for ActionsExtension {
    fn install(&self, interp: &mut Interpreter<VigyHost>) {
        install_rows(interp, ACTIONS);
    }
    fn install_blue(&self, _: &mut Interpreter<VigyHost>) {}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StateExtension;
impl HostExtension for StateExtension {
    fn install(&self, interp: &mut Interpreter<VigyHost>) {
        install_rows(interp, STATE);
    }
    fn install_blue(&self, _: &mut Interpreter<VigyHost>) {}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct KvExtension;
impl HostExtension for KvExtension {
    fn install(&self, interp: &mut Interpreter<VigyHost>) {
        install_rows(interp, KV);
    }
    fn install_blue(&self, _: &mut Interpreter<VigyHost>) {}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ConvergenceExtension;
impl HostExtension for ConvergenceExtension {
    fn install(&self, interp: &mut Interpreter<VigyHost>) {
        install_rows(interp, CONVERGENCE);
    }
    fn install_blue(&self, _: &mut Interpreter<VigyHost>) {}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SchedulingExtension;
impl HostExtension for SchedulingExtension {
    fn install(&self, interp: &mut Interpreter<VigyHost>) {
        install_rows(interp, SCHEDULING);
    }
    fn install_blue(&self, _: &mut Interpreter<VigyHost>) {}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DiagnosticsExtension;
impl HostExtension for DiagnosticsExtension {
    fn install(&self, interp: &mut Interpreter<VigyHost>) {
        install_rows(interp, DIAGNOSTICS);
    }
    fn install_blue(&self, _: &mut Interpreter<VigyHost>) {}
}

pub fn closure_extension<F>(f: F) -> ExtensionHandle
where
    F: Fn(&mut Interpreter<VigyHost>) + Send + Sync + 'static,
{
    struct ClosureExtension<F>(F);
    impl<F: Fn(&mut Interpreter<VigyHost>) + Send + Sync + 'static> HostExtension
        for ClosureExtension<F>
    {
        fn install(&self, interp: &mut Interpreter<VigyHost>) {
            (self.0)(interp);
        }
    }
    std::sync::Arc::new(ClosureExtension(f))
}

fn hosted_frame(name: &str) -> bool {
    static REFUSED: std::sync::OnceLock<std::collections::BTreeSet<&'static str>> =
        std::sync::OnceLock::new();
    !REFUSED
        .get_or_init(|| host_effect_names().into_iter().collect())
        .contains(name)
}

pub fn evaluate_blue(
    program: &str,
    mut host: VigyHost,
    extensions: &[ExtensionHandle],
) -> Result<VigyHost> {
    let mut load = Program::<VigyHost>::builder(Entry::anonymous(program));
    for ext in extensions {
        load = load.extend(move |interp: &mut Interpreter<VigyHost>| ext.install_blue(interp));
    }
    load.frame(&hosted_frame, Budget::tree_walker_default())
        .load(&mut host)
        ?;
    Ok(host)
}

pub fn evaluate_in(
    language: Language,
    program: &str,
    host: VigyHost,
    extensions: &[ExtensionHandle],
) -> Result<VigyHost> {
    match language {
        Language::TataraLisp => evaluate_with_extensions(program, host, extensions),
        Language::Blue => evaluate_blue(program, host, extensions),
    }
}

pub struct ProgramReconciler {
    pub language: Language,
    pub program: String,
    pub extensions: Vec<ExtensionHandle>,
}

impl ProgramReconciler {
    pub fn new(
        language: Language,
        program: impl Into<String>,
        extensions: Vec<ExtensionHandle>,
    ) -> Self {
        Self {
            language,
            program: program.into(),
            extensions,
        }
    }
}

#[async_trait::async_trait]
impl Reconciler for ProgramReconciler {
    async fn tick(&self, host: VigyHost) -> Result<VigyHost> {
        let language = self.language;
        let program = self.program.clone();
        let extensions = self.extensions.clone();
        tokio::task::spawn_blocking(move || evaluate_in(language, &program, host, &extensions))
            .await
            .map_err(|e| EvalErr::Eval(format!("join: {e}")))?
    }
}

// ─────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────

fn text(v: &LispValue) -> std::result::Result<String, IntrinsicError> {
    match v {
        LispValue::Str(s) | LispValue::Symbol(s) | LispValue::Keyword(s) => Ok(s.to_string()),
        other => Err(IntrinsicError::Type {
            expected: "string|symbol|keyword",
            got: other.type_name(),
        }),
    }
}

fn int(v: &LispValue, expected: &'static str) -> std::result::Result<i64, IntrinsicError> {
    match v {
        LispValue::Int(n) => Ok(*n),
        other => Err(IntrinsicError::Type {
            expected,
            got: other.type_name(),
        }),
    }
}

trait LispDisplay {
    fn to_display_string(&self) -> String;
}

impl LispDisplay for LispValue {
    fn to_display_string(&self) -> String {
        match self {
            LispValue::Str(s) => s.to_string(),
            LispValue::Symbol(s) => s.to_string(),
            LispValue::Keyword(s) => s.to_string(),
            LispValue::Int(n) => n.to_string(),
            LispValue::Float(n) => n.to_string(),
            LispValue::Bool(b) => b.to_string(),
            LispValue::Nil => "nil".to_string(),
            other => format!("<{}>", other.type_name()),
        }
    }
}

fn lisp_to_json(v: &LispValue) -> JsonValue {
    match v {
        LispValue::Nil => JsonValue::Null,
        LispValue::Bool(b) => JsonValue::Bool(*b),
        LispValue::Int(n) => JsonValue::Number((*n).into()),
        LispValue::Float(n) => serde_json::Number::from_f64(*n)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        LispValue::Str(s) => JsonValue::String(s.to_string()),
        LispValue::Symbol(s) => JsonValue::String(s.to_string()),
        LispValue::Keyword(s) => JsonValue::String(format!(":{s}")),
        LispValue::List(items) => JsonValue::Array(items.iter().map(lisp_to_json).collect()),
        LispValue::Map(m) => {
            let mut obj = serde_json::Map::new();
            for (k, val) in m.iter() {
                let key_str = match k {
                    tatara_lisp_eval::MapKey::Str(s) => s.to_string(),
                    tatara_lisp_eval::MapKey::Keyword(s) => format!(":{s}"),
                    tatara_lisp_eval::MapKey::Symbol(s) => s.to_string(),
                    tatara_lisp_eval::MapKey::Int(i) => i.to_string(),
                    tatara_lisp_eval::MapKey::Float(bits) => f64::from_bits(*bits).to_string(),
                    tatara_lisp_eval::MapKey::Bool(b) => b.to_string(),
                    tatara_lisp_eval::MapKey::Nil => "null".to_string(),
                };
                obj.insert(key_str, lisp_to_json(val));
            }
            JsonValue::Object(obj)
        }
        _ => JsonValue::String(format!("<{}>", v.type_name())),
    }
}

/// Conservative JSON → tatara-lisp conversion. Used by (vigy-get) to
/// surface kv values back into the program. Objects become Maps,
/// arrays become Lists, numbers preserve int-vs-float, strings stay
/// strings. Round-trips with lisp_to_json for the value shapes a
/// program writes via (vigy-set).
fn json_to_lisp(v: &JsonValue) -> LispValue {
    use std::sync::Arc;
    match v {
        JsonValue::Null => LispValue::Nil,
        JsonValue::Bool(b) => LispValue::Bool(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                LispValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                LispValue::Float(f)
            } else {
                LispValue::Nil
            }
        }
        JsonValue::String(s) => LispValue::Str(Arc::from(s.as_str())),
        JsonValue::Array(items) => {
            let converted: Vec<LispValue> = items.iter().map(json_to_lisp).collect();
            LispValue::List(Arc::new(converted.into()))
        }
        JsonValue::Object(obj) => {
            let mut map = std::collections::HashMap::new();
            for (k, val) in obj {
                map.insert(
                    tatara_lisp_eval::MapKey::Str(Arc::from(k.as_str())),
                    json_to_lisp(val),
                );
            }
            LispValue::Map(Arc::new(map.into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_program() {
        let h = evaluate("", VigyHost::default()).unwrap();
        assert!(h.actions.is_empty());
    }

    // ── action verbs ─────────────────────────────────────────────

    #[test]
    fn typed_action_verbs() {
        let h = evaluate(
            r#"
            (vigy-noop)
            (vigy-defer "waiting on upstream")
            (vigy-pull "session-x")
            (vigy-push "session-y")
            (vigy-create "session-z")
            (vigy-update "session-z")
            (vigy-delete-action "session-w")
            (vigy-apply "session-z")
            (vigy-restart "vigy-runtime")
            "#,
            VigyHost::default(),
        )
        .unwrap();
        let kinds: Vec<ReconcileKind> = h.actions.iter().map(|a| a.kind).collect();
        assert_eq!(
            kinds,
            vec![
                ReconcileKind::Noop,
                ReconcileKind::Defer,
                ReconcileKind::Pull,
                ReconcileKind::Push,
                ReconcileKind::Create,
                ReconcileKind::Update,
                ReconcileKind::Delete,
                ReconcileKind::Apply,
                ReconcileKind::Restart,
            ]
        );
    }

    // ── structured state ────────────────────────────────────────

    #[test]
    fn structured_state_buffers() {
        let h = evaluate(
            r#"
            (vigy-desired "replica_count" 3)
            (vigy-observed "replica_count" 2)
            (vigy-condition "Ready" "false" "BackoffActive" "1 replica unhealthy")
            (vigy-condition "InSync" "true")
            "#,
            VigyHost::default(),
        )
        .unwrap();
        assert_eq!(h.desired.get("replica_count").and_then(|v| v.as_i64()), Some(3));
        assert_eq!(h.observed.get("replica_count").and_then(|v| v.as_i64()), Some(2));
        assert_eq!(h.conditions.len(), 2);
        assert_eq!(h.conditions[0].name, "Ready");
        assert_eq!(h.conditions[0].status, ConditionStatus::False);
        assert_eq!(h.conditions[0].reason.as_deref(), Some("BackoffActive"));
        assert_eq!(h.conditions[1].name, "InSync");
        assert_eq!(h.conditions[1].status, ConditionStatus::True);
    }

    // ── persistent KV ───────────────────────────────────────────

    #[test]
    fn kv_set_get_default() {
        let h = evaluate(
            r#"
            (vigy-set "attempts" 5)
            (vigy-set "label" "production")
            "#,
            VigyHost::default(),
        )
        .unwrap();
        assert_eq!(h.kv.get("attempts").and_then(|v| v.as_i64()), Some(5));
        assert_eq!(h.kv.get("label").and_then(|v| v.as_str()), Some("production"));
        assert!(h.kv_dirty.contains("attempts"));
        assert!(h.kv_dirty.contains("label"));
    }

    #[test]
    fn kv_get_returns_previous_tick_value() {
        // Simulate the runtime hydrating kv before evaluate.
        let mut host = VigyHost::default();
        host.kv.insert(
            "attempts".to_string(),
            serde_json::Value::Number(7.into()),
        );
        let h = evaluate(
            r#"
            (vigy-set "doubled" (* 2 (vigy-get "attempts")))
            "#,
            host,
        )
        .unwrap();
        assert_eq!(h.kv.get("doubled").and_then(|v| v.as_i64()), Some(14));
    }

    #[test]
    fn kv_get_with_default() {
        let h = evaluate(
            r#"(vigy-set "x" (vigy-get "missing" 42))"#,
            VigyHost::default(),
        )
        .unwrap();
        assert_eq!(h.kv.get("x").and_then(|v| v.as_i64()), Some(42));
    }

    #[test]
    fn kv_incr_starts_at_delta_when_absent() {
        let h = evaluate(r#"(vigy-incr "n")"#, VigyHost::default()).unwrap();
        assert_eq!(h.kv.get("n").and_then(|v| v.as_i64()), Some(1));
        let h2 = evaluate(r#"(vigy-incr "n" 4)"#, h).unwrap();
        assert_eq!(h2.kv.get("n").and_then(|v| v.as_i64()), Some(5));
    }

    #[test]
    fn kv_has_and_del() {
        let h = evaluate(
            r#"
            (vigy-set "x" 1)
            (vigy-set "y" 2)
            (vigy-set "x-was-present" (vigy-has? "x"))
            (vigy-del "x")
            (vigy-set "x-present-after-del" (vigy-has? "x"))
            "#,
            VigyHost::default(),
        )
        .unwrap();
        assert_eq!(h.kv.get("x-was-present"), Some(&JsonValue::Bool(true)));
        assert_eq!(
            h.kv.get("x-present-after-del"),
            Some(&JsonValue::Bool(false))
        );
        assert!(h.kv_deleted.contains("x"));
    }

    // ── convergence ─────────────────────────────────────────────

    #[test]
    fn once_fires_only_once() {
        let h = evaluate(r#"(vigy-set "a" (vigy-once "init"))"#, VigyHost::default()).unwrap();
        assert_eq!(h.kv.get("a"), Some(&JsonValue::Bool(true)));
        // Carry kv forward to simulate a second tick.
        let h2 = evaluate(r#"(vigy-set "a" (vigy-once "init"))"#, h).unwrap();
        assert_eq!(h2.kv.get("a"), Some(&JsonValue::Bool(false)));
    }

    #[test]
    fn mark_and_check_converged() {
        let h = evaluate(
            r#"
            (vigy-set "before" (vigy-converged? "target"))
            (vigy-mark-converged "target")
            (vigy-set "after" (vigy-converged? "target"))
            "#,
            VigyHost::default(),
        )
        .unwrap();
        assert_eq!(h.kv.get("before"), Some(&JsonValue::Bool(false)));
        assert_eq!(h.kv.get("after"), Some(&JsonValue::Bool(true)));
    }

    // ── scheduling ─────────────────────────────────────────────

    #[test]
    fn tick_count_and_since_last() {
        let mut host = VigyHost::default();
        host.tick_start_ms = 1000;
        host.previous_tick_ms = Some(800);
        host.tick_count = 7;
        let h = evaluate(
            r#"
            (vigy-set "tick" (vigy-tick))
            (vigy-set "count" (vigy-tick-count))
            (vigy-set "since" (vigy-since-last-tick))
            "#,
            host,
        )
        .unwrap();
        assert_eq!(h.kv.get("tick").and_then(|v| v.as_i64()), Some(1000));
        assert_eq!(h.kv.get("count").and_then(|v| v.as_i64()), Some(7));
        assert_eq!(h.kv.get("since").and_then(|v| v.as_i64()), Some(200));
    }

    #[test]
    fn rate_limit_gates_within_window() {
        let mut host = VigyHost::default();
        host.tick_start_ms = 1000;
        // First call: not limited (no prior record). Records timestamp.
        let h = evaluate(
            r#"(vigy-set "first" (vigy-rate-limited? "k" 500))"#,
            host,
        )
        .unwrap();
        assert_eq!(h.kv.get("first"), Some(&JsonValue::Bool(false)));

        // Second call from a near-future tick should be limited.
        let mut next = h;
        next.tick_start_ms = 1300;
        let h2 = evaluate(
            r#"(vigy-set "second" (vigy-rate-limited? "k" 500))"#,
            next,
        )
        .unwrap();
        assert_eq!(h2.kv.get("second"), Some(&JsonValue::Bool(true)));

        // Third call after the window: not limited.
        let mut later = h2;
        later.tick_start_ms = 1600;
        let h3 = evaluate(
            r#"(vigy-set "third" (vigy-rate-limited? "k" 500))"#,
            later,
        )
        .unwrap();
        assert_eq!(h3.kv.get("third"), Some(&JsonValue::Bool(false)));
    }

    #[test]
    fn backoff_curve_caps_at_30s() {
        let h = evaluate(
            r#"
            (vigy-set "b0" (vigy-backoff-ms 0))
            (vigy-set "b1" (vigy-backoff-ms 1))
            (vigy-set "b3" (vigy-backoff-ms 3))
            (vigy-set "b100" (vigy-backoff-ms 100))
            "#,
            VigyHost::default(),
        )
        .unwrap();
        assert_eq!(h.kv.get("b0").and_then(|v| v.as_i64()), Some(1000));
        assert_eq!(h.kv.get("b1").and_then(|v| v.as_i64()), Some(2000));
        assert_eq!(h.kv.get("b3").and_then(|v| v.as_i64()), Some(8000));
        assert_eq!(h.kv.get("b100").and_then(|v| v.as_i64()), Some(30000));
    }

    // ── diagnostics ────────────────────────────────────────────

    // ── trait surface ──────────────────────────────────────────

    #[tokio::test]
    async fn lisp_reconciler_runs_a_program() {
        let r = LispReconciler::standard("(vigy-noop)");
        let host = r.tick(VigyHost::default()).await.unwrap();
        assert_eq!(host.actions.len(), 1);
        assert_eq!(host.actions[0].kind, ReconcileKind::Noop);
    }

    #[tokio::test]
    async fn noop_reconciler_returns_host_unchanged() {
        let r = NoopReconciler;
        let mut host = VigyHost::default();
        host.tick_start_ms = 42;
        let after = r.tick(host).await.unwrap();
        assert_eq!(after.tick_start_ms, 42);
        assert!(after.actions.is_empty());
    }

    #[tokio::test]
    async fn chain_reconciler_threads_host_through_children() {
        // Three children, each appending a different action via
        // separate LispReconciler instances. Proves composition + that
        // the host buffers accumulate across the chain.
        let chain = ChainReconciler::new(vec![
            Box::new(LispReconciler::standard("(vigy-pull \"first\")")),
            Box::new(LispReconciler::standard("(vigy-push \"second\")")),
            Box::new(LispReconciler::standard("(vigy-apply \"third\")")),
        ]);
        let host = chain.tick(VigyHost::default()).await.unwrap();
        let kinds: Vec<_> = host.actions.iter().map(|a| a.kind).collect();
        assert_eq!(
            kinds,
            vec![
                ReconcileKind::Pull,
                ReconcileKind::Push,
                ReconcileKind::Apply,
            ]
        );
    }

    #[tokio::test]
    async fn custom_host_extension_registers_intrinsic() {
        // Closure-built extension that registers a custom intrinsic.
        // Then a tatara-lisp program that calls it — proves the trait
        // is a real extension point, not just type theater.
        let custom = closure_extension(|interp| {
            interp.register_fn(
                "mado-tear-list-sessions",
                Arity::Exact(0),
                |_args: &[LispValue], host: &mut VigyHost, _sp| {
                    host.actions
                        .push(ReconcileAction::custom(serde_json::json!({"from": "mado"})));
                    Ok(LispValue::Int(3))
                },
            );
        });

        let mut extensions = standard_extensions();
        extensions.push(custom);

        let r = LispReconciler::with_extensions(
            r#"
            (vigy-set "session_count" (mado-tear-list-sessions))
            "#,
            extensions,
        );
        let host = r.tick(VigyHost::default()).await.unwrap();
        assert_eq!(host.kv.get("session_count").and_then(|v| v.as_i64()), Some(3));
        assert_eq!(host.actions.len(), 1);
        assert_eq!(host.actions[0].kind, ReconcileKind::Custom);
    }

    #[test]
    fn trace_metric_event() {
        let h = evaluate(
            r#"
            (vigy-trace "upstream_session_id" "abc-123")
            (vigy-metric "scrollback_bytes" 4096)
            (vigy-metric "lag_ratio" 0.42)
            (vigy-event "Reconciled" "all good")
            "#,
            VigyHost::default(),
        )
        .unwrap();
        assert_eq!(
            h.trace.get("upstream_session_id").and_then(|v| v.as_str()),
            Some("abc-123")
        );
        assert_eq!(h.metrics.get("scrollback_bytes"), Some(&4096.0));
        assert!((h.metrics.get("lag_ratio").unwrap() - 0.42).abs() < 1e-9);
        assert_eq!(h.events.len(), 1);
        assert_eq!(h.events[0].kind, "Reconciled");
    }

    fn blue(program: &str, host: VigyHost) -> VigyHost {
        evaluate_in(Language::Blue, program, host, &standard_extensions()).unwrap()
    }

    fn blue_err(program: &str) -> String {
        evaluate_in(Language::Blue, program, VigyHost::default(), &standard_extensions())
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn a_blue_vigy_emits_desires_conditions_logs_and_keeps_kv() {
        let mut host = VigyHost::default();
        host.kv.insert("attempts".into(), JsonValue::Number(7.into()));
        let h = blue(
            r#"vigy_emit("custom", {"from" => "blue"})
vigy_emit("noop")
vigy_delete_action("session-w")
vigy_desired("replica_count", 3)
vigy_observed("replica_count", 2)
vigy_condition("Ready", "false", "BackoffActive", "1 replica unhealthy")
vigy_condition("InSync", "true")
vigy_set("doubled", 2 * vigy_get("attempts"))
vigy_set("fallback", vigy_get("missing", 42))
vigy_set("absent", vigy_get("missing"))
vigy_incr("n")
vigy_incr("n", 4)
vigy_set("had_n", vigy_has?("n"))
vigy_del("attempts")
vigy_log("warn", "careful")
vigy_trace("upstream", "abc")
vigy_metric("lag", 0.5)
vigy_event("Reconciled", "all good")
vigy_set("first", vigy_once("init"))
vigy_mark_converged("target")
vigy_set("converged", vigy_converged?("target"))
vigy_set("backoff", vigy_backoff_ms(3))
"#,
            host,
        );
        let kinds: Vec<_> = h.actions.iter().map(|a| a.kind).collect();
        assert_eq!(
            kinds,
            [ReconcileKind::Custom, ReconcileKind::Noop, ReconcileKind::Delete]
        );
        assert_eq!(h.actions[0].payload, Some(serde_json::json!({"from": "blue"})));
        assert_eq!(h.actions[1].payload, None);
        assert_eq!(h.desired.get("replica_count").and_then(JsonValue::as_i64), Some(3));
        assert_eq!(h.observed.get("replica_count").and_then(JsonValue::as_i64), Some(2));
        assert_eq!(h.conditions.len(), 2);
        assert_eq!(h.conditions[0].reason.as_deref(), Some("BackoffActive"));
        assert_eq!(h.conditions[0].message.as_deref(), Some("1 replica unhealthy"));
        assert_eq!(h.conditions[1].status, ConditionStatus::True);
        assert_eq!(h.conditions[1].reason, None);
        assert_eq!(h.kv.get("doubled").and_then(JsonValue::as_i64), Some(14));
        assert_eq!(h.kv.get("fallback").and_then(JsonValue::as_i64), Some(42));
        assert_eq!(h.kv.get("absent"), Some(&JsonValue::Null));
        assert_eq!(h.kv.get("n").and_then(JsonValue::as_i64), Some(5));
        assert_eq!(h.kv.get("had_n"), Some(&JsonValue::Bool(true)));
        assert!(h.kv_deleted.contains("attempts"));
        assert_eq!(h.log.len(), 1);
        assert_eq!(h.log[0].level, LogLevel::Warn);
        assert_eq!(h.trace.get("upstream").and_then(JsonValue::as_str), Some("abc"));
        assert_eq!(h.metrics.get("lag"), Some(&0.5));
        assert_eq!(h.events[0].kind, "Reconciled");
        assert_eq!(h.kv.get("first"), Some(&JsonValue::Bool(true)));
        assert_eq!(h.kv.get("converged"), Some(&JsonValue::Bool(true)));
        assert_eq!(h.kv.get("backoff").and_then(JsonValue::as_i64), Some(8000));
    }

    #[test]
    fn a_blue_vigy_reads_tick_metadata_and_rate_limits() {
        let host = VigyHost {
            tick_start_ms: 1000,
            previous_tick_ms: Some(800),
            tick_count: 7,
            ..VigyHost::default()
        };
        let program = "vigy_set(\"since\", vigy_since_last_tick())\nvigy_set(\"count\", vigy_tick_count())\nvigy_set(\"tick\", vigy_tick())\nvigy_set(\"limited\", vigy_rate_limited?(\"k\", 500))\n";
        let h = blue(program, host);
        assert_eq!(h.kv.get("since").and_then(JsonValue::as_i64), Some(200));
        assert_eq!(h.kv.get("count").and_then(JsonValue::as_i64), Some(7));
        assert_eq!(h.kv.get("tick").and_then(JsonValue::as_i64), Some(1000));
        assert_eq!(h.kv.get("limited"), Some(&JsonValue::Bool(false)));
        let mut next = h;
        next.tick_start_ms = 1300;
        let h2 = blue(program, next);
        assert_eq!(h2.kv.get("limited"), Some(&JsonValue::Bool(true)));
    }

    #[test]
    fn a_blue_vigy_that_breaks_an_intrinsic_contract_is_an_error_naming_it() {
        let err = blue_err("vigy_emit(\"sideways\")\n");
        assert!(err.contains("vigy_emit") && err.contains("unknown kind"), "{err}");
        let err = blue_err("vigy_condition(\"Ready\", \"maybe\")\n");
        assert!(err.contains("vigy_condition") && err.contains("true|false|unknown"), "{err}");
        let err = blue_err("vigy_incr(\"n\", \"one\")\n");
        assert!(err.contains("vigy_incr") && err.contains("integer delta"), "{err}");
        let err = blue_err("vigy_emit()\n");
        assert!(err.contains("vigy_emit"), "{err}");
    }

    #[test]
    fn a_tatara_name_is_not_a_blue_name() {
        let err = evaluate_in(Language::Blue, "(vigy-noop)\n", VigyHost::default(), &[]);
        assert!(matches!(err, Err(EvalErr::Blue(_))), "{err:?}");
    }

    #[test]
    fn a_blue_vigy_naming_a_host_effect_is_refused_before_it_runs() {
        let effect = host_effect_names()
            .into_iter()
            .find(|n| !n.contains('-'))
            .expect("a host effect blue can name");
        let err = blue_err(&format!("vigy_noop()\n{effect}(\"x\")\n"));
        assert!(err.contains(effect), "{err}");
    }

    #[test]
    fn a_host_extension_reaches_blue_vigies_through_its_blue_install() {
        let custom = closure_extension(|interp| {
            interp.register_fn(
                "mado_sessions",
                Arity::Exact(0),
                |_args: &[LispValue], host: &mut VigyHost, _sp| {
                    host.actions
                        .push(ReconcileAction::custom(serde_json::json!({"from": "mado"})));
                    Ok(LispValue::Int(3))
                },
            );
        });
        let mut extensions = standard_extensions();
        extensions.push(custom);
        let h = evaluate_in(
            Language::Blue,
            "vigy_set(\"sessions\", mado_sessions())\n",
            VigyHost::default(),
            &extensions,
        )
        .unwrap();
        assert_eq!(h.kv.get("sessions").and_then(JsonValue::as_i64), Some(3));
        assert_eq!(h.actions[0].kind, ReconcileKind::Custom);
    }

    #[tokio::test]
    async fn a_tatara_vigy_under_the_default_language_runs_exactly_as_before() {
        let tick = || VigyHost {
            tick_start_ms: 5000,
            tick_count: 10,
            ..VigyHost::default()
        };
        let broken = include_str!("../../../examples/tear-mirror.tatara");
        let before = LispReconciler::standard(broken).tick(tick()).await.unwrap_err();
        let now = ProgramReconciler::new(Language::default(), broken, standard_extensions())
            .tick(tick())
            .await
            .unwrap_err();
        assert_eq!(now.to_string(), before.to_string());
        for program in [
            include_str!("../../../examples/hello-vigy.tatara"),
            r#"
            (vigy-log "info" "tick")
            (if (vigy-once "init") (vigy-set "seeded" 0) (vigy-log "debug" "again"))
            (if (vigy-rate-limited? "work" 2000)
                (vigy-defer "rate-limited")
                (let ((n (vigy-get "seeded" 0)))
                  (vigy-observed "count" n)
                  (vigy-desired "count" (+ n 1))
                  (vigy-incr "seeded")
                  (vigy-pull "upstream")
                  (vigy-condition "Ready" "true" "InSync" "fine")))
            (vigy-trace "ever" (vigy-converged? "x"))
            (vigy-emit "custom" (vigy-get "seeded"))
            "#,
        ] {
            let before = LispReconciler::standard(program).tick(tick()).await.unwrap();
            let now = ProgramReconciler::new(Language::default(), program, standard_extensions())
                .tick(tick())
                .await
                .unwrap();
            let kinds = |h: &VigyHost| h.actions.iter().map(|a| a.kind).collect::<Vec<_>>();
            let logs = |h: &VigyHost| {
                h.log
                    .iter()
                    .map(|l| (l.level, l.message.clone()))
                    .collect::<Vec<_>>()
            };
            assert_eq!(kinds(&now), kinds(&before));
            assert_eq!(logs(&now), logs(&before));
            assert_eq!(now.kv, before.kv);
            assert_eq!(now.desired, before.desired);
            assert_eq!(now.observed, before.observed);
            assert_eq!(now.trace, before.trace);
            assert!(!kinds(&now).is_empty());
        }
        let h = evaluate("(vigy-log \"info\" \"hello from a vigy\") (vigy-noop)", VigyHost::default()).unwrap();
        assert_eq!(h.log[0].level, LogLevel::Info);
        assert_eq!(h.actions[0].kind, ReconcileKind::Noop);
    }

    #[test]
    fn the_blue_surface_is_the_tatara_surface_respelled() {
        let tatara: BTreeMap<&str, Arity> = TATARA
            .iter()
            .flat_map(|rows| rows.iter())
            .map(|r| (r.name, r.arity))
            .collect();
        let blue: BTreeMap<String, &HostFn> = <VigyHost as Host>::SURFACE
            .iter()
            .map(|f| (f.name.replace('_', "-"), f))
            .collect();
        assert_eq!(
            blue.keys().map(String::as_str).collect::<Vec<_>>(),
            tatara.keys().copied().collect::<Vec<_>>()
        );
        assert_eq!(blue.len(), 30);
        for (name, f) in &blue {
            let least = match tatara[name.as_str()] {
                Arity::Exact(n) | Arity::AtLeast(n) | Arity::Range(n, _) => n,
                Arity::Any => 0,
            };
            assert_eq!(f.required, least, "{name}");
            assert!(f.name.starts_with("vigy_"), "{}", f.name);
        }
    }
}
