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

use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use tatara_lisp::read_spanned;
use tatara_lisp_eval::{
    install_full_stdlib_with, Arity, EvalError, Interpreter, Value as LispValue,
};
use thiserror::Error;
use vigy_types::{Condition, ConditionStatus, ReconcileAction, ReconcileKind};

#[derive(Debug, Error)]
pub enum EvalErr {
    #[error("parse: {0}")]
    Parse(String),
    #[error("eval: {0}")]
    Eval(String),
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

/// Evaluate a vigy program against a fresh host. Returns the host so
/// the runtime can drain the buffers + persist dirty kv keys.
pub fn evaluate(program: &str, mut host: VigyHost) -> Result<VigyHost> {
    let mut interp: Interpreter<VigyHost> = Interpreter::new();
    install_full_stdlib_with(&mut interp, &mut host);
    install_vigy_intrinsics(&mut interp);

    let forms = read_spanned(program).map_err(|e| EvalErr::Parse(format!("{e}")))?;
    interp
        .eval_program(&forms, &mut host)
        .map_err(|e| EvalErr::Eval(format!("{e}")))?;
    Ok(host)
}

/// Register every vigy framework intrinsic. Split out so an embedder
/// (mado, tear-daemon) that wants extra host-specific primitives can
/// call this first then layer its own.
pub fn install_vigy_intrinsics(interp: &mut Interpreter<VigyHost>) {
    install_action_intrinsics(interp);
    install_state_intrinsics(interp);
    install_kv_intrinsics(interp);
    install_convergence_intrinsics(interp);
    install_scheduling_intrinsics(interp);
    install_diagnostic_intrinsics(interp);
}

// ─────────────────────────────────────────────────────────────────
// Actions
// ─────────────────────────────────────────────────────────────────

fn install_action_intrinsics(interp: &mut Interpreter<VigyHost>) {
    // (vigy-emit kind payload?)
    interp.register_fn(
        "vigy-emit",
        Arity::AtLeast(1),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            if args.is_empty() || args.len() > 2 {
                return Err(EvalError::native_fn(
                    "vigy-emit",
                    format!("expected 1 or 2 args (kind, payload?), got {}", args.len()),
                    sp,
                ));
            }
            let kind = parse_kind(&lisp_string(&args[0], sp)?, sp, "vigy-emit")?;
            let payload = if args.len() == 2 {
                Some(lisp_to_json(&args[1]))
            } else {
                None
            };
            host.actions.push(ReconcileAction {
                kind,
                payload,
                result: None,
                message: None,
            });
            Ok(LispValue::Nil)
        },
    );

    // sugar — one per kind. Use the constructors on ReconcileAction
    // so anything wired into the action shape stays consistent.
    register_sugar(interp, "vigy-noop", Arity::Exact(0), |_, host| {
        host.actions.push(ReconcileAction::noop());
        Ok(LispValue::Nil)
    });
    register_sugar(interp, "vigy-defer", Arity::Exact(1), |args, host| {
        host.actions
            .push(ReconcileAction::defer(args[0].to_display_string()));
        Ok(LispValue::Nil)
    });
    register_sugar(interp, "vigy-pull", Arity::Exact(1), |args, host| {
        host.actions.push(ReconcileAction::pull(lisp_to_json(&args[0])));
        Ok(LispValue::Nil)
    });
    register_sugar(interp, "vigy-push", Arity::Exact(1), |args, host| {
        host.actions.push(ReconcileAction::push(lisp_to_json(&args[0])));
        Ok(LispValue::Nil)
    });
    register_sugar(interp, "vigy-create", Arity::Exact(1), |args, host| {
        host.actions
            .push(ReconcileAction::create(lisp_to_json(&args[0])));
        Ok(LispValue::Nil)
    });
    register_sugar(interp, "vigy-update", Arity::Exact(1), |args, host| {
        host.actions
            .push(ReconcileAction::update(lisp_to_json(&args[0])));
        Ok(LispValue::Nil)
    });
    register_sugar(interp, "vigy-delete-action", Arity::Exact(1), |args, host| {
        // Named with -action suffix so it doesn't collide with the kv
        // intrinsic `vigy-del` (which removes a kv key). Programs that
        // emit a delete reconcile action use this verb explicitly.
        host.actions
            .push(ReconcileAction::delete(lisp_to_json(&args[0])));
        Ok(LispValue::Nil)
    });
    register_sugar(interp, "vigy-apply", Arity::Exact(1), |args, host| {
        host.actions
            .push(ReconcileAction::apply(lisp_to_json(&args[0])));
        Ok(LispValue::Nil)
    });
    register_sugar(interp, "vigy-restart", Arity::Exact(1), |args, host| {
        host.actions
            .push(ReconcileAction::restart(lisp_to_json(&args[0])));
        Ok(LispValue::Nil)
    });
}

// ─────────────────────────────────────────────────────────────────
// Structured state
// ─────────────────────────────────────────────────────────────────

fn install_state_intrinsics(interp: &mut Interpreter<VigyHost>) {
    // (vigy-desired key value)
    interp.register_fn(
        "vigy-desired",
        Arity::Exact(2),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            let key = lisp_string(&args[0], sp)?;
            host.desired.insert(key, lisp_to_json(&args[1]));
            Ok(LispValue::Nil)
        },
    );

    // (vigy-observed key value)
    interp.register_fn(
        "vigy-observed",
        Arity::Exact(2),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            let key = lisp_string(&args[0], sp)?;
            host.observed.insert(key, lisp_to_json(&args[1]));
            Ok(LispValue::Nil)
        },
    );

    // (vigy-condition name status [reason [message]])
    interp.register_fn(
        "vigy-condition",
        Arity::AtLeast(2),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            if args.len() < 2 || args.len() > 4 {
                return Err(EvalError::native_fn(
                    "vigy-condition",
                    format!("expected 2..4 args (name, status, reason?, message?), got {}", args.len()),
                    sp,
                ));
            }
            let name = lisp_string(&args[0], sp)?;
            let status_str = lisp_string(&args[1], sp)?;
            let status = match status_str.as_str() {
                "true" | "True" => ConditionStatus::True,
                "false" | "False" => ConditionStatus::False,
                "unknown" | "Unknown" => ConditionStatus::Unknown,
                other => {
                    return Err(EvalError::native_fn(
                        "vigy-condition",
                        format!("status must be true|false|unknown, got {other:?}"),
                        sp,
                    ));
                }
            };
            let reason = if args.len() >= 3 {
                Some(lisp_string(&args[2], sp)?)
            } else {
                None
            };
            let message = if args.len() >= 4 {
                Some(lisp_string(&args[3], sp)?)
            } else {
                None
            };
            host.conditions.push(Condition {
                name,
                status,
                reason,
                message,
                last_transition: time::OffsetDateTime::now_utc(),
            });
            Ok(LispValue::Nil)
        },
    );
}

// ─────────────────────────────────────────────────────────────────
// Persistent KV
// ─────────────────────────────────────────────────────────────────

fn install_kv_intrinsics(interp: &mut Interpreter<VigyHost>) {
    // (vigy-get key default?)
    interp.register_fn(
        "vigy-get",
        Arity::AtLeast(1),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            let key = lisp_string(&args[0], sp)?;
            match host.kv.get(&key).cloned() {
                Some(v) => Ok(json_to_lisp(&v)),
                None if args.len() == 2 => Ok(args[1].clone()),
                None => Ok(LispValue::Nil),
            }
        },
    );

    // (vigy-set key value)
    interp.register_fn(
        "vigy-set",
        Arity::Exact(2),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            let key = lisp_string(&args[0], sp)?;
            let value = lisp_to_json(&args[1]);
            host.kv.insert(key.clone(), value);
            host.kv_dirty.insert(key.clone());
            host.kv_deleted.remove(&key);
            Ok(LispValue::Nil)
        },
    );

    // (vigy-incr key delta?) — defaults delta to 1.
    interp.register_fn(
        "vigy-incr",
        Arity::AtLeast(1),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            let key = lisp_string(&args[0], sp)?;
            let delta = if args.len() >= 2 {
                match &args[1] {
                    LispValue::Int(n) => *n,
                    other => {
                        return Err(EvalError::type_mismatch(
                            "integer delta",
                            other.type_name(),
                            sp,
                        ))
                    }
                }
            } else {
                1
            };
            let current = host
                .kv
                .get(&key)
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let next = current.saturating_add(delta);
            host.kv
                .insert(key.clone(), JsonValue::Number(next.into()));
            host.kv_dirty.insert(key.clone());
            host.kv_deleted.remove(&key);
            Ok(LispValue::Int(next))
        },
    );

    // (vigy-has? key)
    interp.register_fn(
        "vigy-has?",
        Arity::Exact(1),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            let key = lisp_string(&args[0], sp)?;
            Ok(LispValue::Bool(host.kv.contains_key(&key)))
        },
    );

    // (vigy-del key)
    interp.register_fn(
        "vigy-del",
        Arity::Exact(1),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            let key = lisp_string(&args[0], sp)?;
            let existed = host.kv.remove(&key).is_some();
            host.kv_dirty.remove(&key);
            host.kv_deleted.insert(key);
            Ok(LispValue::Bool(existed))
        },
    );
}

// ─────────────────────────────────────────────────────────────────
// Convergence / idempotency
// ─────────────────────────────────────────────────────────────────

fn install_convergence_intrinsics(interp: &mut Interpreter<VigyHost>) {
    // (vigy-once key) — true exactly once across the vigy's lifetime.
    // Internally: checks if "__once::<key>" is set in kv; sets it if not.
    interp.register_fn(
        "vigy-once",
        Arity::Exact(1),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            let key = format!("__once::{}", lisp_string(&args[0], sp)?);
            if host.kv.contains_key(&key) {
                Ok(LispValue::Bool(false))
            } else {
                host.kv.insert(key.clone(), JsonValue::Bool(true));
                host.kv_dirty.insert(key);
                Ok(LispValue::Bool(true))
            }
        },
    );

    // (vigy-mark-converged key) — record permanent achievement.
    interp.register_fn(
        "vigy-mark-converged",
        Arity::Exact(1),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            let key = format!("__converged::{}", lisp_string(&args[0], sp)?);
            host.kv.insert(
                key.clone(),
                JsonValue::String(
                    time::OffsetDateTime::now_utc()
                        .format(&time::format_description::well_known::Rfc3339)
                        .unwrap_or_default(),
                ),
            );
            host.kv_dirty.insert(key);
            Ok(LispValue::Nil)
        },
    );

    // (vigy-converged? key)
    interp.register_fn(
        "vigy-converged?",
        Arity::Exact(1),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            let key = format!("__converged::{}", lisp_string(&args[0], sp)?);
            Ok(LispValue::Bool(host.kv.contains_key(&key)))
        },
    );
}

// ─────────────────────────────────────────────────────────────────
// Scheduling helpers
// ─────────────────────────────────────────────────────────────────

fn install_scheduling_intrinsics(interp: &mut Interpreter<VigyHost>) {
    // (vigy-tick) — current tick's epoch ms.
    interp.register_fn(
        "vigy-tick",
        Arity::Exact(0),
        |_args: &[LispValue], host: &mut VigyHost, _sp| Ok(LispValue::Int(host.tick_start_ms)),
    );

    // (vigy-tick-count) — Nth tick for this vigy.
    interp.register_fn(
        "vigy-tick-count",
        Arity::Exact(0),
        |_args: &[LispValue], host: &mut VigyHost, _sp| Ok(LispValue::Int(host.tick_count)),
    );

    // (vigy-since-last-tick) — ms since previous tick. -1 if first tick.
    interp.register_fn(
        "vigy-since-last-tick",
        Arity::Exact(0),
        |_args: &[LispValue], host: &mut VigyHost, _sp| {
            let v = host
                .previous_tick_ms
                .map(|p| host.tick_start_ms.saturating_sub(p))
                .unwrap_or(-1);
            Ok(LispValue::Int(v))
        },
    );

    // (vigy-rate-limited? key min-interval-ms) — returns true if `key`
    // has been recorded as acted-on within the last min-interval-ms.
    // Side-effect on false: records the current tick under key so the
    // next call gates correctly. Pattern: gate the body of an action,
    // not the observe step.
    interp.register_fn(
        "vigy-rate-limited?",
        Arity::Exact(2),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            let key = format!("__ratelimit::{}", lisp_string(&args[0], sp)?);
            let min_ms = match &args[1] {
                LispValue::Int(n) => *n,
                other => {
                    return Err(EvalError::type_mismatch(
                        "integer min-interval-ms",
                        other.type_name(),
                        sp,
                    ))
                }
            };
            let last = host.kv.get(&key).and_then(|v| v.as_i64()).unwrap_or(0);
            let elapsed = host.tick_start_ms.saturating_sub(last);
            if elapsed < min_ms {
                Ok(LispValue::Bool(true))
            } else {
                host.kv
                    .insert(key.clone(), JsonValue::Number(host.tick_start_ms.into()));
                host.kv_dirty.insert(key);
                Ok(LispValue::Bool(false))
            }
        },
    );

    // (vigy-backoff-ms attempt) — capped exponential backoff.
    //   attempt 0 → 1000ms, 1 → 2000, 2 → 4000, ..., capped at 30000.
    interp.register_fn(
        "vigy-backoff-ms",
        Arity::Exact(1),
        |args: &[LispValue], _host: &mut VigyHost, sp| {
            let attempt = match &args[0] {
                LispValue::Int(n) => (*n).max(0) as u32,
                other => {
                    return Err(EvalError::type_mismatch(
                        "integer attempt",
                        other.type_name(),
                        sp,
                    ))
                }
            };
            let secs = 1u64
                .checked_shl(attempt.min(5))
                .unwrap_or(30);
            let ms = (secs.min(30) * 1000) as i64;
            Ok(LispValue::Int(ms))
        },
    );
}

// ─────────────────────────────────────────────────────────────────
// Diagnostics
// ─────────────────────────────────────────────────────────────────

fn install_diagnostic_intrinsics(interp: &mut Interpreter<VigyHost>) {
    // (vigy-log level message)
    interp.register_fn(
        "vigy-log",
        Arity::Exact(2),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            let level_str = lisp_string(&args[0], sp)?;
            let level = LogLevel::parse(&level_str).ok_or_else(|| {
                EvalError::native_fn(
                    "vigy-log",
                    format!("unknown level {level_str:?}; expected trace|debug|info|warn|error"),
                    sp,
                )
            })?;
            let message = lisp_string(&args[1], sp)?;
            host.log.push(LogEntry { level, message });
            Ok(LispValue::Nil)
        },
    );

    // (vigy-trace key value)
    interp.register_fn(
        "vigy-trace",
        Arity::Exact(2),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            let key = lisp_string(&args[0], sp)?;
            host.trace.insert(key, lisp_to_json(&args[1]));
            Ok(LispValue::Nil)
        },
    );

    // (vigy-metric name value) — value is float-coerced from int/float.
    interp.register_fn(
        "vigy-metric",
        Arity::Exact(2),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            let name = lisp_string(&args[0], sp)?;
            let value = match &args[1] {
                LispValue::Int(n) => *n as f64,
                LispValue::Float(n) => *n,
                other => {
                    return Err(EvalError::type_mismatch(
                        "numeric metric value",
                        other.type_name(),
                        sp,
                    ))
                }
            };
            host.metrics.insert(name, value);
            Ok(LispValue::Nil)
        },
    );

    // (vigy-event kind message)
    interp.register_fn(
        "vigy-event",
        Arity::Exact(2),
        |args: &[LispValue], host: &mut VigyHost, sp| {
            let kind = lisp_string(&args[0], sp)?;
            let message = lisp_string(&args[1], sp)?;
            host.events.push(HostEvent { kind, message });
            Ok(LispValue::Nil)
        },
    );
}

// ─────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────

fn register_sugar<F>(interp: &mut Interpreter<VigyHost>, name: &'static str, arity: Arity, f: F)
where
    F: Fn(&[LispValue], &mut VigyHost) -> std::result::Result<LispValue, EvalError>
        + Send
        + Sync
        + 'static,
{
    interp.register_fn(name, arity, move |args: &[LispValue], host: &mut VigyHost, _sp| {
        f(args, host)
    });
}

fn parse_kind(
    s: &str,
    sp: tatara_lisp::Span,
    fn_name: &'static str,
) -> std::result::Result<ReconcileKind, EvalError> {
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
        other => Err(EvalError::native_fn(
            fn_name,
            format!(
                "unknown kind {other:?}; expected noop|defer|pull|push|create|update|delete|apply|restart|custom"
            ),
            sp,
        )),
    }
}

fn lisp_string(v: &LispValue, sp: tatara_lisp::Span) -> std::result::Result<String, EvalError> {
    match v {
        LispValue::Str(s) => Ok(s.to_string()),
        LispValue::Symbol(s) => Ok(s.to_string()),
        LispValue::Keyword(s) => Ok(s.to_string()),
        other => Err(EvalError::type_mismatch(
            "string|symbol|keyword",
            other.type_name(),
            sp,
        )),
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
            LispValue::List(Arc::new(converted))
        }
        JsonValue::Object(obj) => {
            let mut map = std::collections::HashMap::new();
            for (k, val) in obj {
                map.insert(
                    tatara_lisp_eval::MapKey::Str(Arc::from(k.as_str())),
                    json_to_lisp(val),
                );
            }
            LispValue::Map(Arc::new(map))
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
}
