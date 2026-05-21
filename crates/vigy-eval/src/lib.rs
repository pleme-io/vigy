//! Tatara-lisp host bindings + intrinsics for vigy.
//!
//! ## The host
//!
//! [`VigyHost`] is the per-tick context handed to a vigy program. It
//! carries:
//!   - A *snapshot* of state the runtime promises is consistent for
//!     the duration of one tick.
//!   - An *output buffer* of [`ReconcileAction`]s the program emitted.
//!   - A *log* of structured messages for observability.
//!
//! The host is **per-tick**, not per-vigy. A vigy program never holds
//! mutable state across ticks; persistence is the runtime's job (via
//! `vigy-store`). This is the kubernetes-controller discipline applied
//! at lisp-program scope: each tick is a fresh observation.
//!
//! ## The intrinsics
//!
//! Functions vigy programs can call:
//!
//!   `(vigy-emit kind payload)`   — queue a ReconcileAction.
//!                                  kind ∈ {pull, push, noop, custom}.
//!   `(vigy-pull payload)`        — sugar for (vigy-emit "pull" payload).
//!   `(vigy-push payload)`        — sugar for (vigy-emit "push" payload).
//!   `(vigy-noop)`                — sugar for (vigy-emit "noop" {}).
//!   `(vigy-log level message)`   — emit a structured log line.
//!                                  level ∈ {trace, debug, info, warn, error}.
//!   `(vigy-tick)`                — Unix epoch millis of this tick's start.
//!
//! Plus the full tatara-lisp standard library installed via
//! `install_full_stdlib_with` — arithmetic, comparison, list ops,
//! strings, channels, fibers, higher-order helpers.
//!
//! ## Entry point
//!
//! [`evaluate`] takes a program string + a fresh host, parses + evaluates,
//! and returns the host (now populated with actions + log). The runtime
//! drains the actions for persistence and broadcast.

use serde_json::Value as JsonValue;
use tatara_lisp::read_spanned;
use tatara_lisp_eval::{
    install_full_stdlib_with, Arity, EvalError, Interpreter, Value as LispValue,
};
use thiserror::Error;
use vigy_types::{ReconcileAction, ReconcileKind};

#[derive(Debug, Error)]
pub enum EvalErr {
    #[error("parse: {0}")]
    Parse(String),
    #[error("eval: {0}")]
    Eval(String),
}

pub type Result<T> = std::result::Result<T, EvalErr>;

/// Per-tick host. The vigy program reads from `tick_start_ms` and writes
/// to `actions` + `log`. Both are drained after the tick.
#[derive(Debug, Default)]
pub struct VigyHost {
    pub tick_start_ms: i64,
    pub actions: Vec<ReconcileAction>,
    pub log: Vec<LogEntry>,
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

/// Evaluate a vigy program against a fresh host. Returns the host so
/// the runtime can drain `.actions` and `.log`.
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

/// Register the vigy-specific intrinsics. Split out so a host
/// (mado, tear-daemon) embedding vigy with extra primitives can call
/// `install_vigy_intrinsics` and then layer their own on top.
pub fn install_vigy_intrinsics(interp: &mut Interpreter<VigyHost>) {
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
            let kind_str = lisp_string(&args[0], sp)?;
            let kind = match kind_str.as_str() {
                "pull" => ReconcileKind::Pull,
                "push" => ReconcileKind::Push,
                "noop" => ReconcileKind::Noop,
                "custom" => ReconcileKind::Custom,
                other => {
                    return Err(EvalError::native_fn(
                        "vigy-emit",
                        format!("unknown kind {other:?}; expected pull|push|noop|custom"),
                        sp,
                    ))
                }
            };
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

    // Sugar: (vigy-pull payload)
    interp.register_fn(
        "vigy-pull",
        Arity::Exact(1),
        |args: &[LispValue], host: &mut VigyHost, _sp| {
            host.actions
                .push(ReconcileAction::pull(lisp_to_json(&args[0])));
            Ok(LispValue::Nil)
        },
    );

    // Sugar: (vigy-push payload)
    interp.register_fn(
        "vigy-push",
        Arity::Exact(1),
        |args: &[LispValue], host: &mut VigyHost, _sp| {
            host.actions
                .push(ReconcileAction::push(lisp_to_json(&args[0])));
            Ok(LispValue::Nil)
        },
    );

    // (vigy-noop)
    interp.register_fn(
        "vigy-noop",
        Arity::Exact(0),
        |_args: &[LispValue], host: &mut VigyHost, _sp| {
            host.actions.push(ReconcileAction::noop());
            Ok(LispValue::Nil)
        },
    );

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

    // (vigy-tick) → integer millis (this tick's start time, UTC epoch)
    interp.register_fn(
        "vigy-tick",
        Arity::Exact(0),
        |_args: &[LispValue], host: &mut VigyHost, _sp| Ok(LispValue::Int(host.tick_start_ms)),
    );
}

// ---------- helpers ----------

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

/// Best-effort conversion of a tatara-lisp `Value` into a `serde_json::Value`
/// for embedding in a ReconcileAction payload.
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
        LispValue::List(items) => {
            JsonValue::Array(items.iter().map(lisp_to_json).collect())
        }
        LispValue::Map(m) => {
            // Map → JSON object. Keys stringified; tatara-lisp's MapKey
            // already enforces hashability so we only handle the
            // documented variants.
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
        // Procedures / promises / errors / foreign don't serialise
        // cleanly — record their type name for debugging.
        _ => JsonValue::String(format!("<{}>", v.type_name())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluate_runs_an_empty_program() {
        let h = evaluate("", VigyHost::default()).unwrap();
        assert!(h.actions.is_empty());
    }

    #[test]
    fn vigy_noop_emits_one_action() {
        let h = evaluate("(vigy-noop)", VigyHost::default()).unwrap();
        assert_eq!(h.actions.len(), 1);
        assert_eq!(h.actions[0].kind, ReconcileKind::Noop);
    }

    #[test]
    fn vigy_pull_with_payload() {
        let h = evaluate(
            r#"(vigy-pull "session-abc-123")"#,
            VigyHost::default(),
        )
        .unwrap();
        assert_eq!(h.actions.len(), 1);
        assert_eq!(h.actions[0].kind, ReconcileKind::Pull);
        assert_eq!(
            h.actions[0].payload.as_ref().and_then(|v| v.as_str()),
            Some("session-abc-123")
        );
    }

    #[test]
    fn vigy_log_records_levelled_message() {
        let h = evaluate(
            r#"(vigy-log "info" "everything is fine")"#,
            VigyHost::default(),
        )
        .unwrap();
        assert_eq!(h.log.len(), 1);
        assert_eq!(h.log[0].level, LogLevel::Info);
        assert_eq!(h.log[0].message, "everything is fine");
    }

    #[test]
    fn vigy_tick_returns_host_start_time() {
        let mut host = VigyHost::default();
        host.tick_start_ms = 1_700_000_000_000;
        // Emit the tick as a custom payload so we can read it back without
        // depending on a number-to-string stdlib fn.
        let h = evaluate("(vigy-emit \"custom\" (vigy-tick))", host).unwrap();
        assert_eq!(h.actions.len(), 1);
        assert_eq!(
            h.actions[0].payload.as_ref().and_then(|v| v.as_i64()),
            Some(1_700_000_000_000)
        );
    }

    #[test]
    fn invalid_kind_is_rejected_at_eval_time() {
        let err = evaluate(r#"(vigy-emit "bogus")"#, VigyHost::default());
        assert!(err.is_err());
    }
}
