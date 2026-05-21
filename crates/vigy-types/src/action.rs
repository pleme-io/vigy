//! Reconcile actions — what a vigy decides to do on a given tick.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

/// Classification of a reconcile action. The runtime doesn't *interpret*
/// the payload — interpretation is the host app's responsibility. The
/// runtime just records the action + its result for audit + tail. The
/// kind enum is the typed verb surface — operators can grep for
/// "all Create actions on the asia-southeast1-sync vigy in the last
/// 7 days" without parsing payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ReconcileKind {
    // ── observation / no-op ──────────────────────────────────────
    /// Vigy ticked, observed, decided no change needed. Recorded
    /// for audit (proves the loop is alive + converging).
    Noop,
    /// Vigy chose to skip this tick entirely — usually because a
    /// precondition isn't met (rate-limited, upstream unreachable,
    /// waiting on external signal). Distinct from Noop: Defer says
    /// "try again next tick"; Noop says "everything is fine".
    Defer,

    // ── direction-of-data verbs ──────────────────────────────────
    /// Vigy fetched state from an upstream and wants it applied locally.
    Pull,
    /// Vigy wants to push local state to an upstream.
    Push,

    // ── lifecycle verbs ──────────────────────────────────────────
    /// Vigy detected the target doesn't exist and wants to create it.
    Create,
    /// Vigy detected the target exists but doesn't match desired state.
    Update,
    /// Vigy detected the target should no longer exist.
    Delete,
    /// Vigy wants the host to (re-)apply a complete payload — the
    /// idempotent "make it match" verb. Distinct from Update which is
    /// a delta; Apply is a full desired-state assertion.
    Apply,
    /// Vigy wants the host to restart something (process, service,
    /// connection). Used when state is intact but the runtime is stuck.
    Restart,

    // ── escape hatch ─────────────────────────────────────────────
    /// Anything the standard kinds don't cover — host-app-defined.
    /// Operators should reach for this sparingly; prefer adding a
    /// typed kind upstream when a Custom action repeats.
    Custom,
}

/// The outcome of executing an action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ResultStatus {
    Ok,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReconcileAction {
    pub kind: ReconcileKind,
    /// Arbitrary JSON payload describing the action — schema is up to the
    /// host app + the vigy's tatara-lisp program. Examples:
    ///   pull: { "from": "tear-daemon", "session_id": "xyz" }
    ///   push: { "to": "disk", "snapshot_id": "..." }
    ///   custom: { "op": "rename-pane", "pane_id": "...", "name": "build" }
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<ResultStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl ReconcileAction {
    pub fn noop() -> Self {
        Self {
            kind: ReconcileKind::Noop,
            payload: None,
            result: Some(ResultStatus::Ok),
            message: None,
        }
    }

    /// Constructor pair for every typed kind. Reads as straight-line
    /// English when used from Rust: `ReconcileAction::pull(json!({"from": "tear"}))`.
    pub fn pull(payload: Value) -> Self {
        Self::with_kind(ReconcileKind::Pull, payload)
    }
    pub fn push(payload: Value) -> Self {
        Self::with_kind(ReconcileKind::Push, payload)
    }
    pub fn create(payload: Value) -> Self {
        Self::with_kind(ReconcileKind::Create, payload)
    }
    pub fn update(payload: Value) -> Self {
        Self::with_kind(ReconcileKind::Update, payload)
    }
    pub fn delete(payload: Value) -> Self {
        Self::with_kind(ReconcileKind::Delete, payload)
    }
    pub fn apply(payload: Value) -> Self {
        Self::with_kind(ReconcileKind::Apply, payload)
    }
    pub fn restart(payload: Value) -> Self {
        Self::with_kind(ReconcileKind::Restart, payload)
    }
    pub fn defer(reason: impl Into<String>) -> Self {
        Self {
            kind: ReconcileKind::Defer,
            payload: None,
            result: Some(ResultStatus::Skipped),
            message: Some(reason.into()),
        }
    }
    pub fn custom(payload: Value) -> Self {
        Self::with_kind(ReconcileKind::Custom, payload)
    }

    fn with_kind(kind: ReconcileKind, payload: Value) -> Self {
        Self {
            kind,
            payload: Some(payload),
            result: None,
            message: None,
        }
    }

    pub fn with_result(mut self, result: ResultStatus, message: Option<String>) -> Self {
        self.result = Some(result);
        self.message = message;
        self
    }
}
