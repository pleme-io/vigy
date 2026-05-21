//! Reconcile actions — what a vigy decides to do on a given tick.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

/// Classification of a reconcile action. The runtime doesn't *interpret*
/// the payload — interpretation is the host app's responsibility. The
/// runtime just records the action + its result for audit + tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ReconcileKind {
    /// Vigy fetched state from an upstream and wants it applied locally.
    Pull,
    /// Vigy wants to push local state to an upstream.
    Push,
    /// Vigy ticked and observed: nothing to do. Recorded for audit.
    Noop,
    /// Anything the standard kinds don't cover — host-app-defined.
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

    pub fn pull(payload: Value) -> Self {
        Self {
            kind: ReconcileKind::Pull,
            payload: Some(payload),
            result: None,
            message: None,
        }
    }

    pub fn push(payload: Value) -> Self {
        Self {
            kind: ReconcileKind::Push,
            payload: Some(payload),
            result: None,
            message: None,
        }
    }

    pub fn custom(payload: Value) -> Self {
        Self {
            kind: ReconcileKind::Custom,
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
