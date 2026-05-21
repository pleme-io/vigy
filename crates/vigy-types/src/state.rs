//! Snapshot of a vigy's view of the world at a moment in time.
//!
//! Three structured surfaces a program populates each tick:
//!
//! - `desired` — what should be true, keyed (k/v map)
//! - `observed` — what currently is true, keyed (k/v map)
//! - `conditions` — kubernetes-style health/readiness signals
//! - `pending` — ReconcileActions the program emitted to bridge the gap
//!
//! All four are operator-authored via tatara-lisp intrinsics. The
//! runtime stores them verbatim for audit + the per-vigy state row;
//! only the host app needs to understand the payload schema.

use crate::action::ReconcileAction;
use crate::id::VigyId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use utoipa::ToSchema;

/// Kubernetes-style condition. Programs use these to record
/// readiness / health / convergence signals that get rolled up into
/// VigyState. Distinct from ReconcileActions: a condition describes
/// *state*, an action describes *intent to change state*.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Condition {
    /// Operator-chosen identifier — e.g. "Ready", "InSync", "Healthy",
    /// "Stalled", "AwaitingHumanReview".
    pub name: String,
    /// Tri-state — same shape as kubernetes condition.status.
    pub status: ConditionStatus,
    /// Operator-chosen short reason code (no whitespace) — used for
    /// rollup queries ("show me every vigy whose Ready=False reason is
    /// `BackoffActive`").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Operator-chosen human-readable message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub last_transition: time::OffsetDateTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ConditionStatus {
    True,
    False,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct VigyState {
    pub vigy_id: VigyId,
    /// Operator's declared "what should be true" — populated by
    /// `(vigy-desired k v)` calls in the program. Each key is an
    /// operator-chosen string; each value is arbitrary JSON.
    #[serde(default)]
    pub desired: BTreeMap<String, Value>,
    /// Operator's recorded "what currently is true" — populated by
    /// `(vigy-observed k v)` calls.
    #[serde(default)]
    pub observed: BTreeMap<String, Value>,
    /// Condition list — populated by `(vigy-condition name status reason)`.
    /// Latest entry per name wins; the runtime preserves last-transition
    /// timestamps when only the message changes.
    #[serde(default)]
    pub conditions: Vec<Condition>,
    /// Actions the program emitted on this tick. Drained from the
    /// host's actions buffer.
    #[serde(default)]
    pub pending: Vec<ReconcileAction>,
    pub captured_at: time::OffsetDateTime,
}

impl VigyState {
    pub fn empty(vigy_id: VigyId) -> Self {
        Self {
            vigy_id,
            desired: BTreeMap::new(),
            observed: BTreeMap::new(),
            conditions: Vec::new(),
            pending: Vec::new(),
            captured_at: time::OffsetDateTime::now_utc(),
        }
    }

    /// Look up a condition by name. Operators use this to ask
    /// "is this vigy Ready=True?" from outside.
    pub fn condition(&self, name: &str) -> Option<&Condition> {
        self.conditions.iter().find(|c| c.name == name)
    }
}
