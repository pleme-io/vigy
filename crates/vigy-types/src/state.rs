//! Snapshot of a vigy's view of the world at a moment in time.

use crate::action::ReconcileAction;
use crate::id::VigyId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

/// The state a vigy declared on its most recent tick:
///   - `desired`: what should be true (declarative)
///   - `observed`: what currently is true
///   - `pending`: actions the vigy queued to bridge the gap
///
/// All three are arbitrary JSON — vigy programs author whatever schema
/// suits the reconciler. The runtime stores them verbatim for audit;
/// only the host app needs to understand the shape.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct VigyState {
    pub vigy_id: VigyId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed: Option<Value>,
    #[serde(default)]
    pub pending: Vec<ReconcileAction>,
    pub captured_at: time::OffsetDateTime,
}

impl VigyState {
    pub fn empty(vigy_id: VigyId) -> Self {
        Self {
            vigy_id,
            desired: None,
            observed: None,
            pending: Vec::new(),
            captured_at: time::OffsetDateTime::now_utc(),
        }
    }
}
