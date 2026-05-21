//! One recorded execution of a vigy.

use crate::action::{ReconcileAction, ResultStatus};
use crate::id::{VigyId, VigyRunId};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct VigyRun {
    pub id: VigyRunId,
    pub vigy_id: VigyId,
    pub started_at: time::OffsetDateTime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<time::OffsetDateTime>,
    pub result: ResultStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub actions: Vec<ReconcileAction>,
}

impl VigyRun {
    pub fn started(vigy_id: VigyId) -> Self {
        Self {
            id: VigyRunId::new(),
            vigy_id,
            started_at: time::OffsetDateTime::now_utc(),
            ended_at: None,
            // ResultStatus::Skipped is the "haven't decided yet" placeholder
            // while a run is in-flight; finalised on completion.
            result: ResultStatus::Skipped,
            error: None,
            actions: Vec::new(),
        }
    }

    pub fn complete_ok(mut self, actions: Vec<ReconcileAction>) -> Self {
        self.actions = actions;
        self.result = ResultStatus::Ok;
        self.ended_at = Some(time::OffsetDateTime::now_utc());
        self
    }

    pub fn complete_failed(mut self, error: impl Into<String>) -> Self {
        self.result = ResultStatus::Failed;
        self.error = Some(error.into());
        self.ended_at = Some(time::OffsetDateTime::now_utc());
        self
    }
}
