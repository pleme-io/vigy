//! `vigy_events` table — append-only stream of ReconcileActions for
//! the tail subscription. Keyed by (vigy_id, sequence) so `tail` can
//! resume from a known cursor after a reconnect.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "vigy_events")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub seq: i64,
    pub vigy_id: String,
    pub run_id: String,
    pub kind: String, // matches ReconcileKind values
    pub payload_json: Option<String>,
    pub result: Option<String>,
    pub message: Option<String>,
    pub emitted_at: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::vigy::Entity",
        from = "Column::VigyId",
        to = "super::vigy::Column::Id"
    )]
    Vigy,
    #[sea_orm(
        belongs_to = "super::vigy_run::Entity",
        from = "Column::RunId",
        to = "super::vigy_run::Column::Id"
    )]
    Run,
}

impl Related<super::vigy::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Vigy.def()
    }
}
impl Related<super::vigy_run::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Run.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
