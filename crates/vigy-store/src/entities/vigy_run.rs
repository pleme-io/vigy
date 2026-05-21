//! `vigy_runs` table — one row per executed tick.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "vigy_runs")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub vigy_id: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub result: String, // "ok" | "failed" | "skipped"
    pub error: Option<String>,
    /// Actions as JSON array.
    pub actions_json: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::vigy::Entity",
        from = "Column::VigyId",
        to = "super::vigy::Column::Id"
    )]
    Vigy,
}

impl Related<super::vigy::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Vigy.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
