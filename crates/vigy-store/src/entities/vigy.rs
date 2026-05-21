//! `vigies` table — the registered reconcilers.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "vigies")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub name: String,
    pub program: String,
    pub tick_interval_ms: i64,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
    /// Labels serialised as JSON. SQLite has no native map; this keeps
    /// the schema simple and the data debuggable via `sqlite3 .schema`.
    pub labels_json: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::vigy_run::Entity")]
    Runs,
    #[sea_orm(has_many = "super::vigy_event::Entity")]
    Events,
}

impl Related<super::vigy_run::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Runs.def()
    }
}

impl Related<super::vigy_event::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Events.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
