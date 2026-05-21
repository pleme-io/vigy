//! `vigy_kv` table — per-vigy persistent key/value storage.
//!
//! Powers the "permanent solution over time" verb family:
//!   - retry counters, backoff attempt tracking
//!   - rate-limit cursors
//!   - convergence flags (mark-converged / converged?)
//!   - once-only sentinels
//!   - operator-authored cross-tick memoization
//!
//! Loaded by the runtime at tick start (full per-vigy scan; small),
//! mutated by the tatara-lisp program in-memory, saved back at tick end
//! (only dirty / deleted keys; minimum writes).

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "vigy_kv")]
pub struct Model {
    /// Composite primary key: (vigy_id, key).
    #[sea_orm(primary_key, auto_increment = false)]
    pub vigy_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub key: String,
    /// JSON-encoded value. Schema is whatever the operator put in via
    /// `(vigy-set k v)`. Numbers/strings/bools/objects/arrays all
    /// round-trip via vigy-eval's json↔lisp helpers.
    pub value_json: String,
    pub updated_at: String,
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
