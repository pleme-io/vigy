//! SeaORM entity definitions. The schema is laid out so an operator
//! can `sqlite3 ./vigy.db` and read everything human-eye — column
//! names match the Rust field names, timestamps are RFC3339 strings,
//! JSON payloads are stored as TEXT.

pub mod vigy;
pub mod vigy_event;
pub mod vigy_run;

pub use vigy::{ActiveModel as VigyActive, Column as VigyColumn, Entity as VigyEntity, Model as VigyModel};
pub use vigy_event::{
    ActiveModel as VigyEventActive, Column as VigyEventColumn, Entity as VigyEventEntity,
    Model as VigyEventModel,
};
pub use vigy_run::{
    ActiveModel as VigyRunActive, Column as VigyRunColumn, Entity as VigyRunEntity,
    Model as VigyRunModel,
};
