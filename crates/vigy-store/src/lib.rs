//! SeaORM-backed persistence for vigy.
//!
//! Default backend: **SQLite**, single file at
//! `~/.local/share/vigy/vigy.db` (or `$VIGY_DB`). Chosen so an operator
//! can `sqlite3 ./vigy.db` mid-incident and inspect everything — same
//! debug-friendly principle vitrine uses for evidence.
//!
//! Three entities:
//!   - `vigies` — the registered reconcilers
//!   - `vigy_runs` — one row per tick
//!   - `vigy_events` — append-only stream of ReconcileActions (for tail)
//!
//! `Store` is the facade. Hand-rolled CRUD methods over SeaORM
//! `EntityTrait` so callers don't need to import SeaORM directly.

pub mod entities;
pub mod migrator;
pub mod store;

pub use migrator::Migrator;
pub use store::{Store, StoreError};
