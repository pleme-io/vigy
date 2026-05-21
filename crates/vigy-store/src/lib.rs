//! Persistence layer for vigy — `VigyStore` trait + two impls.
//!
//! ## Trait
//!
//! [`VigyStore`] is the async surface every consumer of vigy state
//! goes through. Two impls in the box:
//!
//! - [`SeaormStore`] — SeaORM/SQLite. Default for the `vigy` binary
//!   and `mado`-embedded runtime. File at
//!   `~/.local/share/vigy/vigy.db`. Operator-debuggable via the
//!   `sqlite3` CLI.
//! - [`InMemoryStore`] — pure in-memory; useful for tests + ephemeral
//!   one-shot tools that don't want a DB file.
//!
//! Adding a Postgres / sled / NATS-KV impl later is a new module
//! implementing the same trait — no consumer changes needed.
//!
//! ## Schema (SeaORM impl)
//!
//! Four entities:
//!   - `vigies`      — registered reconcilers
//!   - `vigy_runs`   — one row per executed tick
//!   - `vigy_events` — append-only stream of ReconcileActions (for tail)
//!   - `vigy_kv`     — per-vigy persistent key/value storage

pub mod entities;
pub mod inmemory;
pub mod migrator;
pub mod store;
pub mod traits;

pub use inmemory::InMemoryStore;
pub use migrator::Migrator;
pub use store::{SeaormStore, Store, StoreError};
pub use traits::VigyStore;
