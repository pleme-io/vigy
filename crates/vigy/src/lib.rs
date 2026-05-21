//! vigy — facade crate.
//!
//! Re-exports every vigy-* lib crate so consumers add a single
//! dependency:
//!
//! ```toml
//! [dependencies]
//! vigy = { git = "https://github.com/pleme-io/vigy" }
//! ```
//!
//! Then:
//!
//! ```ignore
//! use vigy::{runtime::RuntimeHandle, types::{Vigy, TickInterval}};
//! ```
//!
//! Feature flags trim transports out for embedders that only need
//! the core runtime (e.g. mado embedding vigy in-process without
//! exposing REST/gRPC).

pub use vigy_types as types;
pub use vigy_store as store;
pub use vigy_eval as eval;
pub use vigy_runtime as runtime;

#[cfg(feature = "rest")]
pub use vigy_rest as rest;

#[cfg(feature = "graphql")]
pub use vigy_graphql as graphql;

#[cfg(feature = "rpc")]
pub use vigy_rpc as rpc;

#[cfg(feature = "mcp")]
pub use vigy_mcp as mcp;

// Convenience re-exports — the 90% surface most consumers touch.
pub use vigy_runtime::{RuntimeError, RuntimeHandle};
pub use vigy_types::{
    ReconcileAction, ReconcileKind, ResultStatus, TickInterval, Vigy, VigyId, VigyRun,
    VigyState,
};
