//! vigy-types — pure typed domain for the vigy reconciler runtime.
//!
//! Every other crate (`vigy-store`, `vigy-eval`, `vigy-runtime`, the four
//! API surfaces, the CLI) reads or writes these types. Each transport
//! (gRPC, GraphQL, REST) is just a different *serialisation* of the
//! same domain — defined once here, then projected.
//!
//! No I/O. No tokio. No DB. Just types + validation.

pub mod action;
pub mod error;
pub mod id;
pub mod label;
pub mod run;
pub mod state;
pub mod vigy;

pub use action::{ReconcileAction, ReconcileKind, ResultStatus};
pub use error::{Error, Result};
pub use id::{VigyId, VigyRunId};
pub use label::Labels;
pub use run::VigyRun;
pub use state::VigyState;
pub use vigy::{TickInterval, Vigy};
