//! vigy-types error type.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid id {id:?}: {reason}")]
    InvalidId { id: String, reason: &'static str },

    #[error("invalid tick interval {ms} ms: must be >= 100")]
    InvalidTickInterval { ms: u64 },

    #[error("invalid label key {key:?}: {reason}")]
    InvalidLabelKey { key: String, reason: &'static str },

    #[error("invalid label selector {selector:?}: {reason}")]
    InvalidLabelSelector {
        selector: String,
        reason: &'static str,
    },

    #[error("vigy program empty")]
    EmptyProgram,

    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
}
