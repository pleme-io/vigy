//! Identifiers — content-derived for vigies (stable across restarts;
//! same program + name yields the same id), uuidv7 for runs (sortable
//! by time + globally unique).
//!
//! Vigy ids are BLAKE3-derived to match the wider pleme-io convergence-
//! computing stack (tatara, tameshi, sekiban). Same hash family as mado's
//! clipboard hashes and tear's `SessionId`/`PaneId`.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Stable vigy identifier — first 16 hex chars of BLAKE3(name || "\0" || program).
///
/// Two consequences worth knowing:
/// 1. Renaming or editing a vigy mints a *new* id; the runtime treats it
///    as a fresh registration (the old recorded runs stay under the old id).
/// 2. Re-registering the same name+program is idempotent — no duplicate.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct VigyId(String);

impl VigyId {
    pub fn from_name_and_program(name: &str, program: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(name.as_bytes());
        hasher.update(&[0u8]);
        hasher.update(program.as_bytes());
        let hex = hasher.finalize().to_hex();
        Self(hex.as_str()[..16].to_string())
    }

    /// Accept an already-existing id (typically read from the store).
    pub fn parse(s: impl Into<String>) -> Result<Self> {
        let s = s.into();
        if s.len() != 16 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(Error::InvalidId {
                id: s,
                reason: "must be 16 lowercase hex chars",
            });
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for VigyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Run identifier — uuidv7 so it sorts by creation time and is globally
/// unique without coordination across processes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct VigyRunId(String);

impl VigyRunId {
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7().to_string())
    }

    pub fn parse(s: impl Into<String>) -> Result<Self> {
        let s = s.into();
        uuid::Uuid::parse_str(&s).map_err(|_| Error::InvalidId {
            id: s.clone(),
            reason: "must be a uuid",
        })?;
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for VigyRunId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Default for VigyRunId {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vigy_id_is_deterministic() {
        let a = VigyId::from_name_and_program("sync-tear", "(defvigy ...)");
        let b = VigyId::from_name_and_program("sync-tear", "(defvigy ...)");
        assert_eq!(a, b);
    }

    #[test]
    fn vigy_id_changes_when_program_changes() {
        let a = VigyId::from_name_and_program("sync-tear", "(defvigy a)");
        let b = VigyId::from_name_and_program("sync-tear", "(defvigy b)");
        assert_ne!(a, b);
    }

    #[test]
    fn vigy_id_is_16_hex_chars() {
        let id = VigyId::from_name_and_program("x", "y");
        assert_eq!(id.as_str().len(), 16);
        assert!(id.as_str().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn run_id_is_uuidv7() {
        let r = VigyRunId::new();
        assert_eq!(r.as_str().len(), 36);
    }
}
