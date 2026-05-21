//! The Vigy itself — a reconciler authored in tatara-lisp.

use crate::error::{Error, Result};
use crate::id::VigyId;
use crate::label::Labels;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use utoipa::ToSchema;

/// Minimum allowable tick interval (operator footgun guard).
/// 100 ms is fast enough for UI-driven reconciliation, slow enough that
/// a runaway vigy doesn't peg a core.
pub const MIN_TICK_INTERVAL_MS: u64 = 100;

/// Tick interval — newtype around Duration to enforce the minimum at
/// construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct TickInterval {
    #[schema(value_type = u64, format = "milliseconds", minimum = 100)]
    ms: u64,
}

impl TickInterval {
    pub fn from_millis(ms: u64) -> Result<Self> {
        if ms < MIN_TICK_INTERVAL_MS {
            return Err(Error::InvalidTickInterval { ms });
        }
        Ok(Self { ms })
    }

    pub fn as_millis(&self) -> u64 {
        self.ms
    }

    pub fn as_duration(&self) -> Duration {
        Duration::from_millis(self.ms)
    }
}

impl Default for TickInterval {
    fn default() -> Self {
        Self { ms: 1000 }
    }
}

/// One reconciler.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Vigy {
    pub id: VigyId,
    pub name: String,
    /// Raw tatara-lisp source. Evaluated on every tick.
    pub program: String,
    pub tick_interval: TickInterval,
    pub enabled: bool,
    pub created_at: time::OffsetDateTime,
    pub updated_at: time::OffsetDateTime,
    pub labels: Labels,
}

impl Vigy {
    /// Construct a fresh vigy. Id is content-derived from name+program.
    pub fn new(
        name: impl Into<String>,
        program: impl Into<String>,
        tick_interval: TickInterval,
    ) -> Result<Self> {
        let name = name.into();
        let program = program.into();
        if program.trim().is_empty() {
            return Err(Error::EmptyProgram);
        }
        let id = VigyId::from_name_and_program(&name, &program);
        let now = time::OffsetDateTime::now_utc();
        Ok(Self {
            id,
            name,
            program,
            tick_interval,
            enabled: true,
            created_at: now,
            updated_at: now,
            labels: Labels::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_interval_enforces_minimum() {
        assert!(TickInterval::from_millis(99).is_err());
        assert!(TickInterval::from_millis(100).is_ok());
        assert!(TickInterval::from_millis(5000).is_ok());
    }

    #[test]
    fn vigy_id_stable_across_constructions() {
        let v1 = Vigy::new("sync-tear", "(defvigy ...)", TickInterval::default()).unwrap();
        let v2 = Vigy::new("sync-tear", "(defvigy ...)", TickInterval::default()).unwrap();
        assert_eq!(v1.id, v2.id);
    }

    #[test]
    fn empty_program_rejected() {
        let r = Vigy::new("x", "", TickInterval::default());
        assert!(matches!(r, Err(Error::EmptyProgram)));
    }
}
