//! `Schedule` trait + `ScheduleSpec` round-trippable enum.
//!
//! Replaces the bare [`crate::TickInterval`] for vigies whose cadence
//! is more interesting than "every N ms":
//!
//! - **Fixed** — the current behavior. Equivalent to a bare TickInterval.
//! - **Cron** — schedule expression (e.g. `0 */15 * * * *`). Real
//!   parser lands in a follow-up; the variant is declared now so
//!   serialized plans + state survive the upgrade.
//! - **Adaptive** — operator declares min/max bounds + a hint key; the
//!   reconciler reads / writes the hint key in its kv to widen or
//!   tighten the interval based on observed convergence.
//!
//! The `Schedule` trait lives in the same crate as `ScheduleSpec` so
//! consumers don't need a separate dep — but only the `Spec` variants
//! round-trip via serde. `Box<dyn Schedule>` is the runtime shape that
//! [`ScheduleSpec::build`] produces.

use crate::error::{Error, Result};
use crate::vigy::TickInterval;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use utoipa::ToSchema;

/// Runtime trait. Impls compute the next time a vigy should tick from
/// the current time + the previous tick's start. Implementations should
/// be cheap to call (no I/O, no locks) — the runtime queries this on
/// every loop iteration.
pub trait Schedule: Send + Sync {
    /// Given the current time + the previous tick's start (None for the
    /// first tick), return when the next tick should fire.
    fn next_tick(&self, now: SystemTime, previous: Option<SystemTime>) -> SystemTime;

    /// Cheap human-readable description for logs + diagnostics.
    fn describe(&self) -> String;
}

/// Serializable spec — round-trips via serde. The runtime calls
/// [`Self::build`] once per vigy to get a `Box<dyn Schedule>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum ScheduleSpec {
    /// Fire every `interval_ms` milliseconds (≥ 100). The default
    /// + only fully-implemented variant in v0.1.
    Fixed { interval_ms: u64 },
    /// Cron-like expression. Parser lands in a follow-up — calling
    /// `build()` on this variant today returns
    /// `Error::ScheduleNotYetImplemented`.
    Cron { expr: String },
    /// Adaptive interval bounded by min/max. The reconciler reads
    /// `hint_key` from its kv and uses it to choose where in
    /// [min_ms, max_ms] to land. Follow-up.
    Adaptive {
        min_ms: u64,
        max_ms: u64,
        hint_key: String,
    },
}

impl ScheduleSpec {
    /// Materialise the spec into a runtime trait object.
    pub fn build(&self) -> Result<Box<dyn Schedule>> {
        match self {
            ScheduleSpec::Fixed { interval_ms } => {
                Ok(Box::new(FixedSchedule::new(*interval_ms)?))
            }
            ScheduleSpec::Cron { expr } => Err(Error::ScheduleNotYetImplemented {
                variant: "cron",
                detail: format!("expr {expr:?}"),
            }),
            ScheduleSpec::Adaptive { .. } => Err(Error::ScheduleNotYetImplemented {
                variant: "adaptive",
                detail: "min/max/hint_key parsing pending".into(),
            }),
        }
    }
}

impl Default for ScheduleSpec {
    fn default() -> Self {
        ScheduleSpec::Fixed { interval_ms: 1000 }
    }
}

impl From<TickInterval> for ScheduleSpec {
    fn from(t: TickInterval) -> Self {
        ScheduleSpec::Fixed {
            interval_ms: t.as_millis(),
        }
    }
}

// ── Fixed impl ──────────────────────────────────────────────────

/// The default schedule — fires every `interval_ms` milliseconds.
/// Equivalent to the v0.1 `TickInterval` behavior; carrying the same
/// minimum-100ms guard for footgun protection.
pub struct FixedSchedule {
    interval: Duration,
}

impl FixedSchedule {
    pub fn new(interval_ms: u64) -> Result<Self> {
        if interval_ms < crate::vigy::MIN_TICK_INTERVAL_MS {
            return Err(Error::InvalidTickInterval { ms: interval_ms });
        }
        Ok(Self {
            interval: Duration::from_millis(interval_ms),
        })
    }
}

impl Schedule for FixedSchedule {
    fn next_tick(&self, now: SystemTime, previous: Option<SystemTime>) -> SystemTime {
        match previous {
            // Fire immediately on the first tick.
            None => now,
            // Otherwise schedule `interval` from the previous tick's
            // start so we don't drift on a slow tick body.
            Some(prev) => prev + self.interval,
        }
    }

    fn describe(&self) -> String {
        format!("fixed({}ms)", self.interval.as_millis())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_round_trips_via_serde() {
        let s = ScheduleSpec::Fixed { interval_ms: 500 };
        let json = serde_json::to_string(&s).unwrap();
        let back: ScheduleSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }

    #[test]
    fn cron_and_adaptive_build_returns_unimplemented() {
        assert!(matches!(
            ScheduleSpec::Cron {
                expr: "* * * * *".into()
            }
            .build(),
            Err(Error::ScheduleNotYetImplemented { .. })
        ));
        assert!(matches!(
            ScheduleSpec::Adaptive {
                min_ms: 100,
                max_ms: 10_000,
                hint_key: "x".into()
            }
            .build(),
            Err(Error::ScheduleNotYetImplemented { .. })
        ));
    }

    #[test]
    fn fixed_schedule_fires_immediately_on_first_tick() {
        let s = FixedSchedule::new(500).unwrap();
        let now = SystemTime::now();
        assert_eq!(s.next_tick(now, None), now);
    }

    #[test]
    fn fixed_schedule_drifts_from_previous_tick() {
        let s = FixedSchedule::new(500).unwrap();
        let prev = SystemTime::now();
        let now = prev + Duration::from_millis(100);
        let expected = prev + Duration::from_millis(500);
        assert_eq!(s.next_tick(now, Some(prev)), expected);
    }

    #[test]
    fn fixed_schedule_enforces_minimum_interval() {
        assert!(FixedSchedule::new(99).is_err());
    }
}
