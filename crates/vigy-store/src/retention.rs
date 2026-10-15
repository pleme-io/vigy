use std::num::NonZeroU64;

pub const DEFAULT_KEEP_RUNS_PER_VIGY: u64 = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunRetention {
    KeepAll,
    KeepLast(NonZeroU64),
}

impl Default for RunRetention {
    fn default() -> Self {
        Self::KeepLast(NonZeroU64::new(DEFAULT_KEEP_RUNS_PER_VIGY).unwrap_or(NonZeroU64::MIN))
    }
}

impl RunRetention {
    #[must_use]
    pub fn limit(self) -> Option<u64> {
        match self {
            Self::KeepAll => None,
            Self::KeepLast(n) => Some(n.get()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InMemoryStore, SeaormStore, VigyStore};
    use vigy_types::{TickInterval, Vigy, VigyRun};

    async fn exercise(store: &dyn VigyStore, keep: Option<u64>) {
        let a = Vigy::new("a", "(vigy-noop)", TickInterval::default()).unwrap();
        let b = Vigy::new("b", "(vigy-noop)", TickInterval::default()).unwrap();
        store.upsert_vigy(&a).await.unwrap();
        store.upsert_vigy(&b).await.unwrap();
        let mut ids = Vec::new();
        for _ in 0..5 {
            let run = VigyRun::started(a.id.clone()).complete_ok(Vec::new());
            ids.push(run.id.to_string());
            store.insert_run(&run).await.unwrap();
        }
        store
            .insert_run(&VigyRun::started(b.id.clone()).complete_ok(Vec::new()))
            .await
            .unwrap();
        ids.sort_unstable();
        let mut kept: Vec<String> = store
            .recent_runs(&a.id, 100)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.id.to_string())
            .collect();
        kept.sort_unstable();
        let want = keep.map_or(5, |k| usize::try_from(k).unwrap());
        assert_eq!(kept, ids[ids.len() - want..].to_vec());
        assert_eq!(store.recent_runs(&b.id, 100).await.unwrap().len(), 1);
    }

    fn last(n: u64) -> RunRetention {
        RunRetention::KeepLast(NonZeroU64::new(n).unwrap())
    }

    #[tokio::test]
    async fn in_memory_keeps_only_the_newest_runs_per_vigy() {
        exercise(&InMemoryStore::new().with_run_retention(last(3)), Some(3)).await;
    }

    #[tokio::test]
    async fn sqlite_keeps_only_the_newest_runs_per_vigy() {
        let s = SeaormStore::open_in_memory().await.unwrap().with_run_retention(last(3));
        exercise(&s, Some(3)).await;
    }

    #[tokio::test]
    async fn keep_all_is_the_configured_off_switch() {
        exercise(&InMemoryStore::new().with_run_retention(RunRetention::KeepAll), None).await;
        let s = SeaormStore::open_in_memory()
            .await
            .unwrap()
            .with_run_retention(RunRetention::KeepAll);
        exercise(&s, None).await;
    }

    #[test]
    fn the_default_is_bounded() {
        assert_eq!(RunRetention::default().limit(), Some(DEFAULT_KEEP_RUNS_PER_VIGY));
    }
}
