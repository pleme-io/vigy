//! Async facade over the SeaORM entities. Callers in vigy-runtime,
//! vigy-rpc, etc. only touch `Store` — the SeaORM dependency stays
//! contained.

use crate::entities::*;
use crate::Migrator;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectOptions, Database, DatabaseConnection, EntityTrait,
    QueryFilter, QueryOrder, QuerySelect, Set,
};
use sea_orm_migration::MigratorTrait;
use std::path::Path;
use thiserror::Error;
use vigy_types::{Labels, Vigy, VigyId, VigyRun, VigyRunId};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("db: {0}")]
    Db(#[from] sea_orm::DbErr),
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("vigy types: {0}")]
    Types(#[from] vigy_types::Error),
    #[error("not found: {kind} {id}")]
    NotFound { kind: &'static str, id: String },
}

pub type Result<T> = std::result::Result<T, StoreError>;

pub struct Store {
    db: DatabaseConnection,
}

impl Store {
    /// Open / create the SQLite DB at the given path. Runs pending
    /// migrations automatically.
    pub async fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let mut opts = ConnectOptions::new(url);
        opts.sqlx_logging(false);
        let db = Database::connect(opts).await?;
        Migrator::up(&db, None).await?;
        Ok(Self { db })
    }

    /// In-memory DB for tests.
    pub async fn open_in_memory() -> Result<Self> {
        let db = Database::connect("sqlite::memory:").await?;
        Migrator::up(&db, None).await?;
        Ok(Self { db })
    }

    // ---------- vigy CRUD ----------

    pub async fn upsert_vigy(&self, v: &Vigy) -> Result<()> {
        let labels_json = serde_json::to_string(&v.labels)?;
        let am = vigy::ActiveModel {
            id: Set(v.id.to_string()),
            name: Set(v.name.clone()),
            program: Set(v.program.clone()),
            tick_interval_ms: Set(v.tick_interval.as_millis() as i64),
            enabled: Set(v.enabled),
            created_at: Set(v.created_at.format(&time::format_description::well_known::Rfc3339).unwrap()),
            updated_at: Set(v.updated_at.format(&time::format_description::well_known::Rfc3339).unwrap()),
            labels_json: Set(labels_json),
        };
        let existing = VigyEntity::find_by_id(v.id.to_string())
            .one(&self.db)
            .await?;
        match existing {
            Some(_) => {
                am.update(&self.db).await?;
            }
            None => {
                am.insert(&self.db).await?;
            }
        }
        Ok(())
    }

    pub async fn get_vigy(&self, id: &VigyId) -> Result<Vigy> {
        let m = VigyEntity::find_by_id(id.to_string())
            .one(&self.db)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                kind: "vigy",
                id: id.to_string(),
            })?;
        model_to_vigy(m)
    }

    pub async fn list_vigies(&self, label_selector: Option<&str>) -> Result<Vec<Vigy>> {
        let rows = VigyEntity::find().all(&self.db).await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let v = model_to_vigy(r)?;
            match label_selector {
                Some(sel) => {
                    if v.labels.matches_selector(sel)? {
                        out.push(v);
                    }
                }
                None => out.push(v),
            }
        }
        Ok(out)
    }

    pub async fn set_enabled(&self, id: &VigyId, enabled: bool) -> Result<()> {
        let m = VigyEntity::find_by_id(id.to_string())
            .one(&self.db)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                kind: "vigy",
                id: id.to_string(),
            })?;
        let mut am: vigy::ActiveModel = m.into();
        am.enabled = Set(enabled);
        am.updated_at = Set(time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap());
        am.update(&self.db).await?;
        Ok(())
    }

    pub async fn delete_vigy(&self, id: &VigyId) -> Result<bool> {
        let res = VigyEntity::delete_by_id(id.to_string())
            .exec(&self.db)
            .await?;
        Ok(res.rows_affected > 0)
    }

    // ---------- runs ----------

    pub async fn insert_run(&self, run: &VigyRun) -> Result<()> {
        let am = vigy_run::ActiveModel {
            id: Set(run.id.to_string()),
            vigy_id: Set(run.vigy_id.to_string()),
            started_at: Set(run
                .started_at
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap()),
            ended_at: Set(run.ended_at.map(|t| {
                t.format(&time::format_description::well_known::Rfc3339)
                    .unwrap()
            })),
            result: Set(format!("{:?}", run.result).to_lowercase()),
            error: Set(run.error.clone()),
            actions_json: Set(serde_json::to_string(&run.actions)?),
        };
        am.insert(&self.db).await?;
        Ok(())
    }

    pub async fn recent_runs(&self, vigy_id: &VigyId, limit: u64) -> Result<Vec<VigyRun>> {
        let rows = VigyRunEntity::find()
            .filter(vigy_run::Column::VigyId.eq(vigy_id.to_string()))
            .order_by_desc(vigy_run::Column::StartedAt)
            .limit(limit)
            .all(&self.db)
            .await?;
        rows.into_iter().map(model_to_run).collect()
    }

    // ---------- persistent KV (per-vigy scoped) ----------

    /// Load every kv pair for a vigy. Called by the runtime at the
    /// start of each tick.
    pub async fn load_kv(
        &self,
        vigy_id: &VigyId,
    ) -> Result<std::collections::BTreeMap<String, serde_json::Value>> {
        let rows = VigyKvEntity::find()
            .filter(vigy_kv::Column::VigyId.eq(vigy_id.to_string()))
            .all(&self.db)
            .await?;
        let mut out = std::collections::BTreeMap::new();
        for row in rows {
            let v: serde_json::Value = serde_json::from_str(&row.value_json)?;
            out.insert(row.key, v);
        }
        Ok(out)
    }

    /// Save dirty + delete-marked kv changes. Called by the runtime at
    /// tick end with only the keys the program actually touched.
    pub async fn save_kv(
        &self,
        vigy_id: &VigyId,
        dirty: &std::collections::BTreeMap<String, serde_json::Value>,
        deleted: &std::collections::BTreeSet<String>,
    ) -> Result<()> {
        let now = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_default();
        for (k, v) in dirty {
            let value_json = serde_json::to_string(v)?;
            let am = vigy_kv::ActiveModel {
                vigy_id: Set(vigy_id.to_string()),
                key: Set(k.clone()),
                value_json: Set(value_json),
                updated_at: Set(now.clone()),
            };
            // SQLite UPSERT via sea-orm — first try update, fall back to insert.
            let existing = VigyKvEntity::find_by_id((vigy_id.to_string(), k.clone()))
                .one(&self.db)
                .await?;
            match existing {
                Some(_) => {
                    am.update(&self.db).await?;
                }
                None => {
                    am.insert(&self.db).await?;
                }
            }
        }
        for k in deleted {
            let _ = VigyKvEntity::delete_by_id((vigy_id.to_string(), k.clone()))
                .exec(&self.db)
                .await;
        }
        Ok(())
    }
}

fn model_to_vigy(m: vigy::Model) -> Result<Vigy> {
    use vigy_types::TickInterval;
    let id = VigyId::parse(&m.id)?;
    let tick_interval = TickInterval::from_millis(m.tick_interval_ms as u64)?;
    let labels: Labels = serde_json::from_str(&m.labels_json)?;
    let created_at = time::OffsetDateTime::parse(
        &m.created_at,
        &time::format_description::well_known::Rfc3339,
    )
    .map_err(|e| StoreError::Serde(serde_json::Error::io(std::io::Error::other(e.to_string()))))?;
    let updated_at = time::OffsetDateTime::parse(
        &m.updated_at,
        &time::format_description::well_known::Rfc3339,
    )
    .map_err(|e| StoreError::Serde(serde_json::Error::io(std::io::Error::other(e.to_string()))))?;
    Ok(Vigy {
        id,
        name: m.name,
        program: m.program,
        tick_interval,
        enabled: m.enabled,
        created_at,
        updated_at,
        labels,
    })
}

fn model_to_run(m: vigy_run::Model) -> Result<VigyRun> {
    let id = VigyRunId::parse(&m.id)?;
    let vigy_id = VigyId::parse(&m.vigy_id)?;
    let started_at = time::OffsetDateTime::parse(
        &m.started_at,
        &time::format_description::well_known::Rfc3339,
    )
    .map_err(|e| StoreError::Serde(serde_json::Error::io(std::io::Error::other(e.to_string()))))?;
    let ended_at = match m.ended_at {
        Some(s) => Some(
            time::OffsetDateTime::parse(&s, &time::format_description::well_known::Rfc3339)
                .map_err(|e| {
                    StoreError::Serde(serde_json::Error::io(std::io::Error::other(e.to_string())))
                })?,
        ),
        None => None,
    };
    let result = match m.result.as_str() {
        "ok" => vigy_types::ResultStatus::Ok,
        "failed" => vigy_types::ResultStatus::Failed,
        _ => vigy_types::ResultStatus::Skipped,
    };
    let actions = serde_json::from_str(&m.actions_json)?;
    Ok(VigyRun {
        id,
        vigy_id,
        started_at,
        ended_at,
        result,
        error: m.error,
        actions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use vigy_types::TickInterval;

    #[tokio::test]
    async fn open_in_memory_runs_migrations() {
        let s = Store::open_in_memory().await.expect("open");
        let v = Vigy::new("test", "(defvigy test)", TickInterval::default()).unwrap();
        s.upsert_vigy(&v).await.expect("upsert");
        let got = s.get_vigy(&v.id).await.expect("get");
        assert_eq!(got.id, v.id);
        assert_eq!(got.name, "test");
        let all = s.list_vigies(None).await.unwrap();
        assert_eq!(all.len(), 1);
    }

    #[tokio::test]
    async fn upsert_is_idempotent() {
        let s = Store::open_in_memory().await.unwrap();
        let v = Vigy::new("a", "(defvigy a)", TickInterval::default()).unwrap();
        s.upsert_vigy(&v).await.unwrap();
        s.upsert_vigy(&v).await.unwrap();
        let all = s.list_vigies(None).await.unwrap();
        assert_eq!(all.len(), 1);
    }

    #[tokio::test]
    async fn label_selector_filters() {
        let s = Store::open_in_memory().await.unwrap();
        let mut a = Vigy::new("a", "(defvigy a)", TickInterval::default()).unwrap();
        a.labels.insert("host", "mado").unwrap();
        let mut b = Vigy::new("b", "(defvigy b)", TickInterval::default()).unwrap();
        b.labels.insert("host", "tear").unwrap();
        s.upsert_vigy(&a).await.unwrap();
        s.upsert_vigy(&b).await.unwrap();
        let mado_only = s.list_vigies(Some("host=mado")).await.unwrap();
        assert_eq!(mado_only.len(), 1);
        assert_eq!(mado_only[0].name, "a");
    }
}
