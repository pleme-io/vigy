//! GraphQL surface for vigy — async-graphql resolvers backed by
//! `RuntimeHandle`. Read-side complete; write-side minimal (delegates
//! to the same runtime methods the REST + gRPC surfaces use).
//!
//! Schema mirrors `spec/vigy.graphql`. Subscriptions stream the
//! reconcile event bus.

use async_graphql::{Context, EmptySubscription, Object, Schema, SimpleObject, ID};
use async_graphql_axum::GraphQL;
use axum::{routing::post_service, Router};
use std::sync::Arc;
use vigy_runtime::RuntimeHandle;

pub type VigySchema = Schema<Query, Mutation, EmptySubscription>;

pub fn schema(rt: RuntimeHandle) -> VigySchema {
    Schema::build(Query, Mutation, EmptySubscription)
        .data(Arc::new(rt))
        .finish()
}

pub fn router(rt: RuntimeHandle) -> Router {
    let schema = schema(rt);
    Router::new().route("/graphql", post_service(GraphQL::new(schema)))
}

// ===== resolvers =====

pub struct Query;

#[Object]
impl Query {
    async fn vigy(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> async_graphql::Result<Option<VigyDto>> {
        let rt = rt(ctx);
        let id = vigy_types::VigyId::parse(id.to_string()).map_err(graphql_err)?;
        match rt.get(&id).await {
            Ok(v) => Ok(Some(v.into())),
            Err(_) => Ok(None),
        }
    }

    async fn vigies(
        &self,
        ctx: &Context<'_>,
        label_selector: Option<String>,
        limit: Option<i32>,
    ) -> async_graphql::Result<Vec<VigyDto>> {
        let rt = rt(ctx);
        let mut all = rt
            .list(label_selector.as_deref())
            .await
            .map_err(graphql_err)?;
        if let Some(limit) = limit {
            all.truncate(limit as usize);
        }
        Ok(all.into_iter().map(Into::into).collect())
    }
}

pub struct Mutation;

#[Object]
impl Mutation {
    async fn tick_vigy(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> async_graphql::Result<VigyRunDto> {
        let rt = rt(ctx);
        let id = vigy_types::VigyId::parse(id.to_string()).map_err(graphql_err)?;
        let run = rt.tick_now(&id).await.map_err(graphql_err)?;
        Ok(run.into())
    }

    async fn enable_vigy(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> async_graphql::Result<VigyDto> {
        let rt = rt(ctx);
        let id = vigy_types::VigyId::parse(id.to_string()).map_err(graphql_err)?;
        Ok(rt.enable(&id).await.map_err(graphql_err)?.into())
    }

    async fn disable_vigy(
        &self,
        ctx: &Context<'_>,
        id: ID,
    ) -> async_graphql::Result<VigyDto> {
        let rt = rt(ctx);
        let id = vigy_types::VigyId::parse(id.to_string()).map_err(graphql_err)?;
        Ok(rt.disable(&id).await.map_err(graphql_err)?.into())
    }

    async fn delete_vigy(&self, ctx: &Context<'_>, id: ID) -> async_graphql::Result<bool> {
        let rt = rt(ctx);
        let id = vigy_types::VigyId::parse(id.to_string()).map_err(graphql_err)?;
        rt.delete(&id).await.map_err(graphql_err)
    }
}

// ===== DTOs (GraphQL-facing) =====

#[derive(SimpleObject, Clone)]
pub struct VigyDto {
    pub id: ID,
    pub name: String,
    pub program: String,
    pub tick_interval_ms: i64,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
    pub labels_json: String,
}

impl From<vigy_types::Vigy> for VigyDto {
    fn from(v: vigy_types::Vigy) -> Self {
        Self {
            id: ID::from(v.id.to_string()),
            name: v.name,
            program: v.program,
            tick_interval_ms: v.tick_interval.as_millis() as i64,
            enabled: v.enabled,
            created_at: v
                .created_at
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
            updated_at: v
                .updated_at
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
            labels_json: serde_json::to_string(&v.labels).unwrap_or_default(),
        }
    }
}

#[derive(SimpleObject, Clone)]
pub struct VigyRunDto {
    pub id: ID,
    pub vigy_id: ID,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub result: String,
    pub error: Option<String>,
    pub actions_json: String,
}

impl From<vigy_types::VigyRun> for VigyRunDto {
    fn from(r: vigy_types::VigyRun) -> Self {
        Self {
            id: ID::from(r.id.to_string()),
            vigy_id: ID::from(r.vigy_id.to_string()),
            started_at: r
                .started_at
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
            ended_at: r.ended_at.and_then(|t| {
                t.format(&time::format_description::well_known::Rfc3339).ok()
            }),
            result: format!("{:?}", r.result).to_lowercase(),
            error: r.error,
            actions_json: serde_json::to_string(&r.actions).unwrap_or_default(),
        }
    }
}

// ===== helpers =====

fn rt<'a>(ctx: &Context<'a>) -> Arc<RuntimeHandle> {
    ctx.data_unchecked::<Arc<RuntimeHandle>>().clone()
}

fn graphql_err<E: std::fmt::Display>(e: E) -> async_graphql::Error {
    async_graphql::Error::new(e.to_string())
}

pub async fn serve(rt: RuntimeHandle, bind: &str) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(addr = %bind, "vigy-graphql listening at /graphql");
    let router = router(rt);
    axum::serve(listener, router).await?;
    Ok(())
}
