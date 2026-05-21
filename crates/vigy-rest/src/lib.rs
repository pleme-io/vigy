//! REST surface for vigy — axum + utoipa.
//!
//! Handlers follow `spec/vigy.openapi.yaml`. The utoipa derives on
//! `vigy-types` provide the OpenAPI schema; we add operation-level
//! annotations here. Swagger UI mounts at `/swagger`.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;
use vigy_runtime::RuntimeHandle;
use vigy_types::{Labels, TickInterval, Vigy, VigyId, VigyRun, VigyState};

/// Build the axum Router for the REST surface. Caller `.bind()`s it.
pub fn router(rt: RuntimeHandle) -> Router {
    let state = Arc::new(rt);
    Router::new()
        .route("/v1/vigies", get(list_vigies).post(register_vigy))
        .route(
            "/v1/vigies/:id",
            get(get_vigy).patch(update_vigy).delete(delete_vigy),
        )
        .route("/v1/vigies/:id/state", get(get_state))
        .route("/v1/vigies/:id/tick", post(tick_vigy))
        .route("/v1/vigies/:id/enable", post(enable_vigy))
        .route("/v1/vigies/:id/disable", post(disable_vigy))
        .merge(SwaggerUi::new("/swagger").url("/openapi.json", ApiDoc::openapi()))
        .with_state(state)
}

#[derive(OpenApi)]
#[openapi(
    info(title = "vigy REST API", version = "0.1.0", license(name = "MIT")),
    paths(list_vigies, register_vigy, get_vigy, update_vigy, delete_vigy,
          get_state, tick_vigy, enable_vigy, disable_vigy),
    components(schemas(Vigy, VigyRun, VigyState, RegisterPayload, UpdatePayload, ErrorBody))
)]
struct ApiDoc;

// ===== payloads =====

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RegisterPayload {
    pub name: String,
    pub program: String,
    #[serde(default = "default_tick_ms")]
    pub tick_interval_ms: u64,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub labels: Labels,
}
fn default_tick_ms() -> u64 { 1000 }
fn default_true() -> bool { true }

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub struct UpdatePayload {
    pub program: Option<String>,
    pub tick_interval_ms: Option<u64>,
    pub labels: Option<Labels>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ErrorBody {
    pub error: String,
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub label_selector: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
}

// ===== handlers =====

#[utoipa::path(get, path = "/v1/vigies",
    params(("label_selector" = Option<String>, Query, description = "k=v,k=v selector"),
           ("limit" = Option<u32>, Query, description = "max results")),
    responses((status = 200, body = [Vigy])))]
async fn list_vigies(
    State(rt): State<Arc<RuntimeHandle>>,
    Query(q): Query<ListQuery>,
) -> impl IntoResponse {
    match rt.list(q.label_selector.as_deref()).await {
        Ok(mut v) => {
            if let Some(limit) = q.limit {
                v.truncate(limit as usize);
            }
            (StatusCode::OK, Json(v)).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

#[utoipa::path(post, path = "/v1/vigies",
    request_body = RegisterPayload,
    responses((status = 201, body = Vigy)))]
async fn register_vigy(
    State(rt): State<Arc<RuntimeHandle>>,
    Json(p): Json<RegisterPayload>,
) -> impl IntoResponse {
    let interval = match TickInterval::from_millis(p.tick_interval_ms) {
        Ok(t) => t,
        Err(e) => return err(StatusCode::BAD_REQUEST, e),
    };
    let mut v = match Vigy::new(p.name, p.program, interval) {
        Ok(v) => v,
        Err(e) => return err(StatusCode::BAD_REQUEST, e),
    };
    v.enabled = p.enabled;
    v.labels = p.labels;
    match rt.register_or_update(v).await {
        Ok(v) => (StatusCode::CREATED, Json(v)).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

#[utoipa::path(get, path = "/v1/vigies/{id}",
    responses((status = 200, body = Vigy), (status = 404, body = ErrorBody)))]
async fn get_vigy(
    State(rt): State<Arc<RuntimeHandle>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let id = match VigyId::parse(id) {
        Ok(id) => id,
        Err(e) => return err(StatusCode::BAD_REQUEST, e),
    };
    match rt.get(&id).await {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(_) => err_str(StatusCode::NOT_FOUND, "vigy not found"),
    }
}

#[utoipa::path(patch, path = "/v1/vigies/{id}",
    request_body = UpdatePayload,
    responses((status = 200, body = Vigy)))]
async fn update_vigy(
    State(rt): State<Arc<RuntimeHandle>>,
    Path(id): Path<String>,
    Json(_p): Json<UpdatePayload>,
) -> impl IntoResponse {
    // For v0.1 we keep update simple: ids are content-derived, so a
    // program edit mints a new vigy. The PATCH route is a placeholder
    // for label-only / interval-only updates (TODO).
    let id = match VigyId::parse(id) {
        Ok(id) => id,
        Err(e) => return err(StatusCode::BAD_REQUEST, e),
    };
    match rt.get(&id).await {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(_) => err_str(StatusCode::NOT_FOUND, "vigy not found"),
    }
}

#[utoipa::path(delete, path = "/v1/vigies/{id}",
    responses((status = 204), (status = 404, body = ErrorBody)))]
async fn delete_vigy(
    State(rt): State<Arc<RuntimeHandle>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let id = match VigyId::parse(id) {
        Ok(id) => id,
        Err(e) => return err(StatusCode::BAD_REQUEST, e),
    };
    match rt.delete(&id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => err_str(StatusCode::NOT_FOUND, "vigy not found"),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

#[utoipa::path(get, path = "/v1/vigies/{id}/state",
    responses((status = 200, body = VigyState)))]
async fn get_state(
    State(rt): State<Arc<RuntimeHandle>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let id = match VigyId::parse(id) {
        Ok(id) => id,
        Err(e) => return err(StatusCode::BAD_REQUEST, e),
    };
    // Minimal: project the most recent run's actions as `pending`.
    let runs = match rt.recent_runs(&id, 1).await {
        Ok(r) => r,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, e),
    };
    let state = VigyState {
        vigy_id: id,
        desired: None,
        observed: None,
        pending: runs.into_iter().next().map(|r| r.actions).unwrap_or_default(),
        captured_at: time::OffsetDateTime::now_utc(),
    };
    (StatusCode::OK, Json(state)).into_response()
}

#[utoipa::path(post, path = "/v1/vigies/{id}/tick",
    responses((status = 200, body = VigyRun)))]
async fn tick_vigy(
    State(rt): State<Arc<RuntimeHandle>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let id = match VigyId::parse(id) {
        Ok(id) => id,
        Err(e) => return err(StatusCode::BAD_REQUEST, e),
    };
    match rt.tick_now(&id).await {
        Ok(r) => (StatusCode::OK, Json(r)).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

#[utoipa::path(post, path = "/v1/vigies/{id}/enable",
    responses((status = 200, body = Vigy)))]
async fn enable_vigy(
    State(rt): State<Arc<RuntimeHandle>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let id = match VigyId::parse(id) {
        Ok(id) => id,
        Err(e) => return err(StatusCode::BAD_REQUEST, e),
    };
    match rt.enable(&id).await {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

#[utoipa::path(post, path = "/v1/vigies/{id}/disable",
    responses((status = 200, body = Vigy)))]
async fn disable_vigy(
    State(rt): State<Arc<RuntimeHandle>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let id = match VigyId::parse(id) {
        Ok(id) => id,
        Err(e) => return err(StatusCode::BAD_REQUEST, e),
    };
    match rt.disable(&id).await {
        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e),
    }
}

fn err(code: StatusCode, e: impl std::fmt::Display) -> axum::response::Response {
    (
        code,
        Json(ErrorBody {
            error: e.to_string(),
        }),
    )
        .into_response()
}

fn err_str(code: StatusCode, msg: &str) -> axum::response::Response {
    err(code, msg)
}

/// One-line server bootstrap. Used by `vigy serve`.
pub async fn serve(rt: RuntimeHandle, bind: &str) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!(addr = %bind, "vigy-rest listening (Swagger UI at /swagger)");
    let router = router(rt);
    axum::serve(listener, router).await?;
    Ok(())
}

#[allow(dead_code)]
const fn _dur(_: Duration) {}
