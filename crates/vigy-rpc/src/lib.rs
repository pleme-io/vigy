//! gRPC surface for vigy.
//!
//! - `pleme.vigy.v1.*` types are generated from `spec/vigy.proto` by
//!   `build.rs` (tonic-build).
//! - `VigyServer` is the public re-export of the generated server type.
//! - `Svc` is the Service impl backed by a [`vigy_runtime::RuntimeHandle`].
//!
//! Handlers are minimal — they marshal to/from `vigy-types` and call
//! the runtime. The full registration / lifecycle / streaming flow
//! ports from the REST handler in vigy-rest.

#[allow(clippy::all)]
pub mod proto {
    tonic::include_proto!("pleme.vigy.v1");
}

use proto::vigy_service_server::{VigyService, VigyServiceServer};
use proto::*;
use std::pin::Pin;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};
use vigy_runtime::RuntimeHandle;

pub use proto::vigy_service_server::VigyServiceServer as VigyServer;

#[derive(Clone)]
pub struct Svc {
    rt: RuntimeHandle,
}

impl Svc {
    pub fn new(rt: RuntimeHandle) -> Self {
        Self { rt }
    }
}

#[tonic::async_trait]
impl VigyService for Svc {
    type StreamEventsStream =
        Pin<Box<dyn Stream<Item = std::result::Result<VigyRun, Status>> + Send + 'static>>;

    async fn register(
        &self,
        _req: Request<RegisterRequest>,
    ) -> std::result::Result<Response<Vigy>, Status> {
        Err(Status::unimplemented("TODO: implement Register via vigy-runtime"))
    }
    async fn update(
        &self,
        _req: Request<UpdateRequest>,
    ) -> std::result::Result<Response<Vigy>, Status> {
        Err(Status::unimplemented("TODO"))
    }
    async fn disable(
        &self,
        _req: Request<DisableRequest>,
    ) -> std::result::Result<Response<Vigy>, Status> {
        Err(Status::unimplemented("TODO"))
    }
    async fn enable(
        &self,
        _req: Request<EnableRequest>,
    ) -> std::result::Result<Response<Vigy>, Status> {
        Err(Status::unimplemented("TODO"))
    }
    async fn delete(
        &self,
        _req: Request<DeleteRequest>,
    ) -> std::result::Result<Response<DeleteResponse>, Status> {
        Err(Status::unimplemented("TODO"))
    }
    async fn get(
        &self,
        _req: Request<GetRequest>,
    ) -> std::result::Result<Response<Vigy>, Status> {
        Err(Status::unimplemented("TODO"))
    }
    async fn list(
        &self,
        _req: Request<ListRequest>,
    ) -> std::result::Result<Response<ListResponse>, Status> {
        Err(Status::unimplemented("TODO"))
    }
    async fn get_state(
        &self,
        _req: Request<GetStateRequest>,
    ) -> std::result::Result<Response<VigyState>, Status> {
        Err(Status::unimplemented("TODO"))
    }
    async fn tick(
        &self,
        _req: Request<TickRequest>,
    ) -> std::result::Result<Response<VigyRun>, Status> {
        Err(Status::unimplemented("TODO"))
    }
    async fn stream_events(
        &self,
        _req: Request<StreamEventsRequest>,
    ) -> std::result::Result<Response<Self::StreamEventsStream>, Status> {
        Err(Status::unimplemented("TODO"))
    }
}

/// Bind a gRPC server to `addr`. Service handlers are stubs returning
/// `Unimplemented` — the proto + tonic-build pipeline is wired so the
/// concrete handler impls land in a follow-up without further plumbing.
pub async fn serve(rt: RuntimeHandle, addr: &str) -> anyhow::Result<()> {
    let svc = Svc::new(rt);
    let addr = addr.parse()?;
    tracing::info!(?addr, "vigy-rpc gRPC listening");
    tonic::transport::Server::builder()
        .add_service(VigyServer::new(svc))
        .serve(addr)
        .await?;
    Ok(())
}
