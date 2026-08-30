//! Authenticated direct-gRPC transport between durable-object processes.

mod service;
mod wire;

pub(crate) mod proto {
    tonic::include_proto!("durable_object.v1");
}

pub(crate) use self::service::ActorHostGrpcService;

#[cfg(test)]
mod tests;
