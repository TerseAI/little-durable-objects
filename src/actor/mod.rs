//! Distributed actor invocation built on the durable-actor ownership boundary.

mod executor_connection;
mod protocol;

pub(crate) use self::protocol::ActorInvocationDeadline;
pub use self::{
    executor_connection::{
        ActorExecutor, ActorMethodCancellation, ActorMethodInvocation, ActorMethodOutcome,
    },
    protocol::{
        ActorExecutionResult, ActorInvocation, ActorInvocationFailure, ActorKey, ActorScope,
    },
};
pub(crate) use executor_connection::{ActorExecutorListener, MAX_ACTOR_EXECUTOR_MESSAGE_BYTES};

#[cfg(test)]
mod tests;
