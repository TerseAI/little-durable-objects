mod actor_host;
mod confirmed_lease;
mod lease_maintenance;
mod leased_host;
mod process;

use serde::{Deserialize, Serialize};
use std::fmt;

pub use self::process::{ActorHostConfig, serve_actor_host};
pub(crate) use self::{
    actor_host::{ActorHost, ActorHostDependencies},
    lease_maintenance::{HostLeaseMaintainer, LeaseRenewalTask},
    leased_host::LeasedActorHost,
};
pub(crate) use actor_host::ActorDrainReason;
pub(crate) use confirmed_lease::ConfirmedLeaseState;
pub(crate) use leased_host::HOST_ACTOR_DRAIN_TIMEOUT;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ActorProcessRole {
    Workflow,
    Host,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HostId(String);

impl HostId {
    pub fn new<S>(id: S) -> Self
    where
        S: Into<String>,
    {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for HostId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HostEndpoint {
    pub id: HostId,
    pub route: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_roles_use_current_names() -> anyhow::Result<()> {
        assert_eq!(
            serde_json::to_value(ActorProcessRole::Workflow)?,
            "workflow"
        );
        assert_eq!(serde_json::to_value(ActorProcessRole::Host)?, "host");
        assert!(serde_json::from_value::<ActorProcessRole>(serde_json::json!("caller")).is_err());
        Ok(())
    }
}
