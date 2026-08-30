use serde::{Deserialize, Serialize};

use crate::host::HostId;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorOwner {
    #[serde(rename = "node")]
    pub host: HostId,
    pub epoch: u64,
}
