use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MAX_ACTOR_STATE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateSnapshot {
    pub state_version: u64,
    pub owner_epoch: u64,
    pub request_id: String,
    pub state: Value,
    pub result: Value,
}

impl StateSnapshot {
    pub fn new(
        state_version: u64,
        owner_epoch: u64,
        request_id: String,
        state: Value,
        result: Value,
    ) -> Result<Self> {
        let snapshot = Self {
            state_version,
            owner_epoch,
            request_id,
            state,
            result,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let snapshot: Self =
            serde_json::from_slice(bytes).context("decode actor state snapshot")?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
    }

    pub fn replay(&self, request_id: &str) -> Option<&Value> {
        (self.request_id == request_id).then_some(&self.result)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.state_version > 0,
            "actor state version must be positive"
        );
        ensure!(self.owner_epoch > 0, "owner epoch must be positive");
        ensure!(
            !self.request_id.is_empty() && self.request_id.len() <= 255,
            "actor state request ID is invalid"
        );
        ensure!(self.state.is_object(), "actor state must be a JSON object");
        ensure!(
            serde_json::to_vec(&self.state)?.len() <= MAX_ACTOR_STATE_BYTES,
            "actor state exceeds the {MAX_ACTOR_STATE_BYTES}-byte limit"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rejects_oversized_state() {
        let error = StateSnapshot::new(
            1,
            1,
            "request-1".into(),
            json!({"value": "x".repeat(MAX_ACTOR_STATE_BYTES)}),
            Value::Null,
        )
        .expect_err("oversized state");
        assert!(error.to_string().contains("actor state exceeds"));
    }
}
