use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MAX_ACTOR_STATE_BYTES: usize = 1024 * 1024;
pub const MAX_STATE_LOG_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_STATE_LOG_RECORDS: usize = 64;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateRecord {
    pub state_version: u64,
    pub owner_epoch: u64,
    /// `None` is used only by an ownership claim before an actor has committed
    /// its first state snapshot.
    pub state: Option<Value>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StateLog {
    records: Vec<StateRecord>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StateAppend {
    Changed,
    Unchanged,
}

impl StateLog {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() <= MAX_STATE_LOG_BYTES + MAX_ACTOR_STATE_BYTES + 1024,
            "actor state log is too large"
        );
        let mut records: Vec<StateRecord> = Vec::new();
        for (index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
            if line.is_empty() {
                continue;
            }
            let record: StateRecord = serde_json::from_slice(line)
                .with_context(|| format!("decode actor state record {}", index + 1))?;
            validate_record(&record)?;
            if let Some(previous) = records.last() {
                ensure!(
                    record.state_version > previous.state_version,
                    "actor state versions must increase"
                );
            }
            records.push(record);
        }
        ensure!(
            records.len() <= MAX_STATE_LOG_RECORDS,
            "actor state log contains too many records"
        );
        Ok(Self { records })
    }

    pub fn latest(&self) -> Option<&StateRecord> {
        self.records.last()
    }

    pub fn latest_state(&self) -> Option<&Value> {
        self.latest().and_then(|record| record.state.as_ref())
    }

    pub fn claim(&mut self, owner_epoch: u64) -> Result<bool> {
        ensure!(owner_epoch > 0, "owner epoch must be positive");
        match self.records.last_mut() {
            Some(record) if record.owner_epoch == owner_epoch => Ok(false),
            Some(record) => {
                ensure!(
                    owner_epoch > record.owner_epoch,
                    "owner epoch must increase during a claim"
                );
                record.owner_epoch = owner_epoch;
                Ok(true)
            }
            None => {
                self.records.push(StateRecord {
                    state_version: 0,
                    owner_epoch,
                    state: None,
                });
                Ok(true)
            }
        }
    }

    pub fn append(&mut self, owner_epoch: u64, state: Value) -> Result<StateAppend> {
        ensure!(owner_epoch > 0, "owner epoch must be positive");
        ensure!(state.is_object(), "actor state must be a JSON object");
        let state_bytes = serde_json::to_vec(&state)?;
        ensure!(
            state_bytes.len() <= MAX_ACTOR_STATE_BYTES,
            "actor state exceeds the {MAX_ACTOR_STATE_BYTES}-byte limit"
        );
        if self.latest_state() == Some(&state) {
            return Ok(StateAppend::Unchanged);
        }
        if let Some(latest) = self.latest() {
            ensure!(
                latest.owner_epoch == owner_epoch,
                "actor state owner epoch does not match the current claim"
            );
        }
        let state_version = self
            .latest()
            .map_or(1, |record| record.state_version.saturating_add(1));
        ensure!(state_version > 0, "actor state version overflow");
        self.records.push(StateRecord {
            state_version,
            owner_epoch,
            state: Some(state),
        });
        if self.records.len() >= MAX_STATE_LOG_RECORDS
            || self.encode()?.len() >= MAX_STATE_LOG_BYTES
        {
            let latest = self.records.pop().expect("state append added a record");
            self.records.clear();
            self.records.push(latest);
        }
        Ok(StateAppend::Changed)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        for record in &self.records {
            validate_record(record)?;
            serde_json::to_writer(&mut bytes, record)?;
            bytes.push(b'\n');
        }
        Ok(bytes)
    }

    pub fn record_count(&self) -> usize {
        self.records.len()
    }
}

fn validate_record(record: &StateRecord) -> Result<()> {
    ensure!(record.owner_epoch > 0, "owner epoch must be positive");
    match &record.state {
        Some(state) => {
            ensure!(
                record.state_version > 0,
                "committed actor state version must be positive"
            );
            ensure!(state.is_object(), "actor state must be a JSON object");
            ensure!(
                serde_json::to_vec(state)?.len() <= MAX_ACTOR_STATE_BYTES,
                "actor state exceeds the {MAX_ACTOR_STATE_BYTES}-byte limit"
            );
        }
        None => ensure!(
            record.state_version == 0,
            "only an uninitialized claim may omit state"
        ),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn snapshots_round_trip_as_ndjson() -> Result<()> {
        let mut log = StateLog::default();
        assert!(log.claim(1)?);
        assert_eq!(log.append(1, json!({"count": 1}))?, StateAppend::Changed);
        assert_eq!(log.append(1, json!({"count": 1}))?, StateAppend::Unchanged);
        assert_eq!(log.append(1, json!({"count": 2}))?, StateAppend::Changed);

        let bytes = log.encode()?;
        assert_eq!(bytes.iter().filter(|byte| **byte == b'\n').count(), 3);
        assert!(String::from_utf8(bytes.clone())?.contains("\"stateVersion\":2"));
        let decoded = StateLog::decode(&bytes)?;
        assert_eq!(decoded.latest().map(|record| record.state_version), Some(2));
        assert_eq!(decoded.latest_state(), Some(&json!({"count": 2})));
        Ok(())
    }

    #[test]
    fn ownership_claim_changes_epoch_without_changing_state_version() -> Result<()> {
        let mut log = StateLog::default();
        log.claim(3)?;
        log.append(3, json!({"count": 7}))?;
        assert!(log.claim(4)?);

        let latest = log.latest().expect("latest state");
        assert_eq!(latest.state_version, 1);
        assert_eq!(latest.owner_epoch, 4);
        assert_eq!(latest.state, Some(json!({"count": 7})));
        Ok(())
    }

    #[test]
    fn compacts_inline_at_the_record_limit() -> Result<()> {
        let mut log = StateLog::default();
        log.claim(1)?;
        for count in 1..MAX_STATE_LOG_RECORDS {
            log.append(1, json!({"count": count}))?;
        }
        assert_eq!(log.record_count(), 1);
        assert_eq!(
            log.latest().map(|record| record.state_version),
            Some((MAX_STATE_LOG_RECORDS - 1) as u64)
        );
        Ok(())
    }

    #[test]
    fn rejects_oversized_state() {
        let mut log = StateLog::default();
        log.claim(1).expect("claim");
        let error = log
            .append(1, json!({"value": "x".repeat(MAX_ACTOR_STATE_BYTES)}))
            .expect_err("oversized state");
        assert!(error.to_string().contains("actor state exceeds"));
    }
}
