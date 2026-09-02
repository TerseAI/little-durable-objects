use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use crate::{state_log::StateSnapshot, storage_urls::STATE_CONTENT_TYPE};

#[derive(Debug)]
pub struct LoadedState {
    pub snapshot: StateSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateWrite {
    Written,
    AlreadyExists,
}

#[async_trait]
pub trait StateTransport: Send + Sync {
    async fn read(&self, signed_url: &str) -> Result<LoadedState>;
    async fn write(&self, signed_url: &str, bytes: Vec<u8>) -> Result<StateWrite>;
}

#[derive(Clone, Default)]
pub struct HttpStateTransport {
    client: reqwest::Client,
}

impl HttpStateTransport {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl StateTransport for HttpStateTransport {
    async fn read(&self, signed_url: &str) -> Result<LoadedState> {
        validate_url(signed_url)?;
        let response = self
            .client
            .get(signed_url)
            .send()
            .await
            .context("read actor state through signed URL")?;
        ensure!(
            response.status().is_success(),
            "actor-state read failed with HTTP {}",
            response.status()
        );
        let bytes = response
            .bytes()
            .await
            .context("read actor-state response body")?;
        Ok(LoadedState {
            snapshot: StateSnapshot::decode(&bytes)?,
        })
    }

    async fn write(&self, signed_url: &str, bytes: Vec<u8>) -> Result<StateWrite> {
        validate_url(signed_url)?;
        let response = self
            .client
            .put(signed_url)
            .header(reqwest::header::CONTENT_TYPE, STATE_CONTENT_TYPE)
            .body(bytes)
            .send()
            .await
            .context("write actor state through signed URL")?;
        if response.status() == reqwest::StatusCode::PRECONDITION_FAILED {
            return Ok(StateWrite::AlreadyExists);
        }
        ensure!(
            response.status().is_success(),
            "actor-state write failed with HTTP {}",
            response.status()
        );
        Ok(StateWrite::Written)
    }
}

fn validate_url(url: &str) -> Result<()> {
    let url = reqwest::Url::parse(url).context("parse signed actor-state URL")?;
    ensure!(
        matches!(url.scheme(), "http" | "https") && url.host_str().is_some(),
        "signed actor-state URL must be HTTP or HTTPS"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_http_state_urls() {
        assert!(validate_url("file:///tmp/state").is_err());
    }
}
