use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use crate::{state_log::StateLog, storage_urls::STATE_CONTENT_TYPE};

const GENERATION_HEADER: &str = "x-goog-generation";

#[derive(Debug)]
pub struct LoadedState {
    pub log: StateLog,
    pub generation: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateWrite {
    Written,
    GenerationMismatch,
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
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(LoadedState {
                log: StateLog::default(),
                generation: "0".into(),
            });
        }
        ensure!(
            response.status().is_success(),
            "actor-state read failed with HTTP {}",
            response.status()
        );
        let generation = generation_header(&response)?;
        let bytes = response
            .bytes()
            .await
            .context("read actor-state response body")?;
        Ok(LoadedState {
            log: StateLog::decode(&bytes)?,
            generation,
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
            return Ok(StateWrite::GenerationMismatch);
        }
        ensure!(
            response.status().is_success(),
            "actor-state write failed with HTTP {}",
            response.status()
        );
        Ok(StateWrite::Written)
    }
}

fn generation_header(response: &reqwest::Response) -> Result<String> {
    let generation = response
        .headers()
        .get(GENERATION_HEADER)
        .context("GCS response omitted its object generation")?
        .to_str()
        .context("GCS object generation is not ASCII")?;
    ensure!(
        !generation.is_empty() && generation.bytes().all(|byte| byte.is_ascii_digit()),
        "GCS object generation is invalid"
    );
    Ok(generation.to_owned())
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
    fn rejects_non_http_capabilities() {
        assert!(validate_url("file:///tmp/state").is_err());
        assert!(validate_url("https://storage.googleapis.com/bucket/object").is_ok());
    }
}
