use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use aws_lc_rs::signature::{Ed25519KeyPair, KeyPair};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use serde::Serialize;
use serde_json::json;

use crate::host::{ActorProcessRole, HostId};

const WORKFLOW_DEADLINE_GRACE: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub(crate) struct ActorJwtIssuer {
    key_pair: Arc<Ed25519KeyPair>,
    key_id: String,
    issuer: String,
    authority_audience: String,
    invocation_audience: String,
    max_lifetime: Duration,
}

pub(crate) struct IssuedActorToken {
    pub token: String,
    pub expires_at_ms: i64,
}

impl ActorJwtIssuer {
    pub(crate) fn from_base64_pkcs8(
        encoded_key: &str,
        key_id: impl Into<String>,
        issuer: impl Into<String>,
        authority_audience: impl Into<String>,
        invocation_audience: impl Into<String>,
        max_lifetime: Duration,
    ) -> Result<Self> {
        let key_id = key_id.into();
        ensure!(
            !key_id.is_empty(),
            "DURABLE_OBJECT_JWT_KEY_ID must not be empty"
        );
        ensure!(
            !max_lifetime.is_zero(),
            "actor JWT lifetime must be positive"
        );
        let issuer = issuer.into();
        let authority_audience = authority_audience.into();
        let invocation_audience = invocation_audience.into();
        ensure!(!issuer.is_empty(), "actor JWT issuer must not be empty");
        ensure!(
            !authority_audience.is_empty(),
            "actor authority JWT audience must not be empty"
        );
        ensure!(
            !invocation_audience.is_empty(),
            "actor invocation JWT audience must not be empty"
        );
        let pkcs8 = STANDARD
            .decode(encoded_key)
            .context("DURABLE_OBJECT_JWT_SIGNING_KEY must be base64-encoded PKCS#8")?;
        let key_pair = Ed25519KeyPair::from_pkcs8(&pkcs8).map_err(|_| {
            anyhow::anyhow!("DURABLE_OBJECT_JWT_SIGNING_KEY is not an Ed25519 PKCS#8 key")
        })?;
        Ok(Self {
            key_pair: Arc::new(key_pair),
            key_id,
            issuer,
            authority_audience,
            invocation_audience,
            max_lifetime,
        })
    }

    pub(crate) fn public_keys_json(&self) -> Result<String> {
        Ok(serde_json::to_string(&json!({
            self.key_id.clone(): URL_SAFE_NO_PAD.encode(self.key_pair.public_key().as_ref())
        }))?)
    }

    pub(crate) fn jwks_json(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&json!({
            "keys": [{
                "alg": "EdDSA",
                "crv": "Ed25519",
                "kid": self.key_id,
                "kty": "OKP",
                "use": "sig",
                "x": URL_SAFE_NO_PAD.encode(self.key_pair.public_key().as_ref())
            }]
        }))?)
    }

    pub(crate) fn verifier_keys_json(&self) -> Result<String> {
        self.public_keys_json()
    }

    pub(crate) fn issue_workflow(
        &self,
        namespace_id: &str,
        execution_id: &str,
        code_revision: &str,
        region: &str,
        deadline_unix_ms: i64,
    ) -> Result<IssuedActorToken> {
        let now_ms = unix_millis()?;
        ensure!(
            deadline_unix_ms > now_ms,
            "workflow deadline must be in the future"
        );
        let maximum_expiration = now_ms.saturating_add(duration_millis(self.max_lifetime)?);
        let requested_expiration =
            deadline_unix_ms.saturating_add(duration_millis(WORKFLOW_DEADLINE_GRACE)?);
        let expires_at_ms = requested_expiration.min(maximum_expiration);
        let process_id = format!("workflow.v1.{namespace_id}.{}", uuid::Uuid::new_v4());
        self.issue(ActorJwtClaims {
            iss: self.issuer.clone(),
            aud: vec![
                self.authority_audience.clone(),
                self.invocation_audience.clone(),
            ],
            sub: execution_id.to_owned(),
            namespace_id: namespace_id.to_owned(),
            process_id,
            session_id: uuid::Uuid::new_v4().to_string(),
            process_role: ActorProcessRole::Workflow,
            region: region.to_owned(),
            code_revision: Some(code_revision.to_owned()),
            scope: "actor:resolve actor:invoke".into(),
            iat: now_ms / 1000,
            nbf: now_ms / 1000,
            exp: expires_at_ms / 1000,
        })
    }

    pub(crate) fn issue_host(
        &self,
        namespace_id: &str,
        host_id: &HostId,
        session_id: &str,
        code_revision: &str,
        region: &str,
    ) -> Result<IssuedActorToken> {
        let now_ms = unix_millis()?;
        let expires_at_ms = now_ms.saturating_add(duration_millis(self.max_lifetime)?);
        self.issue(ActorJwtClaims {
            iss: self.issuer.clone(),
            aud: vec![self.authority_audience.clone()],
            sub: host_id.as_str().to_owned(),
            namespace_id: namespace_id.to_owned(),
            process_id: host_id.as_str().to_owned(),
            session_id: session_id.to_owned(),
            process_role: ActorProcessRole::Host,
            region: region.to_owned(),
            code_revision: Some(code_revision.to_owned()),
            scope: "actor:authority".into(),
            iat: now_ms / 1000,
            nbf: now_ms / 1000,
            exp: expires_at_ms / 1000,
        })
    }

    fn issue(&self, claims: ActorJwtClaims) -> Result<IssuedActorToken> {
        let expires_at_ms = claims
            .exp
            .checked_mul(1_000)
            .context("issued actor token expiration overflow")?;
        let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&json!({
            "alg": "EdDSA",
            "kid": self.key_id,
            "typ": "JWT"
        }))?);
        let claims = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
        let signing_input = format!("{header}.{claims}");
        let signature =
            URL_SAFE_NO_PAD.encode(self.key_pair.sign(signing_input.as_bytes()).as_ref());
        Ok(IssuedActorToken {
            token: format!("{signing_input}.{signature}"),
            expires_at_ms,
        })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ActorJwtClaims {
    iss: String,
    aud: Vec<String>,
    sub: String,
    namespace_id: String,
    #[serde(rename = "processId")]
    process_id: String,
    session_id: String,
    #[serde(rename = "processRole")]
    process_role: ActorProcessRole,
    #[serde(rename = "storageRegion")]
    region: String,
    code_revision: Option<String>,
    scope: String,
    iat: i64,
    nbf: i64,
    exp: i64,
}

fn unix_millis() -> Result<i64> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    i64::try_from(duration.as_millis()).context("system clock exceeds supported JWT range")
}

fn duration_millis(duration: Duration) -> Result<i64> {
    i64::try_from(duration.as_millis()).context("duration exceeds supported JWT range")
}
