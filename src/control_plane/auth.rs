use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    jwk::{JwkSet, KeyAlgorithm},
};
use serde::{Deserialize, Serialize};
use tonic::{Request, Status};

use crate::actor::ActorScope;
use crate::host::{ActorProcessRole, HostId};

const AUTHORIZATION: &str = "authorization";
const CLOCK_SKEW: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ActorTokenPurpose {
    ControlPlane,
    Invocation,
}

impl ActorTokenPurpose {
    fn claim(self, role: ActorProcessRole) -> &'static str {
        match (self, role) {
            (Self::ControlPlane, ActorProcessRole::Host) => "actor:authority",
            (Self::ControlPlane, ActorProcessRole::Workflow) => "actor:invoke",
            (Self::Invocation, ActorProcessRole::Workflow) => "actor:invoke",
            (Self::Invocation, ActorProcessRole::Host) => "actor:invoke",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ActorPrincipal {
    pub scope: ActorScope,
    pub host_id: HostId,
    pub session_id: String,
    pub process_role: ActorProcessRole,
    pub region: String,
    pub code_revision: Option<String>,
    pub expires_at: i64,
    pub invocation: Option<ActorInvocationCapability>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ActorInvocationCapability {
    pub actor: crate::actor::ActorKey,
    pub host_id: HostId,
    pub owner_epoch: u64,
    pub state_version: u64,
    pub state_read_url: String,
}

impl ActorPrincipal {
    pub(crate) fn validate_host_id(&self, host_id: &str) -> Result<()> {
        ensure!(
            host_id == self.host_id.as_str(),
            "host does not match the authenticated host identity"
        );
        Ok(())
    }

    pub(crate) fn host_id_prefix(&self) -> String {
        format!("host.v1.{}.", self.scope.namespace_id)
    }
}

#[derive(Clone)]
pub(crate) struct ActorJwtVerifier {
    public_keys: Arc<HashMap<String, DecodingKey>>,
    validation: Validation,
    purpose: ActorTokenPurpose,
    max_lifetime: Duration,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActorJwtClaims {
    sub: String,
    namespace_id: String,
    #[serde(rename = "processId")]
    host_id: String,
    session_id: String,
    #[serde(rename = "processRole")]
    process_role: ActorProcessRole,
    #[serde(rename = "storageRegion")]
    region: String,
    code_revision: Option<String>,
    scope: String,
    iat: i64,
    #[serde(rename = "nbf")]
    _not_before: i64,
    exp: i64,
    #[serde(default)]
    invocation: Option<ActorInvocationCapability>,
}

impl ActorJwtVerifier {
    #[cfg(test)]
    pub(crate) fn new(
        public_keys_json: impl AsRef<str>,
        issuer: impl Into<String>,
        audience: impl Into<String>,
        max_lifetime: Duration,
    ) -> Result<Self> {
        Self::for_scope(
            public_keys_json,
            issuer,
            audience,
            ActorTokenPurpose::ControlPlane,
            max_lifetime,
        )
    }

    pub(crate) fn for_scope(
        public_keys_json: impl AsRef<str>,
        issuer: impl Into<String>,
        audience: impl Into<String>,
        purpose: ActorTokenPurpose,
        max_lifetime: Duration,
    ) -> Result<Self> {
        let public_keys = decode_public_keys(public_keys_json.as_ref())?;
        Self::from_decoded_public_keys(public_keys, issuer, audience, purpose, max_lifetime)
    }

    pub(crate) async fn authenticate<T>(
        &self,
        request: &Request<T>,
    ) -> Result<ActorPrincipal, Status> {
        let authorization = request
            .metadata()
            .get(AUTHORIZATION)
            .ok_or_else(|| Status::unauthenticated("actor token is required"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("actor token is not valid metadata"))?;
        self.authenticate_authorization(authorization)
            .map_err(|error| Status::unauthenticated(format!("{error:#}")))
    }

    pub(crate) fn authenticate_authorization(&self, authorization: &str) -> Result<ActorPrincipal> {
        let token = bearer_token(authorization)?;
        let header = decode_header(token).context("actor token header is invalid")?;
        self.verify_with_header(token, header)
    }

    fn from_decoded_public_keys(
        public_keys: HashMap<String, DecodingKey>,
        issuer: impl Into<String>,
        audience: impl Into<String>,
        purpose: ActorTokenPurpose,
        max_lifetime: Duration,
    ) -> Result<Self> {
        ensure!(
            !max_lifetime.is_zero(),
            "actor JWT maximum lifetime must be positive"
        );
        ensure!(
            !public_keys.is_empty(),
            "actor JWT public-key set must not be empty"
        );
        let issuer = issuer.into();
        let audience = audience.into();
        ensure!(!issuer.is_empty(), "actor JWT issuer must not be empty");
        ensure!(!audience.is_empty(), "actor JWT audience must not be empty");
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_issuer(&[issuer]);
        validation.set_audience(&[audience]);
        validation.set_required_spec_claims(&["exp", "nbf", "iss", "aud", "sub"]);
        validation.validate_nbf = true;
        validation.leeway = CLOCK_SKEW.as_secs();
        Ok(Self {
            public_keys: Arc::new(public_keys),
            validation,
            purpose,
            max_lifetime,
        })
    }

    #[cfg(test)]
    fn verify(&self, token: &str) -> Result<ActorPrincipal> {
        self.verify_with_header(token, decode_header(token)?)
    }

    fn verify_with_header(
        &self,
        token: &str,
        header: jsonwebtoken::Header,
    ) -> Result<ActorPrincipal> {
        ensure!(
            header.typ.as_deref().is_none_or(|value| value == "JWT"),
            "actor token has an invalid type"
        );
        let key_id = header.kid.context("actor token key ID is empty")?;
        let public_key = self
            .public_keys
            .get(&key_id)
            .with_context(|| format!("actor token uses unknown key ID {key_id:?}"))?;
        let claims = decode::<ActorJwtClaims>(token, public_key, &self.validation)
            .context("verify actor JWT")?
            .claims;
        self.validate_claims(claims)
    }

    fn validate_claims(&self, claims: ActorJwtClaims) -> Result<ActorPrincipal> {
        ensure!(
            claims
                .scope
                .split_ascii_whitespace()
                .any(|scope| scope == self.purpose.claim(claims.process_role)),
            "actor token credential scope is invalid"
        );
        ensure!(!claims.sub.is_empty(), "actor token subject is empty");
        if claims.process_role == ActorProcessRole::Host {
            ensure!(
                claims.sub == claims.host_id,
                "host token subject does not match its process identity"
            );
        }
        ensure!(
            uuid::Uuid::parse_str(&claims.session_id).is_ok(),
            "actor token session ID is invalid"
        );
        ensure!(claims.exp > claims.iat, "actor token lifetime is invalid");
        let lifetime_seconds = u64::try_from(
            claims
                .exp
                .checked_sub(claims.iat)
                .context("actor token lifetime is invalid")?,
        )
        .context("actor token lifetime is invalid")?;
        ensure!(
            Duration::from_secs(lifetime_seconds) <= self.max_lifetime,
            "actor token lifetime exceeds the configured maximum"
        );

        let now = unix_seconds()?;
        ensure!(claims.exp > now, "actor token has expired");
        let skew_seconds = i64::try_from(CLOCK_SKEW.as_secs()).unwrap_or(i64::MAX);
        ensure!(
            claims.iat <= now.saturating_add(skew_seconds),
            "actor token was issued in the future"
        );
        let principal = ActorPrincipal {
            scope: ActorScope {
                namespace_id: claims.namespace_id,
            },
            host_id: HostId::new(claims.host_id),
            session_id: claims.session_id,
            process_role: claims.process_role,
            region: claims.region,
            code_revision: claims.code_revision,
            expires_at: claims.exp,
            invocation: claims.invocation,
        };
        principal.scope.validate()?;
        let process_prefix = match principal.process_role {
            ActorProcessRole::Host => principal.host_id_prefix(),
            ActorProcessRole::Workflow => {
                format!("workflow.v1.{}.", principal.scope.namespace_id)
            }
        };
        ensure!(
            principal.host_id.as_str().starts_with(&process_prefix),
            "actor token process does not belong to its namespace"
        );
        ensure!(
            !principal.region.is_empty()
                && principal.region.len() <= 64
                && principal
                    .region
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'.' | b'_' | b'-')),
            "actor token storage region is invalid"
        );
        if let Some(capability) = &principal.invocation {
            ensure!(
                principal.process_role == ActorProcessRole::Host,
                "actor invocation capability has an invalid process role"
            );
            ensure!(
                capability.host_id == principal.host_id,
                "actor invocation capability targets another host"
            );
            ensure!(
                principal.scope.contains(&capability.actor),
                "actor invocation capability crossed namespace scope"
            );
            ensure!(
                capability.owner_epoch > 0,
                "actor invocation capability owner epoch is invalid"
            );
            if capability.state_version == 0 {
                ensure!(
                    capability.state_read_url.is_empty(),
                    "uninitialized actor invocation capability has a state URL"
                );
            } else {
                let state_read_url = reqwest::Url::parse(&capability.state_read_url)
                    .context("actor invocation capability state URL is invalid")?;
                ensure!(
                    matches!(state_read_url.scheme(), "http" | "https")
                        && state_read_url.host_str().is_some(),
                    "actor invocation capability state URL must be HTTP or HTTPS"
                );
            }
        }
        Ok(principal)
    }
}

fn decode_public_keys(public_keys_json: &str) -> Result<HashMap<String, DecodingKey>> {
    let keys: JwkSet = serde_json::from_str(public_keys_json)
        .context("parse durable-object JWT public keys as a JWK set")?;
    ensure!(
        !keys.keys.is_empty(),
        "durable-object JWT public keys must contain at least one key"
    );
    keys.keys
        .into_iter()
        .map(|key| {
            let key_id = key
                .common
                .key_id
                .clone()
                .context("actor JWT key ID must not be empty")?;
            ensure!(
                key.common.key_algorithm == Some(KeyAlgorithm::EdDSA),
                "actor JWT public key {key_id:?} must use EdDSA"
            );
            let decoding_key = DecodingKey::from_jwk(&key)
                .with_context(|| format!("decode actor JWT public key {key_id:?}"))?;
            Ok((key_id, decoding_key))
        })
        .collect()
}

fn bearer_token(authorization: &str) -> Result<&str> {
    let token = authorization
        .strip_prefix("Bearer ")
        .context("actor token must use Bearer authentication")?;
    ensure!(
        !token.is_empty() && token.trim() == token,
        "actor token is invalid"
    );
    Ok(token)
}

fn unix_seconds() -> Result<i64> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?;
    i64::try_from(duration.as_secs()).context("system clock exceeds supported JWT range")
}

#[cfg(test)]
mod tests {
    use aws_lc_rs::{
        rand::SystemRandom,
        signature::{Ed25519KeyPair, KeyPair},
    };
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use serde_json::json;

    use super::*;

    #[test]
    fn verifies_a_signed_actor_token() -> Result<()> {
        let (verifier, key_pair) = verifier_and_key_pair()?;
        let now = unix_seconds()?;
        let principal = verifier.verify(&token(
            &key_pair,
            json!({ "alg": "EdDSA", "kid": "test-key", "typ": "JWT" }),
            valid_claims(now),
        )?)?;

        assert_eq!(principal.scope.namespace_id, "namespace-1");
        assert_eq!(
            principal.host_id,
            HostId::new("host.v1.namespace-1.00000000-0000-4000-8000-000000000001")
        );
        assert_eq!(principal.session_id, "00000000-0000-4000-8000-000000000002");
        Ok(())
    }

    #[test]
    fn invocation_credentials_are_distinct_from_control_plane_credentials() -> Result<()> {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref())?;
        let keys = public_key_set(&key_pair)?;
        let invocation = ActorJwtVerifier::for_scope(
            &keys,
            "durable-object-control-plane",
            "durable-object-invoke",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let control_plane = ActorJwtVerifier::for_scope(
            keys,
            "durable-object-control-plane",
            "durable-object-authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(60),
        )?;
        let mut claims = valid_claims(unix_seconds()?);
        claims["aud"] = json!("durable-object-invoke");
        claims["scope"] = json!("actor:invoke");
        claims["processRole"] = json!("workflow");
        claims["sub"] = json!("execution-1");
        claims["processId"] = json!("workflow.v1.namespace-1.00000000-0000-4000-8000-000000000001");
        claims["codeRevision"] = json!("revision-1");
        let token = token(
            &key_pair,
            json!({ "alg": "EdDSA", "kid": "test-key", "typ": "JWT" }),
            claims,
        )?;

        assert!(invocation.verify(&token).is_ok());
        assert!(control_plane.verify(&token).is_err());
        Ok(())
    }

    #[test]
    fn rejects_an_expired_token_during_clock_skew_leeway() -> Result<()> {
        let (verifier, key_pair) = verifier_and_key_pair()?;
        let now = unix_seconds()?;
        let expired = token(
            &key_pair,
            json!({ "alg": "EdDSA", "kid": "test-key", "typ": "JWT" }),
            json!({
                "iss": "durable-object-control-plane",
                "aud": "durable-object-authority",
                "sub": "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
                "namespaceId": "namespace-1",
                "processId": "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
                "sessionId": "00000000-0000-4000-8000-000000000002",
                "processRole": "host",
                "storageRegion": "us-east",
                "scope": "actor:authority",
                "iat": now - 60,
                "nbf": now - 60,
                "exp": now - 1
            }),
        )?;

        assert!(verifier.verify(&expired).is_err());
        Ok(())
    }

    #[test]
    fn rejects_tampering_and_invalid_constraints() -> Result<()> {
        let (verifier, key_pair) = verifier_and_key_pair()?;
        let now = unix_seconds()?;
        let valid = token(
            &key_pair,
            json!({ "alg": "EdDSA", "kid": "test-key", "typ": "JWT" }),
            valid_claims(now),
        )?;
        let mut tampered = valid.into_bytes();
        let last = tampered.len() - 1;
        tampered[last] = if tampered[last] == b'a' { b'b' } else { b'a' };
        assert!(verifier.verify(std::str::from_utf8(&tampered)?).is_err());

        let wrong_audience = token(
            &key_pair,
            json!({ "alg": "EdDSA", "kid": "test-key" }),
            json!({
                "iss": "durable-object-control-plane",
                "aud": "somewhere-else",
                "sub": "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
                "namespaceId": "namespace-1",
                "processId": "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
                "sessionId": "00000000-0000-4000-8000-000000000002",
                "scope": "actor:authority",
                "iat": now,
                "nbf": now,
                "exp": now + 60
            }),
        )?;
        assert!(verifier.verify(&wrong_audience).is_err());

        let expired = token(
            &key_pair,
            json!({ "alg": "EdDSA", "kid": "test-key" }),
            json!({
                "iss": "durable-object-control-plane",
                "aud": "durable-object-authority",
                "sub": "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
                "namespaceId": "namespace-1",
                "processId": "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
                "sessionId": "00000000-0000-4000-8000-000000000002",
                "scope": "actor:authority",
                "iat": now - 60,
                "nbf": now - 60,
                "exp": now - 10
            }),
        )?;
        assert!(verifier.verify(&expired).is_err());

        let unknown_key = token(
            &key_pair,
            json!({ "alg": "EdDSA", "kid": "retired-key" }),
            valid_claims(now),
        )?;
        assert!(verifier.verify(&unknown_key).is_err());

        let too_long = token(
            &key_pair,
            json!({ "alg": "EdDSA", "kid": "test-key" }),
            json!({
                "iss": "durable-object-control-plane",
                "aud": "durable-object-authority",
                "sub": "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
                "namespaceId": "namespace-1",
                "processId": "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
                "sessionId": "00000000-0000-4000-8000-000000000002",
                "scope": "actor:authority",
                "iat": now,
                "nbf": now,
                "exp": now + 61
            }),
        )?;
        assert!(verifier.verify(&too_long).is_err());
        Ok(())
    }

    fn verifier_and_key_pair() -> Result<(ActorJwtVerifier, Ed25519KeyPair)> {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref())?;
        let keys = public_key_set(&key_pair)?;
        Ok((
            ActorJwtVerifier::new(
                keys,
                "durable-object-control-plane",
                "durable-object-authority",
                Duration::from_secs(60),
            )?,
            key_pair,
        ))
    }

    fn token(
        key_pair: &Ed25519KeyPair,
        header: serde_json::Value,
        claims: serde_json::Value,
    ) -> Result<String> {
        let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header)?);
        let claims = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
        let input = format!("{header}.{claims}");
        let signature = URL_SAFE_NO_PAD.encode(key_pair.sign(input.as_bytes()).as_ref());
        Ok(format!("{input}.{signature}"))
    }

    fn public_key_set(key_pair: &Ed25519KeyPair) -> Result<String> {
        Ok(serde_json::to_string(&json!({
            "keys": [{
                "alg": "EdDSA",
                "crv": "Ed25519",
                "kid": "test-key",
                "kty": "OKP",
                "use": "sig",
                "x": URL_SAFE_NO_PAD.encode(key_pair.public_key().as_ref())
            }]
        }))?)
    }

    fn valid_claims(now: i64) -> serde_json::Value {
        json!({
            "iss": "durable-object-control-plane",
            "aud": "durable-object-authority",
            "sub": "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
            "namespaceId": "namespace-1",
            "processId": "host.v1.namespace-1.00000000-0000-4000-8000-000000000001",
            "sessionId": "00000000-0000-4000-8000-000000000002",
            "processRole": "host",
            "storageRegion": "default",
            "scope": "actor:authority",
            "iat": now,
            "nbf": now,
            "exp": now + 60
        })
    }
}
