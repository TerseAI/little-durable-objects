use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use aws_lc_rs::signature::{ED25519, VerificationAlgorithm};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Deserialize;
use tonic::{Request, Status, metadata::MetadataMap};

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
    public_keys: Arc<HashMap<String, Vec<u8>>>,
    issuer: String,
    audience: String,
    purpose: ActorTokenPurpose,
    max_lifetime: Duration,
}

#[derive(Deserialize)]
struct JwtHeader {
    alg: String,
    kid: String,
    typ: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ActorJwtClaims {
    iss: String,
    aud: JwtAudience,
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
    nbf: i64,
    exp: i64,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum JwtAudience {
    One(String),
    Many(Vec<String>),
}

impl JwtAudience {
    fn contains(&self, expected: &str) -> bool {
        match self {
            Self::One(audience) => audience == expected,
            Self::Many(audiences) => audiences.iter().any(|audience| audience == expected),
        }
    }
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

    fn from_decoded_public_keys(
        public_keys: HashMap<String, Vec<u8>>,
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
        Ok(Self {
            public_keys: Arc::new(public_keys),
            issuer,
            audience,
            purpose,
            max_lifetime,
        })
    }

    pub(crate) async fn authenticate<T>(
        &self,
        request: &Request<T>,
    ) -> Result<ActorPrincipal, Status> {
        let token = bearer_token(request.metadata())?;
        let header =
            token_header(token).map_err(|error| Status::unauthenticated(format!("{error:#}")))?;
        self.verify_with_header(token, header)
            .map_err(|error| Status::unauthenticated(format!("{error:#}")))
    }

    #[cfg(test)]
    fn verify(&self, token: &str) -> Result<ActorPrincipal> {
        self.verify_with_header(token, token_header(token)?)
    }

    fn verify_with_header(&self, token: &str, header: JwtHeader) -> Result<ActorPrincipal> {
        let mut segments = token.split('.');
        let encoded_header = segments.next().context("actor token omitted its header")?;
        let encoded_claims = segments.next().context("actor token omitted its claims")?;
        let encoded_signature = segments
            .next()
            .context("actor token omitted its signature")?;
        ensure!(
            segments.next().is_none(),
            "actor token must contain exactly three segments"
        );

        ensure!(header.alg == "EdDSA", "actor token must use EdDSA");
        ensure!(
            header.typ.as_deref().is_none_or(|value| value == "JWT"),
            "actor token has an invalid type"
        );
        let public_key = self
            .public_keys
            .get(&header.kid)
            .with_context(|| format!("actor token uses unknown key ID {:?}", header.kid))?;
        let signature = URL_SAFE_NO_PAD
            .decode(encoded_signature)
            .context("decode actor token signature")?;
        let signing_input = format!("{encoded_header}.{encoded_claims}");
        ED25519
            .verify_sig(public_key, signing_input.as_bytes(), &signature)
            .map_err(|_| anyhow::anyhow!("actor token signature is invalid"))?;

        let claims: ActorJwtClaims = decode_json_segment(encoded_claims, "claims")?;
        ensure!(claims.iss == self.issuer, "actor token issuer is invalid");
        ensure!(
            claims.aud.contains(&self.audience),
            "actor token audience is invalid"
        );
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
        let skew_seconds = i64::try_from(CLOCK_SKEW.as_secs()).unwrap_or(i64::MAX);
        ensure!(
            claims.iat <= now.saturating_add(skew_seconds),
            "actor token was issued in the future"
        );
        ensure!(
            claims.nbf <= now.saturating_add(skew_seconds),
            "actor token is not active yet"
        );
        ensure!(
            claims.exp > now.saturating_sub(skew_seconds),
            "actor token expired"
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
        Ok(principal)
    }
}

fn token_header(token: &str) -> Result<JwtHeader> {
    let encoded_header = token
        .split('.')
        .next()
        .context("actor token omitted its header")?;
    let header: JwtHeader = decode_json_segment(encoded_header, "header")?;
    ensure!(!header.kid.is_empty(), "actor token key ID is empty");
    Ok(header)
}

fn decode_public_keys(public_keys_json: &str) -> Result<HashMap<String, Vec<u8>>> {
    let encoded_keys: HashMap<String, String> = serde_json::from_str(public_keys_json)
        .context("parse durable-object JWT public keys as a JSON object")?;
    ensure!(
        !encoded_keys.is_empty(),
        "durable-object JWT public keys must contain at least one key"
    );
    encoded_keys
        .into_iter()
        .map(|(key_id, encoded)| {
            ensure!(!key_id.is_empty(), "actor JWT key ID must not be empty");
            let key = URL_SAFE_NO_PAD
                .decode(encoded)
                .with_context(|| format!("decode actor JWT public key {key_id:?}"))?;
            ensure!(
                key.len() == 32,
                "actor JWT public key {key_id:?} must be a 32-byte Ed25519 key"
            );
            Ok((key_id, key))
        })
        .collect()
}

fn decode_json_segment<T: for<'de> Deserialize<'de>>(segment: &str, name: &str) -> Result<T> {
    let bytes = URL_SAFE_NO_PAD
        .decode(segment)
        .with_context(|| format!("decode actor token {name}"))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse actor token {name}"))
}

fn bearer_token(metadata: &MetadataMap) -> Result<&str, Status> {
    let value = metadata
        .get(AUTHORIZATION)
        .ok_or_else(|| Status::unauthenticated("actor token is required"))?
        .to_str()
        .map_err(|_| Status::unauthenticated("actor token is not valid metadata"))?;
    let token = value
        .strip_prefix("Bearer ")
        .ok_or_else(|| Status::unauthenticated("actor token must use Bearer authentication"))?;
    if token.is_empty() || token.trim() != token {
        return Err(Status::unauthenticated("actor token is invalid"));
    }
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
    use serde_json::json;

    use super::*;

    fn verifier_and_key_pair() -> Result<(ActorJwtVerifier, Ed25519KeyPair)> {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref())?;
        let keys = serde_json::to_string(&HashMap::from([(
            "test-key",
            URL_SAFE_NO_PAD.encode(key_pair.public_key().as_ref()),
        )]))?;
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
        let keys = serde_json::to_string(&HashMap::from([(
            "test-key",
            URL_SAFE_NO_PAD.encode(key_pair.public_key().as_ref()),
        )]))?;
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
}
