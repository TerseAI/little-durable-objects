use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use crate::{actor::ActorScope, postgres::PostgresDatabase};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HostLaunchSpec {
    pub namespace_id: String,
    pub code_revision: String,
    pub image_ref: String,
    pub working_directory: String,
    pub actor_entrypoint: Option<String>,
}

impl HostLaunchSpec {
    pub(crate) fn validate(&self) -> Result<()> {
        ActorScope {
            namespace_id: self.namespace_id.clone(),
        }
        .validate()?;
        validate_component("code revision", &self.code_revision, 128)?;
        ensure!(
            !self.image_ref.is_empty() && self.image_ref.len() <= 255,
            "sandbox image reference must contain between 1 and 255 bytes"
        );
        ensure!(
            self.working_directory.starts_with('/') && self.working_directory.len() <= 1024,
            "host working directory must be an absolute path of at most 1024 bytes"
        );
        if let Some(entrypoint) = &self.actor_entrypoint {
            ensure!(
                !entrypoint.is_empty() && entrypoint.len() <= 1024,
                "actor entrypoint must contain between 1 and 1024 bytes"
            );
        }
        Ok(())
    }
}

#[async_trait]
pub(crate) trait AdminRegistry: Send + Sync {
    async fn ensure_namespace(&self, namespace_id: &str) -> Result<bool>;
    async fn register_launch_spec(&self, spec: &HostLaunchSpec) -> Result<bool>;
    async fn launch_spec(
        &self,
        namespace_id: &str,
        code_revision: &str,
    ) -> Result<Option<HostLaunchSpec>>;
}

#[derive(Default)]
pub(crate) struct LocalAdminRegistry {
    state: Mutex<LocalAdminState>,
}

#[derive(Default)]
struct LocalAdminState {
    namespaces: HashSet<String>,
    launch_specs: HashMap<(String, String), HostLaunchSpec>,
}

#[async_trait]
impl AdminRegistry for LocalAdminRegistry {
    async fn ensure_namespace(&self, namespace_id: &str) -> Result<bool> {
        validate_namespace(namespace_id)?;
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("admin registry lock poisoned"))?
            .namespaces
            .insert(namespace_id.to_owned()))
    }

    async fn register_launch_spec(&self, spec: &HostLaunchSpec) -> Result<bool> {
        spec.validate()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("admin registry lock poisoned"))?;
        ensure!(
            state.namespaces.contains(&spec.namespace_id),
            "namespace must be ensured before registering a launch spec"
        );
        let key = (spec.namespace_id.clone(), spec.code_revision.clone());
        match state.launch_specs.get(&key) {
            Some(existing) => {
                ensure!(
                    existing == spec,
                    "a different launch spec is already registered for this namespace and revision"
                );
                Ok(false)
            }
            None => {
                state.launch_specs.insert(key, spec.clone());
                Ok(true)
            }
        }
    }

    async fn launch_spec(
        &self,
        namespace_id: &str,
        code_revision: &str,
    ) -> Result<Option<HostLaunchSpec>> {
        validate_namespace(namespace_id)?;
        validate_component("code revision", code_revision, 128)?;
        Ok(self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("admin registry lock poisoned"))?
            .launch_specs
            .get(&(namespace_id.to_owned(), code_revision.to_owned()))
            .cloned())
    }
}

pub(crate) struct PostgresAdminRegistry {
    database: PostgresDatabase,
}

impl PostgresAdminRegistry {
    pub(crate) fn from_database(database: PostgresDatabase) -> Self {
        Self { database }
    }
}

#[async_trait]
impl AdminRegistry for PostgresAdminRegistry {
    async fn ensure_namespace(&self, namespace_id: &str) -> Result<bool> {
        validate_namespace(namespace_id)?;
        Ok(self
            .database
            .client()
            .execute(
                "INSERT INTO durable_object_namespaces (namespace_id) VALUES ($1) ON CONFLICT DO NOTHING",
                &[&namespace_id],
            )
            .await
            .context("ensure PostgreSQL durable-object namespace")?
            == 1)
    }

    async fn register_launch_spec(&self, spec: &HostLaunchSpec) -> Result<bool> {
        spec.validate()?;
        let inserted = self
            .database
            .client()
            .execute(
                "INSERT INTO durable_object_launch_specs \
                 (namespace_id, code_revision, image_ref, working_directory, actor_entrypoint) \
                 VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
                &[
                    &spec.namespace_id,
                    &spec.code_revision,
                    &spec.image_ref,
                    &spec.working_directory,
                    &spec.actor_entrypoint,
                ],
            )
            .await
            .context("register PostgreSQL host launch spec")?;
        if inserted == 1 {
            return Ok(true);
        }
        ensure!(
            self.launch_spec(&spec.namespace_id, &spec.code_revision)
                .await?
                .as_ref()
                == Some(spec),
            "a different launch spec is already registered for this namespace and revision"
        );
        Ok(false)
    }

    async fn launch_spec(
        &self,
        namespace_id: &str,
        code_revision: &str,
    ) -> Result<Option<HostLaunchSpec>> {
        validate_namespace(namespace_id)?;
        validate_component("code revision", code_revision, 128)?;
        Ok(self
            .database
            .client()
            .query_opt(
                "SELECT image_ref, working_directory, actor_entrypoint \
                 FROM durable_object_launch_specs WHERE namespace_id = $1 AND code_revision = $2",
                &[&namespace_id, &code_revision],
            )
            .await
            .context("load PostgreSQL host launch spec")?
            .map(|row| HostLaunchSpec {
                namespace_id: namespace_id.to_owned(),
                code_revision: code_revision.to_owned(),
                image_ref: row.get(0),
                working_directory: row.get(1),
                actor_entrypoint: row.get(2),
            }))
    }
}

fn validate_namespace(namespace_id: &str) -> Result<()> {
    ActorScope {
        namespace_id: namespace_id.to_owned(),
    }
    .validate()
}

fn validate_component(name: &str, value: &str, maximum: usize) -> Result<()> {
    ensure!(!value.is_empty(), "{name} must not be empty");
    ensure!(
        value.len() <= maximum,
        "{name} must be at most {maximum} bytes"
    );
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')),
        "{name} contains unsupported characters"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(image: &str) -> HostLaunchSpec {
        HostLaunchSpec {
            namespace_id: "project-1".into(),
            code_revision: "revision-1".into(),
            image_ref: image.into(),
            working_directory: "/workspace".into(),
            actor_entrypoint: Some("src/durable-objects.ts".into()),
        }
    }

    #[tokio::test]
    async fn registration_is_immutable_and_idempotent() -> Result<()> {
        let registry = LocalAdminRegistry::default();
        assert!(registry.ensure_namespace("project-1").await?);
        assert!(!registry.ensure_namespace("project-1").await?);
        assert!(registry.register_launch_spec(&spec("im-1")).await?);
        assert!(!registry.register_launch_spec(&spec("im-1")).await?);
        assert!(registry.register_launch_spec(&spec("im-2")).await.is_err());
        assert_eq!(
            registry.launch_spec("project-1", "revision-1").await?,
            Some(spec("im-1"))
        );
        Ok(())
    }
}
