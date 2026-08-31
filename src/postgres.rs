use std::{str::FromStr, sync::Arc};

use anyhow::{Context, Result};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Client, Config, NoTls, config::SslMode};
use tracing::error;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS durable_object_host_leases (
    host_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    route TEXT NOT NULL,
    expires_at_ms BIGINT NOT NULL CHECK (expires_at_ms > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS durable_object_placements (
    object_id TEXT PRIMARY KEY,
    owner_host_id TEXT NOT NULL,
    owner_epoch BIGINT NOT NULL CHECK (owner_epoch > 0),
    home_region TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS durable_object_namespaces (
    namespace_id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS durable_object_project_specs (
    namespace_id TEXT PRIMARY KEY REFERENCES durable_object_namespaces(namespace_id),
    code_revision TEXT NOT NULL,
    image_ref TEXT NOT NULL,
    working_directory TEXT NOT NULL,
    actor_entrypoint TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

"#;

#[derive(Clone)]
pub(crate) struct PostgresDatabase {
    client: Arc<Client>,
}

impl PostgresDatabase {
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        let config = Config::from_str(url).context("parse PostgreSQL connection URL")?;
        let client = match config.get_ssl_mode() {
            SslMode::Disable => {
                let (client, connection) = config
                    .connect(NoTls)
                    .await
                    .context("connect to PostgreSQL without TLS")?;
                tokio::spawn(async move {
                    if let Err(error) = connection.await {
                        error!(error = %error, "PostgreSQL connection stopped");
                    }
                });
                client
            }
            _ => {
                let connector = TlsConnector::builder()
                    .build()
                    .context("build PostgreSQL TLS connector")?;
                let (client, connection) = config
                    .connect(MakeTlsConnector::new(connector))
                    .await
                    .context("connect to PostgreSQL with TLS")?;
                tokio::spawn(async move {
                    if let Err(error) = connection.await {
                        error!(error = %error, "PostgreSQL TLS connection stopped");
                    }
                });
                client
            }
        };
        let database = Self {
            client: Arc::new(client),
        };
        database.initialize().await?;
        Ok(database)
    }

    pub(crate) fn client(&self) -> &Client {
        &self.client
    }

    async fn initialize(&self) -> Result<()> {
        self.client
            .simple_query("SELECT pg_advisory_lock(841267719155101)")
            .await
            .context("acquire durable-object PostgreSQL schema initialization lock")?;

        let schema_result = self.client.batch_execute(SCHEMA).await;
        let unlock_result = self
            .client
            .simple_query("SELECT pg_advisory_unlock(841267719155101)")
            .await;

        schema_result.context("initialize durable-object PostgreSQL storage schema")?;
        unlock_result.context("release durable-object PostgreSQL schema initialization lock")?;
        Ok(())
    }
}
