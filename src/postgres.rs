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

CREATE TABLE IF NOT EXISTS durable_object_manifests (
    object_id TEXT PRIMARY KEY,
    format_version INTEGER NOT NULL,
    owner_node TEXT NOT NULL,
    owner_epoch BIGINT NOT NULL CHECK (owner_epoch > 0),
    tip_epoch BIGINT,
    tip_log_generation BIGINT,
    tip_txid BIGINT,
    checkpoint_epoch BIGINT,
    checkpoint_log_generation BIGINT,
    checkpoint_txid BIGINT,
    checkpoint_key TEXT,
    checkpoint_byte_len BIGINT,
    checkpoint_crc32c BIGINT,
    checkpoint_page_size BIGINT,
    checkpoint_checksum TEXT,
    archived_txid BIGINT NOT NULL DEFAULT 0 CHECK (archived_txid >= 0),
    rapid_gc_txid BIGINT NOT NULL DEFAULT 0 CHECK (rapid_gc_txid >= 0),
    storage_region TEXT NOT NULL DEFAULT 'default',
    revision BIGINT NOT NULL CHECK (revision > 0),
    CHECK ((tip_epoch IS NULL) = (tip_log_generation IS NULL)),
    CHECK ((tip_epoch IS NULL) = (tip_txid IS NULL)),
    CHECK (tip_epoch IS NULL OR tip_epoch > 0),
    CHECK (tip_txid IS NULL OR tip_txid > 0),
    CHECK (tip_epoch IS NULL OR tip_epoch <= owner_epoch),
    CHECK ((checkpoint_epoch IS NULL) = (checkpoint_log_generation IS NULL)),
    CHECK ((checkpoint_epoch IS NULL) = (checkpoint_txid IS NULL)),
    CHECK ((checkpoint_epoch IS NULL) = (checkpoint_key IS NULL)),
    CHECK ((checkpoint_epoch IS NULL) = (checkpoint_byte_len IS NULL)),
    CHECK ((checkpoint_epoch IS NULL) = (checkpoint_crc32c IS NULL)),
    CHECK ((checkpoint_epoch IS NULL) = (checkpoint_page_size IS NULL)),
    CHECK ((checkpoint_epoch IS NULL) = (checkpoint_checksum IS NULL)),
    CHECK (checkpoint_txid IS NULL OR checkpoint_txid <= tip_txid),
    CHECK (rapid_gc_txid <= tip_txid)
);

CREATE TABLE IF NOT EXISTS durable_object_namespaces (
    namespace_id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS durable_object_launch_specs (
    namespace_id TEXT NOT NULL REFERENCES durable_object_namespaces(namespace_id),
    code_revision TEXT NOT NULL,
    modal_image_id TEXT NOT NULL,
    working_directory TEXT NOT NULL,
    actor_entrypoint TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, code_revision)
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
