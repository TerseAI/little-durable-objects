use std::{str::FromStr, sync::Arc};

use anyhow::{Context, Result};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Client, Config, NoTls, config::SslMode};
use tracing::error;

mod embedded {
    use refinery::embed_migrations;

    embed_migrations!("migrations");
}

#[derive(Clone)]
pub(crate) struct PostgresDatabase {
    client: Arc<Client>,
}

impl PostgresDatabase {
    pub(crate) async fn connect(url: &str) -> Result<Self> {
        let config = Config::from_str(url).context("parse PostgreSQL connection URL")?;
        let mut client = match config.get_ssl_mode() {
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
        embedded::migrations::runner()
            .run_async(&mut client)
            .await
            .context("run durable-object PostgreSQL migrations")?;
        Ok(Self {
            client: Arc::new(client),
        })
    }

    pub(crate) fn client(&self) -> &Client {
        &self.client
    }
}
