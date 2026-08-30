use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::{ActorStorageKey, SqliteActorDatabase};

pub struct ActorDatabaseStore {
    root: PathBuf,
}

const CACHE_MARKER_FORMAT_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct CacheMarker {
    format_version: u32,
    durable_txid: u64,
}

impl ActorDatabaseStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn open(&self, object: &ActorStorageKey) -> Result<SqliteActorDatabase> {
        let path = self.path_for(object)?;
        debug!(object = %object, path = %path.display(), "opening actor SQLite database");

        std::fs::create_dir_all(path.parent().expect("actor database has parent"))?;

        let persistence = SqliteActorDatabase::connect(path)?;

        Ok(persistence)
    }

    pub fn path_for(&self, object: &ActorStorageKey) -> Result<PathBuf> {
        object.validate()?;
        Ok(self.object_dir(object)?.join("db.sqlite"))
    }

    /// Returns whether the persisted SQLite cache is known to represent the
    /// canonical manifest watermark. The integrity check runs only after a host
    /// restart; process-local readiness handles the hot path.
    pub fn cache_is_current(&self, object: &ActorStorageKey, durable_txid: u64) -> Result<bool> {
        let marker_path = self.cache_marker_path(object)?;
        let marker = match fs::read(&marker_path) {
            Ok(bytes) => match serde_json::from_slice::<CacheMarker>(&bytes) {
                Ok(marker) => marker,
                Err(error) => {
                    debug!(object = %object, error = %error, "ignoring invalid local cache marker");
                    return Ok(false);
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => {
                debug!(object = %object, error = %error, "could not read local cache marker");
                return Ok(false);
            }
        };
        if marker.format_version != CACHE_MARKER_FORMAT_VERSION
            || marker.durable_txid != durable_txid
        {
            return Ok(false);
        }

        let database_path = self.path_for(object)?;
        if !database_path.is_file() {
            return Ok(false);
        }
        let valid = Connection::open(&database_path)
            .and_then(|connection| {
                connection.query_row("PRAGMA quick_check(1)", [], |row| row.get::<_, String>(0))
            })
            .is_ok_and(|result| result == "ok");
        if !valid {
            debug!(object = %object, "local SQLite cache failed its integrity check");
        }
        Ok(valid)
    }

    /// Atomically records the canonical watermark represented by the local
    /// database. A crash before the rename yields a harmless cold restore.
    pub fn mark_cache_current(&self, object: &ActorStorageKey, durable_txid: u64) -> Result<()> {
        let object_dir = self.object_dir(object)?;
        fs::create_dir_all(&object_dir)?;
        let marker_path = self.cache_marker_path(object)?;
        let temporary_path = object_dir.join("cache-marker.tmp");
        let marker = serde_json::to_vec(&CacheMarker {
            format_version: CACHE_MARKER_FORMAT_VERSION,
            durable_txid,
        })?;
        let mut temporary = fs::File::create(&temporary_path)?;
        use std::io::Write as _;
        temporary.write_all(&marker)?;
        temporary.sync_all()?;
        fs::rename(&temporary_path, &marker_path)?;
        fs::File::open(&object_dir)?.sync_all()?;
        Ok(())
    }

    pub fn invalidate_cache(&self, object: &ActorStorageKey) -> Result<()> {
        let marker_path = self.cache_marker_path(object)?;
        match fs::remove_file(marker_path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).context("remove local actor cache marker"),
        }
    }

    /// Removes only the disposable provider-local copy. Canonical durability is
    /// owned by the durability store and is intentionally untouched.
    pub fn remove_cached_actor(&self, object: &ActorStorageKey) -> Result<()> {
        let object_dir = self.object_dir(object)?;
        match fs::remove_dir_all(object_dir) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).context("remove local actor cache"),
        }
    }

    fn object_dir(&self, object: &ActorStorageKey) -> Result<PathBuf> {
        object.validate()?;
        Ok(self.root.join("objects").join(object.as_str()))
    }

    fn cache_marker_path(&self, object: &ActorStorageKey) -> Result<PathBuf> {
        Ok(self.object_dir(object)?.join("cache-marker.json"))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::actor_state::ActorDatabaseTestExt;

    #[test]
    fn opens_database_under_the_object_directory() -> Result<()> {
        let root = TempDir::new()?;
        let storage = ActorDatabaseStore::new(root.path());
        let object = ActorStorageKey::new("object-a");

        let persistence = storage.open(&object)?;
        let expected_path = root
            .path()
            .join("objects")
            .join("object-a")
            .join("db.sqlite");

        assert_eq!(persistence.path(), Some(expected_path.as_path()));
        assert!(expected_path.is_file());

        Ok(())
    }

    #[test]
    fn reopening_an_actor_preserves_its_data() -> Result<()> {
        let root = TempDir::new()?;
        let storage = ActorDatabaseStore::new(root.path());
        let object = ActorStorageKey::new("object-a");

        storage.open(&object)?.set("greeting", "hello")?;

        let persistence = storage.open(&object)?;

        assert_eq!(
            persistence.get::<String>("greeting")?,
            Some("hello".to_owned())
        );

        Ok(())
    }

    #[test]
    fn actors_use_isolated_databases() -> Result<()> {
        let root = TempDir::new()?;
        let storage = ActorDatabaseStore::new(root.path());
        let object_a = ActorStorageKey::new("object-a");
        let object_b = ActorStorageKey::new("object-b");

        let persistence_a = storage.open(&object_a)?;
        let persistence_b = storage.open(&object_b)?;
        persistence_a.set("value", "from-a")?;
        persistence_b.set("value", "from-b")?;

        assert_eq!(
            persistence_a.get::<String>("value")?,
            Some("from-a".to_owned())
        );
        assert_eq!(
            persistence_b.get::<String>("value")?,
            Some("from-b".to_owned())
        );

        Ok(())
    }

    #[test]
    fn rejects_unsafe_storage_keys_that_are_not_safe_path_components() {
        let root = tempfile::tempdir().expect("temporary root");
        let storage = ActorDatabaseStore::new(root.path());

        for object in [
            ActorStorageKey::new("../other"),
            ActorStorageKey::new("nested/object"),
        ] {
            assert!(storage.path_for(&object).is_err());
            assert!(storage.open(&object).is_err());
        }

        assert!(!root.path().join("other").exists());
    }

    #[test]
    fn reuses_only_an_integrity_checked_cache_at_the_manifest_watermark() -> Result<()> {
        let root = TempDir::new()?;
        let storage = ActorDatabaseStore::new(root.path());
        let object = ActorStorageKey::new("object-a");
        storage.open(&object)?.set("value", &7_u64)?;

        storage.mark_cache_current(&object, 4)?;

        assert!(storage.cache_is_current(&object, 4)?);
        assert!(!storage.cache_is_current(&object, 3)?);
        storage.invalidate_cache(&object)?;
        assert!(!storage.cache_is_current(&object, 4)?);
        Ok(())
    }

    #[test]
    fn removing_a_cached_actor_does_not_require_it_to_exist() -> Result<()> {
        let root = TempDir::new()?;
        let storage = ActorDatabaseStore::new(root.path());
        let object = ActorStorageKey::new("object-a");
        storage.open(&object)?;

        storage.remove_cached_actor(&object)?;
        storage.remove_cached_actor(&object)?;

        assert!(!storage.path_for(&object)?.exists());
        Ok(())
    }
}
