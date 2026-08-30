use rusqlite::{Connection, OptionalExtension, Params, Row, params};
use std::{
    fmt,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

#[cfg(test)]
pub(crate) trait ActorDatabaseTestExt {
    fn get<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned;

    fn set<T>(&self, key: &str, value: &T) -> Result<()>
    where
        T: serde::Serialize + ?Sized;
}

pub struct SqliteActorDatabase {
    path: Option<PathBuf>,
    conn: Mutex<Connection>,
}

impl SqliteActorDatabase {
    pub fn connect<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let conn = Connection::open(path)?;

        Self::from_connection(conn, Some(path.to_path_buf()))
    }

    #[cfg(test)]
    pub(crate) fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;

        Self::from_connection(conn, None)
    }

    /// The on-disk location of the database, or `None` for in-memory databases.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub fn query<T, P, F>(&self, sql: &str, params: P, mut map: F) -> Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(sql)?;

        let rows = stmt.query_map(params, |row| map(row))?;

        let values = rows.collect::<rusqlite::Result<Vec<T>>>()?;

        Ok(values)
    }

    pub fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.read_value(key)
    }

    pub fn set_bytes_batch(&self, values: &[(&str, &[u8])]) -> Result<()> {
        self.write_values(values)
    }

    #[cfg(test)]
    pub(crate) fn set_bytes(&self, key: &str, value: &[u8]) -> Result<()> {
        self.write_values(&[(key, value)])
    }

    #[cfg(test)]
    pub(crate) fn execute(&self, sql: &str) -> Result<()> {
        let conn = self.connection()?;
        conn.execute_batch(sql)?;
        Ok(())
    }

    fn read_value(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let conn = self.connection()?;

        let value = conn
            .query_row(
                "
                SELECT value
                FROM __objects
                WHERE key = ?1
                ",
                params![key],
                |row| row.get(0),
            )
            .optional()?;

        Ok(value)
    }

    fn write_values(&self, values: &[(&str, &[u8])]) -> Result<()> {
        let mut conn = self.connection()?;
        let transaction = conn.transaction()?;
        for (key, value) in values {
            transaction.execute(
                "
                INSERT INTO __objects (key, value)
                VALUES (?1, ?2)
                ON CONFLICT(key)
                DO UPDATE SET value = excluded.value
                ",
                params![key, value],
            )?;
        }
        transaction.commit()?;

        Ok(())
    }
}

#[cfg(test)]
impl ActorDatabaseTestExt for SqliteActorDatabase {
    fn get<T>(&self, key: &str) -> Result<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        self.get_bytes(key)?
            .map(|bytes| serde_json::from_slice(&bytes))
            .transpose()
            .map_err(Into::into)
    }

    fn set<T>(&self, key: &str, value: &T) -> Result<()>
    where
        T: serde::Serialize + ?Sized,
    {
        let bytes = serde_json::to_vec(value)?;
        self.set_bytes_batch(&[(key, &bytes)])?;
        Ok(())
    }
}

// SQLite setup

impl SqliteActorDatabase {
    fn from_connection(conn: Connection, path: Option<PathBuf>) -> Result<Self> {
        let object_database = Self {
            path,
            conn: Mutex::new(conn),
        };

        object_database.initialize()?;

        Ok(object_database)
    }

    fn initialize(&self) -> Result<()> {
        let conn = self.connection()?;

        conn.execute_batch(
            "
            PRAGMA foreign_keys = ON;
            -- WAL capture owns checkpoint scheduling. This pragma is connection-local,
            -- so every connection that may write an actor database must disable
            -- SQLite's automatic checkpointing, not only the connection used by the
            -- capture reader.
            PRAGMA wal_autocheckpoint = 0;

            CREATE TABLE IF NOT EXISTS __objects (
                key   TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
            ",
        )?;

        Ok(())
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| ActorDatabaseError::LockPoisoned)
    }
}

// Errors

pub type Result<T> = std::result::Result<T, ActorDatabaseError>;

#[derive(Debug)]
pub enum ActorDatabaseError {
    Sqlite(rusqlite::Error),
    #[cfg(test)]
    Serialization(serde_json::Error),
    LockPoisoned,
}

impl fmt::Display for ActorDatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(err) => {
                write!(f, "sqlite error: {err}")
            }
            #[cfg(test)]
            Self::Serialization(err) => {
                write!(f, "serialization error: {err}")
            }
            Self::LockPoisoned => {
                write!(f, "actor database lock poisoned")
            }
        }
    }
}

impl std::error::Error for ActorDatabaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sqlite(err) => Some(err),
            #[cfg(test)]
            Self::Serialization(err) => Some(err),
            Self::LockPoisoned => None,
        }
    }
}

impl From<rusqlite::Error> for ActorDatabaseError {
    fn from(err: rusqlite::Error) -> Self {
        Self::Sqlite(err)
    }
}

#[cfg(test)]
impl From<serde_json::Error> for ActorDatabaseError {
    fn from(err: serde_json::Error) -> Self {
        Self::Serialization(err)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Person {
        id: i32,
        name: String,
        data: Option<Vec<u8>>,
    }

    #[test]
    fn connects_to_in_memory_database() -> Result<()> {
        let _db = SqliteActorDatabase::in_memory()?;

        Ok(())
    }

    #[test]
    fn executes_raw_sql() -> Result<()> {
        let db = SqliteActorDatabase::in_memory()?;

        db.execute(
            "
            CREATE TABLE person (
                id   INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                data BLOB
            );
            ",
        )?;

        Ok(())
    }

    #[test]
    fn queries_rows() -> Result<()> {
        let db = SqliteActorDatabase::in_memory()?;

        db.execute(
            "
            CREATE TABLE person (
                id   INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                data BLOB
            );

            INSERT INTO person (name)
            VALUES ('Steven');
            ",
        )?;

        let people = db.query(
            "
            SELECT id, name, data
            FROM person
            ",
            [],
            |row| {
                Ok(Person {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    data: row.get(2)?,
                })
            },
        )?;

        assert_eq!(
            people,
            vec![Person {
                id: 1,
                name: "Steven".to_string(),
                data: None,
            }]
        );

        Ok(())
    }

    #[test]
    fn sets_and_gets_structs() -> anyhow::Result<()> {
        let db = SqliteActorDatabase::in_memory()?;

        let person = Person {
            id: 123,
            name: "Steven".to_string(),
            data: None,
        };

        db.set("person:123", &person)?;

        let loaded: Option<Person> = db.get("person:123")?;

        assert_eq!(loaded, Some(person));

        Ok(())
    }

    #[test]
    fn overwrites_existing_value() -> anyhow::Result<()> {
        let db = SqliteActorDatabase::in_memory()?;

        let original = Person {
            id: 123,
            name: "Steven".to_string(),
            data: None,
        };

        let updated = Person {
            id: 123,
            name: "Olivier".to_string(),
            data: Some(vec![1, 2, 3]),
        };

        db.set("person:123", &original)?;
        db.set("person:123", &updated)?;

        let loaded: Option<Person> = db.get("person:123")?;

        assert_eq!(loaded, Some(updated));

        Ok(())
    }

    #[test]
    fn batch_writes_are_atomic() -> Result<()> {
        let db = SqliteActorDatabase::in_memory()?;
        db.set_bytes_batch(&[("first", b"before")])?;
        db.execute(
            "
            CREATE TRIGGER reject_second
            BEFORE INSERT ON __objects
            WHEN NEW.key = 'second'
            BEGIN
                SELECT RAISE(ABORT, 'reject second write');
            END;
            ",
        )?;

        let error = db
            .set_bytes_batch(&[("first", b"after"), ("second", b"value")])
            .expect_err("the second write should abort the transaction");

        assert!(error.to_string().contains("reject second write"));
        assert_eq!(db.get_bytes("first")?, Some(b"before".to_vec()));
        assert_eq!(db.get_bytes("second")?, None);
        Ok(())
    }

    #[test]
    fn returns_none_for_missing_key() -> anyhow::Result<()> {
        let db = SqliteActorDatabase::in_memory()?;

        let person: Option<Person> = db.get("missing")?;

        assert_eq!(person, None);

        Ok(())
    }
}
