//! End-to-end tests for the SQLite WAL capture path.
//!
//! Every test uses a temporary file-backed database: WAL mode is unavailable to the
//! ordinary in-memory database, so `Connection::open_in_memory` cannot exercise any of
//! this.

use crate::{
    actor_state::{ActorDatabaseTestExt, SqliteActorDatabase},
    ltx::{
        capture::{LtxSegment, SqliteLtxCapture, ltx_dir},
        error::{LtxError, Result},
        wal::WalError,
    },
};
use std::{
    fs,
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
use tempfile::TempDir;

struct Fixture {
    dir: TempDir,
}

impl Fixture {
    fn new() -> Result<Self> {
        Ok(Self {
            dir: TempDir::new()?,
        })
    }

    fn db_path(&self) -> PathBuf {
        self.dir.path().join("test.db")
    }

    /// Open the database and attach capture, exactly as a caller would.
    fn open(&self) -> Result<(SqliteActorDatabase, SqliteLtxCapture)> {
        let db = SqliteActorDatabase::connect(self.db_path())?;
        let capture = SqliteLtxCapture::attach(&db)?;

        Ok((db, capture))
    }

    /// Abandon a connection the way a killed process would.
    ///
    /// Closing a SQLite connection cleanly checkpoints the WAL into the database file,
    /// which is precisely what destroys transactions capture has not read yet. Skipping
    /// the destructor leaves the WAL as a crash would.
    fn abandon(&self, db: SqliteActorDatabase) {
        std::mem::forget(db);
    }

    fn create_users(&self, db: &SqliteActorDatabase) -> Result<()> {
        db.execute(
            "
            CREATE TABLE users (
                id   INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
            ",
        )?;

        Ok(())
    }

    fn insert_user(&self, db: &SqliteActorDatabase, name: &str) -> Result<()> {
        db.execute(&format!(
            "
            INSERT INTO users (name)
            VALUES ('{name}');
            "
        ))?;

        Ok(())
    }
}

#[test]
fn attach_enables_wal_mode() -> Result<()> {
    let fixture = Fixture::new()?;
    let (db, _capture) = fixture.open()?;

    let modes = db.query("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))?;

    assert_eq!(modes, vec!["wal".to_string()]);

    Ok(())
}

#[test]
fn attach_disables_autocheckpoint() -> Result<()> {
    let fixture = Fixture::new()?;
    let (db, _capture) = fixture.open()?;

    let thresholds = db.query("PRAGMA wal_autocheckpoint", [], |row| row.get::<_, i64>(0))?;

    assert_eq!(thresholds, vec![0]);

    Ok(())
}

#[test]
fn every_sqlite_connection_disables_autocheckpoint() -> Result<()> {
    let fixture = Fixture::new()?;
    let db = SqliteActorDatabase::connect(fixture.db_path())?;

    let thresholds = db.query("PRAGMA wal_autocheckpoint", [], |row| row.get::<_, i64>(0))?;

    assert_eq!(thresholds, vec![0]);

    Ok(())
}

#[test]
fn captured_wal_can_only_be_checkpointed_after_durable_publication() -> Result<()> {
    let fixture = Fixture::new()?;
    let (db, capture) = fixture.open()?;
    db.set("first", &"one")?;

    let segments = capture.sync()?;
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].max_txid, 1);
    assert!(matches!(
        capture.checkpoint_durable(&db, 0),
        Err(LtxError::DurabilityBehindCapture {
            durable_txid: 0,
            captured_txid: 1,
        })
    ));

    assert!(capture.checkpoint_durable(&db, 1)?);
    db.set("second", &"two")?;

    let segments = capture.sync()?;
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].min_txid, 2);
    assert_eq!(segments[0].max_txid, 2);

    Ok(())
}

#[test]
fn capture_reopens_after_a_durable_checkpoint_and_continues_the_chain() -> Result<()> {
    let fixture = Fixture::new()?;

    {
        let (db, capture) = fixture.open()?;
        db.set("first", &"one")?;
        assert_eq!(capture.sync()?[0].max_txid, 1);
        assert!(capture.checkpoint_durable(&db, 1)?);
    }

    let (db, capture) = fixture.open()?;
    db.set("second", &"two")?;
    let segments = capture.sync()?;

    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].min_txid, 2);
    assert_eq!(segments[0].max_txid, 2);

    Ok(())
}

#[test]
fn capture_survives_more_than_sqlites_default_wal_threshold() -> Result<()> {
    let fixture = Fixture::new()?;
    let (capture_db, capture) = fixture.open()?;
    let writer = SqliteActorDatabase::connect(fixture.db_path())?;

    writer.set_bytes("initial", b"establish the capture cursor")?;
    assert_eq!(capture.sync()?.len(), 1);

    // SQLite's default auto-checkpoint threshold is 1,000 pages. Crossing it and then
    // committing again used to let this writer recycle the WAL generation behind the
    // capture connection because the pragma had only been set on `capture_db`.
    writer.set_bytes("large", &vec![0x5a; 5 * 1024 * 1024])?;
    writer.set_bytes("after-large", b"still in the same captured WAL generation")?;

    let segments = capture.sync()?;
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0].min_txid, 2);
    assert_eq!(segments[1].max_txid, 3);

    // Keep the connection used by capture visibly alive through the assertion.
    assert_eq!(
        capture_db.query("PRAGMA wal_autocheckpoint", [], |row| row.get::<_, i64>(0))?,
        vec![0]
    );

    Ok(())
}

#[test]
fn attach_rejects_in_memory_databases() -> Result<()> {
    let db = SqliteActorDatabase::in_memory()?;

    assert!(SqliteLtxCapture::attach(&db).is_err());

    Ok(())
}

#[test]
fn a_transaction_produces_a_segment() -> Result<()> {
    let fixture = Fixture::new()?;
    let (db, capture) = fixture.open()?;

    assert!(capture.sync()?.is_empty());

    fixture.create_users(&db)?;

    let segments = capture.sync()?;

    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].min_txid, 1);
    assert_eq!(segments[0].max_txid, 1);
    assert!(!segments[0].bytes.is_empty());

    // Nothing new has committed, so a second sync is empty.
    assert!(capture.sync()?.is_empty());

    Ok(())
}

#[test]
fn the_first_segment_is_a_full_snapshot() -> Result<()> {
    let fixture = Fixture::new()?;
    let (db, capture) = fixture.open()?;

    fixture.create_users(&db)?;
    fixture.insert_user(&db, "Steven")?;

    let segments = capture.sync()?;

    assert!(segments[0].is_snapshot(), "txid 1 must stand alone");
    assert!(!segments[1].is_snapshot(), "later segments are change sets");

    // A snapshot carries every page of the database it commits.
    let pages = decode_pages(&segments[0])?;
    let expected: Vec<u32> = (1..=segments[0].commit).collect();

    assert_eq!(
        pages.iter().map(|(pgno, _)| *pgno).collect::<Vec<_>>(),
        expected
    );

    Ok(())
}

#[test]
fn uncommitted_transactions_are_not_emitted() -> Result<()> {
    let fixture = Fixture::new()?;
    let (db, capture) = fixture.open()?;

    fixture.create_users(&db)?;

    let committed = capture.sync()?;
    let captured_bytes = wal_len(&fixture.db_path())?;

    assert_eq!(committed.len(), 1);

    // A tiny page cache forces the rolled-back transaction to spill its pages into the
    // WAL, so there really are uncommitted frames sitting there afterwards.
    db.execute("PRAGMA cache_size = 1;")?;
    db.execute(
        "
        BEGIN;

        WITH RECURSIVE counter(n) AS (
            SELECT 1
            UNION ALL
            SELECT n + 1 FROM counter WHERE n < 5000
        )
        INSERT INTO users (name)
        SELECT 'user-' || n FROM counter;

        ROLLBACK;
        ",
    )?;

    // The rollback really did leave uncommitted frames behind, so the assertion below
    // is about the reader skipping them rather than about an empty WAL.
    assert!(
        wal_len(&fixture.db_path())? > captured_bytes,
        "expected the rolled-back transaction to spill frames into the wal"
    );

    assert!(
        capture.sync()?.is_empty(),
        "a rolled-back transaction has no commit frame and must not be captured"
    );

    // And the database really is unchanged.
    let counts = db.query("SELECT count(*) FROM users", [], |row| row.get::<_, i64>(0))?;

    assert_eq!(counts, vec![0]);

    Ok(())
}

#[test]
fn a_torn_final_frame_is_not_emitted() -> Result<()> {
    let fixture = Fixture::new()?;
    let (db, capture) = fixture.open()?;

    fixture.create_users(&db)?;
    fixture.insert_user(&db, "Steven")?;

    // Truncate the tail of the WAL, as a crash mid-write would.
    let wal = wal_path(&fixture.db_path());
    let len = fs::metadata(&wal)?.len();

    let file = fs::OpenOptions::new().write(true).open(&wal)?;
    file.set_len(len - 1)?;
    drop(file);

    let segments = capture.sync()?;

    assert_eq!(segments.len(), 1, "only the intact transaction is captured");

    Ok(())
}

#[test]
fn txids_advance_monotonically() -> Result<()> {
    let fixture = Fixture::new()?;
    let (db, capture) = fixture.open()?;

    fixture.create_users(&db)?;

    let mut txids = Vec::new();

    for name in ["Steven", "Olivier", "Ada", "Grace"] {
        fixture.insert_user(&db, name)?;

        for segment in capture.sync()? {
            txids.push(segment.max_txid);
        }
    }

    assert_eq!(txids, vec![1, 2, 3, 4, 5]);
    assert_eq!(capture.next_txid()?, 6);

    Ok(())
}

#[test]
fn segments_chain_by_checksum() -> Result<()> {
    let fixture = Fixture::new()?;
    let (db, capture) = fixture.open()?;

    fixture.create_users(&db)?;
    fixture.insert_user(&db, "Steven")?;
    fixture.insert_user(&db, "Olivier")?;

    let segments = capture.sync()?;

    // Each segment's pre-apply checksum must be the previous segment's post-apply
    // checksum, or a replica cannot verify that it applied them in order.
    for pair in segments.windows(2) {
        let (previous, next) = (&pair[0], &pair[1]);
        let (_, header) = litetx::Decoder::new(next.bytes.as_slice())?;

        assert_eq!(
            header.pre_apply_checksum.map(|c| c.into_inner()),
            Some(previous.post_apply_checksum),
        );
    }

    Ok(())
}

#[test]
fn applying_segments_reproduces_the_database() -> Result<()> {
    let fixture = Fixture::new()?;
    let (db, capture) = fixture.open()?;

    fixture.create_users(&db)?;

    for name in ["Steven", "Olivier", "Ada"] {
        fixture.insert_user(&db, name)?;
    }

    db.execute("DELETE FROM users WHERE name = 'Ada';")?;

    let segments = capture.sync()?;

    assert!(!segments.is_empty());

    let rebuilt = fixture.dir.path().join("rebuilt.db");

    for segment in &segments {
        apply(&rebuilt, segment)?;
    }

    // Fold the WAL back into the source database so the two files are comparable.
    db.query("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
        row.get::<_, i64>(0)
    })?;

    assert_eq!(
        fs::read(&rebuilt)?,
        fs::read(fixture.db_path())?,
        "replaying the captured LTX must reproduce the source database byte for byte"
    );

    // And the rebuilt file is a working database.
    let replica = SqliteActorDatabase::connect(&rebuilt)?;
    let names = replica.query("SELECT name FROM users ORDER BY id", [], |row| {
        row.get::<_, String>(0)
    })?;

    assert_eq!(names, vec!["Steven".to_string(), "Olivier".to_string()]);

    Ok(())
}

#[test]
fn captured_ltx_survives_reopening_the_persistence_layer() -> Result<()> {
    let fixture = Fixture::new()?;

    let captured = {
        let (db, capture) = fixture.open()?;

        fixture.create_users(&db)?;
        fixture.insert_user(&db, "Steven")?;

        capture.sync()?
    };

    assert_eq!(captured.len(), 2);

    // Reopen from scratch: nothing is carried over in memory.
    let (db, capture) = fixture.open()?;

    let stored = capture.stored_segments()?;

    assert_eq!(
        stored.iter().map(|s| s.max_txid).collect::<Vec<_>>(),
        vec![1, 2],
    );
    assert_eq!(stored[0].bytes, captured[0].bytes);
    assert_eq!(stored[1].bytes, captured[1].bytes);

    // Capture resumes rather than restarting, so TXIDs stay unique across restarts.
    assert_eq!(capture.next_txid()?, 3);

    fixture.insert_user(&db, "Olivier")?;

    let segments = capture.sync()?;

    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].max_txid, 3);
    assert!(!segments[0].is_snapshot());

    Ok(())
}

// ============================================================================
// Durability of capture state
// ============================================================================

#[test]
fn a_transaction_that_fails_to_persist_is_offered_again() -> Result<()> {
    let fixture = Fixture::new()?;
    let (db, capture) = fixture.open()?;

    fixture.create_users(&db)?;
    fixture.insert_user(&db, "Steven")?;

    // Make the segment directory unwritable so the durable write fails after the
    // transactions have already been read out of the WAL.
    let dir = ltx_dir(&fixture.db_path());
    set_writable(&dir, false)?;

    let failure = capture.sync();

    set_writable(&dir, true)?;

    assert!(failure.is_err(), "the durable write should have failed");
    assert_eq!(
        capture.next_txid()?,
        1,
        "capture state must not advance past a segment that was never written"
    );

    // Both transactions are still on offer: the cursor never moved past them.
    let segments = capture.sync()?;

    assert_eq!(
        segments.iter().map(|s| s.max_txid).collect::<Vec<_>>(),
        vec![1, 2],
    );
    assert!(segments[0].is_snapshot());

    Ok(())
}

#[test]
fn a_failed_generation_record_does_not_advance_the_cursor() -> Result<()> {
    let fixture = Fixture::new()?;
    let dir = ltx_dir(&fixture.db_path());

    {
        let (db, capture) = fixture.open()?;

        fixture.create_users(&db)?;

        // The generation record is the first durable write of a sync. Fail it.
        set_writable(&dir, false)?;

        let failure = capture.sync();

        set_writable(&dir, true)?;

        assert!(failure.is_err(), "the generation write should have failed");

        // The retry has to notice the record is still missing and write it, rather than
        // treating the position as already established and going straight to segments.
        assert_eq!(capture.sync()?.len(), 1);

        fixture.abandon(db);
    }

    assert!(
        dir.join("generation.json").exists(),
        "segments were cut without the metadata that correlates them to the wal"
    );

    // Which is what makes the segment recoverable on reopen at all.
    let (_db, capture) = fixture.open()?;

    assert_eq!(capture.next_txid()?, 2);

    Ok(())
}

#[test]
fn a_segment_that_fails_to_persist_is_offered_again() -> Result<()> {
    let fixture = Fixture::new()?;
    let (db, capture) = fixture.open()?;

    // Sync once so the generation is already recorded and the only thing left to fail
    // is the segment write itself.
    fixture.create_users(&db)?;

    assert_eq!(capture.sync()?.len(), 1);

    fixture.insert_user(&db, "Steven")?;

    let dir = ltx_dir(&fixture.db_path());

    set_writable(&dir, false)?;

    let failure = capture.sync();

    set_writable(&dir, true)?;

    assert!(failure.is_err());
    assert_eq!(capture.next_txid()?, 2);

    let segments = capture.sync()?;

    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].max_txid, 2);

    Ok(())
}

#[test]
fn a_truncated_wal_is_reported_rather_than_treated_as_idle() -> Result<()> {
    let fixture = Fixture::new()?;
    let (db, capture) = fixture.open()?;

    fixture.create_users(&db)?;

    assert_eq!(capture.sync()?.len(), 1);

    // Commits, and then an external checkpoint folds it into the database file and
    // truncates the WAL away. Reporting "nothing to do" would let capture step over the
    // transaction permanently.
    fixture.insert_user(&db, "Steven")?;

    db.query("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
        row.get::<_, i64>(0)
    })?;

    assert!(matches!(
        capture.sync(),
        Err(LtxError::Wal(WalError::GenerationChanged))
    ));

    // And the mismatch is what a re-attach then reports, rather than carrying on.
    assert!(matches!(
        SqliteLtxCapture::attach(&db),
        Err(LtxError::UncapturedWrites { txid: 1, .. })
    ));

    Ok(())
}

#[test]
fn the_position_is_derived_from_the_segment_files() -> Result<()> {
    let fixture = Fixture::new()?;

    {
        let (db, capture) = fixture.open()?;

        fixture.create_users(&db)?;
        fixture.insert_user(&db, "Steven")?;
        fixture.insert_user(&db, "Olivier")?;

        assert_eq!(capture.sync()?.len(), 3);

        fixture.abandon(db);
    }

    // Stands in for a crash after a segment was published but before anything recorded
    // it: drop the newest segment and confirm the position follows the files rather
    // than any separate bookkeeping.
    let dir = ltx_dir(&fixture.db_path());
    fs::remove_file(dir.join("0000000000000003-0000000000000003.ltx"))?;

    let (_db, capture) = fixture.open()?;

    assert_eq!(capture.next_txid()?, 3);

    // And because the WAL still holds it, the dropped transaction is simply captured
    // again rather than lost.
    let segments = capture.sync()?;

    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].max_txid, 3);

    Ok(())
}

#[test]
fn a_gap_in_the_segment_chain_is_reported() -> Result<()> {
    let fixture = Fixture::new()?;

    {
        let (db, capture) = fixture.open()?;

        fixture.create_users(&db)?;
        fixture.insert_user(&db, "Steven")?;
        fixture.insert_user(&db, "Olivier")?;

        capture.sync()?;
    }

    let dir = ltx_dir(&fixture.db_path());
    fs::remove_file(dir.join("0000000000000002-0000000000000002.ltx"))?;

    // Reusing txid 2 would collide with the immutable segment 3 already sitting there,
    // so this has to stop rather than guess.
    let db = SqliteActorDatabase::connect(fixture.db_path())?;

    assert!(matches!(
        SqliteLtxCapture::attach(&db),
        Err(LtxError::SegmentGap {
            expected: 2,
            found: 3
        })
    ));

    Ok(())
}

// ============================================================================
// Restarting
// ============================================================================

#[test]
fn reopening_captures_writes_that_were_never_synchronized() -> Result<()> {
    let fixture = Fixture::new()?;

    {
        let (db, capture) = fixture.open()?;

        fixture.create_users(&db)?;
        fixture.insert_user(&db, "Steven")?;

        assert_eq!(capture.sync()?.len(), 2);

        // Committed, and then the process is killed without a final sync.
        fixture.insert_user(&db, "Olivier")?;

        fixture.abandon(db);
    }

    let (_db, capture) = fixture.open()?;

    // The transaction is still in the WAL, so reopening must pick it up rather than
    // checkpointing it away.
    assert_eq!(capture.next_txid()?, 3);

    let segments = capture.sync()?;

    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].max_txid, 3);
    assert!(!segments[0].is_snapshot());

    // The chain is unbroken, so a replica can still apply it.
    let stored = capture.stored_segments()?;

    assert_eq!(
        stored.iter().map(|s| s.max_txid).collect::<Vec<_>>(),
        vec![1, 2, 3],
    );

    for pair in stored.windows(2) {
        let (_, header) = litetx::Decoder::new(pair[1].bytes.as_slice())?;

        assert_eq!(
            header.pre_apply_checksum.map(|c| c.into_inner()),
            Some(pair[0].post_apply_checksum),
        );
    }

    Ok(())
}

#[test]
fn closing_cleanly_without_syncing_is_reported_on_reopen() -> Result<()> {
    let fixture = Fixture::new()?;

    {
        let (db, capture) = fixture.open()?;

        fixture.create_users(&db)?;

        assert_eq!(capture.sync()?.len(), 1);

        // Closing the connection cleanly checkpoints this into the database file, so it
        // can never become a segment. There is no snapshot to fall back on either: LTX
        // only permits one at TXID 1, which is already spoken for.
        fixture.insert_user(&db, "Steven")?;
    }

    let db = SqliteActorDatabase::connect(fixture.db_path())?;

    assert!(matches!(
        SqliteLtxCapture::attach(&db),
        Err(LtxError::UncapturedWrites { txid: 1, .. })
    ));

    Ok(())
}

#[test]
fn reopening_reports_writes_that_were_checkpointed_away() -> Result<()> {
    let fixture = Fixture::new()?;

    {
        let (db, capture) = fixture.open()?;

        fixture.create_users(&db)?;

        assert_eq!(capture.sync()?.len(), 1);

        // Commits without capture seeing it, and then the WAL is checkpointed and
        // restarted out from under us — so the change survives only in the database
        // file and nothing can reconstruct it as a segment.
        fixture.insert_user(&db, "Steven")?;

        db.query("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            row.get::<_, i64>(0)
        })?;
    }

    let db = SqliteActorDatabase::connect(fixture.db_path())?;

    // Carrying on would emit a segment whose pre-apply checksum no replica could match,
    // silently forking the replica from the primary. Better to refuse.
    assert!(matches!(
        SqliteLtxCapture::attach(&db),
        Err(LtxError::UncapturedWrites { txid: 1, .. })
    ));

    Ok(())
}

#[test]
fn a_clean_reopen_of_an_uncaptured_database_starts_from_a_snapshot() -> Result<()> {
    let fixture = Fixture::new()?;

    {
        let (db, _capture) = fixture.open()?;

        fixture.create_users(&db)?;
        fixture.insert_user(&db, "Steven")?;
    }

    // Nothing was ever captured, so there is no chain to contradict: the database file
    // becomes the base and the first segment is a snapshot of everything so far.
    let (db, capture) = fixture.open()?;

    fixture.insert_user(&db, "Olivier")?;

    let segments = capture.sync()?;

    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].max_txid, 1);
    assert!(segments[0].is_snapshot());

    // That snapshot really does contain the rows written before capture resumed.
    let rebuilt = fixture.dir.path().join("rebuilt.db");

    apply(&rebuilt, &segments[0])?;

    let replica = SqliteActorDatabase::connect(&rebuilt)?;
    let names = replica.query("SELECT name FROM users ORDER BY id", [], |row| {
        row.get::<_, String>(0)
    })?;

    assert_eq!(names, vec!["Steven".to_string(), "Olivier".to_string()]);

    Ok(())
}

// ============================================================================
// Helpers
// ============================================================================

/// Toggle write access to a directory, to make durable writes fail on demand.
fn set_writable(dir: &Path, writable: bool) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mode = if writable { 0o700 } else { 0o500 };

    fs::set_permissions(dir, fs::Permissions::from_mode(mode))?;

    Ok(())
}

fn wal_len(db_path: &Path) -> Result<u64> {
    Ok(fs::metadata(wal_path(db_path))?.len())
}

fn wal_path(db_path: &Path) -> PathBuf {
    let mut name = db_path.as_os_str().to_os_string();
    name.push("-wal");

    PathBuf::from(name)
}

/// Decode a segment into its `(page number, page)` pairs.
fn decode_pages(segment: &LtxSegment) -> Result<Vec<(u32, Vec<u8>)>> {
    let (mut decoder, header) = litetx::Decoder::new(segment.bytes.as_slice())?;

    let mut pages = Vec::new();
    let mut page = vec![0u8; header.page_size.into_inner() as usize];

    while let Some(pgno) = decoder.decode_page(&mut page)? {
        pages.push((pgno.into_inner(), page.clone()));
    }

    decoder.finish()?;

    Ok(pages)
}

/// Apply a segment to a database file the way a replica would.
fn apply(db_path: &Path, segment: &LtxSegment) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(db_path)?;

    for (pgno, page) in decode_pages(segment)? {
        let offset = (u64::from(pgno) - 1) * u64::from(segment.page_size);

        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&page)?;
    }

    file.set_len(u64::from(segment.commit) * u64::from(segment.page_size))?;
    file.sync_all()?;

    Ok(())
}
