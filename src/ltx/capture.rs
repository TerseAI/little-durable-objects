use crate::{
    actor_state::SqliteActorDatabase,
    ltx::{
        error::{LtxError, Result},
        wal::{self, WalCursor, WalTransaction},
    },
};
use litetx::{Checksum, Encoder, Header, HeaderFlags, PageChecksum, PageNum, PageSize, TXID};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt, fs,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::SystemTime,
};

// ============================================================================
// Segments
// ============================================================================

/// One durable LTX file: the page changes of a single committed transaction.
#[derive(Clone, Serialize, Deserialize)]
pub struct LtxSegment {
    /// The lowest transaction ID covered by the segment.
    pub min_txid: u64,
    /// The highest transaction ID covered by the segment, and its replication position.
    pub max_txid: u64,
    /// The database page size the segment was encoded with.
    pub page_size: u32,
    /// The size of the database in pages once the segment is applied.
    pub commit: u32,
    /// The running database checksum after the segment is applied.
    pub post_apply_checksum: u64,
    /// The running database checksum required before this segment is applied. `None`
    /// identifies a full snapshot.
    pub pre_apply_checksum: Option<u64>,
    /// The encoded LTX file.
    pub bytes: Vec<u8>,
}

impl LtxSegment {
    pub fn is_snapshot(&self) -> bool {
        self.pre_apply_checksum.is_none()
    }

    /// The canonical `<min>-<max>.ltx` file name for a segment.
    fn file_name(min_txid: u64, max_txid: u64) -> String {
        format!("{min_txid:016x}-{max_txid:016x}.ltx")
    }

    /// Recover the transaction range a segment file name encodes.
    fn parse_file_name(name: &str) -> Option<(u64, u64)> {
        let (min, max) = name.strip_suffix(".ltx")?.split_once('-')?;

        Some((
            u64::from_str_radix(min, 16).ok()?,
            u64::from_str_radix(max, 16).ok()?,
        ))
    }

    /// Decode a persisted LTX file and recover the metadata carried in its header and
    /// trailer.
    pub(crate) fn decode(bytes: Vec<u8>) -> Result<Self> {
        let (mut decoder, header) = litetx::Decoder::new(bytes.as_slice())?;
        let mut page = vec![0u8; header.page_size.into_inner() as usize];

        while decoder.decode_page(&mut page)?.is_some() {}

        let trailer = decoder.finish()?;

        Ok(Self {
            min_txid: header.min_txid.into_inner(),
            max_txid: header.max_txid.into_inner(),
            page_size: header.page_size.into_inner(),
            commit: header.commit.into_inner(),
            post_apply_checksum: trailer.post_apply_checksum.into_inner(),
            pre_apply_checksum: header.pre_apply_checksum.map(|value| value.into_inner()),
            bytes,
        })
    }
}

impl fmt::Debug for LtxSegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LtxSegment")
            .field("min_txid", &self.min_txid)
            .field("max_txid", &self.max_txid)
            .field("commit", &self.commit)
            .field("pre_apply_checksum", &self.pre_apply_checksum)
            .field("snapshot", &self.is_snapshot())
            .field("bytes", &format_args!("[{} bytes]", self.bytes.len()))
            .finish()
    }
}

// ============================================================================
// Capture
// ============================================================================

/// Reads committed WAL frames out of a file-backed SQLite database and encodes them as
/// durable LTX segments.
pub struct SqliteLtxCapture {
    db_path: PathBuf,
    wal_path: PathBuf,
    dir: PathBuf,
    page_size: PageSize,
    state: Mutex<CaptureState>,
}

struct CaptureState {
    /// The transaction ID the next captured transaction will be assigned.
    next_txid: u64,
    /// How far into the WAL we have durably captured.
    cursor: WalCursor,
    /// Per-page checksums for the whole database, keyed by page number.
    checksums: BTreeMap<u32, Checksum>,
    /// The running database checksum: the XOR of every entry in `checksums`.
    running: Checksum,
}

impl SqliteLtxCapture {
    /// Put `db` into WAL mode and begin capturing.
    ///
    /// Segments live in a sibling `<db>-ltx` directory. If segments are already there,
    /// capture resumes: the WAL position is re-derived by replaying the frames those
    /// segments account for, so transactions that committed after the last
    /// [`sync`](Self::sync) — including across a restart — are still captured rather
    /// than lost.
    ///
    /// Fails with [`LtxError::UncapturedWrites`] if the database has moved on from what
    /// the local segments describe and the WAL no longer holds the difference. That is
    /// unrecoverable from local state, so it is reported rather than papered over.
    pub fn attach(db: &SqliteActorDatabase) -> Result<Self> {
        let db_path = db.path().ok_or(LtxError::NotFileBacked)?.to_path_buf();

        Self::enable_wal(db)?;

        let page_size = Self::configured_page_size(db)?;
        let dir = ltx_dir(&db_path);

        fs::create_dir_all(&dir)?;

        let chain = SegmentChain::scan(&dir)?;

        // With nothing captured yet there is nothing a checkpoint can lose, so fold the
        // WAL in and take the database file as a clean base. On resume this would be
        // destructive: it would bury transactions that never became segments.
        if chain.next_txid == TXID::ONE.into_inner() {
            db.query("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                row.get::<_, i64>(0)
            })?;
        }

        let capture = Self {
            wal_path: wal_path(&db_path),
            db_path,
            dir,
            page_size,
            state: Mutex::new(CaptureState {
                next_txid: chain.next_txid,
                cursor: WalCursor::default(),
                checksums: BTreeMap::new(),
                running: Checksum::new(0),
            }),
        };

        capture.restore(&chain)?;

        Ok(capture)
    }

    /// Rebuild in-memory state from the durable artifacts: the database file, the
    /// segment chain, and the WAL.
    fn restore(&self, chain: &SegmentChain) -> Result<()> {
        let (checksums, running) = page_checksums(&self.db_path, self.page_size)?;

        let mut state = self.lock()?;

        state.checksums = checksums;
        state.running = running;

        // Reading with a fresh cursor positions us at the start of the current WAL
        // generation and reports everything committed in it.
        let mut cursor = WalCursor::default();
        let transactions = wal::read_committed(&self.wal_path, &mut cursor)?;

        state.cursor = cursor;

        if !cursor.is_initialized() {
            // No WAL yet, so the database file is the whole story.
            return self.verify(&state, chain);
        }

        // How many of this generation's transactions the segment chain already accounts
        // for. Anything beyond that is outstanding and will be captured by `sync`.
        let generation = Generation::load(&self.dir)?;

        let captured = match generation {
            Some(generation) if generation.salt() == cursor.salt => {
                state.next_txid.saturating_sub(generation.first_txid)
            }
            _ => {
                Generation::new(cursor.salt, state.next_txid).store(&self.dir)?;

                0
            }
        };

        let captured = usize::try_from(captured).unwrap_or(usize::MAX);

        if captured > transactions.len() {
            return Err(LtxError::ChainAheadOfWal {
                captured,
                available: transactions.len(),
            });
        }

        // Replay what the chain already covers so the checksums and cursor describe the
        // state the last segment left behind.
        for transaction in transactions.iter().take(captured) {
            let pages = committed_pages(transaction);
            let advance =
                checksum_advance(&state.checksums, state.running, &pages, transaction.commit);

            state.apply(advance);
            state.cursor = transaction.cursor_after;
        }

        self.verify(&state, chain)
    }

    /// Confirm the reconstructed database state is exactly what the last segment claims.
    ///
    /// A mismatch means the database moved on without capture seeing it — typically a
    /// process that exited without a final `sync` and then had its WAL checkpointed
    /// away. Continuing would emit a segment whose pre-apply checksum no replica can
    /// match, so this stops at attach instead.
    fn verify(&self, state: &CaptureState, chain: &SegmentChain) -> Result<()> {
        let Some(expected) = chain.last_checksum else {
            return Ok(());
        };

        if state.running != expected {
            return Err(LtxError::UncapturedWrites {
                expected: expected.into_inner(),
                found: state.running.into_inner(),
                txid: state.next_txid.saturating_sub(1),
            });
        }

        Ok(())
    }

    /// Capture every transaction committed to the WAL since the last call.
    ///
    /// Each returned segment is durable before it is returned, and capture state only
    /// advances behind it — so a failure part-way through leaves the remaining
    /// transactions to be picked up by the next call rather than dropping them.
    pub fn sync(&self) -> Result<Vec<LtxSegment>> {
        let mut state = self.lock()?;

        let mut cursor = state.cursor;
        let transactions = wal::read_committed(&self.wal_path, &mut cursor)?;

        // A first-ever read positions the cursor; record which generation the segments
        // that follow belong to, before any of them exists. The position is adopted only
        // once that record is durable — otherwise a failed write here would leave the
        // cursor looking established, so the next call would skip the record entirely and
        // go on to cut segments nothing could later correlate with their WAL frames.
        if cursor != state.cursor {
            Generation::new(cursor.salt, state.next_txid).store(&self.dir)?;

            state.cursor = cursor;
        }

        let mut segments = Vec::with_capacity(transactions.len());

        for transaction in &transactions {
            let (segment, advance) = self.encode(&state, transaction)?;

            // The single durable write. Everything after this point is bookkeeping that
            // a crash can safely lose, because attach re-derives it.
            let path = self
                .dir
                .join(LtxSegment::file_name(segment.min_txid, segment.max_txid));
            write_immutable(&path, &segment.bytes)?;

            state.commit(advance);
            segments.push(segment);
        }

        Ok(segments)
    }

    /// Fold the WAL into the SQLite database and begin a fresh WAL generation after
    /// every locally captured transaction is known to be remotely durable.
    ///
    /// The durability check is deliberately inside capture: a caller cannot recycle
    /// WAL frames merely because it believes publication succeeded. If SQLite cannot
    /// obtain the checkpoint lock, the current generation and cursor remain valid and
    /// the caller can retry after a later publication.
    pub fn checkpoint_durable(&self, db: &SqliteActorDatabase, durable_txid: u64) -> Result<bool> {
        let mut state = self.lock()?;
        let captured_txid = state.next_txid.saturating_sub(1);

        if durable_txid < captured_txid {
            return Err(LtxError::DurabilityBehindCapture {
                durable_txid,
                captured_txid,
            });
        }

        if !state.cursor.is_initialized() {
            return Ok(false);
        }

        let result = db.query("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        let Some((busy, _log_frames, _checkpointed_frames)) = result.first().copied() else {
            return Ok(false);
        };

        if busy != 0 {
            return Ok(false);
        }

        // A successful TRUNCATE invalidates the old byte offset and salt. The checksum
        // and TXID state remain current, so the next sync can start at the first frame
        // of SQLite's next WAL generation without restarting the LTX chain.
        state.cursor = WalCursor::default();

        Ok(true)
    }

    /// The transaction ID the next captured transaction will receive.
    #[cfg(test)]
    pub(crate) fn next_txid(&self) -> Result<u64> {
        Ok(self.lock()?.next_txid)
    }

    /// Every segment durably stored locally, in transaction order.
    ///
    /// Read back from disk rather than from memory, so this reflects what survives a
    /// process restart.
    #[cfg(test)]
    pub(crate) fn stored_segments(&self) -> Result<Vec<LtxSegment>> {
        let mut names: Vec<(u64, u64, PathBuf)> = Vec::new();

        for entry in fs::read_dir(&self.dir)? {
            let path = entry?.path();

            if path.extension().and_then(|ext| ext.to_str()) != Some("ltx") {
                continue;
            }

            let name = path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned();
            let (min, max) = LtxSegment::parse_file_name(&name)
                .ok_or_else(|| LtxError::BadSegmentName(name.clone()))?;

            names.push((min, max, path));
        }

        names.sort();

        names
            .into_iter()
            .map(|(_, _, path)| read_segment(&path))
            .collect()
    }
}

// ============================================================================
// Encoding
// ============================================================================

/// The state change a captured transaction implies, held apart from the state itself so
/// it can be applied only once the segment is durable.
struct StateAdvance {
    cursor: WalCursor,
    next_txid: u64,
    checksums: ChecksumAdvance,
}

impl CaptureState {
    /// Adopt a captured transaction's state change, once its segment is durable.
    fn commit(&mut self, advance: StateAdvance) {
        self.apply(advance.checksums);

        self.cursor = advance.cursor;
        self.next_txid = advance.next_txid;
    }

    fn apply(&mut self, advance: ChecksumAdvance) {
        for (pgno, checksum) in advance.updates {
            match checksum {
                Some(checksum) => self.checksums.insert(pgno, checksum),
                None => self.checksums.remove(&pgno),
            };
        }

        self.running = advance.running;
    }
}

impl SqliteLtxCapture {
    /// Encode one committed WAL transaction as an LTX segment.
    ///
    /// Pure with respect to capture state: it reads, but never mutates. The caller
    /// applies the returned [`StateAdvance`] once the segment is on disk.
    fn encode(
        &self,
        state: &CaptureState,
        transaction: &WalTransaction,
    ) -> Result<(LtxSegment, StateAdvance)> {
        if transaction.page_size != self.page_size.into_inner() {
            return Err(LtxError::PageSizeChanged {
                expected: self.page_size.into_inner(),
                found: transaction.page_size,
            });
        }

        let txid = TXID::new(state.next_txid).map_err(|_| LtxError::ZeroTxid)?;
        let commit = PageNum::new(transaction.commit).map_err(|_| LtxError::EmptyCommit {
            txid: txid.into_inner(),
        })?;

        let mut pages = committed_pages(transaction);

        // The very first segment has to be a full snapshot, since a replica has no
        // prior state to apply a change set on top of. Pages the transaction did not
        // touch still hold their old contents, which we read from the database file.
        let snapshot = txid == TXID::ONE;

        if snapshot {
            self.fill_snapshot(&mut pages, commit)?;
        }

        let pre_apply_checksum = (!snapshot).then_some(state.running);
        let checksums =
            checksum_advance(&state.checksums, state.running, &pages, commit.into_inner());
        let post_apply_checksum = checksums.running;

        let mut bytes = Vec::new();
        let mut encoder = Encoder::new(
            &mut bytes,
            &Header {
                flags: HeaderFlags::empty(),
                page_size: self.page_size,
                commit,
                min_txid: txid,
                max_txid: txid,
                timestamp: SystemTime::now(),
                pre_apply_checksum,
            },
        )?;

        for (pgno, data) in &pages {
            encoder.encode_page(
                PageNum::new(*pgno).map_err(|_| LtxError::ZeroPageNumber)?,
                data,
            )?;
        }

        encoder.finish(post_apply_checksum)?;

        let segment = LtxSegment {
            min_txid: txid.into_inner(),
            max_txid: txid.into_inner(),
            page_size: self.page_size.into_inner(),
            commit: commit.into_inner(),
            post_apply_checksum: post_apply_checksum.into_inner(),
            pre_apply_checksum: pre_apply_checksum.map(|value| value.into_inner()),
            bytes,
        };

        let advance = StateAdvance {
            cursor: transaction.cursor_after,
            next_txid: state.next_txid + 1,
            checksums,
        };

        Ok((segment, advance))
    }

    /// Top up a change set with the untouched pages needed to make it a full snapshot.
    fn fill_snapshot(&self, pages: &mut BTreeMap<u32, Vec<u8>>, commit: PageNum) -> Result<()> {
        let mut file = fs::File::open(&self.db_path)?;
        let lock_page = PageNum::lock_page(self.page_size).into_inner();

        for pgno in 1..=commit.into_inner() {
            if pgno == lock_page || pages.contains_key(&pgno) {
                continue;
            }

            let data = read_page(&mut file, self.page_size, pgno)?
                .ok_or(LtxError::MissingSnapshotPage { pgno })?;

            pages.insert(pgno, data);
        }

        Ok(())
    }

    fn lock(&self) -> Result<MutexGuard<'_, CaptureState>> {
        self.state.lock().map_err(|_| LtxError::LockPoisoned)
    }
}

/// The final page images a transaction commits.
///
/// A page may be written more than once within a transaction; only the last image is
/// part of the committed state. Anything past the commit boundary is not part of the
/// database at all.
fn committed_pages(transaction: &WalTransaction) -> BTreeMap<u32, Vec<u8>> {
    let mut pages = BTreeMap::new();

    for frame in &transaction.frames {
        if frame.pgno <= transaction.commit {
            pages.insert(frame.pgno, frame.data.clone());
        }
    }

    pages
}

// ============================================================================
// Checksums
// ============================================================================

/// A pending change to the database checksum state.
struct ChecksumAdvance {
    running: Checksum,
    /// Page checksums to set, or to remove when `None`.
    updates: Vec<(u32, Option<Checksum>)>,
}

/// Work out how a transaction's pages move the running database checksum.
///
/// The database checksum is the XOR of every live page's checksum, so a page that
/// changes is XOR'd out at its old value and back in at its new one, and a page dropped
/// by a shrinking commit is simply XOR'd out. Computed without mutating anything, so the
/// result can be discarded if the segment fails to persist.
fn checksum_advance(
    checksums: &BTreeMap<u32, Checksum>,
    mut running: Checksum,
    pages: &BTreeMap<u32, Vec<u8>>,
    commit: u32,
) -> ChecksumAdvance {
    let mut updates = Vec::with_capacity(pages.len());

    for (pgno, data) in pages {
        let Ok(page_num) = PageNum::new(*pgno) else {
            continue;
        };

        let checksum = data.page_checksum(page_num);

        if let Some(previous) = checksums.get(pgno) {
            running = running ^ *previous;
        }

        running = running ^ checksum;
        updates.push((*pgno, Some(checksum)));
    }

    for (pgno, previous) in checksums.range(commit.saturating_add(1)..) {
        running = running ^ *previous;
        updates.push((*pgno, None));
    }

    ChecksumAdvance { running, updates }
}

// ============================================================================
// SQLite configuration
// ============================================================================

impl SqliteLtxCapture {
    fn enable_wal(db: &SqliteActorDatabase) -> Result<()> {
        let modes = db.query("PRAGMA journal_mode = WAL", [], |row| {
            row.get::<_, String>(0)
        })?;

        let mode = modes
            .first()
            .map(|mode| mode.to_ascii_lowercase())
            .unwrap_or_default();

        if mode != "wal" {
            return Err(LtxError::WalUnavailable(mode));
        }

        // Capture reads the WAL directly, so SQLite must not checkpoint and recycle it
        // behind our back. Checkpointing becomes the caller's decision, coordinated
        // with the replication position.
        db.query("PRAGMA wal_autocheckpoint = 0", [], |row| {
            row.get::<_, i64>(0)
        })?;

        Ok(())
    }

    fn configured_page_size(db: &SqliteActorDatabase) -> Result<PageSize> {
        let sizes = db.query("PRAGMA page_size", [], |row| row.get::<_, i64>(0))?;
        let size = u32::try_from(sizes.first().copied().unwrap_or(0)).unwrap_or(0);

        PageSize::new(size).map_err(|_| LtxError::UnsupportedPageSize(size))
    }
}

// ============================================================================
// The segment chain
// ============================================================================

/// What the durable segment files say about where capture left off.
struct SegmentChain {
    /// One past the highest transaction covered by the compact base and segment tail.
    next_txid: u64,
    /// The running database checksum the last segment leaves behind.
    last_checksum: Option<Checksum>,
}

impl SegmentChain {
    /// Derive the position from the compact recovery base and segment tail.
    ///
    /// A recovery base is installed atomically with its SQLite database. Subsequent
    /// segment files are the source of truth for the tail. A gap is reported rather than
    /// worked around, since re-issuing a TXID would collide with an immutable segment.
    fn scan(dir: &Path) -> Result<Self> {
        let mut ranges: Vec<(u64, u64, PathBuf)> = Vec::new();

        for entry in fs::read_dir(dir)? {
            let path = entry?.path();

            if path.extension().and_then(|ext| ext.to_str()) != Some("ltx") {
                continue;
            }

            let name = path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned();
            let (min, max) = LtxSegment::parse_file_name(&name)
                .ok_or_else(|| LtxError::BadSegmentName(name.clone()))?;

            ranges.push((min, max, path));
        }

        ranges.sort();

        let base = CaptureBase::load(dir)?;
        let mut next_txid = base
            .as_ref()
            .map_or(TXID::ONE.into_inner(), |base| base.through_txid + 1);
        let mut last_checksum = base
            .as_ref()
            .map(|base| Checksum::new(base.post_apply_checksum));

        for (min, max, path) in ranges {
            if min != next_txid {
                return Err(LtxError::SegmentGap {
                    expected: next_txid,
                    found: min,
                });
            }

            let segment = read_segment(&path)?;
            if segment.pre_apply_checksum != last_checksum.as_ref().map(Checksum::into_inner) {
                return Err(LtxError::SegmentChecksumGap { txid: min });
            }

            next_txid = max + 1;
            last_checksum = Some(Checksum::new(segment.post_apply_checksum));
        }

        Ok(Self {
            next_txid,
            last_checksum,
        })
    }
}

const CAPTURE_BASE_FORMAT_VERSION: u32 = 1;

/// The durable state represented directly by the installed SQLite database.
///
/// This replaces recreating one local LTX file per historical transaction after a
/// cold restore. It is not a second source of truth: the file and database are staged,
/// fsynced, and renamed as one object directory.
#[derive(Serialize, Deserialize)]
struct CaptureBase {
    format_version: u32,
    through_txid: u64,
    post_apply_checksum: u64,
}

impl CaptureBase {
    fn path(dir: &Path) -> PathBuf {
        dir.join("base.json")
    }

    fn load(dir: &Path) -> Result<Option<Self>> {
        let path = Self::path(dir);
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let base: Self = serde_json::from_slice(&bytes)?;
        if base.format_version != CAPTURE_BASE_FORMAT_VERSION || base.through_txid == 0 {
            return Err(LtxError::InvalidCaptureBase);
        }
        Ok(Some(base))
    }
}

/// Install a compact local recovery position beside a restored database.
pub(crate) fn install_capture_base(
    db_path: &Path,
    through_txid: u64,
    post_apply_checksum: u64,
) -> Result<()> {
    if through_txid == 0 {
        return Ok(());
    }
    let dir = ltx_dir(db_path);
    fs::create_dir_all(&dir)?;
    let base = CaptureBase {
        format_version: CAPTURE_BASE_FORMAT_VERSION,
        through_txid,
        post_apply_checksum,
    };
    write_durably(&CaptureBase::path(&dir), &serde_json::to_vec(&base)?)?;
    fs::File::open(&dir)?.sync_all()?;
    Ok(())
}

/// Which WAL generation the segments are being cut from.
///
/// Written before any segment of a generation exists and only when the generation
/// changes, which is what lets attach work out how many of the WAL's transactions the
/// chain already accounts for.
#[derive(Serialize, Deserialize)]
struct Generation {
    /// The WAL salt, hex encoded so the file stays readable.
    salt: String,
    /// The TXID given to the first transaction captured from this generation.
    first_txid: u64,
}

impl Generation {
    fn new(salt: [u8; 8], first_txid: u64) -> Self {
        Self {
            salt: salt.iter().map(|byte| format!("{byte:02x}")).collect(),
            first_txid,
        }
    }

    fn salt(&self) -> [u8; 8] {
        let mut salt = [0u8; 8];

        for (index, byte) in salt.iter_mut().enumerate() {
            let Some(hex) = self.salt.get(index * 2..index * 2 + 2) else {
                return [0; 8];
            };

            *byte = u8::from_str_radix(hex, 16).unwrap_or(0);
        }

        salt
    }

    fn path(dir: &Path) -> PathBuf {
        dir.join("generation.json")
    }

    fn load(dir: &Path) -> Result<Option<Self>> {
        let path = Self::path(dir);

        if !path.exists() {
            return Ok(None);
        }

        Ok(Some(serde_json::from_slice(&fs::read(&path)?)?))
    }

    fn store(&self, dir: &Path) -> Result<()> {
        write_durably(&Self::path(dir), &serde_json::to_vec(self)?)
    }
}

// ============================================================================
// Local durable storage
// ============================================================================

/// The sibling directory holding a database's LTX segments.
pub(crate) fn ltx_dir(db_path: &Path) -> PathBuf {
    sibling(db_path, "-ltx")
}

fn wal_path(db_path: &Path) -> PathBuf {
    sibling(db_path, "-wal")
}

fn sibling(db_path: &Path, suffix: &str) -> PathBuf {
    let mut name = db_path.as_os_str().to_os_string();
    name.push(suffix);

    PathBuf::from(name)
}

/// Distinguishes the temporary files of concurrent writers.
static WRITE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Publish a file that must never be replaced, atomically.
///
/// The staging file is uniquely named, and publication uses `link`, which fails rather
/// than clobbering an existing target. So two writers racing on the same segment cannot
/// interleave or overwrite: exactly one wins, and the loser compares bytes. Identical
/// bytes mean the write already happened and is treated as a success; differing bytes are
/// a genuine conflict worth surfacing.
fn write_immutable(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path.parent().ok_or(LtxError::MissingParent)?;
    let staged = stage(path, bytes)?;

    let result = match fs::hard_link(&staged, path) {
        Ok(()) => {
            fs::File::open(dir)?.sync_all()?;

            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            if fs::read(path)? == bytes {
                Ok(())
            } else {
                Err(LtxError::SegmentExists(path.to_path_buf()))
            }
        }
        Err(err) => Err(LtxError::Io(err)),
    };

    // The staging file has served its purpose either way.
    let _ = fs::remove_file(&staged);

    result
}

/// Write a file that may be replaced, without ever exposing a partial version.
fn write_durably(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path.parent().ok_or(LtxError::MissingParent)?;
    let staged = stage(path, bytes)?;

    fs::rename(&staged, path)?;
    fs::File::open(dir)?.sync_all()?;

    Ok(())
}

/// Write `bytes` to a uniquely named neighbour of `path` and fsync it.
fn stage(path: &Path, bytes: &[u8]) -> Result<PathBuf> {
    let dir = path.parent().ok_or(LtxError::MissingParent)?;

    let sequence = WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed);

    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".{}.{sequence}.tmp", std::process::id()));

    let staged = dir.join(name);

    // Exclusive creation: a unique name should not collide, and if it somehow does we
    // want to hear about it rather than trample another writer's staging file.
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staged)?;

    file.write_all(bytes)?;
    file.sync_all()?;

    Ok(staged)
}

/// Read a persisted segment back, recovering its header fields from the file itself.
fn read_segment(path: &Path) -> Result<LtxSegment> {
    LtxSegment::decode(fs::read(path)?)
}

// ============================================================================
// Database pages
// ============================================================================

/// Checksum every page of the database file, returning the per-page checksums and the
/// running database checksum they XOR to.
fn page_checksums(
    db_path: &Path,
    page_size: PageSize,
) -> Result<(BTreeMap<u32, Checksum>, Checksum)> {
    let mut checksums = BTreeMap::new();
    let mut running = Checksum::new(0);

    let Ok(mut file) = fs::File::open(db_path) else {
        return Ok((checksums, running));
    };

    let pages = u32::try_from(file.metadata()?.len() / u64::from(page_size.into_inner()))
        .unwrap_or(u32::MAX);
    let lock_page = PageNum::lock_page(page_size).into_inner();

    for pgno in 1..=pages {
        if pgno == lock_page {
            continue;
        }

        let Some(data) = read_page(&mut file, page_size, pgno)? else {
            break;
        };

        let checksum =
            data.page_checksum(PageNum::new(pgno).map_err(|_| LtxError::ZeroPageNumber)?);

        checksums.insert(pgno, checksum);
        running = running ^ checksum;
    }

    Ok((checksums, running))
}

fn read_page(file: &mut fs::File, page_size: PageSize, pgno: u32) -> Result<Option<Vec<u8>>> {
    let page_size = u64::from(page_size.into_inner());
    let offset = (u64::from(pgno) - 1) * page_size;

    if file.metadata()?.len() < offset + page_size {
        return Ok(None);
    }

    file.seek(SeekFrom::Start(offset))?;

    let mut data = vec![0u8; page_size as usize];
    file.read_exact(&mut data)?;

    Ok(Some(data))
}
