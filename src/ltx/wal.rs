use std::{
    fmt, fs,
    io::{self, Read, Seek, SeekFrom},
    path::Path,
};

pub const WAL_HEADER_SIZE: u64 = 32;
pub const FRAME_HEADER_SIZE: u64 = 24;

/// WAL magic with a little-endian checksum. The low bit records the byte order the
/// writer used when computing checksums.
const WAL_MAGIC: u32 = 0x377f_0682;

// ============================================================================
// Public API
// ============================================================================

/// A single page image written to the WAL.
#[derive(Clone)]
pub struct WalFrame {
    /// The database page this frame replaces.
    pub pgno: u32,
    /// The page image itself.
    pub data: Vec<u8>,
}

impl fmt::Debug for WalFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalFrame")
            .field("pgno", &self.pgno)
            .field("data", &format_args!("[{} bytes]", self.data.len()))
            .finish()
    }
}

/// A committed run of WAL frames.
#[derive(Debug)]
pub struct WalTransaction {
    /// The size of the database in pages once this transaction is applied.
    pub commit: u32,
    /// The size of a database page in bytes.
    pub page_size: u32,
    /// Every frame in the transaction, in the order it was written.
    pub frames: Vec<WalFrame>,
    /// The cursor to adopt once this transaction has been *durably* captured.
    ///
    /// Reading does not advance the caller's cursor. The caller owns that decision, so
    /// that a transaction which fails to encode or persist is offered again rather than
    /// skipped.
    pub cursor_after: WalCursor,
}

/// A resumable position in the WAL.
///
/// The checksum chain means a frame can only be validated in the context of the frame
/// before it, so the cursor carries the running checksum alongside the byte offset. It
/// only ever advances to a commit boundary, so a torn or in-flight transaction is
/// simply re-read on the next pass.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WalCursor {
    /// Byte offset of the next unread frame, or `0` if the WAL has not been read yet.
    pub offset: u64,
    /// The salt of the WAL generation this offset belongs to.
    pub salt: [u8; 8],
    /// The running checksum as of `offset`.
    pub checksum: (u32, u32),
    /// Whether checksum words are read big-endian.
    pub big_endian: bool,
}

impl WalCursor {
    /// Whether the cursor names a real position in a WAL generation.
    pub fn is_initialized(&self) -> bool {
        self.offset >= WAL_HEADER_SIZE
    }
}

/// Read every committed transaction that follows `cursor`.
///
/// The cursor is *positioned* if it is not yet initialized, but is never advanced past
/// a transaction: each returned [`WalTransaction`] carries the cursor to adopt once it
/// is durably captured. A missing or empty WAL yields nothing.
///
/// If the WAL has been restarted, truncated, or removed underneath an initialized cursor
/// — all things an external checkpoint does — this fails with
/// [`WalError::GenerationChanged`] rather than rewinding or reporting no work. The old
/// frames no longer describe the database, and the caller's checksum state no longer
/// describes the new generation's base, so recovering means re-attaching, which is what
/// re-runs the checksum verification.
pub fn read_committed(
    path: &Path,
    cursor: &mut WalCursor,
) -> Result<Vec<WalTransaction>, WalError> {
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return vanished(cursor),
        Err(err) => return Err(WalError::Io(err)),
    };

    let len = file.metadata()?.len();

    if len < WAL_HEADER_SIZE {
        return vanished(cursor);
    }

    let header = WalHeader::read(&mut file)?;

    if !cursor.is_initialized() {
        *cursor = WalCursor {
            offset: WAL_HEADER_SIZE,
            salt: header.salt,
            checksum: header.checksum,
            big_endian: header.big_endian,
        };
    } else if cursor.salt != header.salt {
        return Err(WalError::GenerationChanged);
    } else if len < cursor.offset {
        // Same generation, but the file no longer reaches our position: it was truncated
        // and partly rewritten. Whatever stood between here and there is unaccounted for.
        return Err(WalError::GenerationChanged);
    }

    let page_size = header.page_size as u64;
    let frame_size = FRAME_HEADER_SIZE + page_size;

    let mut transactions = Vec::new();
    let mut pending: Vec<WalFrame> = Vec::new();
    let mut offset = cursor.offset;
    let mut checksum = cursor.checksum;

    file.seek(SeekFrom::Start(offset))?;

    while offset + frame_size <= len {
        let mut frame_header = [0u8; FRAME_HEADER_SIZE as usize];
        file.read_exact(&mut frame_header)?;

        let mut data = vec![0u8; header.page_size as usize];
        file.read_exact(&mut data)?;

        // Frames from a previous WAL generation can linger past the end of the current
        // one. Their salt won't match, and neither will the checksum chain.
        if frame_header[8..16] != header.salt {
            break;
        }

        let expected = (
            u32::from_be_bytes(frame_header[16..20].try_into().unwrap()),
            u32::from_be_bytes(frame_header[20..24].try_into().unwrap()),
        );

        let running = checksum_bytes(checksum, &frame_header[0..8], header.big_endian);
        let running = checksum_bytes(running, &data, header.big_endian);

        if running != expected {
            break;
        }

        let pgno = u32::from_be_bytes(frame_header[0..4].try_into().unwrap());
        let commit = u32::from_be_bytes(frame_header[4..8].try_into().unwrap());

        if pgno == 0 {
            return Err(WalError::ZeroPageNumber { offset });
        }

        checksum = running;
        offset += frame_size;

        pending.push(WalFrame { pgno, data });

        // A non-zero commit marker is what makes the run durable and visible. Only
        // then do we hand it over, along with the position that supersedes it.
        if commit != 0 {
            transactions.push(WalTransaction {
                commit,
                page_size: header.page_size,
                frames: std::mem::take(&mut pending),
                cursor_after: WalCursor {
                    offset,
                    salt: header.salt,
                    checksum,
                    big_endian: header.big_endian,
                },
            });
        }
    }

    Ok(transactions)
}

/// A WAL that is absent or too short to hold a header.
///
/// With no position established there is simply nothing to read yet. With one, the WAL
/// has been checkpointed and truncated away underneath us, and any transaction it held
/// past our cursor is now only in the database file — so this must not be reported as
/// "no work", which would let capture sail past a transaction it never saw.
fn vanished(cursor: &WalCursor) -> Result<Vec<WalTransaction>, WalError> {
    if cursor.is_initialized() {
        Err(WalError::GenerationChanged)
    } else {
        Ok(Vec::new())
    }
}

// ============================================================================
// WAL header
// ============================================================================

#[derive(Debug)]
struct WalHeader {
    page_size: u32,
    salt: [u8; 8],
    checksum: (u32, u32),
    big_endian: bool,
}

impl WalHeader {
    fn read<R>(mut r: R) -> Result<Self, WalError>
    where
        R: Read,
    {
        let mut buf = [0u8; WAL_HEADER_SIZE as usize];
        r.read_exact(&mut buf)?;

        let magic = u32::from_be_bytes(buf[0..4].try_into().unwrap());

        if magic & !1 != WAL_MAGIC {
            return Err(WalError::Magic(magic));
        }

        // The low bit of the magic records whether the writer's native byte order was
        // big-endian, which is the order its checksum words must be read back in.
        let big_endian = magic & 1 == 1;

        let page_size = u32::from_be_bytes(buf[8..12].try_into().unwrap());

        if !is_valid_page_size(page_size) {
            return Err(WalError::PageSize(page_size));
        }

        let expected = (
            u32::from_be_bytes(buf[24..28].try_into().unwrap()),
            u32::from_be_bytes(buf[28..32].try_into().unwrap()),
        );

        let checksum = checksum_bytes((0, 0), &buf[0..24], big_endian);

        if checksum != expected {
            return Err(WalError::HeaderChecksum);
        }

        Ok(Self {
            page_size,
            salt: buf[16..24].try_into().unwrap(),
            checksum,
            big_endian,
        })
    }
}

fn is_valid_page_size(page_size: u32) -> bool {
    (512..=65536).contains(&page_size) && page_size.is_power_of_two()
}

/// The WAL checksum: a pair of accumulators folded over the input as 32-bit words.
///
/// `data` must be a multiple of eight bytes, which every input SQLite feeds it is.
fn checksum_bytes(seed: (u32, u32), data: &[u8], big_endian: bool) -> (u32, u32) {
    let (mut s0, mut s1) = seed;

    for chunk in data.chunks_exact(8) {
        let (a, b) = if big_endian {
            (
                u32::from_be_bytes(chunk[0..4].try_into().unwrap()),
                u32::from_be_bytes(chunk[4..8].try_into().unwrap()),
            )
        } else {
            (
                u32::from_le_bytes(chunk[0..4].try_into().unwrap()),
                u32::from_le_bytes(chunk[4..8].try_into().unwrap()),
            )
        };

        s0 = s0.wrapping_add(a).wrapping_add(s1);
        s1 = s1.wrapping_add(b).wrapping_add(s0);
    }

    (s0, s1)
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    Magic(u32),
    PageSize(u32),
    HeaderChecksum,
    ZeroPageNumber {
        offset: u64,
    },
    /// The WAL was restarted, truncated, or removed, so an existing cursor no longer
    /// means anything.
    GenerationChanged,
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => {
                write!(f, "wal io error: {err}")
            }
            Self::Magic(magic) => {
                write!(f, "not a sqlite wal: bad magic {magic:#010x}")
            }
            Self::PageSize(page_size) => {
                write!(f, "unsupported wal page size: {page_size}")
            }
            Self::HeaderChecksum => {
                write!(f, "wal header checksum mismatch")
            }
            Self::ZeroPageNumber { offset } => {
                write!(f, "wal frame at offset {offset} has page number zero")
            }
            Self::GenerationChanged => {
                write!(
                    f,
                    "the wal was restarted or truncated: capture must be re-attached"
                )
            }
        }
    }
}

impl std::error::Error for WalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for WalError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

// ============================================================================
// Test support
// ============================================================================

/// Builds syntactically valid WAL files so the reader can be exercised against
/// frame layouts that are awkward to coax out of SQLite itself.
#[cfg(test)]
pub struct WalBuilder {
    bytes: Vec<u8>,
    page_size: u32,
    salt: [u8; 8],
    checksum: (u32, u32),
}

#[cfg(test)]
impl WalBuilder {
    pub fn new(page_size: u32) -> Self {
        Self::with_salt(page_size, [1, 2, 3, 4, 5, 6, 7, 8])
    }

    /// Rebuild the header around a different generation salt.
    pub fn salt(self, salt: [u8; 8]) -> Self {
        Self::with_salt(self.page_size, salt)
    }

    fn with_salt(page_size: u32, salt: [u8; 8]) -> Self {
        let mut bytes = Vec::with_capacity(WAL_HEADER_SIZE as usize);
        bytes.extend_from_slice(&WAL_MAGIC.to_be_bytes());
        bytes.extend_from_slice(&3_007_000u32.to_be_bytes());
        bytes.extend_from_slice(&page_size.to_be_bytes());
        bytes.extend_from_slice(&0u32.to_be_bytes());
        bytes.extend_from_slice(&salt);

        let checksum = checksum_bytes((0, 0), &bytes, false);
        bytes.extend_from_slice(&checksum.0.to_be_bytes());
        bytes.extend_from_slice(&checksum.1.to_be_bytes());

        Self {
            bytes,
            page_size,
            salt,
            checksum,
        }
    }

    /// Append a frame. A `commit` of zero marks it as part of an unfinished
    /// transaction.
    pub fn frame(mut self, pgno: u32, commit: u32, fill: u8) -> Self {
        let data = vec![fill; self.page_size as usize];

        let mut header = Vec::with_capacity(FRAME_HEADER_SIZE as usize);
        header.extend_from_slice(&pgno.to_be_bytes());
        header.extend_from_slice(&commit.to_be_bytes());
        header.extend_from_slice(&self.salt);

        self.checksum = checksum_bytes(self.checksum, &header[0..8], false);
        self.checksum = checksum_bytes(self.checksum, &data, false);

        header.extend_from_slice(&self.checksum.0.to_be_bytes());
        header.extend_from_slice(&self.checksum.1.to_be_bytes());

        self.bytes.extend_from_slice(&header);
        self.bytes.extend_from_slice(&data);

        self
    }

    pub fn write(self, path: &Path) -> io::Result<()> {
        fs::write(path, &self.bytes)
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn wal_path(dir: &TempDir) -> std::path::PathBuf {
        dir.path().join("test.db-wal")
    }

    #[test]
    fn missing_wal_yields_nothing() -> Result<(), WalError> {
        let dir = TempDir::new().map_err(WalError::Io)?;
        let mut cursor = WalCursor::default();

        let transactions = read_committed(&wal_path(&dir), &mut cursor)?;

        assert!(transactions.is_empty());
        assert_eq!(cursor, WalCursor::default());

        Ok(())
    }

    #[test]
    fn reads_a_committed_transaction() -> Result<(), WalError> {
        let dir = TempDir::new().map_err(WalError::Io)?;
        let path = wal_path(&dir);

        WalBuilder::new(512)
            .frame(1, 0, 0xaa)
            .frame(2, 2, 0xbb)
            .write(&path)
            .map_err(WalError::Io)?;

        let mut cursor = WalCursor::default();
        let transactions = read_committed(&path, &mut cursor)?;

        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].commit, 2);
        assert_eq!(transactions[0].page_size, 512);

        let pgnos: Vec<u32> = transactions[0].frames.iter().map(|f| f.pgno).collect();
        assert_eq!(pgnos, vec![1, 2]);

        Ok(())
    }

    #[test]
    fn ignores_frames_after_the_last_commit() -> Result<(), WalError> {
        let dir = TempDir::new().map_err(WalError::Io)?;
        let path = wal_path(&dir);

        WalBuilder::new(512)
            .frame(1, 1, 0xaa)
            // An in-flight second transaction: spilled to the WAL, never committed.
            .frame(2, 0, 0xbb)
            .frame(3, 0, 0xcc)
            .write(&path)
            .map_err(WalError::Io)?;

        let mut cursor = WalCursor::default();
        let transactions = read_committed(&path, &mut cursor)?;

        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].frames.len(), 1);

        // Reading positions the cursor but does not advance it past the transaction;
        // the position that supersedes it rides along with it instead.
        assert_eq!(cursor.offset, WAL_HEADER_SIZE);
        assert_eq!(
            transactions[0].cursor_after.offset,
            WAL_HEADER_SIZE + FRAME_HEADER_SIZE + 512
        );

        Ok(())
    }

    #[test]
    fn resumes_from_a_cursor_without_replaying() -> Result<(), WalError> {
        let dir = TempDir::new().map_err(WalError::Io)?;
        let path = wal_path(&dir);

        WalBuilder::new(512)
            .frame(1, 1, 0xaa)
            .write(&path)
            .map_err(WalError::Io)?;

        let mut cursor = WalCursor::default();
        let transactions = read_committed(&path, &mut cursor)?;

        assert_eq!(transactions.len(), 1);

        // The caller adopts the position only once it has done something durable with
        // the transaction.
        cursor = transactions[0].cursor_after;

        assert!(read_committed(&path, &mut cursor)?.is_empty());

        // Growing the WAL only surfaces the new transaction.
        WalBuilder::new(512)
            .frame(1, 1, 0xaa)
            .frame(2, 2, 0xbb)
            .write(&path)
            .map_err(WalError::Io)?;

        let transactions = read_committed(&path, &mut cursor)?;

        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].frames[0].pgno, 2);

        Ok(())
    }

    #[test]
    fn an_unadopted_cursor_offers_the_transaction_again() -> Result<(), WalError> {
        let dir = TempDir::new().map_err(WalError::Io)?;
        let path = wal_path(&dir);

        WalBuilder::new(512)
            .frame(1, 1, 0xaa)
            .write(&path)
            .map_err(WalError::Io)?;

        // Standing in for a caller whose encode or durable write failed: it read the
        // transaction but never adopted the position.
        let mut cursor = WalCursor::default();

        assert_eq!(read_committed(&path, &mut cursor)?.len(), 1);
        assert_eq!(read_committed(&path, &mut cursor)?.len(), 1);

        Ok(())
    }

    #[test]
    fn a_restarted_wal_invalidates_an_existing_cursor() -> Result<(), WalError> {
        let dir = TempDir::new().map_err(WalError::Io)?;
        let path = wal_path(&dir);

        WalBuilder::new(512)
            .frame(1, 1, 0xaa)
            .write(&path)
            .map_err(WalError::Io)?;

        let mut cursor = WalCursor::default();
        let transactions = read_committed(&path, &mut cursor)?;

        cursor = transactions[0].cursor_after;

        // A checkpoint restarts the WAL with a fresh salt. The old position means
        // nothing in the new generation, and silently rewinding would replay
        // transactions against stale checksum state.
        WalBuilder::new(512)
            .salt([9, 9, 9, 9, 9, 9, 9, 9])
            .frame(1, 1, 0xcc)
            .write(&path)
            .map_err(WalError::Io)?;

        assert!(matches!(
            read_committed(&path, &mut cursor),
            Err(WalError::GenerationChanged)
        ));

        Ok(())
    }

    #[test]
    fn a_removed_wal_invalidates_an_existing_cursor() -> Result<(), WalError> {
        let dir = TempDir::new().map_err(WalError::Io)?;
        let path = wal_path(&dir);

        WalBuilder::new(512)
            .frame(1, 1, 0xaa)
            .write(&path)
            .map_err(WalError::Io)?;

        let mut cursor = WalCursor::default();
        let transactions = read_committed(&path, &mut cursor)?;

        cursor = transactions[0].cursor_after;

        // A checkpoint truncated the WAL away. Anything it held past the cursor now
        // exists only in the database file, so this is not "nothing to do".
        fs::write(&path, []).map_err(WalError::Io)?;

        assert!(matches!(
            read_committed(&path, &mut cursor),
            Err(WalError::GenerationChanged)
        ));

        fs::remove_file(&path).map_err(WalError::Io)?;

        assert!(matches!(
            read_committed(&path, &mut cursor),
            Err(WalError::GenerationChanged)
        ));

        // With no position established there is genuinely nothing to read.
        let mut fresh = WalCursor::default();

        assert!(read_committed(&path, &mut fresh)?.is_empty());

        Ok(())
    }

    #[test]
    fn a_wal_shorter_than_the_cursor_invalidates_it() -> Result<(), WalError> {
        let dir = TempDir::new().map_err(WalError::Io)?;
        let path = wal_path(&dir);

        WalBuilder::new(512)
            .frame(1, 1, 0xaa)
            .frame(2, 2, 0xbb)
            .write(&path)
            .map_err(WalError::Io)?;

        let mut cursor = WalCursor::default();
        let transactions = read_committed(&path, &mut cursor)?;

        cursor = transactions[1].cursor_after;

        // Truncated and partly rewritten under the same salt: the file no longer
        // reaches our position, so what stood in between is unaccounted for.
        WalBuilder::new(512)
            .frame(1, 1, 0xaa)
            .write(&path)
            .map_err(WalError::Io)?;

        assert!(matches!(
            read_committed(&path, &mut cursor),
            Err(WalError::GenerationChanged)
        ));

        Ok(())
    }

    #[test]
    fn stale_frames_from_a_previous_generation_are_ignored() -> Result<(), WalError> {
        let dir = TempDir::new().map_err(WalError::Io)?;
        let path = wal_path(&dir);

        // A WAL restarted in place: two committed frames of the current generation,
        // followed by debris from the last one. The debris is structurally valid and
        // self-consistent — only its salt gives it away.
        let current = WalBuilder::new(512).frame(1, 1, 0xaa);
        let stale = WalBuilder::new(512)
            .salt([7, 7, 7, 7, 7, 7, 7, 7])
            .frame(9, 9, 0xff);

        let mut bytes = current.into_bytes();
        bytes.extend_from_slice(&stale.into_bytes()[WAL_HEADER_SIZE as usize..]);
        fs::write(&path, &bytes).map_err(WalError::Io)?;

        let mut cursor = WalCursor::default();
        let transactions = read_committed(&path, &mut cursor)?;

        assert_eq!(transactions.len(), 1, "the stale frame must not be read");
        assert_eq!(transactions[0].frames[0].pgno, 1);

        Ok(())
    }

    #[test]
    fn stops_at_a_corrupt_frame() -> Result<(), WalError> {
        let dir = TempDir::new().map_err(WalError::Io)?;
        let path = wal_path(&dir);

        WalBuilder::new(512)
            .frame(1, 1, 0xaa)
            .frame(2, 2, 0xbb)
            .write(&path)
            .map_err(WalError::Io)?;

        // Corrupt the page image of the second frame, invalidating its checksum.
        let mut bytes = fs::read(&path).map_err(WalError::Io)?;
        let second = (WAL_HEADER_SIZE + FRAME_HEADER_SIZE + 512 + FRAME_HEADER_SIZE) as usize;
        bytes[second] ^= 0xff;
        fs::write(&path, &bytes).map_err(WalError::Io)?;

        let mut cursor = WalCursor::default();
        let transactions = read_committed(&path, &mut cursor)?;

        assert_eq!(transactions.len(), 1);

        Ok(())
    }

    #[test]
    fn rejects_a_bad_header_checksum() -> Result<(), WalError> {
        let dir = TempDir::new().map_err(WalError::Io)?;
        let path = wal_path(&dir);

        WalBuilder::new(512)
            .frame(1, 1, 0xaa)
            .write(&path)
            .map_err(WalError::Io)?;

        let mut bytes = fs::read(&path).map_err(WalError::Io)?;
        bytes[24] ^= 0xff;
        fs::write(&path, &bytes).map_err(WalError::Io)?;

        let mut cursor = WalCursor::default();

        assert!(matches!(
            read_committed(&path, &mut cursor),
            Err(WalError::HeaderChecksum)
        ));

        Ok(())
    }
}
