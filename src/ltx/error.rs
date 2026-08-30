//! Errors raised while capturing or replicating LTX.

use crate::{actor_state::ActorDatabaseError, ltx::wal::WalError};
use std::{fmt, io, path::PathBuf};

pub type Result<T> = std::result::Result<T, LtxError>;

#[derive(Debug)]
pub enum LtxError {
    Persistence(ActorDatabaseError),
    Wal(WalError),
    Io(io::Error),
    Serialization(serde_json::Error),
    Encode(litetx::EncodeError),
    Decode(litetx::DecodeError),
    /// Capture needs a real file to read a WAL from.
    NotFileBacked,
    /// SQLite refused to switch into WAL mode; the resulting journal mode is reported.
    WalUnavailable(String),
    UnsupportedPageSize(u32),
    PageSizeChanged {
        expected: u32,
        found: u32,
    },
    /// A committed transaction reported a database size of zero pages.
    EmptyCommit {
        txid: u64,
    },
    /// A snapshot is missing a page the database file should have supplied.
    MissingSnapshotPage {
        pgno: u32,
    },
    /// The database has moved on from what the local segments describe, and the WAL no
    /// longer holds the difference.
    UncapturedWrites {
        expected: u64,
        found: u64,
        txid: u64,
    },
    /// The segment chain is missing a transaction, so the position cannot be derived.
    SegmentGap {
        expected: u64,
        found: u64,
    },
    /// A segment tail does not continue the checksum recorded by its compact base.
    SegmentChecksumGap {
        txid: u64,
    },
    /// The segments claim more of this WAL generation than the WAL actually holds.
    ChainAheadOfWal {
        captured: usize,
        available: usize,
    },
    /// A caller tried to recycle the WAL before every locally captured transaction was
    /// visible through the canonical durable manifest.
    DurabilityBehindCapture {
        durable_txid: u64,
        captured_txid: u64,
    },
    /// A file in the segment directory is not a recognisable segment name.
    BadSegmentName(String),
    InvalidCaptureBase,
    ZeroTxid,
    ZeroPageNumber,
    /// A segment already exists at this position with different contents.
    SegmentExists(PathBuf),
    MissingParent,
    LockPoisoned,
}

impl fmt::Display for LtxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Persistence(err) => {
                write!(f, "persistence error: {err}")
            }
            Self::Wal(err) => {
                write!(f, "wal error: {err}")
            }
            Self::Io(err) => {
                write!(f, "io error: {err}")
            }
            Self::Serialization(err) => {
                write!(f, "serialization error: {err}")
            }
            Self::Encode(err) => {
                write!(f, "ltx encode error: {err}")
            }
            Self::Decode(err) => {
                write!(f, "ltx decode error: {err}")
            }
            Self::NotFileBacked => {
                write!(
                    f,
                    "ltx capture requires a file-backed database: an in-memory database has no wal"
                )
            }
            Self::WalUnavailable(mode) => {
                write!(f, "could not enable wal mode: journal mode is {mode:?}")
            }
            Self::UnsupportedPageSize(size) => {
                write!(f, "unsupported database page size: {size}")
            }
            Self::PageSizeChanged { expected, found } => {
                write!(
                    f,
                    "page size changed from {expected} to {found} mid-capture"
                )
            }
            Self::EmptyCommit { txid } => {
                write!(f, "transaction {txid} committed a zero-page database")
            }
            Self::MissingSnapshotPage { pgno } => {
                write!(f, "snapshot is missing page {pgno}")
            }
            Self::UncapturedWrites {
                expected,
                found,
                txid,
            } => {
                write!(
                    f,
                    "database has writes that were never captured: expected checksum \
                     {expected:016x} after txid {txid}, found {found:016x}. \
                     transactions committed after the last sync were checkpointed away"
                )
            }
            Self::SegmentGap { expected, found } => {
                write!(f, "ltx segment chain jumps from txid {expected} to {found}")
            }
            Self::SegmentChecksumGap { txid } => {
                write!(f, "ltx checksum chain breaks before txid {txid}")
            }
            Self::ChainAheadOfWal {
                captured,
                available,
            } => {
                write!(
                    f,
                    "segments claim {captured} transactions but the wal holds {available}"
                )
            }
            Self::DurabilityBehindCapture {
                durable_txid,
                captured_txid,
            } => {
                write!(
                    f,
                    "refusing to checkpoint WAL at durable TXID {durable_txid}: local capture has reached TXID {captured_txid}"
                )
            }
            Self::BadSegmentName(name) => {
                write!(f, "unrecognised ltx segment name: {name}")
            }
            Self::InvalidCaptureBase => {
                write!(f, "invalid local ltx capture base")
            }
            Self::ZeroTxid => {
                write!(f, "transaction ids must be non-zero")
            }
            Self::ZeroPageNumber => {
                write!(f, "page numbers must be non-zero")
            }
            Self::SegmentExists(path) => {
                write!(
                    f,
                    "ltx segments are immutable: {} already exists",
                    path.display()
                )
            }
            Self::MissingParent => {
                write!(f, "cannot write a file without a parent directory")
            }
            Self::LockPoisoned => {
                write!(f, "ltx capture lock poisoned")
            }
        }
    }
}

impl std::error::Error for LtxError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Persistence(err) => Some(err),
            Self::Wal(err) => Some(err),
            Self::Io(err) => Some(err),
            Self::Serialization(err) => Some(err),
            Self::Encode(err) => Some(err),
            Self::Decode(err) => Some(err),
            _ => None,
        }
    }
}

impl From<ActorDatabaseError> for LtxError {
    fn from(err: ActorDatabaseError) -> Self {
        Self::Persistence(err)
    }
}

impl From<WalError> for LtxError {
    fn from(err: WalError) -> Self {
        Self::Wal(err)
    }
}

impl From<io::Error> for LtxError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<serde_json::Error> for LtxError {
    fn from(err: serde_json::Error) -> Self {
        Self::Serialization(err)
    }
}

impl From<litetx::EncodeError> for LtxError {
    fn from(err: litetx::EncodeError) -> Self {
        Self::Encode(err)
    }
}

impl From<litetx::DecodeError> for LtxError {
    fn from(err: litetx::DecodeError) -> Self {
        Self::Decode(err)
    }
}
