use std::{
    sync::OnceLock,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> Result<u64>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

static ANCHOR: OnceLock<(SystemTime, Instant)> = OnceLock::new();

impl Clock for SystemClock {
    fn now_ms(&self) -> Result<u64> {
        let (wall, monotonic) = *ANCHOR.get_or_init(|| (SystemTime::now(), Instant::now()));
        let now = wall + monotonic.elapsed();
        Ok(u64::try_from(now.duration_since(UNIX_EPOCH)?.as_millis())?)
    }
}
