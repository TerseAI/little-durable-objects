use super::*;
use crate::{
    actor_state::ActorDatabaseTestExt,
    durability::{ActorChangeCapture, LocalActorChangeCapture},
    ltx::ltx_dir,
};
use tempfile::TempDir;

async fn captured_history(root: &Path, object: &ActorStorageKey) -> Result<Vec<LtxSegment>> {
    let capture = LocalActorChangeCapture::new(root);
    capture.prepare(object).await?;
    let database = ActorDatabaseStore::new(root).open(object)?;
    database.set("first", &"one")?;
    database.set("second", &"two")?;

    Ok(capture.capture(object).await?.segments().to_vec())
}

// ========================================================================
// SQLite reconstruction from canonical LTX
// ========================================================================

#[tokio::test]
async fn restores_sqlite_and_compact_local_capture_base() -> Result<()> {
    let dir = TempDir::new()?;
    let object = ActorStorageKey::new("object-x");
    let source = dir.path().join("source");
    let target = dir.path().join("target");
    let segments = captured_history(&source, &object).await?;
    let target_factory = ActorDatabaseStore::new(&target);
    let target_path = target_factory.path_for(&object)?;

    restore_sqlite_from_ltx(&target_path, &segments)?;

    let restored = target_factory.open(&object)?;
    assert_eq!(restored.get::<String>("first")?, Some("one".into()));
    assert_eq!(restored.get::<String>("second")?, Some("two".into()));
    assert!(ltx_dir(&target_path).join("base.json").is_file());
    assert_eq!(
        std::fs::read_dir(ltx_dir(&target_path))?
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("ltx"))
            .count(),
        0
    );

    Ok(())
}

#[tokio::test]
async fn capture_continues_after_the_restored_txid() -> Result<()> {
    let dir = TempDir::new()?;
    let object = ActorStorageKey::new("object-x");
    let source = dir.path().join("source");
    let target = dir.path().join("target");
    let segments = captured_history(&source, &object).await?;
    let target_factory = ActorDatabaseStore::new(&target);
    let target_path = target_factory.path_for(&object)?;
    restore_sqlite_from_ltx(&target_path, &segments)?;
    let capture = LocalActorChangeCapture::new(&target);
    capture.prepare(&object).await?;
    target_factory.open(&object)?.set("third", &"three")?;

    let captured = capture.capture(&object).await?;

    assert_eq!(captured.len(), 1);
    assert_eq!(captured.segments()[0].min_txid, 3);
    assert_eq!(captured.segments()[0].max_txid, 3);

    Ok(())
}

#[tokio::test]
async fn a_gap_fails_before_replacing_existing_sqlite() -> Result<()> {
    let dir = TempDir::new()?;
    let object = ActorStorageKey::new("object-x");
    let source = dir.path().join("source");
    let target = dir.path().join("target");
    let segments = captured_history(&source, &object).await?;
    let target_factory = ActorDatabaseStore::new(&target);
    target_factory.open(&object)?.set("existing", &"keep")?;

    restore_sqlite_from_ltx(&target_factory.path_for(&object)?, &segments[1..])
        .expect_err("a missing first segment must fail");

    assert_eq!(
        target_factory.open(&object)?.get::<String>("existing")?,
        Some("keep".into())
    );

    Ok(())
}

#[tokio::test]
async fn corrupt_ltx_fails_before_replacing_existing_sqlite() -> Result<()> {
    let dir = TempDir::new()?;
    let object = ActorStorageKey::new("object-x");
    let source = dir.path().join("source");
    let target = dir.path().join("target");
    let mut segments = captured_history(&source, &object).await?;
    let target_factory = ActorDatabaseStore::new(&target);
    target_factory.open(&object)?.set("existing", &"keep")?;
    let middle = segments[0].bytes.len() / 2;
    segments[0].bytes[middle] ^= 0xff;

    restore_sqlite_from_ltx(&target_factory.path_for(&object)?, &segments)
        .expect_err("corrupt LTX must fail");

    assert_eq!(
        target_factory.open(&object)?.get::<String>("existing")?,
        Some("keep".into())
    );

    Ok(())
}
