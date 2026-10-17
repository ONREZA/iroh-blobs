use std::{
    collections::HashMap,
    future::{poll_fn, Future},
    path::{Path, PathBuf},
    sync::{LazyLock, Mutex},
    task::Poll,
    time::Duration,
};

use bao_tree::ChunkRanges;
use n0_future::StreamExt;
use testresult::TestResult;
use tokio::{sync::oneshot, time::timeout};

use super::{
    options::InlineOptions,
    phase_boundary_tests::{CommitGate, COMMIT_GATES},
    tests::{round_up_request, test_data},
    FsStore, Options,
};
use crate::{
    api::blobs::Bitfield,
    protocol::ChunkRangesExt,
    store::util::{read_checksummed, tests::create_n0_bao},
    Hash,
};

#[cfg(unix)]
use crate::{
    api::blobs::{AddPathOptions, ImportMode},
    BlobFormat,
};

const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FileEvent {
    SidecarWrite,
    DataSynced,
    OutboardSynced,
}

static FILE_EVENTS: LazyLock<Mutex<HashMap<PathBuf, Vec<FileEvent>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(super) fn record_file_event(path: &Path, event: FileEvent) {
    if let Some(events) = FILE_EVENTS.lock().unwrap().get_mut(path) {
        events.push(event);
    }
}

#[cfg(unix)]
type SyncedFileIdentity = (FileEvent, u64, u64);

#[cfg(unix)]
static SYNC_IDENTITIES: LazyLock<Mutex<HashMap<PathBuf, Vec<SyncedFileIdentity>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(unix)]
pub(super) fn record_sync_identity(path: &Path, event: FileEvent, metadata: &std::fs::Metadata) {
    use std::os::unix::fs::MetadataExt;

    if let Some(identities) = SYNC_IDENTITIES.lock().unwrap().get_mut(path) {
        identities.push((event, metadata.dev(), metadata.ino()));
    }
}

fn record_events(path: &Path) {
    FILE_EVENTS.lock().unwrap().insert(path.into(), Vec::new());
}

fn events(path: &Path) -> Vec<FileEvent> {
    FILE_EVENTS.lock().unwrap()[path].clone()
}

#[tokio::test]
async fn live_partial_sync_rearms_after_duplicate_and_new_writes() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let options = Options::new(dir.path());
    let store = timeout(
        DEADLINE,
        FsStore::load_with_opts(dir.path().join("blobs.db"), options.clone()),
    )
    .await??;
    let data = test_data(1024 * 1024);
    let ranges = round_up_request(data.len() as u64, &ChunkRanges::last_chunk());
    let (hash, encoded) = create_n0_bao(&data, &ranges)?;
    let _pin = store.tags().temp_tag(hash).await?;
    let path = options.path.bitfield_path(&hash);
    record_events(&path);
    let mut observer = timeout(DEADLINE, store.observe(hash).stream()).await??;
    timeout(DEADLINE, observer.next()).await?.unwrap();

    timeout(
        DEADLINE,
        store.import_bao_bytes(hash, ranges.clone(), encoded.clone()),
    )
    .await??;
    timeout(DEADLINE, store.sync_hash(hash)).await??;
    let persisted: Bitfield = read_checksummed(&path)?;
    assert_eq!(persisted.ranges, ranges);
    assert_eq!(events(&path), [FileEvent::SidecarWrite]);
    timeout(DEADLINE, async {
        tokio::try_join!(store.sync_hash(hash), store.sync_hash(hash))
    })
    .await??;
    assert_eq!(events(&path), [FileEvent::SidecarWrite]);
    assert_eq!(
        timeout(
            DEADLINE,
            store.export_bao(hash, ranges.clone()).bao_to_vec()
        )
        .await??,
        encoded,
    );

    timeout(
        DEADLINE,
        store.import_bao_bytes(hash, ranges.clone(), encoded.clone()),
    )
    .await??;
    assert_eq!(std::fs::metadata(&path)?.len(), 0);
    timeout(DEADLINE, store.sync_hash(hash)).await??;
    assert_eq!(read_checksummed::<Bitfield>(&path)?, persisted);
    assert_eq!(events(&path).len(), 2);

    let added = round_up_request(data.len() as u64, &ChunkRanges::chunk(0));
    let (_, added_encoded) = create_n0_bao(&data, &added)?;
    timeout(
        DEADLINE,
        store.import_bao_bytes(hash, added.clone(), added_encoded),
    )
    .await??;
    assert_eq!(std::fs::metadata(&path)?.len(), 0);
    timeout(DEADLINE, store.sync_hash(hash)).await??;
    let newer: Bitfield = read_checksummed(&path)?;
    assert_eq!(newer.ranges, ranges.clone() | added);
    assert!(!newer.is_complete());
    assert_eq!(events(&path).len(), 3);
    assert_eq!(
        timeout(DEADLINE, store.export_bao(hash, ranges).bao_to_vec()).await??,
        encoded,
    );
    drop(observer);
    timeout(DEADLINE, store.wait_idle()).await??;
    assert_eq!(events(&path).len(), 3);
    assert_eq!(read_checksummed::<Bitfield>(&path)?, newer);
    timeout(DEADLINE, store.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn reopening_valid_partial_invalidates_then_syncs_without_retirement_rewrite(
) -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let options = Options::new(dir.path());
    let store = timeout(
        DEADLINE,
        FsStore::load_with_opts(dir.path().join("blobs.db"), options.clone()),
    )
    .await??;
    let data = test_data(1024 * 1024);
    let ranges = round_up_request(data.len() as u64, &ChunkRanges::last_chunk());
    let (hash, encoded) = create_n0_bao(&data, &ranges)?;
    let _pin = store.tags().temp_tag(hash).await?;
    let path = options.path.bitfield_path(&hash);
    record_events(&path);
    let mut first_observer = timeout(DEADLINE, store.observe(hash).stream()).await??;
    timeout(DEADLINE, first_observer.next()).await?.unwrap();
    timeout(
        DEADLINE,
        store.import_bao_bytes(hash, ranges.clone(), encoded),
    )
    .await??;
    timeout(DEADLINE, store.sync_hash(hash)).await??;
    let expected: Bitfield = read_checksummed(&path)?;
    drop(first_observer);
    timeout(DEADLINE, store.wait_idle()).await??;
    assert_eq!(events(&path), [FileEvent::SidecarWrite]);
    assert_eq!(read_checksummed::<Bitfield>(&path)?, expected);

    let mut reopened = timeout(DEADLINE, store.observe(hash).stream()).await??;
    let observed = timeout(DEADLINE, reopened.next()).await?.unwrap();
    assert_eq!(observed, expected);
    assert_eq!(std::fs::metadata(&path)?.len(), 0);
    timeout(DEADLINE, store.sync_hash(hash)).await??;
    assert_eq!(read_checksummed::<Bitfield>(&path)?, expected);
    assert_eq!(events(&path).len(), 2);
    drop(reopened);
    timeout(DEADLINE, store.wait_idle()).await??;
    assert_eq!(events(&path).len(), 2);
    assert_eq!(read_checksummed::<Bitfield>(&path)?, expected);
    timeout(DEADLINE, store.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn sidecar_error_preserves_live_reads_and_retry_stays_dirty() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let options = Options::new(dir.path());
    let store = timeout(
        DEADLINE,
        FsStore::load_with_opts(dir.path().join("blobs.db"), options.clone()),
    )
    .await??;
    let data = test_data(1024 * 1024);
    let ranges = round_up_request(data.len() as u64, &ChunkRanges::last_chunk());
    let (hash, encoded) = create_n0_bao(&data, &ranges)?;
    let _pin = store.tags().temp_tag(hash).await?;
    let path = options.path.bitfield_path(&hash);
    record_events(&path);
    let mut observer = timeout(DEADLINE, store.observe(hash).stream()).await??;
    timeout(DEADLINE, observer.next()).await?.unwrap();
    timeout(
        DEADLINE,
        store.import_bao_bytes(hash, ranges.clone(), encoded.clone()),
    )
    .await??;
    std::fs::create_dir(&path)?;
    assert!(timeout(DEADLINE, store.sync_hash(hash)).await?.is_err());
    assert_eq!(events(&path), [FileEvent::SidecarWrite]);
    assert_eq!(
        timeout(
            DEADLINE,
            store.export_bao(hash, ranges.clone()).bao_to_vec()
        )
        .await??,
        encoded,
    );
    std::fs::remove_dir(&path)?;
    timeout(DEADLINE, store.sync_hash(hash)).await??;
    assert_eq!(read_checksummed::<Bitfield>(&path)?.ranges, ranges);
    assert_eq!(events(&path).len(), 2);
    drop(observer);
    timeout(DEADLINE, store.wait_idle()).await??;
    assert_eq!(events(&path).len(), 2);
    timeout(DEADLINE, store.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn complete_sync_covers_owned_data_outboard_and_inline_state() -> TestResult<()> {
    for (inline, size, expected) in [
        (
            InlineOptions::NO_INLINE,
            32769,
            vec![FileEvent::DataSynced, FileEvent::OutboardSynced],
        ),
        (InlineOptions::default(), 100, Vec::new()),
    ] {
        let dir = tempfile::tempdir()?;
        let mut options = Options::new(dir.path());
        options.inline = inline;
        let store = timeout(
            DEADLINE,
            FsStore::load_with_opts(dir.path().join("blobs.db"), options.clone()),
        )
        .await??;
        let data = test_data(size);
        let hash = Hash::new(&data);
        let path = options.path.bitfield_path(&hash);
        record_events(&path);
        let mut observer = timeout(DEADLINE, store.observe(hash).stream()).await??;
        timeout(DEADLINE, observer.next()).await?.unwrap();
        let tag = timeout(DEADLINE, async { store.add_bytes(data.clone()).await }).await??;
        assert_eq!(tag.hash, hash);
        timeout(DEADLINE, store.sync_hash(hash)).await??;
        assert_eq!(events(&path), expected);
        assert!(!path.exists());
        assert_eq!(timeout(DEADLINE, store.get_bytes(hash)).await??, data);
        drop(observer);
        timeout(DEADLINE, store.shutdown()).await??;
    }
    Ok(())
}

#[tokio::test]
async fn hash_sync_waits_for_actual_metadata_prefix_after_file_sync() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("blobs.db");
    let (entered_tx, entered_rx) = oneshot::channel();
    let (queued_tx, queued_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    COMMIT_GATES.lock().unwrap().insert(
        db_path.clone(),
        CommitGate {
            entered: entered_tx,
            sync_db_queued: queued_tx,
            release: release_rx,
        },
    );
    let mut options = Options::new(dir.path());
    options.inline = InlineOptions::NO_INLINE;
    options.batch.max_write_batch = 2;
    options.batch.max_read_duration = Duration::from_secs(60);
    let store = timeout(DEADLINE, FsStore::load_with_opts(db_path, options.clone())).await??;
    let data = test_data(32769);
    let hash = Hash::new(&data);
    let path = options.path.bitfield_path(&hash);
    record_events(&path);
    let mut observer = timeout(DEADLINE, store.observe(hash).stream()).await??;
    timeout(DEADLINE, observer.next()).await?.unwrap();
    let import_store = store.clone();
    let import = tokio::spawn(async move { import_store.add_bytes(data).await });
    timeout(DEADLINE, entered_rx).await??;
    timeout(DEADLINE, import).await???;

    let sync_store = store.clone();
    let sync = tokio::spawn(async move { sync_store.sync_hash(hash).await });
    tokio::pin!(sync);
    timeout(DEADLINE, queued_rx).await??;
    assert_eq!(
        events(&path),
        [FileEvent::DataSynced, FileEvent::OutboardSynced]
    );
    assert!(poll_fn(|cx| Poll::Ready(sync.as_mut().poll(cx)))
        .await
        .is_pending());
    release_tx.send(()).unwrap();
    timeout(DEADLINE, sync).await???;
    drop(observer);
    timeout(DEADLINE, store.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn missing_poisoned_and_shutdown_states_fail_but_empty_hash_syncs() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let mut options = Options::new(dir.path());
    options.inline = InlineOptions::NO_INLINE;
    let store = timeout(
        DEADLINE,
        FsStore::load_with_opts(dir.path().join("blobs.db"), options.clone()),
    )
    .await??;
    let error = timeout(DEADLINE, store.sync_hash(Hash::new(b"missing")))
        .await?
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    timeout(DEADLINE, store.sync_hash(Hash::EMPTY)).await??;
    let tag = timeout(DEADLINE, async { store.add_bytes(test_data(32769)).await }).await??;
    timeout(DEADLINE, store.sync_db()).await??;
    timeout(DEADLINE, store.wait_idle()).await??;
    std::fs::remove_file(options.path.data_path(&tag.hash))?;
    assert!(timeout(DEADLINE, store.sync_hash(tag.hash)).await?.is_err());
    timeout(DEADLINE, store.shutdown()).await??;
    assert!(timeout(DEADLINE, store.sync_hash(Hash::EMPTY))
        .await?
        .is_err());
    Ok(())
}

#[tokio::test]
async fn full_local_import_invalidates_live_partial_checkpoint_and_retries_errors() -> TestResult<()>
{
    let dir = tempfile::tempdir()?;
    let options = Options::new(dir.path());
    let store = timeout(
        DEADLINE,
        FsStore::load_with_opts(dir.path().join("blobs.db"), options.clone()),
    )
    .await??;
    let data = test_data(1024 * 1024);
    let ranges = round_up_request(data.len() as u64, &ChunkRanges::last_chunk());
    let (hash, encoded) = create_n0_bao(&data, &ranges)?;
    let _pin = store.tags().temp_tag(hash).await?;
    let path = options.path.bitfield_path(&hash);
    record_events(&path);
    let mut observer = timeout(DEADLINE, store.observe(hash).stream()).await??;
    timeout(DEADLINE, observer.next()).await?.unwrap();
    timeout(
        DEADLINE,
        store.import_bao_bytes(hash, ranges.clone(), encoded.clone()),
    )
    .await??;
    timeout(DEADLINE, store.sync_hash(hash)).await??;
    let expected: Bitfield = read_checksummed(&path)?;

    std::fs::remove_file(&path)?;
    std::fs::create_dir(&path)?;
    assert!(
        timeout(DEADLINE, async { store.add_bytes(data.clone()).await })
            .await?
            .is_err()
    );
    assert_eq!(
        timeout(DEADLINE, store.export_bao(hash, ranges).bao_to_vec()).await??,
        encoded,
    );
    std::fs::remove_dir(&path)?;
    timeout(DEADLINE, store.sync_hash(hash)).await??;
    assert_eq!(read_checksummed::<Bitfield>(&path)?, expected);

    let tag = timeout(DEADLINE, async { store.add_bytes(data.clone()).await }).await??;
    assert_eq!(tag.hash, hash);
    if path.exists() {
        assert_eq!(std::fs::metadata(&path)?.len(), 0);
    }
    timeout(DEADLINE, store.sync_hash(hash)).await??;
    assert_eq!(timeout(DEADLINE, store.get_bytes(hash)).await??, data);
    assert!(!path.exists());
    assert_eq!(
        events(&path),
        [
            FileEvent::SidecarWrite,
            FileEvent::SidecarWrite,
            FileEvent::DataSynced
        ]
    );
    drop(observer);
    timeout(DEADLINE, store.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn partial_write_invalidation_error_preserves_reads_and_retry() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let options = Options::new(dir.path());
    let store = timeout(
        DEADLINE,
        FsStore::load_with_opts(dir.path().join("blobs.db"), options.clone()),
    )
    .await??;
    let data = test_data(1024 * 1024);
    let original_ranges = round_up_request(data.len() as u64, &ChunkRanges::last_chunk());
    let (hash, original_encoded) = create_n0_bao(&data, &original_ranges)?;
    let added_ranges = round_up_request(data.len() as u64, &ChunkRanges::chunk(0));
    let (_, added_encoded) = create_n0_bao(&data, &added_ranges)?;
    let _pin = store.tags().temp_tag(hash).await?;
    let path = options.path.bitfield_path(&hash);
    let mut observer = timeout(DEADLINE, store.observe(hash).stream()).await??;
    timeout(DEADLINE, observer.next()).await?.unwrap();
    timeout(
        DEADLINE,
        store.import_bao_bytes(hash, original_ranges.clone(), original_encoded.clone()),
    )
    .await??;
    timeout(DEADLINE, store.sync_hash(hash)).await??;
    assert_eq!(read_checksummed::<Bitfield>(&path)?.ranges, original_ranges);

    std::fs::remove_file(&path)?;
    std::fs::create_dir(&path)?;
    assert!(timeout(
        DEADLINE,
        store.import_bao_bytes(hash, added_ranges.clone(), added_encoded.clone()),
    )
    .await?
    .is_err());
    let original = timeout(
        DEADLINE,
        store.export_bao(hash, original_ranges.clone()).bao_to_vec(),
    )
    .await?;
    assert!(
        original.is_ok(),
        "pre-mutation invalidation failure must preserve original ranges: {original:?}"
    );
    assert_eq!(original?, original_encoded);

    std::fs::remove_dir(&path)?;
    timeout(
        DEADLINE,
        store.import_bao_bytes(hash, added_ranges.clone(), added_encoded.clone()),
    )
    .await??;
    timeout(DEADLINE, store.sync_hash(hash)).await??;
    assert_eq!(
        read_checksummed::<Bitfield>(&path)?.ranges,
        original_ranges | added_ranges.clone(),
    );
    assert_eq!(
        timeout(DEADLINE, store.export_bao(hash, added_ranges).bao_to_vec()).await??,
        added_encoded,
    );
    drop(observer);
    timeout(DEADLINE, store.shutdown()).await??;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn duplicate_complete_import_syncs_current_canonical_inodes() -> TestResult<()> {
    use std::os::unix::fs::MetadataExt;

    let dir = tempfile::tempdir()?;
    let mut options = Options::new(dir.path());
    options.inline = InlineOptions::NO_INLINE;
    let store = timeout(
        DEADLINE,
        FsStore::load_with_opts(dir.path().join("blobs.db"), options.clone()),
    )
    .await??;
    let data = test_data(32769);
    let hash = Hash::new(&data);
    let key = options.path.bitfield_path(&hash);
    let data_path = options.path.data_path(&hash);
    let outboard_path = options.path.outboard_path(&hash);
    SYNC_IDENTITIES
        .lock()
        .unwrap()
        .insert(key.clone(), Vec::new());
    let mut observer = timeout(DEADLINE, store.observe(hash).stream()).await??;
    timeout(DEADLINE, observer.next()).await?.unwrap();
    let first_tag = timeout(DEADLINE, async { store.add_bytes(data.clone()).await }).await??;
    assert_eq!(first_tag.hash, hash);
    timeout(DEADLINE, store.sync_hash(hash)).await??;
    let original_data = std::fs::metadata(&data_path)?;
    let original_outboard = std::fs::metadata(&outboard_path)?;
    let original_identities = [
        (
            FileEvent::DataSynced,
            original_data.dev(),
            original_data.ino(),
        ),
        (
            FileEvent::OutboardSynced,
            original_outboard.dev(),
            original_outboard.ino(),
        ),
    ];
    assert_eq!(SYNC_IDENTITIES.lock().unwrap()[&key], original_identities);

    let duplicate_tag = timeout(DEADLINE, async { store.add_bytes(data.clone()).await }).await??;
    assert_eq!(duplicate_tag.hash, hash);
    let current_data = std::fs::metadata(&data_path)?;
    let current_outboard = std::fs::metadata(&outboard_path)?;
    let current_identities = [
        (
            FileEvent::DataSynced,
            current_data.dev(),
            current_data.ino(),
        ),
        (
            FileEvent::OutboardSynced,
            current_outboard.dev(),
            current_outboard.ino(),
        ),
    ];
    eprintln!("duplicate import canonical identities: before={original_identities:?}, after={current_identities:?}");
    timeout(DEADLINE, store.sync_hash(hash)).await??;
    for expected in current_identities {
        let synced = SYNC_IDENTITIES.lock().unwrap()[&key]
            .iter()
            .rev()
            .find(|identity| identity.0 == expected.0)
            .copied();
        assert_eq!(
            synced,
            Some(expected),
            "sync receipt must cover each current canonical inode, not a replaced open inode"
        );
    }
    assert_eq!(timeout(DEADLINE, store.get_bytes(hash)).await??, data);
    drop(observer);
    timeout(DEADLINE, store.shutdown()).await??;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn complete_owned_external_transitions_sync_retained_canonical_files() -> TestResult<()> {
    use std::os::unix::fs::MetadataExt;

    for first_external in [true, false] {
        let dir = tempfile::tempdir()?;
        let mut options = Options::new(dir.path());
        options.inline = InlineOptions::NO_INLINE;
        let store = timeout(
            DEADLINE,
            FsStore::load_with_opts(dir.path().join("blobs.db"), options.clone()),
        )
        .await??;
        let data = test_data(32769);
        let hash = Hash::new(&data);
        let external = dir.path().join("external.data");
        std::fs::write(&external, &data)?;
        let key = options.path.bitfield_path(&hash);
        SYNC_IDENTITIES
            .lock()
            .unwrap()
            .insert(key.clone(), Vec::new());
        let mut observer = timeout(DEADLINE, store.observe(hash).stream()).await??;
        timeout(DEADLINE, observer.next()).await?.unwrap();
        let reference_options = || AddPathOptions {
            path: external.clone(),
            mode: ImportMode::TryReference,
            format: BlobFormat::Raw,
        };
        let first = timeout(DEADLINE, async {
            if first_external {
                store.add_path_with_opts(reference_options()).await
            } else {
                store.add_bytes(data.clone()).await
            }
        })
        .await??;
        assert_eq!(first.hash, hash);
        timeout(DEADLINE, store.sync_hash(hash)).await??;
        assert_eq!(options.path.data_path(&hash).exists(), !first_external);
        let second = timeout(DEADLINE, async {
            if first_external {
                store.add_bytes(data.clone()).await
            } else {
                store.add_path_with_opts(reference_options()).await
            }
        })
        .await??;
        assert_eq!(second.hash, hash);
        timeout(DEADLINE, store.sync_hash(hash)).await??;
        let canonical_data = std::fs::metadata(options.path.data_path(&hash))?;
        let canonical_outboard = std::fs::metadata(options.path.outboard_path(&hash))?;
        let external_metadata = std::fs::metadata(&external)?;
        assert_ne!(
            (canonical_data.dev(), canonical_data.ino()),
            (external_metadata.dev(), external_metadata.ino()),
        );
        for expected in [
            (
                FileEvent::DataSynced,
                canonical_data.dev(),
                canonical_data.ino(),
            ),
            (
                FileEvent::OutboardSynced,
                canonical_outboard.dev(),
                canonical_outboard.ino(),
            ),
        ] {
            let synced = SYNC_IDENTITIES.lock().unwrap()[&key]
                .iter()
                .rev()
                .find(|identity| identity.0 == expected.0)
                .copied();
            assert_eq!(synced, Some(expected), "first_external={first_external}");
        }
        std::fs::remove_file(&external)?;
        assert_eq!(timeout(DEADLINE, store.get_bytes(hash)).await??, data);
        drop(observer);
        timeout(DEADLINE, store.wait_idle()).await??;
        assert_eq!(timeout(DEADLINE, store.get_bytes(hash)).await??, data);
        timeout(DEADLINE, store.shutdown()).await??;
    }
    Ok(())
}

#[tokio::test]
async fn duplicate_after_options_reopen_preserves_inline_outboard() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("blobs.db");
    let data = test_data(32769);
    let hash = Hash::new(&data);
    let store = timeout(
        DEADLINE,
        FsStore::load_with_opts(db_path.clone(), Options::new(dir.path())),
    )
    .await??;
    let first = timeout(DEADLINE, async { store.add_bytes(data.clone()).await }).await??;
    assert_eq!(first.hash, hash);
    timeout(DEADLINE, store.sync_hash(hash)).await??;
    timeout(DEADLINE, store.shutdown()).await??;

    let mut options = Options::new(dir.path());
    options.inline = InlineOptions::NO_INLINE;
    let store = timeout(DEADLINE, FsStore::load_with_opts(db_path, options.clone())).await??;
    let path = options.path.bitfield_path(&hash);
    record_events(&path);
    let mut observer = timeout(DEADLINE, store.observe(hash).stream()).await??;
    assert!(timeout(DEADLINE, observer.next())
        .await?
        .unwrap()
        .is_complete());
    let duplicate = timeout(DEADLINE, async { store.add_bytes(data.clone()).await }).await??;
    assert_eq!(duplicate.hash, hash);
    timeout(DEADLINE, store.sync_hash(hash)).await??;
    assert_eq!(events(&path), [FileEvent::DataSynced]);
    let unused_outboard = options.path.outboard_path(&hash);
    assert!(unused_outboard.exists());
    std::fs::remove_file(&unused_outboard)?;
    let (_, expected) = create_n0_bao(&data, &ChunkRanges::all())?;
    assert_eq!(
        timeout(
            DEADLINE,
            store.export_bao(hash, ChunkRanges::all()).bao_to_vec()
        )
        .await??,
        expected,
    );
    drop(observer);
    timeout(DEADLINE, store.wait_idle()).await??;
    assert_eq!(
        timeout(
            DEADLINE,
            store.export_bao(hash, ChunkRanges::all()).bao_to_vec()
        )
        .await??,
        expected,
    );
    timeout(DEADLINE, store.shutdown()).await??;
    Ok(())
}
