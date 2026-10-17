use std::{
    collections::HashMap,
    future::{poll_fn, Future},
    path::{Path, PathBuf},
    sync::{LazyLock, Mutex},
    task::Poll,
    time::Duration,
};

use n0_future::StreamExt;
use testresult::TestResult;
use tokio::{sync::oneshot, time::timeout};

use super::{tests::round_up_request, tests::test_data, FsStore, Options};
use crate::{
    api::blobs::Bitfield,
    protocol::ChunkRangesExt,
    store::util::{read_checksummed, tests::create_n0_bao},
};
use bao_tree::ChunkRanges;

const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(super) struct CommitGate {
    pub entered: oneshot::Sender<()>,
    pub sync_db_queued: oneshot::Sender<()>,
    pub release: oneshot::Receiver<()>,
}

// Each store takes its own one-shot gate, so parallel tests cannot stall each other.
pub(super) static COMMIT_GATES: LazyLock<Mutex<HashMap<PathBuf, CommitGate>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(super) fn take_commit_gate(path: &Path) -> Option<CommitGate> {
    COMMIT_GATES.lock().unwrap().remove(path)
}

#[tokio::test]
async fn import_and_tag_receipts_precede_commit_but_db_sync_waits() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("blobs.db");
    let (entered_tx, entered_rx) = oneshot::channel();
    let (sync_db_queued_tx, sync_db_queued_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    COMMIT_GATES.lock().unwrap().insert(
        db_path.clone(),
        CommitGate {
            entered: entered_tx,
            sync_db_queued: sync_db_queued_tx,
            release: release_rx,
        },
    );
    let mut options = Options::new(dir.path());
    // Import Update and CreateTag end this batch without waiting for a timer.
    options.batch.max_write_batch = 2;
    options.batch.max_read_duration = Duration::from_secs(60);
    let store = timeout(DEADLINE, FsStore::load_with_opts(db_path.clone(), options)).await??;
    let data = b"unique inline payload proving the API receipt precedes the actual metadata commit";
    let import_store = store.clone();
    let import = tokio::spawn(async move { import_store.add_slice(data).await });
    timeout(DEADLINE, entered_rx).await??;
    let tag = timeout(DEADLINE, import).await???;

    let barrier = store.sync_db();
    tokio::pin!(barrier);
    assert!(poll_fn(|cx| Poll::Ready(barrier.as_mut().poll(cx)))
        .await
        .is_pending());
    timeout(DEADLINE, sync_db_queued_rx).await??;
    // The DB owner has the real reply sender queued behind the blocked commit.
    assert!(poll_fn(|cx| Poll::Ready(barrier.as_mut().poll(cx)))
        .await
        .is_pending());
    // Exact-version disk characterization, not a general redb layout contract.
    let before_commit = std::fs::read(&db_path)?;
    assert!(!before_commit.windows(data.len()).any(|bytes| bytes == data));

    release_tx.send(()).unwrap();
    timeout(DEADLINE, barrier).await??;
    let after_commit = std::fs::read(&db_path)?;
    assert!(after_commit.windows(data.len()).any(|bytes| bytes == data));
    assert_eq!(
        timeout(DEADLINE, store.get_bytes(tag.hash)).await??,
        data[..]
    );
    timeout(DEADLINE, store.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn partial_receipt_and_db_sync_leave_bitfield_until_observer_retires() -> TestResult<()> {
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
    let bitfield_path = options.path.bitfield_path(&hash);
    let mut observer = timeout(DEADLINE, store.observe(hash).stream()).await??;
    assert!(!timeout(DEADLINE, observer.next())
        .await?
        .unwrap()
        .is_complete());

    timeout(
        DEADLINE,
        store.import_bao_bytes(hash, ranges.clone(), encoded.clone()),
    )
    .await??;
    timeout(DEADLINE, store.sync_db()).await??;
    let observed = timeout(DEADLINE, observer.next()).await?.unwrap();
    assert_eq!(observed.ranges, ranges);
    assert!(!observed.is_complete());
    assert!(options.path.data_path(&hash).exists());
    assert!(options.path.sizes_path(&hash).exists());
    assert!(!bitfield_path.exists());
    assert_eq!(
        timeout(
            DEADLINE,
            store.export_bao(hash, ranges.clone()).bao_to_vec()
        )
        .await??,
        encoded,
    );
    assert!(!bitfield_path.exists());

    drop(observer);
    // Test observation only: the isolated store has no remaining streams after this drop.
    timeout(DEADLINE, store.wait_idle()).await??;
    let persisted: Bitfield = read_checksummed(&bitfield_path)?;
    assert_eq!(persisted, observed);

    let complete_data = test_data(1024 * 1024 + 1);
    let (complete_hash, complete_encoded) = create_n0_bao(&complete_data, &ChunkRanges::all())?;
    timeout(
        DEADLINE,
        store.import_bao_bytes(complete_hash, ChunkRanges::all(), complete_encoded),
    )
    .await??;
    timeout(DEADLINE, store.sync_db()).await??;
    timeout(DEADLINE, store.wait_idle()).await??;
    assert!(!options.path.bitfield_path(&complete_hash).exists());
    assert_eq!(
        timeout(DEADLINE, store.get_bytes(complete_hash)).await??,
        complete_data
    );
    timeout(DEADLINE, store.shutdown()).await??;
    Ok(())
}
