use std::{thread::JoinHandle, time::Duration};

use testresult::TestResult;
use tokio::time::timeout;

use super::*;
use crate::store::{GcConfig, ProtectOutcome};

const DEADLINE: Duration = Duration::from_secs(5);

async fn join_owner(owner: JoinHandle<()>) -> TestResult<()> {
    timeout(DEADLINE, async {
        while !owner.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "store owner did not retire"))?;
    owner.join().expect("store owner panicked");
    Ok(())
}

#[tokio::test]
async fn opening_errors_return_without_modifying_invalid_database() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("root-file");
    fs::write(&root, b"not a directory")?;
    assert!(timeout(DEADLINE, FsStore::load(&root)).await?.is_err());

    let root = dir.path().join("invalid-db");
    fs::create_dir(&root)?;
    let invalid = b"not a redb database";
    fs::write(root.join("blobs.db"), invalid)?;
    assert!(timeout(DEADLINE, FsStore::load(&root)).await?.is_err());
    assert_eq!(fs::read(root.join("blobs.db"))?, invalid);
    Ok(())
}

#[tokio::test]
async fn opening_error_retires_owner() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("root-file");
    fs::write(&root, b"not a directory")?;
    let (store, ready, owner) =
        FsStore::start_with_opts(root.join("blobs.db"), Options::new(&root), ())?;
    assert!(timeout(DEADLINE, ready).await??.is_err());
    drop(store);
    join_owner(owner).await
}

#[tokio::test]
async fn duplicate_database_open_returns_error_without_stopping_first_store() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let (store, ready, owner) =
        FsStore::start_with_opts(root.join("blobs.db"), Options::new(root), ())?;
    timeout(DEADLINE, ready).await???;
    assert!(timeout(DEADLINE, FsStore::load(root)).await?.is_err());
    let tag = store
        .add_bytes(Bytes::from_static(b"first store still works"))
        .await?;
    assert_eq!(
        store.get_bytes(tag.hash).await?,
        b"first store still works"[..]
    );
    store.shutdown().await?;
    drop(store);
    join_owner(owner).await
}

#[tokio::test]
async fn converted_store_outlives_fs_store_and_retires_with_gc() -> TestResult<()> {
    for gc in [false, true] {
        for clean_shutdown in [false, true] {
            let dir = tempfile::tempdir()?;
            let root = dir.path();
            let mut options = Options::new(root);
            if gc {
                options.gc = Some(GcConfig {
                    interval: Duration::from_secs(3600),
                    add_protected: None,
                });
            }
            let (store, ready, owner) =
                FsStore::start_with_opts(root.join("blobs.db"), options, ())?;
            timeout(DEADLINE, ready).await???;
            let converted: Store = store.clone().into();
            drop(store);
            let tag = converted
                .add_bytes(Bytes::from_static(b"converted store"))
                .await?;
            let hash = tag.hash;
            assert_eq!(converted.get_bytes(hash).await?, b"converted store"[..]);
            converted.tags().set("retained", hash).await?;
            drop(tag);
            if clean_shutdown {
                converted.shutdown().await?;
            }
            drop(converted);
            join_owner(owner).await?;

            let store = timeout(DEADLINE, FsStore::load(root)).await??;
            if clean_shutdown {
                assert_eq!(store.get_bytes(hash).await?, b"converted store"[..]);
            }
            store.shutdown().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_startup_retires_before_and_after_readiness_with_gc() -> TestResult<()> {
    for gc in [false, true] {
        for ready_state in 0..3 {
            let dir = tempfile::tempdir()?;
            let root = dir.path();
            let mut options = Options::new(root);
            if gc {
                options.gc = Some(GcConfig {
                    interval: Duration::from_secs(3600),
                    add_protected: None,
                });
            }
            let (store, ready, owner) =
                FsStore::start_with_opts(root.join("blobs.db"), options, ())?;
            match ready_state {
                0 => drop(ready),
                1 => {
                    timeout(DEADLINE, async {
                        while ready.is_empty() {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    })
                    .await?;
                    drop(ready);
                }
                _ => timeout(DEADLINE, ready).await???,
            }
            // These are the resources held by a pending load_with_opts future.
            drop(store);
            join_owner(owner).await?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_store_cancels_pending_gc_callback_and_retires_owner() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let entered = Arc::new(tokio::sync::Notify::new());
    let mut options = Options::new(root);
    options.gc = Some(GcConfig {
        interval: Duration::from_millis(1),
        add_protected: Some(Arc::new({
            let entered = entered.clone();
            move |_| {
                let entered = entered.clone();
                Box::pin(async move {
                    entered.notify_one();
                    std::future::pending::<ProtectOutcome>().await
                })
            }
        })),
    });
    let (store, ready, owner) = FsStore::start_with_opts(root.join("blobs.db"), options, ())?;
    timeout(DEADLINE, ready).await???;
    timeout(DEADLINE, entered.notified()).await?;
    drop(store);
    join_owner(owner).await
}

struct NotifyOnDrop(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            tx.send(()).ok();
        }
    }
}

pub(super) struct BlockingGc {
    release: NotifyOnDrop,
    pub(super) entered: Arc<tokio::sync::Notify>,
    pub(super) exited: tokio::sync::oneshot::Receiver<()>,
}

impl BlockingGc {
    pub(super) fn new(root: &Path) -> (Options, Self) {
        thread_local! {
            static THREAD_EXIT: std::cell::RefCell<Option<NotifyOnDrop>> = const {
                std::cell::RefCell::new(None)
            };
        }

        let entered = Arc::new(tokio::sync::Notify::new());
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (exited_tx, exited) = tokio::sync::oneshot::channel();
        let controls = Arc::new(std::sync::Mutex::new(Some((release_rx, exited_tx))));
        let mut options = Options::new(root);
        options.gc = Some(GcConfig {
            interval: Duration::from_millis(1),
            add_protected: Some(Arc::new({
                let entered = entered.clone();
                move |_| {
                    let entered = entered.clone();
                    let controls = controls.clone();
                    Box::pin(async move {
                        let controls = controls.lock().unwrap().take();
                        if let Some((release_rx, exited_tx)) = controls {
                            tokio::task::spawn_blocking(move || {
                                THREAD_EXIT.with(|slot| {
                                    *slot.borrow_mut() = Some(NotifyOnDrop(Some(exited_tx)));
                                });
                                entered.notify_one();
                                release_rx.blocking_recv().ok();
                            })
                            .await
                            .ok();
                        }
                        std::future::pending::<ProtectOutcome>().await
                    })
                }
            })),
        });
        (
            options,
            Self {
                // Release native work on every error or panic path.
                release: NotifyOnDrop(Some(release_tx)),
                entered,
                exited,
            },
        )
    }

    pub(super) fn release(&mut self) {
        drop(self.release.0.take());
    }

    async fn worker_exited(&mut self) -> TestResult<()> {
        // A native worker exits when its owning private runtime is dropped.
        timeout(DEADLINE, &mut self.exited).await??;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_shutdown_waits_for_private_runtime_blocking_work() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let (options, mut native) = BlockingGc::new(root);
    let store = timeout(
        DEADLINE,
        FsStore::load_with_opts(root.join("blobs.db"), options),
    )
    .await??;
    let converted: Store = store.clone().into();
    let started = timeout(DEADLINE, native.entered.notified()).await;
    let mut shutdown = Box::pin(converted.shutdown());
    let observed = timeout(Duration::from_millis(50), &mut shutdown).await;
    let returned_before_release = observed.is_ok();

    native.release();
    let shutdown_result = match observed {
        Ok(result) => Ok(result),
        Err(_) => timeout(DEADLINE, &mut shutdown).await,
    };
    drop(shutdown);
    drop(store);
    drop(converted);
    let retired = native.worker_exited().await;

    started?;
    shutdown_result??;
    retired?;
    assert!(
        !returned_before_release,
        "public shutdown returned before private runtime blocking work was released"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_shutdown_rpc_error_still_waits_for_retirement() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let (options, mut native) = BlockingGc::new(root);
    let store = FsStore::load_with_opts(root.join("blobs.db"), options).await?;
    let converted: Store = store.clone().into();
    let mut retired = Box::pin(store.wait_retired());
    let started = timeout(DEADLINE, native.entered.notified()).await;
    let mut first = Box::pin(converted.shutdown());
    let first_observed = timeout(Duration::from_millis(50), &mut first).await;
    let first_returned = first_observed.is_ok();
    let mut second = Box::pin(store.shutdown());
    let second_observed = timeout(Duration::from_millis(50), &mut second).await;
    let second_returned = second_observed.is_ok();
    let witness_returned = timeout(Duration::from_millis(50), &mut retired)
        .await
        .is_ok();

    native.release();
    let first_result = match first_observed {
        Ok(result) => Ok(result),
        Err(_) => timeout(DEADLINE, &mut first).await,
    };
    let second_result = match second_observed {
        Ok(result) => Ok(result),
        Err(_) => timeout(DEADLINE, &mut second).await,
    };
    drop(first);
    drop(second);
    drop(store);
    drop(converted);
    let worker_exited = native.worker_exited().await;
    if !witness_returned {
        timeout(DEADLINE, retired).await?;
    }

    started?;
    first_result??;
    assert!(second_result?.is_err());
    worker_exited?;
    assert!(!first_returned);
    assert!(!second_returned, "RPC error bypassed runtime retirement");
    assert!(
        !witness_returned,
        "retirement witness resolved before runtime drop"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_shutdown_waiter_does_not_cancel_retirement() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let (options, mut native) = BlockingGc::new(root);
    let store = FsStore::load_with_opts(root.join("blobs.db"), options).await?;
    let mut retired = Box::pin(store.wait_retired());
    let started = timeout(DEADLINE, native.entered.notified()).await;
    let mut shutdown = Box::pin(store.shutdown());
    let returned = timeout(Duration::from_millis(50), &mut shutdown)
        .await
        .is_ok();
    drop(shutdown);
    drop(store);
    let witness_returned = timeout(Duration::from_millis(50), &mut retired)
        .await
        .is_ok();

    native.release();
    let worker_exited = native.worker_exited().await;
    if !witness_returned {
        timeout(DEADLINE, retired).await?;
    }

    started?;
    worker_exited?;
    assert!(!returned);
    assert!(
        !witness_returned,
        "cancelled waiter released retirement early"
    );
    let reopened = timeout(DEADLINE, FsStore::load(root)).await??;
    reopened.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn retirement_future_does_not_retain_store_command_senders() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let store = FsStore::load(dir.path()).await?;
    let retired = store.wait_retired();
    drop(store);
    timeout(DEADLINE, retired).await?;
    Ok(())
}

#[tokio::test]
async fn startup_panic_returns_error_and_retires_owner() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    // A root path has no parent, exercising the existing startup panic boundary.
    let (store, ready, owner) =
        FsStore::start_with_opts(PathBuf::from("/"), Options::new(dir.path()), ())?;
    let error = timeout(DEADLINE, ready).await??.unwrap_err();
    assert!(error.to_string().contains("file store actor panicked"));
    assert!(timeout(DEADLINE, store.shutdown()).await?.is_err());
    let retired = store.wait_retired();
    drop(store);
    timeout(DEADLINE, retired).await?;
    join_owner(owner).await
}

#[tokio::test]
async fn retained_response_handles_do_not_keep_owner_alive() -> TestResult<()> {
    use n0_future::StreamExt;

    for clean_shutdown in [false, true] {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        let (store, ready, owner) =
            FsStore::start_with_opts(root.join("blobs.db"), Options::new(root), ())?;
        timeout(DEADLINE, ready).await???;
        let mut observed = store.observe(Hash::new(b"observed")).stream().await?;
        assert!(timeout(DEADLINE, observed.next()).await?.is_some());
        let imported = store
            .import_bao(Hash::new(b"pending import"), NonZeroU64::new(1).unwrap(), 1)
            .await?;
        // The second observation proves the import command has reached the entity manager.
        let _ = store.observe(Hash::new(b"pending import")).await?;
        if clean_shutdown {
            timeout(DEADLINE, store.shutdown()).await??;
        }
        drop(store);
        join_owner(owner).await?;
        assert!(timeout(DEADLINE, observed.next()).await?.is_none());
        assert!(timeout(DEADLINE, imported.rx).await?.is_err());
        drop(imported.tx);
    }
    Ok(())
}

#[tokio::test]
async fn wait_idle_waits_for_entity_response_streams() -> TestResult<()> {
    use n0_future::StreamExt;

    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let (store, ready, owner) =
        FsStore::start_with_opts(root.join("blobs.db"), Options::new(root), ())?;
    timeout(DEADLINE, ready).await???;
    let mut observed = store.observe(Hash::new(b"observed")).stream().await?;
    assert!(timeout(DEADLINE, observed.next()).await?.is_some());
    assert!(timeout(Duration::from_millis(50), store.wait_idle())
        .await
        .is_err());
    drop(observed);
    timeout(DEADLINE, store.wait_idle()).await??;
    drop(store);
    join_owner(owner).await
}
