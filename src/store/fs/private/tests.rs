use super::*;
use bytes::Bytes;

fn profile() -> RuntimeProfile {
    RuntimeProfile {
        worker_threads: NonZeroUsize::new(2).unwrap(),
        blocking_threads: NonZeroUsize::new(3).unwrap(),
        thread_stack_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        queue_capacity: NonZeroUsize::new(7).unwrap(),
        receive_slots: NonZeroUsize::new(2).unwrap(),
    }
}

#[test]
fn runtime_claim_counts_owner_all_workers_and_all_native_queues() {
    assert_eq!(
        profile().requirements().unwrap(),
        StartupRequirements {
            thread_count: 6,
            requested_stack_bytes: 6 * 1024 * 1024,
            queue_count: 4,
            queue_slots: 28,
            receive_slots: 2,
        }
    );
    let mut value = profile();
    value.thread_stack_bytes = NonZeroUsize::new(usize::MAX).unwrap();
    assert!(value.requirements().is_err());
    value = profile();
    value.receive_slots = NonZeroUsize::new(usize::MAX).unwrap();
    assert!(value.requirements().is_err());
}

#[test]
fn importer_claim_counts_complete_raw_response_not_channel_capacity() {
    let tiny = ExpectedBlob {
        hash: Hash::EMPTY,
        size: 1,
    }
    .requirements()
    .unwrap();
    let large = ExpectedBlob {
        hash: Hash::EMPTY,
        size: 1024 * 1024,
    }
    .requirements()
    .unwrap();
    assert_eq!(
        ExpectedBlob {
            hash: Hash::EMPTY,
            size: 0
        }
        .requirements()
        .unwrap()
        .maximum_items,
        1
    );
    assert_eq!(tiny.maximum_items, 1);
    assert!(large.maximum_items >= 1024);
    assert!(large.batch_backing_request_bytes > tiny.batch_backing_request_bytes);
    assert!(
        large.batch_backing_request_bytes
            >= large.maximum_items * size_of::<BaoContentItem>() as u64
    );
    assert_eq!(large.outboard_bytes, raw_outboard_size(large.data_bytes));
    assert!(ExpectedBlob {
        hash: Hash::new(b"nonempty"),
        size: 0
    }
    .requirements()
    .is_err());
}

#[tokio::test]
async fn native_error_keeps_last_aggregate_claim_until_runtime_retirement() -> Result<()> {
    use std::{
        future::{poll_fn, Future},
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        task::Poll,
        time::Duration,
    };
    use tokio::time::timeout;

    struct Hold {
        aggregate: bool,
        active: Arc<AtomicUsize>,
        exited: Arc<AtomicBool>,
        early: Arc<AtomicBool>,
    }
    impl Drop for Hold {
        fn drop(&mut self) {
            if self.aggregate
                && self.active.fetch_sub(1, Ordering::SeqCst) == 1
                && !self.exited.load(Ordering::SeqCst)
            {
                self.early.store(true, Ordering::SeqCst);
            }
        }
    }

    let deadline = Duration::from_secs(10);
    let directory = tempfile::tempdir().anyerr()?;
    let active = Arc::new(AtomicUsize::new(0));
    let exited = Arc::new(AtomicBool::new(false));
    let early = Arc::new(AtomicBool::new(false));
    let admission = NativeAdmission::new({
        let active = active.clone();
        let exited = exited.clone();
        let early = early.clone();
        move |claim| {
            let aggregate = matches!(claim, NativeClaim::Batch(_));
            if aggregate {
                active.fetch_add(1, Ordering::SeqCst);
            }
            Ok(Hold {
                aggregate,
                active: active.clone(),
                exited: exited.clone(),
                early: early.clone(),
            })
        }
    });
    let mut options = Options::new(directory.path());
    options.batch.max_write_batch = 1;
    let store = PrivateFsStore::open_fresh_with_admission(
        directory.path().join("blobs.db"),
        options,
        profile(),
        admission,
        |_| Ok(()),
    )
    .await?;
    let (entered, entered_rx) = oneshot::channel();
    let (release, released) = oneshot::channel::<()>();
    let blocking = store.runtime.spawn_blocking({
        let exited = exited.clone();
        move || {
            entered.send(()).ok();
            released.blocking_recv().ok();
            exited.store(true, Ordering::SeqCst);
        }
    });
    timeout(deadline, entered_rx).await.anyerr()?.anyerr()?;
    let shutdown = store.store.shutdown();
    tokio::pin!(shutdown);
    let outcome = async {
        // The private surface exposes no generic tags. This test-only access
        // first funds a real Get, then drives the actual capability error.
        assert!(timeout(
            deadline,
            store.store.get_bytes(Hash::new(b"absent private blob"))
        )
        .await
        .anyerr()?
        .is_err());
        assert_eq!(active.load(Ordering::SeqCst), 1);
        assert!(timeout(
            deadline,
            store
                .store
                .tags()
                .set("outside-private-profile", Hash::EMPTY)
        )
        .await
        .anyerr()?
        .is_err());
        assert!(poll_fn(|cx| Poll::Ready(shutdown.as_mut().poll(cx)))
            .await
            .is_pending());
        assert_eq!(active.load(Ordering::SeqCst), 1);
        assert!(!early.load(Ordering::SeqCst));
        Ok::<_, n0_error::AnyError>(())
    }
    .await;
    // Dropping the release sender also unblocks the worker on assertion unwind.
    drop(release);
    let stopped = timeout(deadline, shutdown).await;
    if stopped.is_err() {
        let retained = directory.keep();
        return Err(anyerr!(
            "private error retirement timed out; retained {}",
            retained.display()
        ));
    }
    assert!(stopped.anyerr()?.is_err());
    blocking.await.anyerr()?;
    assert!(exited.load(Ordering::SeqCst));
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(!early.load(Ordering::SeqCst));
    outcome
}

#[tokio::test]
async fn startup_refusal_precedes_file_creation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("blobs.db");
    let options = Options::new(directory.path());
    let result = PrivateFsStore::open_fresh(path.clone(), options, profile(), |_| {
        Err::<(), _>(anyerr!("funding refused"))
    })
    .await;
    assert!(result.is_err());
    assert!(!path.exists());
    assert!(!directory.path().join("data").exists());
    assert!(!directory.path().join("temp").exists());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn fresh_metadata_serial_imports_seal_without_waiting_for_more_commands() -> Result<()> {
    use std::{fs::File, os::unix::fs::PermissionsExt, sync::Mutex, time::Instant};

    let directory = tempfile::tempdir().anyerr()?;
    let paths = super::super::options::PathOptions::new(directory.path());
    for path in [&paths.data_path, &paths.temp_path] {
        std::fs::create_dir(path).anyerr()?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).anyerr()?;
    }
    let database = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(directory.path().join("blobs.db"))
        .anyerr()?;
    let slots = Arc::new(Mutex::new(Vec::new()));
    let admission = NativeAdmission::new({
        let slots = slots.clone();
        move |claim| {
            if let NativeClaim::BatchStorage { slots: count, .. } = claim {
                slots.lock().unwrap().push(count);
            }
            Ok(())
        }
    });
    let store = PrivateFsStore::open_fresh_metadata(
        MetadataSeed {
            database,
            payloads: File::open(&paths.data_path).anyerr()?,
            temporary: File::open(&paths.temp_path).anyerr()?,
        },
        profile(),
        admission,
        |_| Ok(()),
    )
    .await?;
    for value in 0..3 {
        let bytes = [value; 2048];
        let hash = Hash::new(bytes);
        let path = paths.data_path(&hash);
        std::fs::write(&path, bytes).anyerr()?;
        let started = Instant::now();
        store
            .import_verified(
                ExpectedBlob { hash, size: 2048 },
                File::open(path).anyerr()?,
                None,
                |_| Ok(()),
            )
            .await?;
        eprintln!("serial tiny import {value}: {:?}", started.elapsed());
    }
    store.retire().await?;
    // A sealed batch starts with one command; one slot cannot await a batch-fill timer.
    assert_eq!(*slots.lock().unwrap(), [1, 1, 1]);
    Ok(())
}

#[tokio::test]
async fn existing_native_lineage_is_not_opened_by_private_constructor() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("blobs.db");
    std::fs::write(&path, b"retained native state").unwrap();
    let result = PrivateFsStore::open_fresh(
        path.clone(),
        Options::new(directory.path()),
        profile(),
        |_| Ok(()),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(std::fs::read(path).unwrap(), b"retained native state");
    assert!(!directory.path().join("data").exists());
}

struct ReceiveCustody(Option<oneshot::Sender<()>>);
impl Drop for ReceiveCustody {
    fn drop(&mut self) {
        self.0.take().unwrap().send(()).ok();
    }
}

#[tokio::test]
async fn cancelled_receive_waiter_keeps_native_job_slot_and_funded_custody() -> Result<()> {
    raw_receive_keeps_native_custody(false, &[42], true).await
}

#[tokio::test]
async fn cancelled_verify_waiter_keeps_custody_and_never_mutates_native_data() -> Result<()> {
    raw_receive_keeps_native_custody(true, &[42], true).await
}

#[tokio::test]
async fn exact_empty_required_raw_response_is_verified_without_native_data() -> Result<()> {
    raw_receive_keeps_native_custody(true, &[], false).await
}

async fn raw_receive_keeps_native_custody(
    verify_only: bool,
    bytes: &'static [u8],
    cancel_waiter: bool,
) -> Result<()> {
    use iroh::{endpoint::presets, Endpoint, RelayMode};
    use std::{net::Ipv4Addr, time::Duration};
    use tokio::time::timeout;
    let deadline = Duration::from_secs(10);
    let endpoint = async {
        Ok::<_, n0_error::AnyError>(
            Endpoint::builder(presets::Minimal)
                .relay_mode(RelayMode::Disabled)
                .clear_ip_transports()
                .bind_addr((Ipv4Addr::LOCALHOST, 0))?
                .alpns(vec![crate::ALPN.to_vec()])
                .bind()
                .await?,
        )
    };
    let server = endpoint.await?;
    let client = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr((Ipv4Addr::LOCALHOST, 0))?
        .bind()
        .await?;
    let expected = ExpectedBlob {
        hash: Hash::new(bytes),
        size: bytes.len() as u64,
    };
    let (header_tx, header_rx) = oneshot::channel();
    let (body_tx, body_rx) = oneshot::channel();
    let provider = tokio::spawn({
        let server = server.clone();
        async move {
            let connection = server.accept().await.unwrap().await?;
            let (mut send, mut receive) = connection.accept_bi().await.anyerr()?;
            let (request, _) = crate::protocol::Request::read_async(&mut receive).await?;
            let crate::protocol::Request::Get(request) = request else {
                return Err(anyerr!("expected raw Get"));
            };
            if request.hash != expected.hash {
                return Err(anyerr!("request identity mismatch"));
            }
            send.write_all(&expected.size.to_le_bytes())
                .await
                .anyerr()?;
            header_tx.send(()).ok();
            body_rx.await.anyerr()?;
            send.write_all(bytes).await.anyerr()?;
            send.finish().anyerr()?;
            send.stopped().await.anyerr()?;
            Ok::<_, n0_error::AnyError>(())
        }
    });
    let directory = tempfile::tempdir()?;
    let mut topology = profile();
    topology.receive_slots = NonZeroUsize::new(1).unwrap();
    let native_claims = Arc::new(std::sync::Mutex::new(Vec::new()));
    let admission = NativeAdmission::new({
        let native_claims = native_claims.clone();
        move |claim| {
            native_claims.lock().unwrap().push(claim);
            Ok(())
        }
    });
    let store = Arc::new(
        PrivateFsStore::open_fresh_with_admission(
            directory.path().join("blobs.db"),
            Options::new(directory.path()),
            topology,
            admission,
            |_| Ok(()),
        )
        .await?,
    );
    let outcome = async {
        let connection = client.connect(server.addr(), crate::ALPN).await?;
        let (released_tx, mut released_rx) = oneshot::channel();
        let waiter = tokio::spawn({
            let store = store.clone();
            async move {
                let fund = |claim: ReceiveRequirements| {
                    if claim.data_bytes != expected.size {
                        return Err(anyerr!("admitted size differs from expected member"));
                    }
                    Ok(ReceiveCustody(Some(released_tx)))
                };
                if verify_only {
                    store.verify_raw(connection, expected, fund).await
                } else {
                    store.receive(connection, expected, fund).await
                }
            }
        });
        timeout(deadline, header_rx).await.anyerr()?.anyerr()?;
        let waiter = if cancel_waiter {
            waiter.abort();
            if !matches!(waiter.await, Err(error) if error.is_cancelled()) {
                return Err(anyerr!("outer receive waiter did not cancel"));
            }
            None
        } else {
            Some(waiter)
        };
        if !matches!(
            released_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ) || store.receives.available_permits() != 0
        {
            return Err(anyerr!("cancelled waiter released native job ownership"));
        }
        body_tx.send(()).ok();
        if let Some(waiter) = waiter {
            timeout(deadline, waiter).await.anyerr()?.anyerr()??;
        }
        timeout(deadline, released_rx).await.anyerr()?.anyerr()?;
        let returned = timeout(deadline, store.receives.clone().acquire_owned())
            .await
            .anyerr()?
            .anyerr()?;
        drop(returned);
        if store.receives.available_permits() != 1 {
            return Err(anyerr!("completed native receive did not return its slot"));
        }
        timeout(deadline, provider).await.anyerr()?.anyerr()??;
        if verify_only {
            if (!bytes.is_empty() && store.store.get_bytes(expected.hash).await.is_ok())
                || native_claims.lock().unwrap().iter().any(|claim| {
                    matches!(claim, NativeClaim::Batch(batch) if batch.inserted_points[0] > 0)
                })
            {
                return Err(anyerr!("verify/discard mutated native data"));
            }
        } else {
            if store.store.get_bytes(expected.hash).await? != Bytes::from_static(&[42]) {
                return Err(anyerr!(
                    "native receive did not preserve exact verified bytes"
                ));
            }
            if !native_claims.lock().unwrap().iter().any(|claim| {
                matches!(claim, NativeClaim::Batch(batch) if batch.inserted_points[0] > 0
                    && batch.normal_page_requests > 0 && batch.tracker_high_water >= 1000
                    && batch.page_buffer_requested_bytes > 0)
            }) {
                return Err(anyerr!(
                    "actual private receiver did not fund expanded native writes"
                ));
            }
        }
        Ok(())
    }
    .await;
    client.close().await;
    server.close().await;
    let stopped = timeout(deadline, store.store.shutdown()).await;
    // A failed retirement must not delete files while their native owner lives.
    if stopped.is_err() {
        let retained = directory.keep();
        return Err(anyerr!(
            "native retirement timed out; retained {}",
            retained.display()
        ));
    }
    stopped.anyerr()?.anyerr()?;
    outcome
}
