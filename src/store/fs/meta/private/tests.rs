use super::*;
use crate::{store::fs::meta::Update, Hash};
use redb::ReadableTable;
use std::{
    future::{poll_fn, Future},
    task::Poll,
};

pub(super) struct CommitGate {
    pub entered: tokio::sync::oneshot::Sender<()>,
    pub release: tokio::sync::oneshot::Receiver<()>,
}

fn actor(
    directory: &std::path::Path,
    admission: Admission,
) -> Result<(super::super::Actor, tokio::sync::mpsc::Sender<Command>)> {
    let mut options = crate::store::fs::options::Options::new(directory);
    options.batch.max_write_batch = 1;
    let (_, deletes) = crate::store::fs::delete_set::pair(Arc::new(options.path));
    let (commands, inbox) = tokio::sync::mpsc::channel(4);
    let actor = super::super::Actor::new_with_admission(
        directory.join("blobs.db"),
        inbox,
        deletes,
        options.batch,
        Some(admission),
    )?;
    Ok((actor, commands))
}

fn update_command(hash: Hash, state: EntryState<Bytes>) -> Command {
    Update {
        hash,
        state,
        tx: None,
        span: tracing::Span::none(),
    }
    .into()
}

#[tokio::test]
async fn rejected_batch_funding_leaves_native_rows_and_history_untouched() -> Result<()> {
    let directory = tempfile::tempdir().anyerr()?;
    let funded = Arc::new(Mutex::new(Vec::new()));
    let admission = Admission::new({
        let funded = funded.clone();
        move |claim| {
            funded.lock().unwrap().push(claim);
            if matches!(claim, Claim::Batch(_)) {
                return Err(anyerr!("batch funding refused"));
            }
            Ok(())
        }
    });
    let (actor, commands) = actor(directory.path(), admission.clone())?;
    commands
        .try_send(update_command(
            Hash::new(b"rejected"),
            EntryState::Partial { size: Some(7) },
        ))
        .anyerr()?;
    drop(commands);
    actor.run().await;
    let state = admission.state.lock().unwrap();
    assert!(state.stopped);
    assert_eq!(state.history, Some(History::default()));
    assert!(state.hold.is_none());
    drop(state);
    assert!(
        matches!(&funded.lock().unwrap()[..], [Claim::BatchStorage { .. }, Claim::Batch(batch)] if batch.inserted_points == [1, 0, 0])
    );
    let db = redb::Database::open(directory.path().join("blobs.db")).anyerr()?;
    let tx = db.begin_read().anyerr()?;
    let tables = ReadOnlyTables::new(&tx).anyerr()?;
    assert_eq!(tables.blobs().len().anyerr()?, 0);
    assert_eq!(tables.inline_data().len().anyerr()?, 0);
    Ok(())
}

#[tokio::test]
async fn replacement_aggregate_is_acquired_before_previous_hold_drops() -> Result<()> {
    struct Hold {
        id: Option<usize>,
        events: Arc<Mutex<Vec<(bool, usize)>>>,
    }
    impl Drop for Hold {
        fn drop(&mut self) {
            if let Some(id) = self.id {
                self.events.lock().unwrap().push((false, id));
            }
        }
    }
    let directory = tempfile::tempdir().anyerr()?;
    let events = Arc::new(Mutex::new(Vec::new()));
    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let admission = Admission::new({
        let events = events.clone();
        move |claim| {
            let id = if matches!(claim, Claim::Batch(_)) {
                let id = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                events.lock().unwrap().push((true, id));
                Some(id)
            } else {
                None
            };
            Ok(Hold {
                id,
                events: events.clone(),
            })
        }
    });
    let (actor, commands) = actor(directory.path(), admission.clone())?;
    for hash in [Hash::new(b"first"), Hash::new(b"second")] {
        commands
            .try_send(update_command(hash, EntryState::Partial { size: Some(1) }))
            .anyerr()?;
    }
    drop(commands);
    actor.run().await;
    assert!(!admission.state.lock().unwrap().stopped);
    assert_eq!(
        admission.state.lock().unwrap().history.unwrap().entries,
        [2, 0, 0]
    );
    assert_eq!(*events.lock().unwrap(), [(true, 0), (true, 1), (false, 0)]);
    drop(admission);
    assert_eq!(
        *events.lock().unwrap(),
        [(true, 0), (true, 1), (false, 0), (false, 1)]
    );
    Ok(())
}

#[tokio::test]
async fn history_waits_for_native_commit_and_real_sidefile_commit() -> Result<()> {
    let directory = tempfile::tempdir().anyerr()?;
    let admission = Admission::new(|_| Ok(()));
    let (actor, commands) = actor(directory.path(), admission.clone())?;
    let data = Bytes::from_static(b"exact private commit history witness");
    let hash = Hash::new(&data);
    commands
        .try_send(update_command(
            hash,
            EntryState::Partial {
                size: Some(u64::try_from(data.len()).anyerr()?),
            },
        ))
        .anyerr()?;
    let mut actor = Box::pin(actor.run());
    assert!(poll_fn(|cx| Poll::Ready(actor.as_mut().poll(cx)))
        .await
        .is_pending());
    let before = admission.state.lock().unwrap().history.unwrap();
    assert_eq!(before.entries, [1, 0, 0]);
    let paths = crate::store::fs::options::PathOptions::new(directory.path());
    std::fs::create_dir_all(&paths.data_path).anyerr()?;
    let sizes = paths.sizes_path(&hash);
    let bitfield = paths.bitfield_path(&hash);
    std::fs::write(&sizes, b"retained partial sizes").anyerr()?;
    std::fs::write(&bitfield, b"retained partial bitfield").anyerr()?;
    let (native_entered, mut native_observed) = tokio::sync::oneshot::channel();
    let (native_release, native_released) = tokio::sync::oneshot::channel();
    let (files_entered, mut files_observed) = tokio::sync::oneshot::channel();
    let (files_release, files_released) = tokio::sync::oneshot::channel();
    admission.state.lock().unwrap().commit_gates = [
        Some(CommitGate {
            entered: native_entered,
            release: native_released,
        }),
        Some(CommitGate {
            entered: files_entered,
            release: files_released,
        }),
    ];
    commands
        .try_send(update_command(
            hash,
            EntryState::Complete {
                data_location: DataLocation::Inline(data.clone()),
                outboard_location: OutboardLocation::NotNeeded,
            },
        ))
        .anyerr()?;
    assert!(poll_fn(|cx| Poll::Ready(actor.as_mut().poll(cx)))
        .await
        .is_pending());
    native_observed.try_recv().anyerr()?;
    assert_eq!(admission.state.lock().unwrap().history, Some(before));
    assert!(sizes.exists() && bitfield.exists());
    native_release.send(()).unwrap();
    assert!(poll_fn(|cx| Poll::Ready(actor.as_mut().poll(cx)))
        .await
        .is_pending());
    files_observed.try_recv().anyerr()?;
    assert_eq!(admission.state.lock().unwrap().history, Some(before));
    assert!(sizes.exists() && bitfield.exists());
    let committed_bytes = std::fs::read(directory.path().join("blobs.db")).anyerr()?;
    assert!(committed_bytes
        .windows(data.len())
        .any(|bytes| bytes == data));
    files_release.send(()).unwrap();
    drop(commands);
    actor.await;
    assert!(!sizes.exists() && !bitfield.exists());
    assert_eq!(
        admission.state.lock().unwrap().history.unwrap().entries,
        [2, 1, 0]
    );
    assert!(!admission.state.lock().unwrap().stopped);
    let db = redb::Database::open(directory.path().join("blobs.db")).anyerr()?;
    let tx = db.begin_read().anyerr()?;
    let tables = ReadOnlyTables::new(&tx).anyerr()?;
    let actual = tables.inline_data().get(hash).anyerr()?.unwrap();
    assert_eq!(actual.value(), data.as_ref());
    Ok(())
}

fn batch() -> SealedBatch {
    SealedBatch {
        commands: Vec::with_capacity(1),
        counts: Counts::default(),
        _storage_hold: Box::new(()),
    }
}

fn update(state: EntryState<Bytes>) -> NonTopLevelCommand {
    NonTopLevelCommand::ReadWrite(ReadWriteCommand::Update(Update {
        hash: Hash::EMPTY,
        state,
        tx: None,
        span: tracing::Span::none(),
    }))
}

#[test]
fn actual_inline_values_expand_to_three_paid_points() -> Result<()> {
    let mut batch = batch();
    batch.push(update(EntryState::Complete {
        data_location: DataLocation::Inline(Bytes::from_static(b"abc")),
        outboard_location: OutboardLocation::Inline(Bytes::from_static(b"12345")),
    }))?;
    assert_eq!(batch.counts.inserts, [1, 1, 1]);
    assert_eq!(batch.counts.inline_bytes, 8);
    assert_eq!(batch.counts.commands, 1);
    Ok(())
}

#[test]
fn external_and_nondefault_inline_shape_never_enter_the_batch() {
    let mut batch = batch();
    assert!(batch
        .push(update(EntryState::Complete {
            data_location: DataLocation::External(vec!["outside".into()], 1),
            outboard_location: OutboardLocation::NotNeeded,
        }))
        .is_err());
    assert!(batch.commands.is_empty());
    assert!(batch
        .push(update(EntryState::Complete {
            data_location: DataLocation::Inline(Bytes::from(vec![0; 16 * 1024 + 1])),
            outboard_location: OutboardLocation::NotNeeded,
        }))
        .is_err());
    assert!(batch.commands.is_empty());
}

#[test]
fn page_buffers_charge_geometry_and_old_new_arcs_not_soft_cache_size() -> Result<()> {
    let small = page_buffers(39, 16)?;
    let larger = page_buffers(78, 16)?;
    assert!(small > 2 * 39 * 16 * 4096);
    assert!(larger > small);
    assert!(page_buffers(u64::MAX, 16).is_err());
    assert!(page_buffers(39, u64::MAX).is_err());
    Ok(())
}
