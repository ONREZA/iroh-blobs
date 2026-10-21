//! Explicit fresh derived metadata over already authenticated retained payloads.
//! Directory/file descriptors are capabilities supplied by the fenced owner;
//! generated native names never establish payload authority.
use std::{
    fs::File,
    io::{Seek, Write},
};
#[cfg(target_os = "linux")]
use std::{
    io::Read,
    os::{fd::AsRawFd, unix::fs::MetadataExt},
    path::PathBuf,
    sync::Arc,
};

#[cfg(target_os = "linux")]
use bao_tree::{
    io::{outboard::PreOrderOutboard, sync::CreateOutboard},
    BaoTree,
};
#[cfg(target_os = "linux")]
use bytes::Bytes;
use n0_error::{anyerr, Result, StdResultExt};
#[cfg(target_os = "linux")]
use tokio::sync::{oneshot, Semaphore};

use super::{ExpectedBlob, PrivateFsStore, ReceiveRequirements};
#[cfg(target_os = "linux")]
use super::{NativeAdmission, RuntimeProfile, RuntimeStartup, StartupRequirements};
#[cfg(target_os = "linux")]
use crate::{
    store::{
        fs::{
            entry_state::{DataLocation, EntryState, OutboardLocation},
            options::{Options, PathOptions},
        },
        IROH_BLOCK_SIZE,
    },
    Hash,
};

/// Only an empty, independently create_new metadata inode is accepted. The
/// permanent native epoch owner must have stopped every previous writer first.
/// Linux procfs resolves these retained descriptor capabilities.
#[cfg(target_os = "linux")]
pub struct MetadataSeed {
    pub database: File,
    pub payloads: File,
    pub temporary: File,
}

#[cfg(target_os = "linux")]
impl MetadataSeed {
    fn paths(&self) -> Result<(PathBuf, PathOptions)> {
        let db = self.database.metadata()?;
        let data = self.payloads.metadata()?;
        let temporary = self.temporary.metadata()?;
        if !db.is_file()
            || db.len() != 0
            || db.nlink() != 1
            || db.mode() & 0o077 != 0
            || !data.is_dir()
            || !temporary.is_dir()
            || data.mode() & 0o077 != 0
            || temporary.mode() & 0o077 != 0
            || data.dev() != temporary.dev()
        {
            return Err(anyerr!("invalid trusted fresh metadata descriptors"));
        }
        let path = |file: &File| PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()));
        Ok((
            path(&self.database),
            PathOptions {
                data_path: path(&self.payloads),
                temp_path: path(&self.temporary),
            },
        ))
    }
}

impl PrivateFsStore {
    /// Rebuild only: this never opens or repairs a prior native database. The
    /// opaque custody retains the caller's epoch, physical credit and pool proof.
    #[cfg(target_os = "linux")]
    pub async fn open_fresh_metadata<C: Send + 'static>(
        seed: MetadataSeed,
        profile: RuntimeProfile,
        admission: NativeAdmission,
        fund: impl FnOnce(StartupRequirements) -> Result<C>,
    ) -> Result<Self> {
        let custody = fund(profile.requirements()?)?;
        let (database, paths) = seed.paths()?;
        let mut options = Options::new(&PathBuf::new());
        options.path = paths.clone();
        let (handle, native) = oneshot::channel();
        let receives = Arc::new(Semaphore::new(profile.receive_slots.get()));
        let (store, ready, owner) = super::super::FsStore::start_with_profile(
            database,
            options,
            (custody, seed, admission.clone()),
            Some(RuntimeStartup {
                profile,
                handle,
                admission: Some(admission),
            }),
        )?;
        ready.await.anyerr()??;
        let native = native.await.anyerr()?;
        Ok(Self {
            store,
            runtime: native.runtime,
            metadata: native.metadata,
            receives,
            paths,
            owner: Some(owner),
        })
    }

    /// Decode directly into the caller's durable ingress inode. There is no
    /// native database/payload write before this entire authenticated file exists.
    /// The caller retains its creation stamp and records complete custody before
    /// invoking import_verified. Cancellation retains the supplied custody in the
    /// native task until its last write and fsync actually complete.
    pub async fn receive_to_file<C: Send + 'static>(
        &self,
        connection: iroh::endpoint::Connection,
        expected: ExpectedBlob,
        payload: File,
        fund: impl FnOnce(ReceiveRequirements) -> Result<C>,
    ) -> Result<(File, crate::get::Stats)> {
        let slot = self
            .receives
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyerr!("private receive admission unavailable"))?;
        let requirements = expected.requirements()?;
        let custody = fund(requirements)?;
        self.runtime
            .spawn(async move {
                let _slot = slot;
                let _custody = custody;
                let (payload, stats) = receive_body(
                    connection,
                    expected,
                    Some(payload),
                    requirements.maximum_items,
                )
                .await?;
                Ok((
                    payload.ok_or_else(|| anyerr!("retained ingress sink missing"))?,
                    stats,
                ))
            })
            .await
            .anyerr()?
    }

    /// Verify a required raw response without rewriting a shared retained inode.
    /// The same complete-response admission, slot and native-owned task apply;
    /// cancelling its waiter never releases custody before decoder retirement.
    pub async fn verify_raw<C: Send + 'static>(
        &self,
        connection: iroh::endpoint::Connection,
        expected: ExpectedBlob,
        fund: impl FnOnce(ReceiveRequirements) -> Result<C>,
    ) -> Result<crate::get::Stats> {
        let slot = self
            .receives
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyerr!("private receive admission unavailable"))?;
        let requirements = expected.requirements()?;
        let custody = fund(requirements)?;
        self.runtime
            .spawn(async move {
                let _slot = slot;
                let _custody = custody;
                let (_, stats) =
                    receive_body(connection, expected, None, requirements.maximum_items).await?;
                Ok(stats)
            })
            .await
            .anyerr()?
    }

    /// A verified retained inode remains independent of the derived DB even for
    /// inline values. Existing inline layout and native names are preserved.
    /// An owned outboard descriptor is required exactly when it exceeds 16 KiB;
    /// the caller stamps it before this function's first write.
    #[cfg(target_os = "linux")]
    pub async fn import_verified<C: Send + 'static>(
        &self,
        expected: ExpectedBlob,
        payload: File,
        outboard: Option<File>,
        fund: impl FnOnce(ReceiveRequirements) -> Result<C>,
    ) -> Result<()> {
        let slot = self
            .receives
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyerr!("private import admission unavailable"))?;
        let custody = fund(expected.requirements()?)?;
        let paths = self.paths.clone();
        let metadata = self.metadata.clone();
        let store = self.store.clone();
        self.runtime
            .spawn(async move {
                let _slot = slot;
                let _custody = custody;
                let state = reconstruct(&paths, expected, payload, outboard)?;
                metadata.set(expected.hash, state).await?;
                // A Set reply precedes its enclosing commit. This queue barrier
                // waits for both native commit and the sidefile transaction.
                store.sync_db().await.anyerr()?;
                Ok::<_, n0_error::AnyError>(())
            })
            .await
            .anyerr()?
    }

    /// No mutation is started by the barrier; it follows previously queued work.
    pub async fn sync(&self) -> Result<()> {
        self.store.sync_db().await.anyerr()
    }

    /// A join witnesses retirement, including failure. It is never a clean-close
    /// proof or permission to open/repair the old database.
    pub async fn retire(mut self) -> Result<()> {
        let result = self.store.shutdown().await;
        if let Some(owner) = self.owner.take() {
            owner
                .join()
                .map_err(|_| anyerr!("native owner thread panicked"))?;
        }
        result.anyerr()
    }
}

#[cfg(target_os = "linux")]
fn exact_file(actual: &File, expected: &File) -> Result<()> {
    let a = actual.metadata()?;
    let e = expected.metadata()?;
    if !a.is_file() || a.dev() != e.dev() || a.ino() != e.ino() || a.nlink() != 1 {
        return Err(anyerr!("native name differs from authenticated descriptor"));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn reconstruct(
    paths: &PathOptions,
    expected: ExpectedBlob,
    mut payload: File,
    outboard: Option<File>,
) -> Result<EntryState<Bytes>> {
    if payload.metadata()?.len() != expected.size {
        return Err(anyerr!(
            "retained payload length differs from authenticated member"
        ));
    }
    exact_file(&File::open(paths.data_path(&expected.hash))?, &payload)?;
    payload.rewind()?;
    let outboard_bytes = super::super::meta::raw_outboard_size(expected.size);
    let tree = BaoTree::new(expected.size, IROH_BLOCK_SIZE);
    let empty = bao_tree::blake3::Hash::from_bytes([0; 32]);
    let outboard_location = if outboard_bytes <= 16 * 1024 {
        if outboard.is_some() {
            return Err(anyerr!("unexpected owned inline outboard"));
        }
        let mut native = PreOrderOutboard {
            tree,
            root: empty,
            data: Vec::new(),
        };
        native.init_from(&mut payload)?;
        if Hash::from(native.root) != expected.hash {
            return Err(anyerr!("retained payload digest mismatch"));
        }
        if native.data.len() as u64 != outboard_bytes {
            return Err(anyerr!("native outboard size mismatch"));
        }
        OutboardLocation::inline(Bytes::from(native.data))
    } else {
        let mut file = outboard.ok_or_else(|| anyerr!("missing funded owned outboard"))?;
        exact_file(&File::open(paths.outboard_path(&expected.hash))?, &file)?;
        // Reusing an already complete outboard preserves its allocated extent.
        // A partial differently sized inode is covered by the caller's full
        // reconstruction allowance before this truncation and first write.
        if file.metadata()?.len() != outboard_bytes {
            file.set_len(0)?;
        }
        let mut native = PreOrderOutboard {
            tree,
            root: empty,
            data: &mut file,
        };
        native.init_from(&mut payload)?;
        if Hash::from(native.root) != expected.hash {
            return Err(anyerr!("retained payload digest mismatch"));
        }
        file.sync_all()?;
        OutboardLocation::Owned
    };
    let data_location = if expected.size <= 16 * 1024 {
        payload.rewind()?;
        let mut data = vec![0; usize::try_from(expected.size).anyerr()?];
        payload.read_exact(&mut data)?;
        DataLocation::Inline(Bytes::from(data))
    } else {
        DataLocation::Owned(expected.size)
    };
    Ok(EntryState::Complete {
        data_location,
        outboard_location,
    })
}

async fn receive_body(
    connection: iroh::endpoint::Connection,
    expected: ExpectedBlob,
    mut payload: Option<File>,
    maximum_items: u64,
) -> Result<(Option<File>, crate::get::Stats)> {
    use crate::{
        get::fsm,
        protocol::{ChunkRangesSeq, GetRequest},
    };
    if payload
        .as_ref()
        .map(File::metadata)
        .transpose()?
        .is_some_and(|metadata| metadata.len() != 0)
    {
        return Err(anyerr!("retained ingress inode is not empty"));
    }
    let request = GetRequest::new(
        expected.hash,
        ChunkRangesSeq::from_ranges([bao_tree::ChunkRanges::all()]),
    );
    let connected = fsm::start(connection, request, Default::default())
        .next()
        .await?;
    let fsm::ConnectedNext::StartRoot(root) = connected.next().await? else {
        return Err(anyerr!("expected raw root"));
    };
    let (mut content, size) = root.next().next().await?;
    if size != expected.size || content.hash() != expected.hash {
        return Err(anyerr!("raw response differs from authenticated member"));
    }
    if let Some(payload) = &mut payload {
        payload.rewind()?;
    }
    let mut written = 0u64;
    let mut items = 0u64;
    let end = loop {
        match content.next().await {
            fsm::BlobContentNext::More((next, item)) => {
                items = items
                    .checked_add(1)
                    .ok_or_else(|| anyerr!("raw item count overflow"))?;
                if items > maximum_items {
                    return Err(anyerr!("raw response item bound exceeded"));
                }
                if let bao_tree::io::BaoContentItem::Leaf(leaf) = item? {
                    if leaf.offset != written {
                        return Err(anyerr!("raw response is not contiguous"));
                    }
                    written = written
                        .checked_add(leaf.data.len() as u64)
                        .ok_or_else(|| anyerr!("raw response length overflow"))?;
                    if written > expected.size {
                        return Err(anyerr!("raw response exceeds member"));
                    }
                    if let Some(payload) = &mut payload {
                        payload.write_all(&leaf.data)?;
                    }
                }
                content = next;
            }
            fsm::BlobContentNext::Done(end) => break end,
        }
    };
    if written != expected.size {
        return Err(anyerr!("raw response is incomplete"));
    }
    let fsm::EndBlobNext::Closing(closing) = end.next() else {
        return Err(anyerr!("unexpected raw child"));
    };
    let stats = closing.next().await?;
    if let Some(payload) = &payload {
        payload.sync_all()?;
    }
    Ok((payload, stats))
}
