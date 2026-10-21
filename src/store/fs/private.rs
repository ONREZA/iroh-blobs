//! Explicit private startup and raw receive admission. These resource counts are
//! inputs to the NAS envelope, not a complete engine heap or RSS certificate.
use std::{
    fs::OpenOptions,
    mem::size_of,
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    sync::Arc,
};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use bao_tree::{io::BaoContentItem, ChunkRanges};
use iroh::endpoint::Connection;
use n0_error::{anyerr, Result, StdResultExt};
use tokio::{
    runtime::Handle,
    sync::{oneshot, Semaphore},
};

use super::{
    meta::raw_outboard_size,
    options::{InlineOptions, Options},
    FsStore,
};
use crate::{
    get::{fsm, Stats},
    protocol::{ChunkRangesSeq, GetRequest},
    Hash,
};

pub use super::meta::private::{
    Admission as NativeAdmission, BatchRequirements, Claim as NativeClaim,
};

/// Native topology selected and funded by the trusted nonproduction caller.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeProfile {
    pub worker_threads: NonZeroUsize,
    pub blocking_threads: NonZeroUsize,
    pub thread_stack_bytes: NonZeroUsize,
    pub queue_capacity: NonZeroUsize,
    pub receive_slots: NonZeroUsize,
}

/// Requested stacks and the four store/metadata mailboxes. Entity/stream
/// inboxes, Tokio and database allocations are separate envelope contributors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StartupRequirements {
    pub thread_count: usize,
    pub requested_stack_bytes: usize,
    pub queue_count: usize,
    pub queue_slots: usize,
    pub receive_slots: usize,
}

impl RuntimeProfile {
    pub fn requirements(self) -> Result<StartupRequirements> {
        if self.queue_capacity.get() > Semaphore::MAX_PERMITS
            || self.receive_slots.get() > Semaphore::MAX_PERMITS
        {
            return Err(anyerr!(
                "native queue topology exceeds semaphore representation"
            ));
        }
        let thread_count = self
            .worker_threads
            .get()
            .checked_add(self.blocking_threads.get())
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| anyerr!("native thread count overflow"))?;
        Ok(StartupRequirements {
            thread_count,
            requested_stack_bytes: thread_count
                .checked_mul(self.thread_stack_bytes.get())
                .ok_or_else(|| anyerr!("native stack request overflow"))?,
            queue_count: 4,
            queue_slots: self
                .queue_capacity
                .get()
                .checked_mul(4)
                .ok_or_else(|| anyerr!("native queue count overflow"))?,
            receive_slots: self.receive_slots.get(),
        })
    }
}

pub(super) struct RuntimeStartup {
    pub profile: RuntimeProfile,
    pub handle: oneshot::Sender<NativeHandle>,
    pub admission: Option<NativeAdmission>,
}

pub(super) struct NativeHandle {
    pub runtime: Handle,
    #[cfg(target_os = "linux")]
    pub metadata: super::meta::Db,
}

mod reconstruction;
#[cfg(target_os = "linux")]
pub use reconstruction::MetadataSeed;
/// Raw authenticated transport handle; no generic store access is exposed.
pub type RawConnection = iroh::endpoint::Connection;

/// Authorized immutable identity supplied by NAS; it does not grant reuse.
#[derive(Clone, Copy, Debug)]
pub struct ExpectedBlob {
    pub hash: Hash,
    pub size: u64,
}

/// Source-bound full-response importer payload and vector inputs. The native
/// batch is not bounded by the capacity-one decoder channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceiveRequirements {
    pub data_bytes: u64,
    pub outboard_bytes: u64,
    pub maximum_items: u64,
    pub batch_backing_request_bytes: u64,
}

impl ExpectedBlob {
    pub fn requirements(self) -> Result<ReceiveRequirements> {
        if self.size == 0 && self.hash != Hash::EMPTY {
            return Err(anyerr!("nonempty hash with zero authenticated size"));
        }
        let outboard_bytes = raw_outboard_size(self.size);
        // One parent per outboard pair and at most one leaf per BLAKE3
        // chunk, including the authenticated empty root's single zero-byte leaf.
        let leaves = (self.size / 1024)
            .checked_add(u64::from(!self.size.is_multiple_of(1024)))
            .ok_or_else(|| anyerr!("receive leaf count overflow"))?;
        let maximum_items = leaves
            .max(1)
            .checked_add(outboard_bytes / 64)
            .ok_or_else(|| anyerr!("receive item count overflow"))?;
        // Vec::push doubles backing capacity; count the old and new requests
        // during growth rather than assuming len equals allocated capacity.
        let capacity = maximum_items
            .max(4)
            .checked_next_power_of_two()
            .ok_or_else(|| anyerr!("receive vector capacity overflow"))?;
        let batch_backing_request_bytes = capacity
            .checked_mul(2)
            .and_then(|n| n.checked_mul(size_of::<BaoContentItem>() as u64))
            .ok_or_else(|| anyerr!("receive vector allocation overflow"))?;
        if batch_backing_request_bytes > isize::MAX as u64 {
            return Err(anyerr!("receive vector exceeds addressable allocation"));
        }
        Ok(ReceiveRequirements {
            data_bytes: self.size,
            outboard_bytes,
            maximum_items,
            batch_backing_request_bytes,
        })
    }
}

/// No generic Store/Deref escape: this workload exposes only one-hash receives.
/// Full native heap/physical funding remains the caller's prerequisite.
pub struct PrivateFsStore {
    store: FsStore,
    runtime: Handle,
    receives: Arc<Semaphore>,
    #[cfg(target_os = "linux")]
    metadata: super::meta::Db,
    #[cfg(target_os = "linux")]
    paths: super::options::PathOptions,
    owner: Option<std::thread::JoinHandle<()>>,
}

impl PrivateFsStore {
    /// Fund before native channels, threads, filesystem or database allocation.
    /// The returned custody is retained by the native owner through retirement.
    pub async fn open_fresh<C: Send + 'static>(
        db_path: PathBuf,
        options: Options,
        profile: RuntimeProfile,
        fund: impl FnOnce(StartupRequirements) -> Result<C>,
    ) -> Result<Self> {
        Self::open_fresh_inner(db_path, options, profile, None, fund).await
    }

    /// Bind the actual private metadata actor to paid sealed batches. The
    /// startup and receive claims still require the trusted complete envelope.
    pub async fn open_fresh_with_admission<C: Send + 'static>(
        db_path: PathBuf,
        options: Options,
        profile: RuntimeProfile,
        admission: NativeAdmission,
        fund: impl FnOnce(StartupRequirements) -> Result<C>,
    ) -> Result<Self> {
        Self::open_fresh_inner(db_path, options, profile, Some(admission), fund).await
    }

    async fn open_fresh_inner<C: Send + 'static>(
        db_path: PathBuf,
        options: Options,
        profile: RuntimeProfile,
        admission: Option<NativeAdmission>,
        fund: impl FnOnce(StartupRequirements) -> Result<C>,
    ) -> Result<Self> {
        let defaults = InlineOptions::default();
        if options.gc.is_some()
            || options.inline.max_data_inlined != defaults.max_data_inlined
            || options.inline.max_outboard_inlined != defaults.max_outboard_inlined
        {
            return Err(anyerr!(
                "private fresh workload requires existing inline layout and NAS-owned GC"
            ));
        }
        let requirements = profile.requirements()?;
        let custody = fund(requirements)?;
        if options.path.data_path.try_exists()? || options.path.temp_path.try_exists()? {
            return Err(anyerr!(
                "private fresh workload requires fresh data and temporary paths"
            ));
        }
        // A private protected parent is a provisioning prerequisite. Never
        // reinterpret an existing native DB as a checked fresh lineage.
        let mut seed_options = OpenOptions::new();
        seed_options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        seed_options.mode(0o600);
        let seed = seed_options.open(&db_path)?;
        let (handle_tx, handle_rx) = oneshot::channel();
        let receives = Arc::new(Semaphore::new(profile.receive_slots.get()));
        #[cfg(target_os = "linux")]
        let paths = options.path.clone();
        let (store, ready, owner) = FsStore::start_with_profile(
            db_path,
            options,
            (custody, seed, admission.clone()),
            Some(RuntimeStartup {
                profile,
                handle: handle_tx,
                admission,
            }),
        )?;
        ready.await.anyerr()??;
        let native = handle_rx.await.anyerr()?;
        Ok(Self {
            store,
            runtime: native.runtime,
            receives,
            #[cfg(target_os = "linux")]
            metadata: native.metadata,
            #[cfg(target_os = "linux")]
            paths,
            owner: Some(owner),
        })
    }

    /// Fund the complete receive before opening the protocol/body. Cancelling
    /// this waiter detaches the native-owned job, retaining its slot and guard.
    pub async fn receive<C: Send + 'static>(
        &self,
        connection: Connection,
        expected: ExpectedBlob,
        fund: impl FnOnce(ReceiveRequirements) -> Result<C>,
    ) -> Result<Stats> {
        let slot = self
            .receives
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyerr!("private receive admission unavailable"))?;
        let custody = fund(expected.requirements()?)?;
        let store = self.store.clone();
        self.runtime
            .spawn(async move {
                let _slot = slot;
                let _custody = custody;
                receive_raw(&store, connection, expected).await
            })
            .await
            .anyerr()?
    }
}

async fn receive_raw(
    store: &FsStore,
    connection: Connection,
    expected: ExpectedBlob,
) -> Result<Stats> {
    let request = GetRequest::new(
        expected.hash,
        ChunkRangesSeq::from_ranges([ChunkRanges::all()]),
    );
    let connected = fsm::start(connection, request, Default::default())
        .next()
        .await?;
    let fsm::ConnectedNext::StartRoot(root) = connected.next().await? else {
        return Err(anyerr!("expected one raw root"));
    };
    let (mut content, size) = root.next().next().await?;
    if size != expected.size || content.hash() != expected.hash {
        return Err(anyerr!("raw response differs from authenticated member"));
    }
    let end = if let Some(size) = NonZeroU64::new(size) {
        let handle = store.import_bao(expected.hash, size, 1).await?;
        let tx = tokio::sync::mpsc::Sender::try_from(handle.tx)
            .map_err(|_| anyerr!("expected native local import channel"))?;
        if tx.max_capacity() != 1 {
            return Err(anyerr!("native import channel lost its bound"));
        }
        let write = async move {
            loop {
                match content.next().await {
                    fsm::BlobContentNext::More((next, item)) => {
                        tx.send(item?).await.anyerr()?;
                        content = next;
                    }
                    fsm::BlobContentNext::Done(end) => {
                        drop(tx);
                        break Ok::<_, n0_error::AnyError>(end);
                    }
                }
            }
        };
        let complete = async move {
            handle.rx.await.anyerr()??;
            Ok::<_, n0_error::AnyError>(())
        };
        let (end, imported) = tokio::join!(write, complete);
        imported?;
        end?
    } else {
        content.drain().await?
    };
    let fsm::EndBlobNext::Closing(closing) = end.next() else {
        return Err(anyerr!("unexpected child in raw response"));
    };
    Ok(closing.next().await?)
}

#[cfg(test)]
mod tests;
