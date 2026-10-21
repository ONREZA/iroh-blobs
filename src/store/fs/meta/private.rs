//! The fixed private actor's paid, sealed metadata boundary.
//! Generic FsStore callers keep their existing batching and command surface.
mod envelope;

use std::{
    alloc::Layout,
    fmt,
    mem::size_of,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use n0_error::{anyerr, Result, StdResultExt};
use redb::{ReadableDatabase, ReadableTableMetadata};

use super::{
    tables::{ReadOnlyTables, ReadableTables},
    Command, NonTopLevelCommand, ReadOnlyCommand, ReadWriteCommand,
};
use crate::store::fs::{
    entry_state::{DataLocation, EntryState, OutboardLocation},
    options::BatchOptions,
    util::PeekableReceiver,
};
use envelope::{Envelope, Family, History};

pub(super) async fn run_batch(
    admission: &Admission,
    db: &mut redb::Database,
    first: NonTopLevelCommand,
    number: u64,
    protected: &mut std::collections::HashSet<crate::Hash>,
    ds: &mut crate::store::fs::delete_set::DeleteHandle,
    source: (&mut PeekableReceiver<Command>, &BatchOptions),
) -> super::ActorResult<()> {
    let mut batch = admission.seal(first, source.0, source.1).await?;
    let estimate = admission.claim(db, &batch)?;
    if batch
        .commands
        .iter()
        .any(|command| matches!(command, NonTopLevelCommand::ReadWrite(_)))
    {
        let ftx = ds.begin_write();
        let tx = db.begin_write()?;
        let mut tables = super::Tables::new(&tx, &ftx)?;
        for command in batch.commands.drain(..) {
            super::Actor::handle_non_toplevel(
                protected,
                &mut tables,
                command,
                super::TxnNum::Write(number),
            )
            .await?;
        }
        drop(tables);
        #[cfg(test)]
        admission.pause_before_commit(CommitPhase::Native).await;
        tx.commit()?;
        #[cfg(test)]
        admission.pause_before_commit(CommitPhase::Files).await;
        ftx.commit();
        admission.committed(estimate)?;
    } else {
        let tx = db.begin_read()?;
        let tables = ReadOnlyTables::new(&tx)?;
        for command in batch.commands.drain(..) {
            let NonTopLevelCommand::ReadOnly(command) = command else {
                unreachable!()
            };
            super::Actor::handle_readonly(protected, &tables, command, super::TxnNum::Read(number))
                .await?;
        }
    }
    Ok(())
}

/// Inputs to the trusted receiver's complete resource claim. This exposes the
/// source-bound native page-buffer contributor separately from total heap/RSS.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchRequirements {
    pub commands: u64,
    pub inserted_points: [u64; 3],
    pub inline_value_bytes: u64,
    pub normal_page_requests: u64,
    pub tail_page_requests: u64,
    pub retired_user_handles: u64,
    pub usable_page_high_water: u64,
    pub region_high_water: u64,
    pub tracker_high_water: u64,
    pub maximum_page_order: u64,
    pub close_passes: u64,
    pub close_serialized_value_bytes: u64,
    pub page_buffer_requested_bytes: u64,
}

/// The batch vector is claimed before its first allocation. Every later native
/// batch claim is aggregate: it includes retained page/geometry high-water.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Claim {
    BatchStorage {
        slots: usize,
        requested_bytes: usize,
    },
    Batch(BatchRequirements),
}

type Hold = Box<dyn Send>;
type Fund = dyn Fn(Claim) -> Result<Hold> + Send + Sync;

struct State {
    history: Option<History>,
    paid: Option<BatchRequirements>,
    hold: Option<Hold>,
    stopped: bool,
    #[cfg(test)]
    commit_gates: [Option<tests::CommitGate>; 2],
}

#[cfg(test)]
enum CommitPhase {
    Native,
    Files,
}

/// A per-lineage authority supplied only to the private fresh constructor.
/// The constructor retains a clone in native-owner custody, through close and
/// runtime retirement. A claim covers requested live allocations, not RSS.
#[derive(Clone)]
pub struct Admission {
    fund: Arc<Fund>,
    state: Arc<Mutex<State>>,
}

impl fmt::Debug for Admission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrivateNativeAdmission")
            .finish_non_exhaustive()
    }
}

impl Admission {
    // DEPRECATED: exact native/sidefile commit interleaving hook; remove when
    // replaced by an equivalent causal history-advancement regression.
    #[cfg(test)]
    async fn pause_before_commit(&self, phase: CommitPhase) {
        let index = match phase {
            CommitPhase::Native => 0,
            CommitPhase::Files => 1,
        };
        let gate = self.state.lock().unwrap().commit_gates[index].take();
        if let Some(gate) = gate {
            gate.entered.send(()).ok();
            gate.release.await.ok();
        }
    }

    pub fn new<C: Send + 'static>(
        fund: impl Fn(Claim) -> Result<C> + Send + Sync + 'static,
    ) -> Self {
        Self {
            fund: Arc::new(move |claim| Ok(Box::new(fund(claim)?))),
            state: Arc::new(Mutex::new(State {
                history: None,
                paid: None,
                hold: None,
                stopped: false,
                #[cfg(test)]
                commit_gates: [None, None],
            })),
        }
    }

    /// Called once, after the fresh constructor's prepaid bootstrap. The FD
    /// creation fence and native owner are the seed authority; row counts alone
    /// would not certify an arbitrary reopened database.
    pub(super) fn bind_fresh(&self, db: &redb::Database) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyerr!("private admission poisoned"))?;
        if state.history.is_some() || state.stopped {
            return Err(anyerr!("private native seed was already bound or stopped"));
        }
        let tx = db.begin_read().anyerr()?;
        let tables = ReadOnlyTables::new(&tx).anyerr()?;
        if tables.blobs().len().anyerr()? != 0
            || tables.tags().len().anyerr()? != 0
            || tables.inline_data().len().anyerr()? != 0
            || tables.inline_outboard().len().anyerr()? != 0
        {
            return Err(anyerr!("private bootstrap is not empty"));
        }
        drop(tables);
        drop(tx);
        redb::private_heap::geometry(db)
            .ok_or_else(|| anyerr!("private native allocator unavailable"))?;
        state.history = Some(History::default());
        Ok(())
    }

    pub(super) async fn seal(
        &self,
        first: NonTopLevelCommand,
        cmds: &mut PeekableReceiver<Command>,
        options: &BatchOptions,
    ) -> Result<SealedBatch> {
        let slots = options.max_write_batch.max(1);
        let layout = Layout::array::<NonTopLevelCommand>(slots)
            .map_err(|_| anyerr!("private batch vector exceeds allocation domain"))?;
        let storage_hold = (self.fund)(Claim::BatchStorage {
            slots,
            requested_bytes: layout.size(),
        })?;
        let mut batch = SealedBatch {
            commands: Vec::with_capacity(slots),
            counts: Counts::default(),
            _storage_hold: storage_hold,
        };
        batch.push(first)?;
        let timeout = n0_future::time::sleep(options.max_write_duration);
        tokio::pin!(timeout);
        while batch.commands.len() < slots {
            let Some(command) = cmds.extract(Command::non_top_level, &mut timeout).await else {
                break;
            };
            batch.push(command)?;
        }
        Ok(batch)
    }

    /// No await, allocation or native mutation is between successful funding
    /// and returning the prospective state to the actor. Errors stop the lineage
    /// rather than advancing a successful descriptor across an unknown commit.
    pub(super) fn claim(&self, db: &redb::Database, batch: &SealedBatch) -> Result<Envelope> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyerr!("private admission poisoned"))?;
        if state.stopped {
            return Err(anyerr!("private native admission stopped"));
        }
        let history = state
            .history
            .ok_or_else(|| anyerr!("private native seed not bound"))?;
        let geometry = redb::private_heap::geometry(db)
            .ok_or_else(|| anyerr!("private native allocator unavailable"))?;
        if let Some(previous) = state.paid {
            if geometry.usable_pages > previous.usable_page_high_water
                || u64::from(geometry.regions) > previous.region_high_water
                || u64::from(geometry.tracker_regions) > previous.tracker_high_water
            {
                return Err(anyerr!(
                    "native geometry exceeded the retained private claim"
                ));
            }
        }
        let tx = db.begin_read().anyerr()?;
        let tables = ReadOnlyTables::new(&tx).anyerr()?;
        let actual = [
            tables.blobs().len().anyerr()?,
            tables.inline_data().len().anyerr()?,
            tables.inline_outboard().len().anyerr()?,
        ];
        if tables.tags().len().anyerr()? != 0
            || actual
                .into_iter()
                .zip(history.entries)
                .any(|(actual, bound)| actual > bound)
        {
            return Err(anyerr!("native root lengths exceeded the private lineage"));
        }
        drop(tables);
        drop(tx);
        let mut families = [Family::default(); 3];
        for (index, value_bytes) in [13, 16 * 1024, 16 * 1024].into_iter().enumerate() {
            families[index] = Family {
                entries: history.entries[index],
                inserts: batch.counts.inserts[index],
                leaf_pages: envelope::leaf_pages(value_bytes)?,
            };
        }
        let estimate = envelope::estimate(geometry, history, families)?;
        let mut requirements = requirements(&estimate, batch.counts)?;
        if let Some(previous) = state.paid {
            requirements.usable_page_high_water = requirements
                .usable_page_high_water
                .max(previous.usable_page_high_water);
            requirements.region_high_water = requirements
                .region_high_water
                .max(previous.region_high_water);
            requirements.tracker_high_water = requirements
                .tracker_high_water
                .max(previous.tracker_high_water);
            requirements.maximum_page_order = requirements
                .maximum_page_order
                .max(previous.maximum_page_order);
            requirements.page_buffer_requested_bytes = page_buffers(
                requirements.usable_page_high_water,
                requirements.maximum_page_order,
            )?;
        }
        let hold = (self.fund)(Claim::Batch(requirements))?;
        // Acquire replacement coverage before retiring the previous aggregate
        // claim. Native cache/bitmap capacity is never released on a shrink.
        state.hold = Some(hold);
        state.paid = Some(requirements);
        Ok(estimate)
    }

    pub(super) fn committed(&self, estimate: Envelope) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyerr!("private admission poisoned"))?;
        if state.stopped {
            return Err(anyerr!("private native admission stopped"));
        }
        state.history = Some(estimate.next);
        Ok(())
    }

    pub(super) fn stop(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.stopped = true;
        }
        // Keep the aggregate hold: an opaque native close error is not proof of
        // a clean seed or permission to release caches before retirement.
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Counts {
    commands: u64,
    inserts: [u64; 3],
    inline_bytes: u64,
}

pub(super) struct SealedBatch {
    pub commands: Vec<NonTopLevelCommand>,
    counts: Counts,
    _storage_hold: Hold,
}

impl SealedBatch {
    fn push(&mut self, command: NonTopLevelCommand) -> Result<()> {
        let state = match &command {
            NonTopLevelCommand::ReadOnly(ReadOnlyCommand::Get(_)) => None,
            NonTopLevelCommand::ReadWrite(ReadWriteCommand::Update(cmd)) => Some(&cmd.state),
            NonTopLevelCommand::ReadWrite(ReadWriteCommand::Set(cmd)) => Some(&cmd.state),
            _ => return Err(anyerr!("command is outside the private receive lineage")),
        };
        self.counts.commands = checked_add(self.counts.commands, 1)?;
        if let Some(state) = state {
            self.counts.inserts[0] = checked_add(self.counts.inserts[0], 1)?;
            if let EntryState::Complete {
                data_location,
                outboard_location,
            } = state
            {
                match data_location {
                    DataLocation::External(_, _) => {
                        return Err(anyerr!(
                            "external data is outside the private receive lineage"
                        ))
                    }
                    DataLocation::Inline(bytes) => self.inline(1, bytes)?,
                    DataLocation::Owned(_) => {}
                }
                if let OutboardLocation::Inline(bytes) = outboard_location {
                    self.inline(2, bytes)?;
                }
            }
        }
        self.commands.push(command);
        Ok(())
    }

    fn inline(&mut self, index: usize, bytes: &Bytes) -> Result<()> {
        if bytes.len() > 16 * 1024 {
            return Err(anyerr!("private inline value exceeds the existing layout"));
        }
        self.counts.inserts[index] = checked_add(self.counts.inserts[index], 1)?;
        self.counts.inline_bytes = checked_add(
            self.counts.inline_bytes,
            u64::try_from(bytes.len()).anyerr()?,
        )?;
        Ok(())
    }
}

fn checked_add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b)
        .ok_or_else(|| anyerr!("private batch count overflow"))
}

fn requirements(estimate: &Envelope, counts: Counts) -> Result<BatchRequirements> {
    let maximum_page_order = estimate.close_page_order.max(16);
    Ok(BatchRequirements {
        commands: counts.commands,
        inserted_points: counts.inserts,
        inline_value_bytes: counts.inline_bytes,
        normal_page_requests: estimate.normal_page_requests,
        tail_page_requests: estimate.tail_page_requests,
        retired_user_handles: estimate.retired_user_handles,
        usable_page_high_water: estimate.usable_page_high_water,
        region_high_water: estimate.region_high_water,
        tracker_high_water: estimate.tracker_high_water,
        maximum_page_order,
        close_passes: estimate.close_passes,
        close_serialized_value_bytes: estimate.close_serialized_value_bytes,
        page_buffer_requested_bytes: page_buffers(
            estimate.usable_page_high_water,
            maximum_page_order,
        )?,
    })
}

fn page_buffers(usable_pages: u64, largest_extent: u64) -> Result<u64> {
    let extent = usize::try_from(
        largest_extent
            .checked_mul(4096)
            .ok_or_else(|| anyerr!("native extent overflow"))?,
    )
    .anyerr()?;
    // ArcInner<[u8]> has the two atomic counters followed by the payload.
    // Fixed private V3 has no region headers: add the super-header offset.
    let arc = Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
        .extend(
            Layout::array::<u8>(extent)
                .map_err(|_| anyerr!("native page payload layout overflow"))?,
        )
        .map_err(|_| anyerr!("native page Arc layout overflow"))?
        .0
        .pad_to_align()
        .size();
    let offsets = checked_add(usable_pages, 1)?;
    // Both caches, every eligible offset, plus a read-miss transient. The fixed
    // private actor never exposes snapshots or concurrent native table writers.
    let buffers = checked_add(
        offsets
            .checked_mul(2)
            .ok_or_else(|| anyerr!("native buffer count overflow"))?,
        1,
    )?;
    buffers
        .checked_mul(u64::try_from(arc).anyerr()?)
        .ok_or_else(|| anyerr!("native page buffer request overflow"))
}

const _: () = assert!(size_of::<usize>() == size_of::<std::sync::atomic::AtomicUsize>());

#[cfg(test)]
mod tests;
