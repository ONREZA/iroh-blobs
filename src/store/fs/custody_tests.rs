use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    fs::{File, OpenOptions, TryLockError},
    future::{pending, poll_fn, Future},
    io,
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{mpsc, Arc, Condvar, LazyLock, Mutex},
    task::Poll,
    time::Duration,
};

use n0_future::FutureExt as _;
use testresult::TestResult;
use tokio::{sync::oneshot, time::timeout};

use super::{startup_tests::BlockingGc, FsStore, Options};
use crate::api::Store;

const DEADLINE: Duration = Duration::from_secs(5);

// Cell deliberately makes this owned guard Send but not Sync; it is not Clone.
struct Custody {
    file: File,
    worker_exited: Option<oneshot::Receiver<()>>,
    report: Arc<Completion>,
    _not_sync: Cell<()>,
}

impl Custody {
    fn new(
        path: &Path,
        worker_exited: Option<oneshot::Receiver<()>>,
    ) -> io::Result<(Self, Arc<Completion>)> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        file.lock()?;
        let report = Arc::new(Completion::default());
        Ok((
            Self {
                file,
                worker_exited,
                report: report.clone(),
                _not_sync: Cell::new(()),
            },
            report,
        ))
    }
}

impl Drop for Custody {
    fn drop(&mut self) {
        let worker_exited = self
            .worker_exited
            .as_mut()
            .is_none_or(|exited| exited.try_recv().is_ok());
        let result = self.file.unlock().map(|()| worker_exited);
        *self.report.result.lock().unwrap() = Some(result);
        self.report.changed.notify_all();
    }
}

fn probe(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).write(true).open(path)
}

fn assert_locked(probe: &File) {
    assert!(matches!(probe.try_lock(), Err(TryLockError::WouldBlock)));
}

fn assert_released(probe: &File) -> io::Result<()> {
    probe.try_lock().map_err(io::Error::other)?;
    probe.unlock()
}

#[derive(Default)]
struct Completion {
    result: Mutex<Option<io::Result<bool>>>,
    changed: Condvar,
}

impl Completion {
    fn pending(&self) -> bool {
        self.result.lock().unwrap().is_none()
    }

    fn wait(&self) -> io::Result<()> {
        let (result, _) = self
            .changed
            .wait_timeout_while(self.result.lock().unwrap(), DEADLINE, |result| {
                result.is_none()
            })
            .unwrap();
        match result.as_ref() {
            Some(Ok(true)) => Ok(()),
            Some(Ok(false)) => Err(io::Error::other(
                "custody released before native worker exit",
            )),
            Some(Err(error)) => Err(io::Error::other(error.to_string())),
            None => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "native custody not released",
            )),
        }
    }
}

type Retirement = Pin<Box<dyn Future<Output = ()> + Send>>;
type Outcome = std::thread::Result<TestResult<()>>;

struct Fixture {
    directory: Option<tempfile::TempDir>,
    report: Arc<Completion>,
}

impl Fixture {
    fn new(directory: tempfile::TempDir, report: Arc<Completion>) -> Self {
        Self {
            directory: Some(directory),
            report,
        }
    }

    fn finish(mut self, outcome: Outcome, cleanup: io::Result<()>) -> TestResult<()> {
        // Native completion is independent of the test's Tokio runtime after parent join.
        let completed = self.report.wait();
        let cleanup = cleanup.and(completed);
        if cleanup.is_ok() {
            drop(self.directory.take());
        }
        match outcome {
            Err(panic) => resume_unwind(panic),
            Ok(result) => {
                cleanup?;
                result
            }
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(directory) = self.directory.take() {
            eprintln!(
                "retaining unretired native fixture: {}",
                directory.keep().display()
            );
        }
    }
}

async fn stop_task<T>(task: &mut tokio::task::JoinHandle<T>, joined: bool) -> io::Result<()> {
    if !joined {
        task.abort();
        let result = timeout(DEADLINE, task).await.map_err(io::Error::other)?;
        drop(result);
    }
    Ok(())
}

async fn retire(retired: Option<Retirement>) -> io::Result<()> {
    if let Some(retired) = retired {
        timeout(DEADLINE, retired).await.map_err(io::Error::other)?;
    }
    Ok(())
}

struct StartupGate {
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
    exited: oneshot::Sender<()>,
}

static STARTUP_GATES: LazyLock<Mutex<HashMap<PathBuf, StartupGate>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

struct ThreadExit(Option<oneshot::Sender<()>>);

impl Drop for ThreadExit {
    fn drop(&mut self) {
        if let Some(exited) = self.0.take() {
            exited.send(()).ok();
        }
    }
}

pub(super) async fn startup_gate(path: &Path) {
    let gate = STARTUP_GATES.lock().unwrap().remove(path);
    if let Some(gate) = gate {
        tokio::task::spawn_blocking(move || {
            thread_local! {
                static EXIT: RefCell<Option<ThreadExit>> = const { RefCell::new(None) };
            }
            EXIT.with(|exit| *exit.borrow_mut() = Some(ThreadExit(Some(gate.exited))));
            gate.entered.send(()).ok();
            gate.release.blocking_recv().ok();
        })
        .await
        .expect("startup gate worker panicked");
    }
}

struct StartupBlock {
    path: PathBuf,
    entered: oneshot::Receiver<()>,
    release: Option<oneshot::Sender<()>>,
    exited: Option<oneshot::Receiver<()>>,
}

impl StartupBlock {
    fn new(path: PathBuf) -> Self {
        let (entered, received) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let (exited, exit_received) = oneshot::channel();
        assert!(STARTUP_GATES
            .lock()
            .unwrap()
            .insert(
                path.clone(),
                StartupGate {
                    entered,
                    release: released,
                    exited,
                },
            )
            .is_none());
        Self {
            path,
            entered: received,
            release: Some(release),
            exited: Some(exit_received),
        }
    }

    fn release(&mut self) {
        drop(self.release.take());
    }
}

impl Drop for StartupBlock {
    fn drop(&mut self) {
        // Closing release also unblocks native work on every failed test path.
        self.release();
        STARTUP_GATES.lock().unwrap().remove(&self.path);
    }
}

#[test]
fn never_polled_load_releases_custody_without_starting_native_work() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("store");
    let lock_path = dir.path().join("custody.lock");
    let (custody, report) = Custody::new(&lock_path, None)?;
    let probe = probe(&lock_path)?;
    let load =
        FsStore::load_with_opts_and_custody(root.join("blobs.db"), Options::new(&root), custody);
    assert_locked(&probe);
    assert!(!root.exists());
    drop(load);
    report.wait()?;
    assert_released(&probe)?;
    assert!(!root.exists());
    Ok(())
}

#[tokio::test]
async fn converted_store_keeps_native_custody_until_shutdown() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let lock_path = dir.path().join("custody.lock");
    let db_path = dir.path().join("blobs.db");
    let options = Options::new(dir.path());
    let (custody, report) = Custody::new(&lock_path, None)?;
    let probe = probe(&lock_path)?;
    let fixture = Fixture::new(dir, report.clone());
    let mut retired = None;
    let outcome = AssertUnwindSafe(async {
        let store = timeout(
            DEADLINE,
            FsStore::load_with_opts_and_custody(db_path, options, custody),
        )
        .await??;
        retired = Some(Box::pin(store.wait_retired()) as Retirement);
        let converted: Store = store.into();
        assert_locked(&probe);
        timeout(DEADLINE, converted.shutdown()).await??;
        report.wait()?;
        assert_released(&probe)?;
        Ok(())
    })
    .catch_unwind()
    .await;
    let cleanup = retire(retired).await;
    fixture.finish(outcome, cleanup)
}

#[tokio::test]
async fn parent_abort_keeps_custody_while_native_blocking_work_runs() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let lock_path = dir.path().join("custody.lock");
    let db_path = dir.path().join("blobs.db");
    let (options, mut native) = BlockingGc::new(dir.path());
    let exited = std::mem::replace(&mut native.exited, oneshot::channel().1);
    let (custody, report) = Custody::new(&lock_path, Some(exited))?;
    let probe = probe(&lock_path)?;
    let fixture = Fixture::new(dir, report.clone());
    let (loaded, mut received) = oneshot::channel();
    let mut parent = tokio::spawn(async move {
        let store = FsStore::load_with_opts_and_custody(db_path, options, custody)
            .await
            .unwrap();
        loaded
            .send(Box::pin(store.wait_retired()) as Retirement)
            .ok();
        pending::<()>().await;
        drop(store);
    });
    let mut joined = false;
    let mut retired = None;
    let outcome = AssertUnwindSafe(async {
        retired = Some(timeout(DEADLINE, &mut received).await??);
        timeout(DEADLINE, native.entered.notified()).await?;
        parent.abort();
        let result = timeout(DEADLINE, &mut parent).await?;
        joined = true;
        assert!(result.unwrap_err().is_cancelled());
        assert_locked(&probe);
        assert!(report.pending());
        assert!(
            poll_fn(|cx| Poll::Ready(retired.as_mut().unwrap().as_mut().poll(cx)))
                .await
                .is_pending()
        );
        Ok(())
    })
    .catch_unwind()
    .await;
    native.release();
    let stopped = stop_task(&mut parent, joined).await;
    let cleanup = stopped.and(retire(retired).await);
    fixture.finish(outcome, cleanup)?;
    assert_released(&probe)?;
    Ok(())
}

#[test]
fn parent_runtime_drop_keeps_custody_while_native_blocking_work_runs() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let lock_path = dir.path().join("custody.lock");
    let db_path = dir.path().join("blobs.db");
    let (options, mut native) = BlockingGc::new(dir.path());
    let exited = std::mem::replace(&mut native.exited, oneshot::channel().1);
    let (custody, report) = Custody::new(&lock_path, Some(exited))?;
    let probe = probe(&lock_path)?;
    let fixture = Fixture::new(dir, report.clone());
    let mut parent = Some(
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?,
    );
    let mut retired = None;
    let mut dropper = None;
    let outcome = catch_unwind(AssertUnwindSafe(|| -> TestResult<()> {
        parent.as_ref().unwrap().block_on(async {
            let (loaded, received) = oneshot::channel();
            drop(tokio::spawn(async move {
                let store = FsStore::load_with_opts_and_custody(db_path, options, custody)
                    .await
                    .unwrap();
                loaded
                    .send(Box::pin(store.wait_retired()) as Retirement)
                    .ok();
                pending::<()>().await;
                drop(store);
            }));
            retired = Some(timeout(DEADLINE, received).await??);
            timeout(DEADLINE, native.entered.notified()).await?;
            Ok::<_, testresult::TestError>(())
        })?;
        let (dropped, received) = mpsc::channel();
        let runtime = parent.take().unwrap();
        dropper = Some(std::thread::spawn(move || {
            drop(runtime);
            dropped.send(()).ok();
        }));
        received.recv_timeout(DEADLINE)?;
        assert_locked(&probe);
        assert!(report.pending());
        Ok(())
    }));
    native.release();
    // Both early preparation failure and runtime-drop failure retire before fixture deletion.
    drop(parent.take());
    let joined = dropper.take().is_none_or(|dropper| dropper.join().is_ok());
    let observer = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build();
    let cleanup = observer.and_then(|observer| observer.block_on(retire(retired)));
    let cleanup = cleanup.and(if joined {
        Ok(())
    } else {
        Err(io::Error::other("runtime dropper panicked"))
    });
    fixture.finish(outcome, cleanup)?;
    assert_released(&probe)?;
    Ok(())
}

#[tokio::test]
async fn cancelled_load_keeps_custody_during_native_runtime_startup() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let lock_path = dir.path().join("custody.lock");
    let db_path = dir.path().join("blobs.db");
    let options = Options::new(dir.path());
    let mut startup = StartupBlock::new(options.path.data_path.clone());
    let (custody, report) = Custody::new(&lock_path, startup.exited.take())?;
    let probe = probe(&lock_path)?;
    let fixture = Fixture::new(dir, report.clone());
    let mut load = tokio::spawn(FsStore::load_with_opts_and_custody(
        db_path, options, custody,
    ));
    let mut joined = false;
    let outcome = AssertUnwindSafe(async {
        timeout(DEADLINE, &mut startup.entered).await??;
        load.abort();
        let result = timeout(DEADLINE, &mut load).await?;
        joined = true;
        assert!(result.unwrap_err().is_cancelled());
        assert_locked(&probe);
        assert!(report.pending());
        Ok(())
    })
    .catch_unwind()
    .await;
    startup.release();
    let cleanup = stop_task(&mut load, joined).await;
    fixture.finish(outcome, cleanup)?;
    assert_released(&probe)?;
    Ok(())
}

#[tokio::test]
async fn startup_error_releases_custody_after_native_workers_exit() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("root-file");
    std::fs::write(&root, b"not a directory")?;
    let lock_path = dir.path().join("custody.lock");
    let db_path = root.join("blobs.db");
    let options = Options::new(&root);
    let mut startup = StartupBlock::new(options.path.data_path.clone());
    let (custody, report) = Custody::new(&lock_path, startup.exited.take())?;
    let probe = probe(&lock_path)?;
    let fixture = Fixture::new(dir, report.clone());
    let mut load = tokio::spawn(FsStore::load_with_opts_and_custody(
        db_path, options, custody,
    ));
    let mut joined = false;
    let outcome = AssertUnwindSafe(async {
        timeout(DEADLINE, &mut startup.entered).await??;
        assert_locked(&probe);
        assert!(!load.is_finished());
        startup.release();
        let result = timeout(DEADLINE, &mut load).await?;
        joined = true;
        assert!(result?.is_err());
        report.wait()?;
        assert_released(&probe)?;
        Ok(())
    })
    .catch_unwind()
    .await;
    startup.release();
    let cleanup = stop_task(&mut load, joined).await;
    fixture.finish(outcome, cleanup)
}

#[tokio::test]
async fn startup_panic_releases_custody_after_native_workers_exit() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let lock_path = dir.path().join("custody.lock");
    let options = Options::new(dir.path());
    let mut startup = StartupBlock::new(options.path.data_path.clone());
    let (custody, report) = Custody::new(&lock_path, startup.exited.take())?;
    let probe = probe(&lock_path)?;
    let fixture = Fixture::new(dir, report.clone());
    // A root DB path reaches the existing missing-parent panic inside Actor::new.
    let mut load = tokio::spawn(FsStore::load_with_opts_and_custody(
        PathBuf::from("/"),
        options,
        custody,
    ));
    let mut joined = false;
    let outcome = AssertUnwindSafe(async {
        timeout(DEADLINE, &mut startup.entered).await??;
        assert_locked(&probe);
        assert!(!load.is_finished());
        startup.release();
        let result = timeout(DEADLINE, &mut load).await?;
        joined = true;
        let error = result?.unwrap_err();
        assert!(error.to_string().contains("file store actor panicked"));
        assert!(!report.pending());
        assert_released(&probe)?;
        Ok(())
    })
    .catch_unwind()
    .await;
    startup.release();
    let cleanup = stop_task(&mut load, joined).await;
    fixture.finish(outcome, cleanup)
}

#[tokio::test]
async fn held_native_work_is_retired_before_original_test_panic_is_resumed() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().to_path_buf();
    let options = Options::new(&root);
    let mut startup = StartupBlock::new(options.path.data_path.clone());
    let (custody, report) = Custody::new(&root.join("custody.lock"), startup.exited.take())?;
    let fixture = Fixture::new(dir, report.clone());
    let mut load = tokio::spawn(FsStore::load_with_opts_and_custody(
        root.join("blobs.db"),
        options,
        custody,
    ));
    let outcome: Outcome = AssertUnwindSafe(async {
        timeout(DEADLINE, &mut startup.entered).await??;
        assert!(report.pending());
        panic!("original held-native-work test panic");
    })
    .catch_unwind()
    .await;
    startup.release();
    let cleanup = stop_task(&mut load, false).await;
    let panic = catch_unwind(AssertUnwindSafe(|| fixture.finish(outcome, cleanup))).unwrap_err();
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"original held-native-work test panic")
    );
    report.wait()?;
    assert!(
        !root.exists(),
        "fixture was not removed after native retirement"
    );
    Ok(())
}
