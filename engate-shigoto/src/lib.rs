//! engate-shigoto — wrap an engate Attach lifecycle as a shigoto
//! `RecordingJob` so cluster-wide work orchestration (mado launch,
//! kenshi pod-test, hiroba chat-attach, etc.) gets typed Job gates
//! for free.
//!
//! # Pattern
//!
//! 1. Operator declares an `AttachJob<Prod, Cons>` with a producer +
//!    consumer pair (mado + tear; kenshi + testpod; ...).
//! 2. `AttachJob::execute_body` drives the full engate lifecycle:
//!    `Attach::builder() → subscribe() → replay(history) → start_live()`
//!    and returns the live consumer wrapped in `AttachedHandle`.
//! 3. Downstream Jobs declare `AllUpstreamsTerminal`-style DAG edges
//!    against the AttachJob's `JobId`; the scheduler enforces the
//!    HistoryFetched + SubscriptionLive invariants implicitly via
//!    the FSM transition `Pending → Gated → Ready → Running →
//!    Succeeded` (RecordingJob can only emit Succeeded after the
//!    full attach completes — the typestate guarantees no halfway
//!    state is observable).
//!
//! # What you don't write
//!
//! - You don't write a "HistoryFetchedGate" or "SubscriptionLiveGate"
//!   as separate Gate impls. The typestate INSIDE engate-attach
//!   guarantees that an `AttachJob::execute_body` either returns
//!   `Ok(AttachedHandle)` (history fetched AND subscription live AND
//!   render-ready) or `Err(AttachError)` (terminal failure). The
//!   scheduler's standard `AllUpstreamsTerminal` gate is sufficient
//!   — no engate-specific gate is needed. Pillar 12: generation over
//!   composition (no per-domain Gates when one typestate suffices).

use std::sync::Arc;

use async_trait::async_trait;
use engate_attach::{Attach, Consumer, Producer};
use engate_types::AttachError;
use shigoto_types::{
    JobScope, JobSubject, OutputSink, RecordingJob,
};

// ── AttachedHandle ─────────────────────────────────────────────────

/// Typed output of an `AttachJob`: a fully attached consumer in the
/// `Live` engate phase. Downstream jobs that need the consumer's
/// terminal state hold this.
///
/// `Clone` required by shigoto's RecordingJob blanket impl so the
/// output sink can record copies. We wrap in Arc to keep the consumer
/// itself cheaply cloneable even when the underlying type isn't.
pub struct AttachedHandle<Cons: Send + Sync + 'static>(pub Arc<Cons>);

// Manual Clone so AttachedHandle: Clone holds even when Cons: !Clone
// (the whole point of the Arc wrapper).
impl<Cons: Send + Sync + 'static> Clone for AttachedHandle<Cons> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

// Manual Debug so AttachedHandle: Debug holds without requiring
// Cons: Debug. Output sinks and Result formatters frequently need
// this; printing the inner consumer is rarely what callers want
// anyway (it's a renderer / terminal model / network sink — not
// human-readable).
impl<Cons: Send + Sync + 'static> std::fmt::Debug for AttachedHandle<Cons> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttachedHandle")
            .field("arc_strong_count", &Arc::strong_count(&self.0))
            .finish()
    }
}

// ── AttachJob ──────────────────────────────────────────────────────

/// A shigoto RecordingJob that performs an engate attach lifecycle.
///
/// Construct via `AttachJob::new`. The job's `execute_body` runs the
/// full Attach typestate transition; on success the consumer is
/// wrapped in `AttachedHandle` and emitted; on failure an
/// `AttachJobError` is returned.
///
/// Generic over the (Producer, Consumer) pair so engate-shigoto stays
/// substrate-level — every fleet consumer (mado/tear, kenshi/testpod,
/// hiroba clients, ...) reuses this one struct.
pub struct AttachJob<Prod, Cons>
where
    Prod: Producer,
    Cons: Consumer<Item = Prod::Item, Snap = Prod::Snap> + Sync,
{
    scope: JobScope,
    subject: JobSubject,
    /// Held in Option<...> so `execute_body` can take ownership via
    /// interior mutability (RecordingJob takes `&self`). One-shot per
    /// AttachJob instance.
    inner: tokio::sync::Mutex<Option<(Prod, Cons)>>,
    sink: Option<Arc<dyn OutputSink<AttachedHandle<Cons>>>>,
}

impl<Prod, Cons> AttachJob<Prod, Cons>
where
    Prod: Producer,
    Cons: Consumer<Item = Prod::Item, Snap = Prod::Snap> + Sync,
{
    /// Build a fresh AttachJob. The producer + consumer are consumed
    /// at execute_body time (one-shot semantics).
    pub fn new(scope: JobScope, subject: JobSubject, producer: Prod, consumer: Cons) -> Self {
        Self {
            scope,
            subject,
            inner: tokio::sync::Mutex::new(Some((producer, consumer))),
            sink: None,
        }
    }

    /// Attach a typed output sink — used by shigoto's RecordingJob
    /// blanket impl to persist successful attaches across the audit
    /// chain.
    #[must_use]
    pub fn with_sink(
        mut self,
        sink: Arc<dyn OutputSink<AttachedHandle<Cons>>>,
    ) -> Self {
        self.sink = Some(sink);
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AttachJobError {
    #[error("engate attach failed: {0}")]
    Attach(#[from] AttachError),

    #[error("AttachJob is one-shot and has already executed")]
    AlreadyConsumed,
}

#[async_trait]
impl<Prod, Cons> RecordingJob for AttachJob<Prod, Cons>
where
    Prod: Producer,
    Cons: Consumer<Item = Prod::Item, Snap = Prod::Snap> + Sync,
{
    type Output = AttachedHandle<Cons>;
    type Error = AttachJobError;

    const KIND: &'static str = "engate.attach";

    fn scope(&self) -> JobScope {
        self.scope.clone()
    }

    fn subject(&self) -> JobSubject {
        self.subject.clone()
    }

    fn output_sink(&self) -> Option<&Arc<dyn OutputSink<Self::Output>>> {
        self.sink.as_ref()
    }

    async fn execute_body(&self) -> Result<Self::Output, Self::Error> {
        let (producer, consumer) = self
            .inner
            .lock()
            .await
            .take()
            .ok_or(AttachJobError::AlreadyConsumed)?;

        // Drive the engate typestate. Each transition either advances
        // the type-level phase or returns an AttachError — there is
        // no halfway state to leak.
        let attach = Attach::builder().producer(producer).consumer(consumer).build();
        let (attach, history) = attach.subscribe()?;
        let attach = attach.replay(history)?;
        // start_live is infallible by construction (Synced has all
        // the pieces); the actual draining of live items happens via
        // attach.poll_one() or attach.run() — the consumer owns that
        // policy. Here we just hand back the live handle.
        //
        // For RecordingJob semantics we extract the consumer at the
        // current point (snapshot replayed, live tx ready). Downstream
        // jobs receive AttachedHandle and decide how to drain.
        let attach = attach.start_live();
        // run() blocks until producer drops its sender — for shigoto
        // Job semantics we DON'T want to block here; the consumer's
        // ownership will be passed downstream via the sink.
        //
        // run() returns the consumer at the end of the live stream.
        // For an indefinite stream (terminal that stays open), we'd
        // want a different shape — TODO(engate M3.1): provide both
        // `AttachJob::run_to_completion` (blocking) and
        // `AttachJob::start_and_handoff` (non-blocking, handoff via
        // sink). For now: blocking, suitable for finite producers
        // (test cases, batch jobs).
        let consumer = attach.run();
        Ok(AttachedHandle(Arc::new(consumer)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::mpsc;

    struct TestProducer {
        snap: Vec<u8>,
        shared_tx: Arc<Mutex<Option<mpsc::Sender<u8>>>>,
    }

    impl Producer for TestProducer {
        type Item = u8;
        type Snap = Vec<u8>;

        fn snapshot(&self) -> Result<Self::Snap, AttachError> {
            Ok(self.snap.clone())
        }

        fn subscribe(&self) -> Result<mpsc::Receiver<Self::Item>, AttachError> {
            let (tx, rx) = mpsc::channel();
            *self.shared_tx.lock().unwrap() = Some(tx);
            Ok(rx)
        }
    }

    #[derive(Default)]
    struct TestConsumer(Arc<Mutex<Vec<u8>>>);

    impl Consumer for TestConsumer {
        type Item = u8;
        type Snap = Vec<u8>;

        fn replay(&mut self, snapshot: Self::Snap) {
            self.0.lock().unwrap().extend(snapshot);
        }

        fn consume(&mut self, item: Self::Item) {
            self.0.lock().unwrap().push(item);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn attach_job_runs_full_lifecycle() {
        let observed = Arc::new(Mutex::new(Vec::<u8>::new()));
        let tx_handle: Arc<Mutex<Option<mpsc::Sender<u8>>>> = Arc::new(Mutex::new(None));
        let prod = TestProducer {
            snap: vec![1, 2, 3],
            shared_tx: tx_handle.clone(),
        };
        let cons = TestConsumer(observed.clone());

        let job = AttachJob::new(
            JobScope::Global,
            JobSubject::Pinned("test-attach".into()),
            prod,
            cons,
        );

        // Spawn the attach in the background — it blocks on run()
        // until we drop the tx.
        let handle = tokio::task::spawn(async move { job.execute_body().await });

        // Yield so the spawned task runs subscribe + replay.
        tokio::task::yield_now().await;
        // Wait briefly for the subscribe to register (real impls would
        // synchronize on a channel; this is test-only).
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Drain post-subscribe items + close.
        let tx_opt = tx_handle.lock().unwrap().take();
        if let Some(tx) = tx_opt {
            for b in [4u8, 5, 6] {
                tx.send(b).unwrap();
            }
            drop(tx);
        }

        let result = handle.await.unwrap();
        assert!(result.is_ok(), "attach job should succeed: {result:?}");
        assert_eq!(*observed.lock().unwrap(), vec![1, 2, 3, 4, 5, 6]);
    }
}
