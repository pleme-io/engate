//! engate-attach — typed attach lifecycle.
//!
//! See `engate-types` for the philosophy + phase markers. This crate
//! provides the runtime machinery: `Producer` / `Consumer` traits,
//! the typestate-enforced `Attach<P>` value, the `AttachBuilder`, and
//! the linear-ish `History<S>` handle.
//!
//! # Quick example
//!
//! ```ignore
//! use engate_attach::{Attach, Producer, Consumer};
//!
//! let attach = Attach::builder()
//!     .producer(my_producer)
//!     .consumer(my_consumer)
//!     .build()                   // -> Attach<Spawned>
//!     .subscribe()?              // -> Attach<Subscribed>, also returns History<S>
//!     .replay(history)?          // -> Attach<Synced>
//!     .start_live();             // -> Attach<Live>
//!
//! // Only an Attach<Live> can render.
//! attach.run();
//! ```
//!
//! # Why typestate over runtime FSM
//!
//! A `Result` return + match on phase string would also work, but the
//! compiler would let you write `attach.replay()` on a `Subscribed`
//! that you forgot to `subscribe()` first. Typestate makes the
//! malformed call a compile error: `Spawned` has no `replay` method;
//! `Subscribed` has no `start_live` method. You CAN'T write the bug.

use std::marker::PhantomData;
use std::sync::mpsc;

use drop_bomb::DropBomb;
use engate_types::{AttachError, Live, Phase, Snapshot, Spawned, Subscribed, Synced};

// ── Producer / Consumer traits ──────────────────────────────────────

/// A producer of live data + bootable snapshots. The trait is the
/// only thing engate needs to provide attach semantics; concrete
/// producers (tear pane, WS channel, K8s log stream) implement it.
pub trait Producer: Send + Sync + 'static {
    /// Items emitted on the live stream.
    type Item: Send + 'static;

    /// Snapshot representation. Whatever the consumer needs to
    /// reach the producer's current state without replaying every
    /// historical item.
    type Snap: Snapshot;

    /// Capture the producer's current state. Must NOT be racy with
    /// `subscribe` — the contract is "no item between snapshot
    /// capture and live-stream registration may be lost"; concrete
    /// implementors typically subscribe first, then snapshot.
    fn snapshot(&self) -> Result<Self::Snap, AttachError>;

    /// Register a live subscriber. Returns a receiver; items are
    /// pushed asynchronously by the producer. Dropping the receiver
    /// unsubscribes.
    fn subscribe(&self) -> Result<mpsc::Receiver<Self::Item>, AttachError>;
}

/// A consumer of producer items + snapshots. Mirrors the producer's
/// associated types so the type-checker enforces compatible pairs.
pub trait Consumer: Send + 'static {
    /// Items received from the producer's live stream.
    type Item: Send + 'static;

    /// Snapshot representation accepted by `replay`.
    type Snap: Snapshot;

    /// Bootstrap the consumer's local model from a producer
    /// snapshot. Called exactly once during the
    /// `Subscribed → Synced` transition.
    fn replay(&mut self, snapshot: Self::Snap);

    /// Apply a single live item. Called once per item after
    /// `start_live` is invoked.
    fn consume(&mut self, item: Self::Item);
}

// ── History — linear-ish snapshot handle ────────────────────────────

/// A snapshot in flight from producer to consumer. `#[must_use]` +
/// runtime drop-bomb together approximate linear typing: forgetting
/// to consume `History` panics in debug builds, and `clippy` flags
/// the dropped result in CI.
///
/// The only way to consume `History` is to pass it to
/// `Attach<Subscribed>::replay()`, which moves the inner snapshot
/// into the consumer and defuses the bomb.
#[must_use = "engate::History must be passed to Attach::replay() — dropping it loses the producer's pre-attach state and reintroduces the bug class engate exists to eliminate"]
pub struct History<S: Snapshot> {
    snapshot: S,
    bomb: DropBomb,
}

impl<S: Snapshot> History<S> {
    fn new(snapshot: S) -> Self {
        Self {
            snapshot,
            bomb: DropBomb::new(
                "engate::History dropped without being consumed — pass it to Attach::replay()",
            ),
        }
    }

    /// Move out the snapshot, defusing the drop-bomb. Called inside
    /// `Attach::replay`. Public so external code that needs to
    /// inspect the snapshot can do so without panicking on drop.
    pub fn into_inner(mut self) -> S {
        self.bomb.defuse();
        self.snapshot
    }

    /// Approximate byte size — useful for tracing replay cost.
    pub fn size_bytes(&self) -> usize {
        self.snapshot.size_bytes()
    }
}

// ── Attach typestate ────────────────────────────────────────────────

/// The typed attach handle. `P` is the current phase; the available
/// methods are gated on `P` so malformed call sequences are compile
/// errors rather than runtime mismatches.
pub struct Attach<P: Phase, Prod: Producer, Cons: Consumer> {
    producer: Prod,
    consumer: Cons,
    // Set after `subscribe()`; consumed by `start_live()`.
    rx: Option<mpsc::Receiver<Prod::Item>>,
    _phase: PhantomData<P>,
}

// ── Builder (Spawned constructor) ───────────────────────────────────

/// Builder for the initial `Attach<Spawned>`. `typed-builder` enforces
/// that both `producer` and `consumer` are set before `.build()` can
/// be called.
#[derive(typed_builder::TypedBuilder)]
#[builder(build_method(into = AttachSpawned<Prod, Cons>))]
pub struct AttachConfig<Prod, Cons>
where
    Prod: Producer,
    Cons: Consumer<Item = Prod::Item, Snap = Prod::Snap>,
{
    pub producer: Prod,
    pub consumer: Cons,
}

/// Type alias for clarity at call sites.
pub type AttachSpawned<Prod, Cons> = Attach<Spawned, Prod, Cons>;

impl<Prod, Cons> From<AttachConfig<Prod, Cons>> for AttachSpawned<Prod, Cons>
where
    Prod: Producer,
    Cons: Consumer<Item = Prod::Item, Snap = Prod::Snap>,
{
    fn from(c: AttachConfig<Prod, Cons>) -> Self {
        Attach {
            producer: c.producer,
            consumer: c.consumer,
            rx: None,
            _phase: PhantomData,
        }
    }
}

impl<Prod, Cons> Attach<Spawned, Prod, Cons>
where
    Prod: Producer,
    Cons: Consumer<Item = Prod::Item, Snap = Prod::Snap>,
{
    /// Public entry point — `Attach::builder()`.
    pub fn builder() -> AttachConfigBuilder<Prod, Cons, ((), ())> {
        AttachConfig::builder()
    }
}

// ── Transitions ─────────────────────────────────────────────────────

impl<Prod, Cons> Attach<Spawned, Prod, Cons>
where
    Prod: Producer,
    Cons: Consumer<Item = Prod::Item, Snap = Prod::Snap>,
{
    /// `Spawned → Subscribed`. Subscribe to the live stream FIRST
    /// (so no item between subscribe and snapshot is lost), then
    /// capture the snapshot. Returns the new phase + a `History`
    /// handle that must be passed to `replay` (drop-bomb prevents
    /// forgetting).
    pub fn subscribe(
        self,
    ) -> Result<(Attach<Subscribed, Prod, Cons>, History<Prod::Snap>), AttachError> {
        let rx = self.producer.subscribe()?;
        let snap = self.producer.snapshot()?;
        tracing::debug!(
            target: "engate::attach",
            snapshot_bytes = snap.size_bytes(),
            "subscribe complete — snapshot captured"
        );
        let history = History::new(snap);
        let next = Attach {
            producer: self.producer,
            consumer: self.consumer,
            rx: Some(rx),
            _phase: PhantomData,
        };
        Ok((next, history))
    }
}

impl<Prod, Cons> Attach<Subscribed, Prod, Cons>
where
    Prod: Producer,
    Cons: Consumer<Item = Prod::Item, Snap = Prod::Snap>,
{
    /// `Subscribed → Synced`. Consumes the `History` handle and
    /// replays the snapshot into the consumer. `History` is moved
    /// (not borrowed) so the type system enforces "exactly once".
    pub fn replay(
        self,
        history: History<Prod::Snap>,
    ) -> Result<Attach<Synced, Prod, Cons>, AttachError> {
        let snap = history.into_inner();
        let bytes = snap.size_bytes();
        let mut consumer = self.consumer;
        consumer.replay(snap);
        tracing::debug!(target: "engate::attach", bytes, "replay complete");
        Ok(Attach {
            producer: self.producer,
            consumer,
            rx: self.rx,
            _phase: PhantomData,
        })
    }
}

impl<Prod, Cons> Attach<Synced, Prod, Cons>
where
    Prod: Producer,
    Cons: Consumer<Item = Prod::Item, Snap = Prod::Snap>,
{
    /// `Synced → Live`. Items queued in the live receiver since
    /// subscribe begin flowing into the consumer. Returns
    /// `Attach<Live>` — the only phase from which `run` is reachable.
    pub fn start_live(self) -> Attach<Live, Prod, Cons> {
        tracing::debug!(target: "engate::attach", "starting live stream");
        Attach {
            producer: self.producer,
            consumer: self.consumer,
            rx: self.rx,
            _phase: PhantomData,
        }
    }
}

impl<Prod, Cons> Attach<Live, Prod, Cons>
where
    Prod: Producer,
    Cons: Consumer<Item = Prod::Item, Snap = Prod::Snap>,
{
    /// Drain the live receiver synchronously, forwarding each item
    /// to the consumer. Blocks until the producer drops its sender
    /// (clean closure) or the receiver errors. Returns the consumer
    /// so the caller can inspect terminal state.
    pub fn run(mut self) -> Cons {
        let rx = self.rx.take().expect("Attach<Live> always has rx");
        while let Ok(item) = rx.recv() {
            self.consumer.consume(item);
        }
        self.consumer
    }

    /// Non-blocking single-item drain. Returns `true` if an item
    /// was consumed; `false` if the channel is empty or closed.
    /// Useful for embedding the drain inside an existing event loop
    /// (winit, tokio task, etc.) instead of dedicating a thread.
    pub fn poll_one(&mut self) -> bool {
        let rx = self.rx.as_ref().expect("Attach<Live> always has rx");
        match rx.try_recv() {
            Ok(item) => {
                self.consumer.consume(item);
                true
            }
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Mutex;

    // ── Minimal test producer + consumer ─────────────────────────────

    struct VecProducer {
        snapshot_data: Vec<u8>,
        live_data: Mutex<Vec<u8>>,
        tx: Mutex<Option<mpsc::Sender<u8>>>,
    }

    impl VecProducer {
        fn new(snapshot: Vec<u8>, live: Vec<u8>) -> Self {
            Self {
                snapshot_data: snapshot,
                live_data: Mutex::new(live),
                tx: Mutex::new(None),
            }
        }

        /// Push all queued live items through the subscriber tx and
        /// then drop the tx so `Attach::run` can terminate cleanly.
        fn flush_live_and_close(&self) {
            // Move the Sender out so dropping ends the channel after
            // we've pushed our items.
            let tx_opt = self.tx.lock().unwrap().take();
            if let Some(tx) = tx_opt {
                for b in self.live_data.lock().unwrap().drain(..) {
                    let _ = tx.send(b);
                }
                drop(tx);
            }
        }
    }

    impl Producer for VecProducer {
        type Item = u8;
        type Snap = Vec<u8>;

        fn snapshot(&self) -> Result<Self::Snap, AttachError> {
            Ok(self.snapshot_data.clone())
        }

        fn subscribe(&self) -> Result<mpsc::Receiver<Self::Item>, AttachError> {
            let (tx, rx) = mpsc::channel();
            *self.tx.lock().unwrap() = Some(tx);
            Ok(rx)
        }
    }

    #[derive(Default)]
    struct VecConsumer(Arc<Mutex<Vec<u8>>>);

    impl Consumer for VecConsumer {
        type Item = u8;
        type Snap = Vec<u8>;

        fn replay(&mut self, snapshot: Self::Snap) {
            self.0.lock().unwrap().extend(snapshot);
        }

        fn consume(&mut self, item: Self::Item) {
            self.0.lock().unwrap().push(item);
        }
    }

    #[test]
    fn full_lifecycle_observes_snapshot_then_live() {
        let observed = Arc::new(Mutex::new(Vec::<u8>::new()));
        let prod = VecProducer::new(vec![1, 2, 3], vec![4, 5, 6]);
        let cons = VecConsumer(observed.clone());

        let attach = Attach::builder().producer(prod).consumer(cons).build();
        let (attach, history) = attach.subscribe().unwrap();
        let attach = attach.replay(history).unwrap();
        // simulate producer emitting live items AFTER subscribe
        // by reaching into the test producer (real producers push
        // asynchronously). Closing the tx lets run() terminate.
        attach.producer.flush_live_and_close();
        let attach = attach.start_live();
        let _cons = attach.run();

        assert_eq!(*observed.lock().unwrap(), vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    #[should_panic(expected = "History dropped without being consumed")]
    fn dropping_history_panics_via_dropbomb() {
        let prod = VecProducer::new(vec![], vec![]);
        let cons = VecConsumer::default();
        let attach = Attach::builder().producer(prod).consumer(cons).build();
        let (_attach, _history) = attach.subscribe().unwrap();
        // _history dropped here without replay — DropBomb fires
    }

    #[test]
    fn history_size_bytes_reflects_snapshot() {
        let prod = VecProducer::new(vec![1, 2, 3, 4, 5], vec![]);
        let cons = VecConsumer::default();
        let attach = Attach::builder().producer(prod).consumer(cons).build();
        let (attach, history) = attach.subscribe().unwrap();
        assert_eq!(history.size_bytes(), 5);
        let _ = attach.replay(history);
    }

    #[test]
    fn poll_one_returns_false_on_empty_channel() {
        let prod = VecProducer::new(vec![], vec![]);
        let cons = VecConsumer::default();
        let attach = Attach::builder().producer(prod).consumer(cons).build();
        let (attach, history) = attach.subscribe().unwrap();
        let attach = attach.replay(history).unwrap();
        let mut attach = attach.start_live();
        assert!(!attach.poll_one());
    }
}
