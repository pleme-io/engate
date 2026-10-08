//! Property tests for the engate attach contract.
//!
//! Invariant under test: for every (pre_attach_emit, post_attach_emit)
//! split, the consumer observes pre_attach_emit ++ post_attach_emit in
//! order. The engate Producer impl is responsible for guaranteeing
//! this via subscribe-before-snapshot ordering; this test confirms a
//! reference impl follows the contract regardless of payload shape.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;

use engate_attach::{Attach, Consumer, Producer, ReplaySource};
use engate_types::AttachError;
use proptest::prelude::*;

/// Reference producer mirroring tear's pattern: subscribe FIRST so
/// no items emitted between snapshot capture and subscribe-register
/// are lost, then snapshot. The post-subscribe live items + Sender
/// handle are held in shared Arc<Mutex> so the test can drive the
/// flush from outside the engate Attach handle (which moves the
/// producer into private storage).
struct RefProducer {
    pre_subscribe: Vec<u8>,
    shared_tx: Arc<Mutex<Option<mpsc::Sender<u8>>>>,
}

impl RefProducer {
    fn new_with_handle(pre: Vec<u8>) -> (Self, Arc<Mutex<Option<mpsc::Sender<u8>>>>) {
        let tx_handle: Arc<Mutex<Option<mpsc::Sender<u8>>>> = Arc::new(Mutex::new(None));
        let p = Self {
            pre_subscribe: pre,
            shared_tx: tx_handle.clone(),
        };
        (p, tx_handle)
    }
}

impl Producer for RefProducer {
    type Item = u8;
    type Snap = Vec<u8>;

    fn snapshot(&self) -> Result<Self::Snap, AttachError> {
        Ok(self.pre_subscribe.clone())
    }

    fn subscribe(&self) -> Result<mpsc::Receiver<Self::Item>, AttachError> {
        let (tx, rx) = mpsc::channel();
        *self.shared_tx.lock().unwrap() = Some(tx);
        Ok(rx)
    }
}

/// Drain helper run from the test side: takes the Sender out of the
/// shared slot, sends every byte in `live`, drops the Sender so
/// `Attach::run` can terminate.
fn drain_live(tx_handle: &Arc<Mutex<Option<mpsc::Sender<u8>>>>, live: Vec<u8>) {
    let tx_opt = tx_handle.lock().unwrap().take();
    if let Some(tx) = tx_opt {
        for b in live {
            let _ = tx.send(b);
        }
        drop(tx);
    }
}

#[derive(Default)]
struct VecConsumer(std::sync::Arc<Mutex<Vec<u8>>>);

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

proptest! {
    /// For any split of bytes into (pre, post) the consumer must
    /// observe pre ++ post in order. This is the load-bearing
    /// invariant of the engate attach contract — it is the bug
    /// class engate exists to eliminate.
    #[test]
    fn observed_equals_pre_subscribe_then_post_subscribe(
        pre in proptest::collection::vec(any::<u8>(), 0..256),
        post in proptest::collection::vec(any::<u8>(), 0..256),
    ) {
        let observed = Arc::new(Mutex::new(Vec::<u8>::new()));
        let cons = VecConsumer(observed.clone());
        let expected: Vec<u8> = pre.iter().chain(post.iter()).copied().collect();
        let (prod, tx_handle) = RefProducer::new_with_handle(pre);

        let attach = Attach::builder().producer(prod).consumer(cons).build();
        let (attach, history) = attach.subscribe().unwrap();
        let attach = attach.replay(history).unwrap();
        drain_live(&tx_handle, post);
        let attach = attach.start_live();
        let _ = attach.run();

        prop_assert_eq!(&*observed.lock().unwrap(), &expected);
    }
}

struct StreamRef {
    history: Vec<u8>,
    shared_tx: Arc<Mutex<Option<mpsc::Sender<u8>>>>,
}

impl Producer for StreamRef {
    type Item = u8;
    type Snap = Vec<u8>;

    fn snapshot(&self) -> Result<Self::Snap, AttachError> {
        Err(AttachError::SnapshotFailed(
            "a stream-carried producer is never snapshotted".into(),
        ))
    }

    fn subscribe(&self) -> Result<mpsc::Receiver<Self::Item>, AttachError> {
        let (tx, rx) = mpsc::channel();
        for b in &self.history {
            let _ = tx.send(*b);
        }
        *self.shared_tx.lock().unwrap() = Some(tx);
        Ok(rx)
    }

    fn replay_source(&self) -> ReplaySource {
        ReplaySource::Stream
    }
}

proptest! {
    #[test]
    fn a_stream_carried_history_is_observed_once_before_the_live_items(
        pre in proptest::collection::vec(any::<u8>(), 1..256),
        post in proptest::collection::vec(any::<u8>(), 0..256),
    ) {
        let observed = Arc::new(Mutex::new(Vec::<u8>::new()));
        let cons = VecConsumer(observed.clone());
        let expected: Vec<u8> = pre.iter().chain(post.iter()).copied().collect();
        let tx_handle: Arc<Mutex<Option<mpsc::Sender<u8>>>> = Arc::new(Mutex::new(None));
        let prod = StreamRef { history: pre, shared_tx: tx_handle.clone() };

        let attach = Attach::builder().producer(prod).consumer(cons).build();
        let (attach, history) = attach.subscribe().unwrap();
        let attach = attach.replay(history).unwrap();
        drain_live(&tx_handle, post);
        let _ = attach.start_live().run();

        prop_assert_eq!(&*observed.lock().unwrap(), &expected);
    }
}
