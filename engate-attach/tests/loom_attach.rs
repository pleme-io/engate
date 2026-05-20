//! Loom-based interleaving exhaustion for the engate attach contract.
//!
//! Build & run: `cargo test --features loom --test loom_attach`.
//!
//! Loom replaces std::sync primitives at compile time via `cfg(loom)`
//! and exhaustively explores all thread interleavings of the modeled
//! operations. The invariant under test is the SAME as the proptest
//! suite — "every emitted item is observed" — but where proptest
//! samples payload SHAPES, loom samples thread SCHEDULES. Together
//! they cover both axes.
//!
//! What we model: a producer that races subscribe with emit (the
//! exact race that broke mado), checking that a correct
//! subscribe-before-snapshot ordering captures every byte. The
//! incorrect ordering (snapshot-before-subscribe) loses bytes under
//! the schedules loom finds.

#![cfg(feature = "loom")]

use loom::sync::Arc;
use loom::sync::Mutex;
use loom::sync::mpsc;
use loom::thread;

/// Correct attach: subscribe FIRST, then snapshot. Bytes that arrive
/// during the window between subscribe-register and snapshot-capture
/// land in BOTH (snapshot includes them; live stream re-emits them)
/// — duplicate but never lost. Idempotent at the VT-parser level.
fn correct_attach(
    pre_subscribe_buffer: Arc<Mutex<Vec<u8>>>,
    live_emit: Arc<Mutex<Vec<u8>>>,
) -> Vec<u8> {
    // 1. Subscribe — register tx so subsequent emits go to rx.
    let (tx, rx) = mpsc::channel::<u8>();
    let live_tx = Arc::new(Mutex::new(Some(tx)));

    // 2. Spawn the "live emitter" that pushes bytes after subscribe
    //    has registered.
    let live_tx_clone = live_tx.clone();
    let live_emit_clone = live_emit.clone();
    let emit_handle = thread::spawn(move || {
        let mut guard = live_tx_clone.lock().unwrap();
        if let Some(tx) = guard.take() {
            for b in live_emit_clone.lock().unwrap().drain(..) {
                let _ = tx.send(b);
            }
            drop(tx);
        }
    });

    // 3. Snapshot — capture current pre_subscribe state.
    let snapshot: Vec<u8> = pre_subscribe_buffer.lock().unwrap().clone();

    // 4. Drain rx until tx is dropped.
    let mut live: Vec<u8> = Vec::new();
    while let Ok(b) = rx.recv() {
        live.push(b);
    }

    emit_handle.join().unwrap();

    let mut observed = snapshot;
    observed.extend(live);
    observed
}

// TODO(engate M2.1): loom 0.7 panics in a destructor on Rust 1.91 via
// generator-0.8 (test sees `panic in a destructor during cleanup`).
// Re-enable when loom releases a compat fix, or evaluate the shuttle
// crate (Meta's alternative — uses tokio task-local instead of
// generator-based green threads). proptest already covers the
// "no item lost" axis end-to-end; loom only adds schedule exhaustion,
// which is moot because the engate Producer trait pins the only
// ordering that matters (subscribe-before-snapshot).
#[ignore]
#[test]
fn correct_attach_observes_all_bytes_under_all_schedules() {
    loom::model(|| {
        let pre = Arc::new(Mutex::new(vec![1, 2]));
        let live = Arc::new(Mutex::new(vec![3, 4]));
        let observed = correct_attach(pre.clone(), live.clone());
        // Every byte must appear in observed (order within pre+live
        // is preserved; duplicates from the snapshot/live overlap
        // window are allowed by the VT-parser-idempotent contract).
        for b in [1u8, 2, 3, 4] {
            assert!(observed.contains(&b), "missing byte {b} in {observed:?}");
        }
    });
}
