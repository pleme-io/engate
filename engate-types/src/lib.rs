//! engate-types — typed phase markers + traits for the engate attach
//! primitive. The actual attach machinery lives in `engate-attach`;
//! this crate is the small dependency-free contract so consumers can
//! depend on shapes without pulling in `statig` / `typed-builder`.
//!
//! # Why
//!
//! The bug class engate kills: a producer (PTY, WS, MQ, network)
//! emits data BEFORE a consumer subscribes; subscribe returns only
//! NEW data; the consumer's local model stays empty even though the
//! producer's state is full. Today's hand-wired attach paths in the
//! pleme-io fleet (mado↔tear, kenshi↔testpod, hiroba clients,
//! ayatsuri↔mado, namimado↔CDP) all suffer this class.
//!
//! The fix: model attach as a 4-state typestate
//! (`Spawned → Subscribed → Synced → Live`) where the consumer can
//! only render from a `Live` value, `Live` can only be reached via
//! `Synced.start_live()`, `Synced` can only be reached via
//! `Subscribed.replay(history)`, and `history` is a `#[must_use]`
//! linear-ish handle that cannot be dropped without being consumed.
//! Reaching the render path WITHOUT history is unrepresentable in
//! the type system.
//!
//! See `pleme-io/CLAUDE.md` task tracker (#123-#129) for the full
//! M0-M6 roadmap.

use serde::{Deserialize, Serialize};

// ── Phase typestate markers ──────────────────────────────────────────

/// Sealed marker trait for the four attach phases. Implementations
/// live in this crate only — downstream code cannot add new phases.
mod sealed {
    pub trait Sealed {}
}

/// Phase of an attach lifecycle. Marker only — carries no data.
pub trait Phase: sealed::Sealed + Send + Sync + 'static {
    /// Human-readable phase name, used in tracing + debug output.
    fn name() -> &'static str;
}

/// Initial phase: producer has been identified (e.g. a pane id, a
/// channel name) but no connection has been opened yet.
pub struct Spawned;
impl sealed::Sealed for Spawned {}
impl Phase for Spawned {
    fn name() -> &'static str {
        "Spawned"
    }
}

/// The consumer has registered with the producer's live emission
/// stream. No history has been replayed yet — calling render here
/// would show an empty model even if the producer's state is full.
pub struct Subscribed;
impl sealed::Sealed for Subscribed {}
impl Phase for Subscribed {
    fn name() -> &'static str {
        "Subscribed"
    }
}

/// Historical snapshot has been replayed into the consumer. The
/// consumer's local model now matches the producer's state as of
/// the snapshot. Live stream may have items queued but not yet
/// forwarded.
pub struct Synced;
impl sealed::Sealed for Synced {}
impl Phase for Synced {
    fn name() -> &'static str {
        "Synced"
    }
}

/// Live items from the producer are flowing into the consumer. This
/// is the only phase from which `render()` is reachable. Construction
/// gated on `Synced.start_live()` — reaching `Live` without going
/// through `Synced` is impossible in the type system.
pub struct Live;
impl sealed::Sealed for Live {}
impl Phase for Live {
    fn name() -> &'static str {
        "Live"
    }
}

// ── Snapshot trait ──────────────────────────────────────────────────

/// Producer-side snapshot of current state. Whatever the consumer
/// needs to bootstrap its local model to match the producer at attach
/// time. For tear: a `PaneSnapshot` (grid + cursor + flags). For a
/// WebSocket: the last N messages. For a Kubernetes log stream: the
/// tail of stdout up to attach time.
///
/// Implementors keep the trait minimal so engate stays generic;
/// transport-specific serialization is the implementor's concern.
pub trait Snapshot: Send + Sync + 'static {
    /// Approximate byte size of the snapshot. Used for tracing /
    /// metrics ("how much history did we replay on attach?").
    fn size_bytes(&self) -> usize {
        0
    }
}

// Implement Snapshot for common shapes so simple cases are zero-LoC.
impl Snapshot for Vec<u8> {
    fn size_bytes(&self) -> usize {
        self.len()
    }
}

impl Snapshot for String {
    fn size_bytes(&self) -> usize {
        self.len()
    }
}

// ── Errors ──────────────────────────────────────────────────────────

/// Errors that can occur during an attach lifecycle.
///
/// Distinct from per-consumer errors (producer disconnect, malformed
/// frame, etc.) which surface through the consumer's `consume` impl
/// and are not engate's concern.
#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub enum AttachError {
    /// The producer's snapshot operation failed. Engate cannot reach
    /// `Synced` without a snapshot.
    #[error("snapshot failed: {0}")]
    SnapshotFailed(String),

    /// Subscribing to the live stream failed.
    #[error("subscribe failed: {0}")]
    SubscribeFailed(String),

    /// The producer reports the requested entity does not exist.
    #[error("no such entity: {0}")]
    NoSuchEntity(String),

    /// Generic transport / I/O error during attach.
    #[error("transport: {0}")]
    Transport(String),
}

// ── EngateSpec — author surface ─────────────────────────────────────

/// Declarative spec for one engate attach point. Authored either by
/// hand in Rust (`EngateSpec { ... }`) or via tatara-lisp's
/// `(defengate ...)` form (M5.1 — `#[derive(TataraDomain)]` lands
/// when tatara-lisp's macro story is mature enough to consume this
/// without a manual `register()` boilerplate dance).
///
/// The spec is purely descriptive — it does NOT instantiate the
/// engate machinery (that's compile-time generics in `engate-attach`).
/// It serves as:
///
/// 1. A registry every operator-facing tool can enumerate (mado
///    publishes its engate specs, kenshi publishes its, etc.).
/// 2. The contract substrate's `mkEngateFlake` Nix builder reads to
///    generate per-consumer Nix outputs (module trio, attestation
///    fixture verifier, test invocations).
/// 3. The shape the `(defengate ...)` Lisp form serializes to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EngateSpec {
    /// Stable identifier for this engate point — used as the JobId
    /// subject when wrapped by engate-shigoto.
    pub name: String,

    /// Producer crate + path. e.g. `("tear-client", "tear_client::PaneProducer")`.
    pub producer: TypePath,

    /// Consumer crate + path. e.g. `("mado", "mado::terminal::Terminal")`.
    pub consumer: TypePath,

    /// Whether history replay is required. `true` (the default) =
    /// the typestate enforces snapshot-before-render; `false` = the
    /// consumer opts out (rare — only for genuinely stateless
    /// producers like a beep channel).
    #[serde(default = "default_true_bool")]
    pub history_required: bool,

    /// Optional attestation fixture path. When present, substrate
    /// emits a CI job that runs the engate and asserts the recorded
    /// fixture matches.
    #[serde(default)]
    pub attestation_fixture: Option<String>,
}

fn default_true_bool() -> bool {
    true
}

/// A crate + dotted type path. Two strings instead of one because
/// the Nix side needs the crate name for the closure graph and the
/// type path for the Rust import.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypePath {
    pub crate_name: String,
    pub path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_names_round_trip() {
        assert_eq!(Spawned::name(), "Spawned");
        assert_eq!(Subscribed::name(), "Subscribed");
        assert_eq!(Synced::name(), "Synced");
        assert_eq!(Live::name(), "Live");
    }

    #[test]
    fn vec_u8_snapshot_size() {
        let v: Vec<u8> = vec![1, 2, 3, 4, 5];
        assert_eq!(<Vec<u8> as Snapshot>::size_bytes(&v), 5);
    }

    #[test]
    fn attach_error_display() {
        let e = AttachError::SnapshotFailed("disk full".into());
        assert_eq!(e.to_string(), "snapshot failed: disk full");
    }

    #[test]
    fn engate_spec_round_trips_through_serde() {
        let s = EngateSpec {
            name: "mado-tear-pane".into(),
            producer: TypePath {
                crate_name: "tear-client".into(),
                path: "tear_client::PaneProducer".into(),
            },
            consumer: TypePath {
                crate_name: "mado".into(),
                path: "mado::terminal::Terminal".into(),
            },
            history_required: true,
            attestation_fixture: Some("fixtures/mado-tear.engate.json".into()),
        };
        let yaml = serde_json::to_string(&s).unwrap();
        let back: EngateSpec = serde_json::from_str(&yaml).unwrap();
        assert_eq!(s, back);
    }

    // ── Expanded coverage ─────────────────────────────────────────

    #[test]
    fn engate_spec_history_required_defaults_to_true() {
        // The field has #[serde(default = "default_true_bool")] so an
        // input missing the key should deserialize to true. Operators
        // who omit the field get safe-by-default semantics.
        let json = r#"{
            "name": "x",
            "producer": { "crate_name": "p-crate", "path": "p::Type" },
            "consumer": { "crate_name": "c-crate", "path": "c::Type" }
        }"#;
        let s: EngateSpec = serde_json::from_str(json).unwrap();
        assert!(s.history_required);
        assert!(s.attestation_fixture.is_none());
    }

    #[test]
    fn engate_spec_clone_equals_original() {
        let s = EngateSpec {
            name: "n".into(),
            producer: TypePath { crate_name: "p".into(), path: "p::T".into() },
            consumer: TypePath { crate_name: "c".into(), path: "c::T".into() },
            history_required: false,
            attestation_fixture: None,
        };
        assert_eq!(s, s.clone());
    }

    #[test]
    fn type_path_equality_per_field() {
        let a = TypePath { crate_name: "x".into(), path: "x::Y".into() };
        let b = TypePath { crate_name: "x".into(), path: "x::Y".into() };
        let c = TypePath { crate_name: "x".into(), path: "x::Z".into() };
        let d = TypePath { crate_name: "z".into(), path: "x::Y".into() };
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
    }

    #[test]
    fn all_attach_error_variants_constructible_and_displayable() {
        let errs = [
            AttachError::SnapshotFailed("disk full".into()),
            AttachError::SubscribeFailed("permission".into()),
            AttachError::NoSuchEntity("pane-x".into()),
            AttachError::Transport("connection reset".into()),
        ];
        for e in errs {
            let s = e.to_string();
            assert!(!s.is_empty(), "Display non-empty for {e:?}");
        }
    }

    #[test]
    fn attach_error_round_trips_through_serde() {
        // AttachError derives Serialize + Deserialize so it can travel
        // across IPC boundaries (e.g. shigoto's audit chain).
        let e = AttachError::Transport("connection reset by peer".into());
        let json = serde_json::to_string(&e).unwrap();
        let back: AttachError = serde_json::from_str(&json).unwrap();
        assert_eq!(e.to_string(), back.to_string());
    }

    #[test]
    fn snapshot_impl_for_string() {
        let s: String = "hello".into();
        assert_eq!(<String as Snapshot>::size_bytes(&s), 5);
    }

    #[test]
    fn snapshot_default_size_bytes_is_zero() {
        // Custom impls that don't override size_bytes() get 0 — the
        // default. Useful for typed-marker snapshots that carry no
        // payload (just a "subscribed at" timestamp etc.).
        struct EmptyMarker;
        impl Snapshot for EmptyMarker {}
        assert_eq!(<EmptyMarker as Snapshot>::size_bytes(&EmptyMarker), 0);
    }
}
