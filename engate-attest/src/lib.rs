//! engate-attest — minimum-viable attestation chain for engate
//! attaches.
//!
//! # What this gives you
//!
//! For each attach the consumer records a fingerprint of:
//! - the producer snapshot bytes that bootstrapped the consumer
//! - the live items the consumer observed (concatenated bytes)
//! - the final consumer state (whatever bytes the consumer hashes)
//!
//! The three hashes are folded into one BLAKE3 root via a fixed
//! Merkle-style concat. Two attaches that observe the same producer
//! state produce byte-identical roots; any drift (lost prompt,
//! reordered bytes, dropped attr) flips the root.
//!
//! CI consumers serialize the root to a `.engate-fixture.json` file
//! and re-run the attestation on every PR. Mismatch = build red.
//!
//! # tameshi migration path
//!
//! When tameshi/sekiban/kensa integration matures (engate M4.1),
//! replace `AttestationChain` with `sekiban::SignedChain<EngateRecord>`
//! and route fixtures through the canonical 4-repo attestation chain
//! (`tameshi → sekiban → kensa → inshou`). The wire format we ship
//! here is a strict subset of what sekiban accepts so the migration
//! is a typed deserialize + re-emit, not a re-instrumentation.

use serde::{Deserialize, Serialize};

/// A finalized attestation for one attach lifecycle.
///
/// The three component hashes are public so consumers can drill in
/// when CI flags a drift ("snapshot hash changed → producer ANSI
/// serialization regressed" vs "observed hash changed → consumer VT
/// parser regressed").
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attestation {
    /// BLAKE3 of the producer snapshot bytes (the ANSI replay the
    /// daemon emitted on Subscribe).
    pub snapshot_hash: String,

    /// BLAKE3 of the concatenated live items the consumer observed.
    pub live_hash: String,

    /// BLAKE3 of the final consumer state (whatever the consumer's
    /// own canonical serializer produces).
    pub observed_hash: String,

    /// BLAKE3 root over `snapshot_hash || live_hash || observed_hash`
    /// — the single comparison point CI cares about.
    pub root_hash: String,

    /// Size in bytes of the snapshot — useful for tracing replay
    /// cost across attestations.
    pub snapshot_bytes: usize,

    /// Number of live items observed.
    pub live_item_count: usize,
}

impl Attestation {
    /// Compute an Attestation from raw inputs. All three hashes are
    /// hex-encoded 32-byte BLAKE3 digests.
    #[must_use]
    pub fn from_components(
        snapshot: &[u8],
        live: &[u8],
        observed: &[u8],
        live_item_count: usize,
    ) -> Self {
        let snapshot_hash = blake3::hash(snapshot).to_hex().to_string();
        let live_hash = blake3::hash(live).to_hex().to_string();
        let observed_hash = blake3::hash(observed).to_hex().to_string();

        // Root = hash of the three hashes concatenated. Fixed-width
        // (64 hex chars × 3 = 192 bytes) so the concat is deterministic.
        let mut concat = String::with_capacity(192);
        concat.push_str(&snapshot_hash);
        concat.push_str(&live_hash);
        concat.push_str(&observed_hash);
        let root_hash = blake3::hash(concat.as_bytes()).to_hex().to_string();

        Self {
            snapshot_hash,
            live_hash,
            observed_hash,
            root_hash,
            snapshot_bytes: snapshot.len(),
            live_item_count,
        }
    }

    /// Serialize to JSON for fixture files / CI comparison.
    pub fn to_fixture(&self) -> Result<String, AttestationError> {
        serde_json::to_string_pretty(self).map_err(|e| AttestationError::Serialize(e.to_string()))
    }

    /// Parse a fixture file.
    pub fn from_fixture(s: &str) -> Result<Self, AttestationError> {
        serde_json::from_str(s).map_err(|e| AttestationError::Deserialize(e.to_string()))
    }

    /// Compare against a recorded fixture. Returns `Ok(())` on match;
    /// `Err(AttestationDrift)` with the divergent component on
    /// mismatch. Designed for `assert!(actual.verify_against(fixture).is_ok())`
    /// at the top of every CI attestation test.
    pub fn verify_against(&self, expected: &Self) -> Result<(), AttestationDrift> {
        if self.root_hash != expected.root_hash {
            return Err(AttestationDrift {
                component: drift_component(self, expected),
                expected: Box::new(expected.clone()),
                actual: Box::new(self.clone()),
            });
        }
        Ok(())
    }
}

fn drift_component(actual: &Attestation, expected: &Attestation) -> DriftComponent {
    if actual.snapshot_hash != expected.snapshot_hash {
        DriftComponent::Snapshot
    } else if actual.live_hash != expected.live_hash {
        DriftComponent::Live
    } else if actual.observed_hash != expected.observed_hash {
        DriftComponent::Observed
    } else {
        // Root differs but every component matches → impossible by
        // construction, but degrades safely.
        DriftComponent::Root
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DriftComponent {
    Snapshot,
    Live,
    Observed,
    Root,
}

#[derive(Debug, thiserror::Error)]
#[error("attestation drift in {component:?}")]
pub struct AttestationDrift {
    pub component: DriftComponent,
    pub expected: Box<Attestation>,
    pub actual: Box<Attestation>,
}

#[derive(Debug, thiserror::Error)]
pub enum AttestationError {
    #[error("serialize: {0}")]
    Serialize(String),

    #[error("deserialize: {0}")]
    Deserialize(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_inputs_yield_identical_root() {
        let a = Attestation::from_components(b"snap", b"live", b"obs", 4);
        let b = Attestation::from_components(b"snap", b"live", b"obs", 4);
        assert_eq!(a.root_hash, b.root_hash);
    }

    #[test]
    fn differing_snapshot_flips_root() {
        let a = Attestation::from_components(b"snap-a", b"live", b"obs", 4);
        let b = Attestation::from_components(b"snap-b", b"live", b"obs", 4);
        assert_ne!(a.root_hash, b.root_hash);
        assert_ne!(a.snapshot_hash, b.snapshot_hash);
    }

    #[test]
    fn fixture_round_trips() {
        let a = Attestation::from_components(b"s", b"l", b"o", 1);
        let s = a.to_fixture().unwrap();
        let b = Attestation::from_fixture(&s).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn verify_against_diagnoses_snapshot_drift() {
        let expected = Attestation::from_components(b"old-snap", b"live", b"obs", 1);
        let actual = Attestation::from_components(b"new-snap", b"live", b"obs", 1);
        let drift = actual.verify_against(&expected).unwrap_err();
        assert_eq!(drift.component, DriftComponent::Snapshot);
    }

    #[test]
    fn verify_against_diagnoses_live_drift() {
        let expected = Attestation::from_components(b"snap", b"old-live", b"obs", 1);
        let actual = Attestation::from_components(b"snap", b"new-live", b"obs", 1);
        let drift = actual.verify_against(&expected).unwrap_err();
        assert_eq!(drift.component, DriftComponent::Live);
    }

    #[test]
    fn matching_attestations_verify_ok() {
        let a = Attestation::from_components(b"snap", b"live", b"obs", 1);
        let b = Attestation::from_components(b"snap", b"live", b"obs", 1);
        assert!(a.verify_against(&b).is_ok());
    }
}
