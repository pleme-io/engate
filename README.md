# engate

> Typed producer↔consumer attach primitive — eliminates the
> attach-race bug class by construction.

## The bug class

A producer (PTY, WebSocket, Kubernetes log stream, message queue)
emits data BEFORE a consumer subscribes; `subscribe()` only delivers
items emitted AFTER the call; the consumer's local model stays empty
even though the producer's state is full.

Concrete fleet examples:

| Pair | Symptom before engate |
|---|---|
| mado ↔ tear | window opens, prompt already on tear's grid, mado paints empty |
| kenshi ↔ testpod | pod logs scroll past before kenshi attaches |
| hiroba ↔ ws | recent chat history missing from a reconnect |
| ayatsuri ↔ mado | hotkeys arrive but initial config snapshot lost |
| namimado ↔ CDP | DOM mutations between page-load and attach lost |

## How engate kills it

```rust
let attach = engate::Attach::builder()
    .producer(my_producer)
    .consumer(my_consumer)
    .build();                  // Attach<Spawned>

let (attach, history) = attach.subscribe()?;     // Attach<Subscribed>
let attach = attach.replay(history)?;            // Attach<Synced>
let attach = attach.start_live();                // Attach<Live>
let _ = attach.run();                            // Consumer drained
```

Four properties the type system enforces:

1. **`Live` is the only render-capable phase.** `Attach<Subscribed>`
   has no `render()` method; `Attach<Synced>` doesn't either. You
   physically cannot write the render call before history is replayed.
2. **`subscribe()` returns `(Attach<Subscribed>, History)`.** Both
   must be moved forward. `History` is `#[must_use]` + drop-bomb;
   dropping it panics with a clear message ("pass it to replay()").
3. **`replay()` consumes the `History` by move.** Pass it twice =
   compile error.
4. **Subscribe ALWAYS happens before snapshot.** The Producer trait
   contract pins this ordering; reference impls in `engate-attach`
   demonstrate; proptest covers the invariant; loom (gated on
   compat-fix) exhaustively schedules.

## Crates

| Crate | Purpose |
|---|---|
| `engate-types` | Dependency-free contract — Phase markers (`Spawned`/`Subscribed`/`Synced`/`Live`), `Snapshot` trait, `AttachError`, `EngateSpec` (declarative authoring shape) |
| `engate-attach` | Runtime machinery — `Attach<P>` typestate, `AttachBuilder`, `Producer`/`Consumer` traits, `History` linear-ish handle |
| `engate-shigoto` | Bridge — `AttachJob` impl `RecordingJob` so cluster orchestration gets engate-flavored gates for free |
| `engate-attest` | Minimal attestation chain — BLAKE3 over (snapshot, live, observed); CI fixture comparison; tameshi migration path documented |

## Migrating a consumer

1. **Implement `Producer` on your existing client type.**
   ```rust
   impl engate_attach::Producer for MyClient {
       type Item = MyItem;
       type Snap = MySnapshot;
       fn snapshot(&self) -> Result<Self::Snap, AttachError> { ... }
       fn subscribe(&self) -> Result<mpsc::Receiver<Self::Item>, AttachError> { ... }
   }
   ```
   **Critical:** in `subscribe()`, register the live tx BEFORE snapshotting.
   This is the only ordering invariant the framework cannot enforce in types.

2. **Implement `Consumer` on your model.**
   ```rust
   impl engate_attach::Consumer for MyModel {
       type Item = MyItem;
       type Snap = MySnapshot;
       fn replay(&mut self, snapshot: Self::Snap) { ... }
       fn consume(&mut self, item: Self::Item) { ... }
   }
   ```

3. **Replace your ad-hoc attach call site with `Attach::builder()`.**
   The old call site goes from `~30 lines` of subscribe/snapshot/race-protection
   to `~5 lines` of typed transitions. Compiler enforces correctness.

4. **Add an attestation fixture in CI.**
   ```rust
   #[test] fn engate_attestation_holds() {
       let observed = run_attach();
       let actual = Attestation::from_components(...);
       let expected = Attestation::from_fixture(include_str!("fixtures/x.engate.json"))?;
       actual.verify_against(&expected)?;
   }
   ```

## Fleet migration status

| Consumer | M0 patch | engate-typed | Attestation fixture |
|---|---|---|---|
| mado ↔ tear | ✅ shipped (tear@cd9290b) | ⏳ next | ⏳ next |
| kenshi ↔ testpod | n/a | scheduled | scheduled |
| hiroba ↔ ws clients | n/a | scheduled | scheduled |
| ayatsuri ↔ mado MCP | n/a | scheduled | scheduled |
| namimado ↔ CDP | n/a | scheduled | scheduled |

## Theory

See `pleme-io/theory/` for the unified compounding-substrate frame;
engate is one application of the "typed primitive that makes the bug
class unrepresentable" pattern (Pillar 12 — generation over
composition; Pillar 10 — proof discipline). The CLAUDE.md tracker
in pleme-io lists the M0–M6 phase plan.

## License

MIT
