# Code Review — Peruncs Data Grid

Read-only static review of `src/main/java` (~100 files, ~28k lines) against the `AGENTS.md`
guardrails, with focus on cluster correctness, performance/liveliness, package structure,
naming, and javadocs. No builds or tests were run; findings cite file and approximate line.

Ordered roughly by priority. Section legend: **C** = correctness, **P** = performance,
**S** = structure/design, **N** = naming, **D** = docs.

---

## P0 — Correctness bugs (fix first)

### C1. `AeronReplicationPublisher.failed` written outside the monitor
`storage/aeron/writer/AeronReplicationPublisher.java` (`closeInternal`, ~995 and ~1012).
`failed` is a plain `boolean`; every read happens under `synchronized(this)`, but the writes
`this.failed = true` in the abort and Error paths run *outside* the monitor — no
happens-before edge, so readers can indefinitely observe `failed == false` and keep
admitting writes into a terminally failed publisher.
**Fix:** move both assignments inside `synchronized(this)` (or make it `volatile`; the
lock-based pattern argues for scoping the writes).

### C2. Cold-restart resume at the persisted cursor may be off-by-one on the terminal marker
`reader/TransactionAssembler.java` `accept()` (~226–276) + `commit()`; `reader/AeronArchiveReader.reconnectAfterArchiveLoss()`.
Duplicate terminals are tolerated only when `lastResolutionKind` (in-memory witness) is set;
the witness is **not persisted** in `AeronReplicationCheckpoint`/`AeronReplicationCursor`.
If startup resumes the replay inclusively at `lastResolvedPosition` (start of the COMMIT/ABORT
frame), the first frame throws "terminal witness is unavailable" → permanent fail-closed.
In-process reconnects are safe; only cold start is suspect.
**Fix:** trace how the durable cursor becomes `Configuration.startPosition`. Either persist
the terminal witness fields (kind + crc + lengths — the cursor record already has the columns)
or resume at `terminal position + aligned frame length`. Write a crash-cycle test that
proves cold restart over a COMMIT boundary resumes cleanly.

### C3. `TransactionAssembler.Delivery.stage()` — native buffer leak if `receiveDataOwned` throws before taking ownership
`reader/TransactionAssembler.java` ~802–813. `detachDataStorage()` runs before
`receiver.receiveDataOwned(binary)`; if the receiver throws before recording ownership, the
native `ByteBuffer` is freed by nobody. Leak on an already-error path.
**Fix:** document+enforce the ownership contract on `StorageBinaryDataReceiver.receiveDataOwned`
(owns the buffer on *every* exit path), or detach only on confirmed success. Add a native-memory
assertion test with a throwing receiver.

### C4. `NodeHousekeeper`: one task's success clears another task's degradation latch
`node/NodeHousekeeper.java:62–75`. `degradedFailure` is a single node-wide slot cleared by
*any* successful run; a failing backup task and a succeeding GC task flip the node healthy
while the backup streak continues.
**Fix:** keep per-task latches (`Map<String, Throwable>`); `failure()` returns any entry,
preferring fatal `Error`.

### C5. Foundation close ordering: housekeeper keeps firing after its executors are closed
`ClusterFoundation.java:1295–1331`. Close stages: backup executor → storage executor →
storage manager (which closes the housekeeper). Between stages, housekeeper tasks call into
already-closed executors → spurious ERROR logs and degradation flaps on every orderly shutdown.
**Fix:** stop the housekeeper *first* (explicit stage), and keep the close inside
`closeHousekeeperAndReplication` only as a failure-path net.

### C6. `BackupNodeManager`'s "deferred client disposal" is unreachable — client leaked on close-during-backup
`BackupNodeManager.java:159–190` + `ClusterFoundation.java:1333–1343`. The manager returns
without disposing the data client when a backup is running, expecting a later close; but
`ClusterFoundation.close()` marks `closed` unconditionally and can never be retried. Reader
polling thread + Aeron subscription leak whenever close races a backup.
**Fix:** at foundation level, await/cancel the backup executor (stage 1, bounded) *then*
close the backup node manager, or make `close()` genuinely retryable for the documented stages.

### C7. `StorageBinaryDataMerger.scheduleMaterialization` — missing flush signal contradicts its own comment; multi-second stalls
`storage/types/StorageBinaryDataMerger.java:493–507`. Comment says the coalescing worker is
signalled first; no code sets `flushRequested`/`signal()` before `awaitQueueDrainedBelowLimit()`.
Under a burst exceeding `cachedBytesLimit` with the worker in `awaitFlushRequestOrTimeout()` (10 s),
the delivery thread parks with nobody waking the worker.
**Fix:** inside the `queueLock` block set `flushRequested = true; flushCondition.signal()`
(mirror `awaitApplied()`), and fix or delete the false comment.

### C8. `ClusterFoundation.start()` can throw raw `ClassCastException` on sneaky checked failures
`ClusterFoundation.java:800–808`: `throw (RuntimeException) failure;`.
**Fix:** rethrow `Error`/`RuntimeException` as-is, otherwise wrap in `NodeLibraryException`.

### C9. `ClusterIndexValidation` — a null Lucene context passes validation
`ClusterIndexValidation.java:345–348`: `if (context != null) validateLuceneContext(context);`
contradicts the class's fail-closed contract.
**Fix:** throw when the context is null ("cannot prove embedded configuration").

### C10. `ClusterIndexMaintenance`/`ClusterIndexValidation` synchronize on the user-visible `GigaMap`
`ClusterIndexMaintenance.java:75,126`. Javadoc claims the map monitor is "the same lock
queries use" — unverified against upstream internals, and shares a monitor with arbitrary
application code that syncs on the map (deadlock vector; silently breaks if Store internals change).
**Fix:** synchronize via the already-held coordinator write section or an explicit private
lock owned by `ClusterStoreIndexes`; remove the unverifiable javadoc claim.

### C11. `StorageBinaryData*Exception` hierarchy extends `IllegalStateException`
`storage/types/StorageBinaryData{Exception, LifecycleException, ReseedException}`.
Domain signals (corruption, lifecycle, reseed) are indistinguishable from incidental
`IllegalStateException` catches and from the merger's own plain `ISE("merger has failed")`.
**Fix:** extend `RuntimeException` directly (or a dedicated `ReplicationException` base).

### C12. `StorageBackupManager.stopDataClient()` TOCTOU ordering
`node/backup/StorageBackupManager.java:395–421`: checks outcome before failure; a client that
resolves its boundary *and* fails in the same instant proceeds to backup with a failed reader.
**Fix:** check `failure()` → `RESOLVED_BOUNDARY` → `failure()` again.

### C13. `StoreIndexReflection` field resolution is first-match (silently nondeterministic)
`StoreIndexReflection.java:91–107` (+ `VectorGraphFields`). First assignable field wins; an
upstream layout change silently binds the wrong one instead of failing loudly — this class's
whole stated purpose.
**Fix:** collect all matches, throw unless exactly one; same uniqueness assert per field name.

---

## P1 — Performance / liveliness

### P1. `StorageBinaryDataMaterializer.materialize` — per-transaction allocation storm
`storage/types/StorageBinaryDataMaterializer.java:66–101`. Each transaction allocates a new
`ObjectMaterializer` (+`persistenceManager.createLoader()`), `Arrays.copyOfRange(buffers, …)`,
`ImportedBinarySource`, anonymous `PersistenceSource`, `LoadItemsChain`, `BinaryLoader` —
directly violating AGENTS.md #29 on the replication hot path.
**Fix:** hoist stable per-merger machinery into `Configuration`; one reused `ObjectMaterializer`
per batch; avoid `copyOfRange` by passing `(buffers, offset, length)` views into the import source.

### P2. Role/env re-parsing per replicated message
`ClusterFoundation.java:388–398`: `DataMessageAppliedListener.onApplied` calls
`props.nodeRole()` per applied message — re-reads the env map, trims, iterates enums.
**Fix:** capture the boolean once when creating the listener.

### P3. Reader builder silently derives the wire nonce the wire layer says must never be derived
`AeronArchiveReader.Configuration.Builder.build()` ~231 vs `AeronReplicationEnvelope.defaultWireNonce`
javadoc ("fixture convenience, never a deployment default"). Writer `New(...)` also derives.
The asymmetric default is exactly the configuration-drift hazard the nonce exists against.
**Fix:** pick one policy — require the nonce explicitly in the builder (fail at build), or make
derivation the documented default everywhere.

### P4. `NodeHousekeeper` uses 2 platform threads for blocking maintenance while other executors use virtual threads
`NodeHousekeeper.java:43–49` vs `StorageTaskExecutor`/`StorageBackupTaskExecutor`.
Unexplained asymmetry; GC/backup/directory-walk tasks are textbook virtual-thread workloads.
**Fix:** schedule on virtual threads (or document why not).

### P5. Storage-limit WARNING storm without edge trigger
`node/store/StorageLimitGate.java:101–105`: WARN flood every interval while over the limit.
**Fix:** log on rising edge + throttled repeat (match `StorageDiskSpaceReader` throttling).

### P6. `StorageDiskSpaceReader` full recursive walk under one `synchronized`
`StorageDiskSpaceReader.java:66–76`: all monitoring scrapes serialize behind a directory
tree walk on cache miss.
**Fix:** single-flight refresh: serve stale value while one refresh runs.

### P7. Smaller hot-path allocations
- `AeronArchiveReplicationPublisher.awaitRecorded` allocates a fresh `BackoffIdleStrategy` per commit (~362) — hoist one per publisher.
- `ReplicationCursor` constructor runs `toLowerCase` unconditionally (~52–56) — allocate only on uppercase; better, reject uppercase (fail-closed equality).
- `AeronReplicationEnvelope.payload()` clones on every accessor call (~609–640) — constructor clone is enough; document non-mutation or cache.
- `StorageBinaryBuffers` `ScopedValue` scratch lists buy nothing (allocated per call, never recursed) — replace with locals or a two-pass count (M: remove the ScopedValue machinery).
- `StorageBinaryDataMerger.dictionarySource` retains the last full dictionary string forever (~940) — clear after `provideTypeDictionary()` returns.

---

## P2 — Design & structure (bold but simplifying)

### S1. Break the `storage.* ↔ node.*` layering cycle (biggest structural win)
Upward imports: `AeronArchiveReader → node.aeron.AeronClusterReplicationTransportProvider`,
`RejectingPersistenceTarget → node.exceptions.ReaderWriteRejectedException`, writer trio →
`WriterFencedException`, `BackupMetadata → storage.aeron` codec. The "transport-neutral
`storage` layer" claim in the package-info is not true.
**Fix:** (a) consolidate all shared exceptions (`NodeLibraryException`, `WriterFencedException`,
`ReseedRequiredException`, …) into one transport-neutral `cluster.exceptions` (or `storage.types`)
package; (b) invert the reader→provider seam behind a small interface; (c) keep the arrow
strictly `node.* → storage.*`.

### S2. Split the `storage.types` god package (28 files, ≥5 concerns)
- `storage.types` — binary replication contract only (`Distributor/Client/Receiver/Merger/Materializer/Importer/Buffers`, type-dictionary exporter).
- `storage.indexes` — `ClusterStoreIndexes`, `ClusterIndexValidation`, `ClusterIndexMaintenance`, `StoreIndexReflection`.
- `cluster.internal.files` (or `node.files`) — `AtomicFileWriter`, `PathSecurity`, `Crc32c`, `StorageFileOperations`.
- Move `DistributedStorage`/`ObjectGraphUpdateHandler`/`StorageGraphCoordinator` to `node.store`
  (consumed only by the assembly layer).
The package-info's "visibility forces one package" argument is demonstrably false: most of
these classes are already public and cross-used.

### S3. Collapse duplicated concept hierarchies (the strongest simplification available)
| Concept | Duplicates | Proposal |
|---|---|---|
| Retry | `ReplicationRetry` statics + `AeronRetryPolicy` record | merge into `AeronRetryPolicy` (or one `cluster.internal.RetryClock`) |
| Position/cursor | `ReplicationCursor`, `AeronReplicationCursor`, `CursorSnapshot`, `AeronWriterBoundary`, `AeronReplicationCheckpoint`, `AeronReaderWatermark` (6 types, 4 packages) | one position model in `storage.aeron.checkpoint`; aliases for the transport-neutral `ReplicationCursor` |
| Cursor persistence | `ReplicationCursorStore` (static codec) + `StoredReplicationCursorManager` (1-impl interface) | single final class `DurableCursorFile` with codec inline |
| Health/metrics | `ReplicationHealth` → `StorageNodeHealthCheck` (pure pass-through) → `StorageNodeManager` (9 `-1` defaults) → `ReplicationMetrics` → `NodeStatus` | delete the pass-through layer; `StorageNodeControl.replicationMetrics()` reads `ReplicationHealth` directly; replace the 9 default `-1` hooks with one `ReplicationStats`-like record |
| Aeron config | `AeronReplicationConfiguration` (storage) + `AeronSettings` (node, 1126 lines) | one config stack in `storage.aeron.config`; split `AeronSettings` into `Directories/Channels/Archive/Auth/Identity` records + one parser |

### S4. Split two god classes
- `AeronClusterReplicationTransportProvider` (1664 lines): writer+reader+runtime+health+fencing
  in one synchronized class — the 1-writer/N-reader invariants are hard to audit. Split into
  `AeronWriterTransport` / `AeronReaderTransport` over a shared `AeronRuntime`, thin dispatcher
  by role.
- `StorageBinaryDataMerger` (1208 lines): queue/accounting/conditions + type-dictionary
  planning + batch worker + disposal FSM. Split into `MergerQueue`, `DictionaryMerger`,
  batch worker. The interleaved-lock preamble becomes per-class contracts.
- Also: `ClusterFoundation` (1352 lines, 21 lazy collaborators, reseed gate duplicated 3×),
  `AeronSettings` (see S3).

### S5. Fix four redundant close/health failures-conventions
`CloseSequencer`, `StorageNodeManager.CloseFailures` (private duplicate), two hand-rolled
try/catch accumulations. Delete `CloseFailures`; route everything through `CloseSequencer`.

### S6. Shrink the non-exported `public` surface
Module exports only `api`, yet ~60 types are public. Package-private candidates:
`NodeLibraryPropertiesProvider`(+`Env`/`EnvKeys`), `NodeRole`, `CloseSequencer` (its javadoc
admits it), `AeronPositionProvider`, plus whatever S1–S3 consolidations free up. Where public
is load-bearing across packages, the package split (S2) usually lets it go package-private.

### S7. Small targeted removals / demotions
- `WriterLeaseGate.of(...)` single-check overload — apparently unused; delete.
- `WriterFencingLease.isCurrent()` vs `isCurrentUncached()` — identical behavior under misleading
  names; delete one or implement a real uncached check (`checkOwnershipNow()`).
- `AeronPositionProvider.close()` no-op + dedicated foundation close stage — dead ceremony; drop both.
- `AeronCheckpointCodec` interface-of-statics → final class + private constructor (AGENTS.md #18).
- `StorageBinaryDataDistributor` silent no-op defaults + `null` dictionary-as-clear-signal —
  make `messageIndex()/ignoreDistribution()` abstract; add explicit `clearStagedTypeDictionary()`.
- `NodeHousekeeper` as single writer: consider renaming `NodeMaintenanceScheduler` (see N-table).
- Plain check hierarchy fix: `PathSecurity.ensureNoSymbolicLinks(null)` no-op → `requireNonNull`.

### S8. Constructor arity (AGENTS.md #16/17)
13-arg `AeronHealth`, 9-arg `AeronPositionProvider`, 7-arg `BackupRestorePolicy`, 11-component
`StorageBinaryDataMerger.Configuration`, 21-component `NodeConfiguration` (whose public builder
exposes only 4 fields — 17 are private test seams). Introduce small records
(`AeronHealthProbes`, `WriterBoundarySource`, `CursorLifecycle`) or builders seeded from defaults;
for `NodeConfiguration` either expose a real builder or drop the unused plumbing.

---

## P3 — Naming (compile-time renames, all-javadoc updates)

| Current | Problem | Proposal |
|---|---|---|
| `ClusterFoundation` / `.Node` | Store jargon; assembly+runtime | `ClusterNodeRuntime` / `AssembledNode` |
| `NodeLibraryPropertiesProvider` | "Library" meaningless | `NodeSettings` + `fromEnvironment()` |
| `AeronClusterReplicationTransportProvider` | 1.6k-line mouthful | `AeronReplicationTransport` (drop provider indirection — Aeron is the only transport) |
| `StoredReplicationCursorManager` / `ReplicationCursorStore` | two "cursor store" names, not managers | `DurableCursorFile` + codec folded in |
| `StorageBinaryDataClient` | "client" of nothing | `ReplicationApplier` / `StorageDataApplier` |
| `DataMessageAppliedListener` | means "commit applied + persist cursor" | `CommitAppliedListener` |
| `StorageTaskExecutor` / `StorageBackupTaskExecutor` | run one kind of check/backup | `StorageCheckRunner` / `BackupScheduler` |
| `NodeHousekeeper` | purposely vague | `NodeMaintenanceScheduler` |
| `StorageDiskSpaceReader` | usage of one directory | `StorageUsageGauge` |
| `AeronReaderWatermark.Quorum.rejectsReader` | answer conflates unknown vs retired | return enum `{ACTIVE, RETIRED, UNKNOWN}` or `isNotActiveReader` |
| `StorageBackupManager` vs `BackupNodeManager` | "archive creation policy" vs "backup node control" | `BackupArchiveCreator` / `BackupNodeControlImpl`; route backup-node health through `StorageNodeHealthCheck` wrapper |

Package moves: `ReseedRequiredException` → the consolidated exceptions package (used by
transport-neutral code); `BackupRestorePolicy` → `node.backup`; `BackupBusyException` →
align with the exception-package rule.

---

## P4 — Javadocs/docs fixes

1. `module-info.java`: typos (`authntication`, `air-gaped`); remove the historical
   "removed HTTP backup transport" note (belongs in an ADR).
2. `node/replication/package-info.java` first sentence claims the package "carries Aeron
   replication between nodes" — it is explicitly transport-neutral; rewrite the first sentence.
3. `storage/types/package-info.java` — the 47-line god-package defense disappears with S2.
4. `ReplicationHealth` leaks `System.Logger LOGGER` as an interface constant — move to the impl.
5. `NodeHousekeeper` javadoc grammar ("Runs a task must fail consecutively…").
6. `ReplicationMetrics` contains a broken markdown link (`[ #lagTransactions()]`); sweep for
   backticks that should be `{@code}`/`{@link}`.
7. `AeronReplicationCheckpoint.fencingToken` javadoc ("always positive") contradicts the
   compact constructor's `0` sentinel for unresolved readers — consolidate the sentinel story
   in one place.
8. `api/ClusterNode` first sentence is jargon; "Owns one datagrid node: its Store, replication,
   and lifecycle" reads better.

---

## API package assessment

`api` is small and directional-correct (records, no Optional misuse, minimal exports). Two
fixes:
- `NodeStatus.transport` is a free `String` while `ReplicationState` duplicates
  `ReplicationHealth.State` with a hand switch — inconsistent insulation; make transport an enum
  or drop `ReplicationState`.
- `NodeStatus`'s 13 flat components with 6 separate `-1`-sentinels: group the replication
  fields into one nested record, documenting the sentinel once.

## Misc verified-quiet areas (no action)
Offer-result handling (BACK_PRESSURED/NOT_CONNECTED retry with full-jitter, fail-fast on
CLOSED/MAX_POSITION_EXCEEDED), lock ordering (writer coordinator → publisher monitor →
offerLock), archive-ack waits outside locks, checkpoint write-then-rename with NOFOLLOW_LINKS,
and the barrier batching in `TransactionAssembler` appear sound. Low-confidence suspect flagged
in C2 (cold-restart resume) is the one that needs a dedicated reproduction test.

---

*Review only — no code was modified and no builds were run.*
