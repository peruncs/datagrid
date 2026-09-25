# MIMO Review — Peruncs Data Grid (cluster focus)

Static review only (no builds, no tests, no code changes). Scope: cluster correctness,
liveliness/throughput/memory, package structure, naming, visibility, Javadocs.

Evidence base: full reads of the hot paths cited below; codebase-memory graph
(`Users-hristo-projects-github-peruncs-datagrid`, index 2026-09-20) for fan-in and
call structure; `check_index_coverage` reported one partial-parse gap
(`storage/types/ClusterIndexValidation.java` ~L415) and non-tracked paths — claims
below were verified against source. Existing `SPARK_REVIEW.md` / `SOL_REVIEW.md`
overlap partially; this file is the prioritized actionable set for the focus areas.

Priority: **P0** correctness / data-loss or self-fencing under load · **P1** liveness,
throughput, structural debt that misleads · **P2** robustness, polish.

---

## P0 — Correctness (fix first)

### C1. Half-initialized writer is returned after recovery failure
**Where.** `AeronClusterReplicationTransportProvider.ensureWriterLocked`
(`AeronClusterReplicationTransportProvider.java:887–963`): `this.writer` is assigned
at L909/L923; the `catch` at L954–960 sets `writerRecoveryState` but does **not**
clear `this.writer`. The fast path `if (this.writer != null) return this.writer`
(L891) then hands out a publisher whose recording/boundary validation never finished.

**Consequence.** Subsequent writes/extensions run against a half-recovered writer;
fail-closed recovery state is visible in health while the write path still believes
the writer is live.

**Fix.** Assign `this.writer` only after all fallible init succeeds, or null it in
both `catch` blocks (and in `finally` when `writerRecoveryState != null`). Add a
crash-matrix cell that fails after publisher construction and asserts the next
`ensureWriterLocked` retries recovery instead of returning the stale publisher.

---

### C2. Terminal-marker offer holds the interprocess lease lock for the whole retry loop
**Where.** `WriterFencingLease.executeUnderOwnership` (`WriterFencingLease.java:518–559`)
holds `writer-lease.lock` (`FileLock`, L527–529) across `offer.offer(...)` (L540).
`AeronOfferRetryer.offerLoop` (`AeronOfferRetryer.java:73–120`) can park/retry until
`offerTimeoutNanos` while that lock is held. The background heartbeat renew
(`refreshHeartbeat*`, same lock path) and `close()` (L606–608) block behind it.

**Consequence.** Back-pressure or a slow subscriber for longer than lease staleness
self-fences the writer mid-commit (heartbeat cannot renew → successor steals →
`WriterFencedException` / dual-writer risk window). Close and peer takeover also
stall for the full offer timeout.

**Fix.** Split the critical section: short lock to snapshot ownership + refresh
heartbeat; run offer retries **outside** the file lock; re-check ownership under a
short lock per attempt and after a successful offer. Keep the “refresh before
release on failure” invariant (L547–558) without wrapping the offer itself. Bound
the time from lease-check to durable terminal well below `maxStaleness`, and fail
fast if the remaining budget is insufficient.

---

### C3. Checkpoint fsync runs while `writeLock` is held (prepare/enqueue phases)
**Where.** `AeronReplicationWriteCoordinator.prepareLocked`
(`AeronReplicationWriteCoordinator.java:333–431`) runs under `writeLock`
(`prepare` L324–330) and calls `notifyState(PREPARING, …)` at L391–394.
`markEnqueuedLocked` (L443–459) similarly holds the lock across `notifyState(ENQUEUED, …)`.
`notifyState` → provider listener → `persistWriterCheckpoint` → `AtomicFileWriter.write`
(file + directory fsync). `lockWriteAdmission` (L585–598) is a fixed ~100 µs
`tryLock` park with no `commitDone` wait.

**Consequence.** Every ARCHIVE_FIRST transaction pays 1–2 forced file writes while
all write admission, dictionary staging, and `withWritesPaused` maintenance queue
behind fsync latency. Throughput ceiling = disk sync rate; readers see offer
back-pressure cascades.

**Fix.** Treat checkpoint writes like the commit slow phase: flip in-memory
transaction state under `writeLock`, run `AtomicFileWriter` unlocked, re-acquire
briefly to publish the durable boundary / clear fences. Never call
`persistWriterCheckpoint` under `writeLock`. Prefer a group-commit append log
(one fsync per batch/window) as the recovery boundary; keep the crash-matrix
oracle (`CONTINUE` vs `RESEED_REQUIRED`) as the acceptance gate.

---

### C4. Shared Store `PersistenceManager` is closed through the adapter
**Where.** `ClusterStorageManager.BinaryPersistenceManagerAdapter.close`
(`ClusterStorageManager.java:695–697`) does `delegate.close()` on the Store’s live
`PersistenceManager` (lazy wrapper around `delegate.persistenceManager()`, L196–197).

**Consequence.** Any caller that closes the adapter (or a framework path that closes
the `PersistenceManager` it was handed) closes the shared Store PM under the live
graph → use-after-close / corruption on the next read or write.

**Fix.** Adapter `close()` must be a no-op (or ref-counted with the owning `Store`).
Only the Store lifecycle closes the PM. Add a test that closes the adapter and
asserts subsequent `store`/`read` still work.

---

### C5. `lastResolvedSequence` advances before the durability callback
**Where.** `TransactionAssembler.flushDeliveries`
(`TransactionAssembler.java:860–909`): inside the drain loop, `lastResolvedSequence` /
`lastResolvedPosition` / witness fields are set (L886–896); only after the loop does
`transactionResolved.run()` run (L903). `cursorSnapshot()` (L508) reads the same fields.

**Consequence.** A crash, `cursorSnapshot`, or health scrape between L889 and L903
observes a sequence whose Store force has not completed → restart can treat a
sequence as applied when the durable Store image does not match (torn-boundary /
spurious reseed depending on checkpoint pairing).

**Fix.** Invoke `transactionResolved` (Store force + cursor write) **before** publishing
`lastResolved*`, or make snapshots read only from a state written after force.
Keep the “one barrier = one durability unit” comment, but order the stores to match it.

---

## P1 — Liveliness, throughput, structural debt

### L1. Housekeeper clears global degradation on any single task success
**Where.** `NodeHousekeeper.runGuarded` (`NodeHousekeeper.java:58–67`): success does
`consecutiveFailures.remove(name)` **and** `degradedFailure.set(null)`.

**Consequence.** Task A can stay over the failure threshold while task B’s success
hides A’s degradation from `failure()` / readiness until A fails again.

**Fix.** Track failure state per task id; recompute the aggregate from the map (clear
only that task’s contribution). Keep fatal `Error` sticky as today.

---

### L2. `ensureCoordinator()` ignores `writerRecoveryState`
**Where.** `AeronClusterReplicationTransportProvider.ensureCoordinator` (~L1025)
builds/returns a coordinator without consulting `writerRecoveryState` or forcing
`ensureWriterLocked` recovery first (unlike the writer path).

**Fix.** Treat non-null `writerRecoveryState` like the writer path: run recovery or
throw fail-closed before exposing a coordinator.

---

### L3. Retention agent: blocking `Future.get` + permanent `terminalFailure`
**Where.** `AeronArchiveRetention.onAgent` (`AeronArchiveRetention.java:149–190`)
submits to a single virtual-thread agent and blocks on `Future.get` with
`operationTimeoutMillis` (default 60 s, L82). Timeout latches `terminalFailure`
(L173) permanently; the in-flight Archive purge/stop/extend is not cancelled.
Watermark recording (`recordReaderWatermark` → `onAgent`) therefore pins the
watermark worker for up to 60 s and can terminal-fail retention on a slow purge.

**Fix.** Fire-and-forget watermark record from the channel callback (errors via
`channelFailure()` / `failure()`); do not block the watermark worker on retention.
 Allow retry after a timeout if the purge future completes later, or split
`deleteThrough` onto its own agent so a watermark never queues behind a purge.
 Debounce `persistState` (L605) off the hot path (max one fsync per N ms / position delta).

---

### L4. Deferred watermarks dropped on any `RuntimeException`
**Where.** `WatermarkFanIn.drainDeferred` (`WatermarkFanIn.java:160–179`): on
`recordReaderWatermark` failure the entry is removed (L176) and counted as rejected.

**Consequence.** A transient failure (agent busy, re-entrancy, I/O) permanently
discards a valid quorum ack; retention waits for the reader’s next cursor.

**Fix.** Drop only on permanent validation errors (epoch/id/cluster mismatch). On
transient failure, keep the deferred entry (bounded retry / next drain).

---

### L5. Reader reconnect budget ignores archive liveness without new data
**Where.** `AeronArchiveReader.reconnectAfterArchiveLoss` (~L645) +
`failIfReconnectBudgetExpired`: budget starts once and is refreshed only by resolved
progress (`incidentBaselineSequence` vs `lastResolvedSequence`). A healthy Archive
with no new traffic can exhaust the stop-timeout budget → spurious
`StorageBinaryDataReseedException` / `RESEED_REQUIRED`.

**Fix.** Count successful `PersistentSubscription.create` / position probes as budget
refresh, not only sequence movement.

---

### L6. Null-subscription poll ignores `disposeRequested`
**Where.** `AeronArchiveReader.pollSubscription` (`AeronArchiveReader.java:525–529`):
`current == null` calls `reconnectAfterArchiveLoss()` unconditionally; the
`ArchiveException` branch (L558) correctly checks `!active.get() || disposeRequested`.

**Consequence.** Dispose can spend the whole stop budget in reconnect attempts.

**Fix.** Guard the null branch with `if (!active.get() || disposeRequested) return 0;`.

---

### L7. Partial close resets `closing` → non-idempotent retry of half-closed state
**Where.** `AeronWatermarkChannel.close` resets `closing = false` on failure paths
(L331, L337, L389, L394); `AeronClusterReplicationTransportProvider.close` same
pattern (L1475, L1486).

**Fix.** Keep `closing` true until a full success; on failure leave a terminal
“close failed” state (or mark per-resource closed flags) so a second `close()` only
retries unfinished resources, never re-opens half-torn ones.

---

### L8. Reseed terminal for resumed reader on duplicate terminal (witness null)
**Where.** `TransactionAssembler.accept` (`TransactionAssembler.java:252–256`): after
restart `lastResolutionKind == null`; a redelivered COMMIT/ABORT throws
`"terminal witness is unavailable"`.

**Consequence.** Live-join redelivery of the last commit forces fail-closed instead of
idempotent accept.

**Fix.** Treat null witness as “unknown”: accept a terminal that matches the durable
cursor position/sequence idempotently (or persist the witness in the cursor).
Prefer persisting witness fields in the cursor/checkpoint (kind, crc, length, chunks)
so the check stays strict.

---

### L9. Double CRC on writer and reader hot paths
**Where.**
- Writer: `AeronReplicationPublisher.transactionMetadata` → `computeDataCrc`
  (`AeronReplicationPublisher.java:345`) then `publishDataChunks` recomputes
  (`L385` / `computeDataCrc` L599–600) for verification/commit CRC.
- Reader: envelope decode checksums payload; `Transaction.add` → `updateDataCrc`
  (`TransactionAssembler.java:652–664`) walks the same bytes again into the
  long-lived `CRC32C`.

**Consequence.** ~2× CRC32C CPU and memory bandwidth on both directions; dominates
64 MiB transactions and backlog replay.

**Fix.** Compute the full payload CRC once on the writer and pass it into chunk
publish (per-chunk CRCs only while copying). On the reader, feed the decoder’s
payload pass into the assembler accumulator; keep only the wire-header check and the
terminal commit-CRC compare.

---

### L10. Per-call `ByteBuffer.duplicate()` in `Crc32c.update`
**Where.** `Crc32c.update` (`Crc32c.java:90`): `buffer.duplicate()` every call.
Hot on every envelope header/payload checksum (`ChecksumContext.compute`,
`AeronReplicationEnvelope.java:64–68`).

**Fix.** Hold one reusable `ByteBuffer` view in the checksum context; set
`position`/`limit` instead of duplicating (mirror `TransactionAssembler.dataCrcView`).

---

### L11. `newWriterCheckpoint` → `recordingId()` under publisher monitor / possibly under lock
**Where.** `AeronArchiveReplicationPublisher.recordingId` is `synchronized` and may
`findRecordingId` (Archive catalog scan + control RPC) (`L614–626`). Checkpoint path
from `persistWriterCheckpoint` / `newWriterCheckpoint` (`Provider` ~L1217–1243)
calls it on every checkpoint.

**Fix.** Cache recording id in an `AtomicLong` set once at publisher create/extend;
checkpoint encode reads only primitives already on the transport.

---

### L12. Archive await loops poll `pollForErrorResponse` every spin
**Where.** `AeronArchiveReplicationPublisher.awaitRecorded` and siblings
(`L407`, `L429`, `L581`): `pollForErrorResponse` on every idle cycle while waiting
for recorded position — `synchronized(archive)` contention with retention/extend.

**Fix.** Poll errors only on the same cadence as `nextArchiveProbe`
(`archiveProbeDelayNanos`), or every N idle steps.

---

### L13. `forceDirectory` resolves `os.name` on every metadata write
**Where.** `AtomicFileWriter.forceDirectory` (L251–255),
`StorageFileOperations.forceDirectory` (L220–221), `PathSecurity` (L64): property
lookup + locale + compare per call; multiplies with checkpoint/cursor/lease fsync.

**Fix.** `static final boolean WINDOWS = ...` (and mac flag) at class init.

---

### L14. `lockWriteAdmission` fixed 100 µs park, no condition wait
**Where.** `AeronReplicationWriteCoordinator.lockWriteAdmission` (L585–598):
`while (!tryLock()) parkNanos(100_000)` — ignores `commitDone` already used at L870.

**Fix.** On failure, `commitDone.awaitNanos(...)` (or exponential backoff 1 µs→1 ms)
so waiters do not spin 10×/ms for the whole fsync hold.

---

### L15. Envelope `payload.clone()` twice on the wire object path
**Where.** `AeronReplicationEnvelope.Envelope` (L613, L639): clone on construction
and again on `payload()` access.

**Fix.** Encode straight into the CBE buffer / offer path (already done for publish);
for the record form, keep a single copy or expose a read-only view. Remove the
second clone from the accessor.

---

### S1. `storage` → `node` layering inversion
**Where.**
- `storage/aeron/reader/AeronArchiveReader.java:8` imports
  `node.aeron.AeronClusterReplicationTransportProvider` (lower layer → god provider).
- `storage/aeron/writer/{AeronOfferRetryer,WriterLeaseGate,AeronReplicationWriteCoordinator}`
  and `storage/types/RejectingPersistenceTarget` import `node.exceptions.*`.

**Fix.** Move fencing/rejection exceptions into a leaf package both sides may import
(`cluster.errors` or `storage.types`), or define a tiny SPI in `storage.aeron.reader`
for whatever the reader needs from the provider. Break the provider import.

---

### S2. Dual Aeron trees: watermark + fencing split across `node.aeron` and `storage.aeron.*`
**Where.** Watermark: `node.aeron.AeronWatermarkChannel` / `WatermarkFanIn` /
`AeronArchiveRetention` vs `storage.aeron.checkpoint.AeronReaderWatermark`.
Fencing: `node.aeron.WriterFencingLease` vs `storage.aeron.writer.WriterLeaseGate`.

**Fix.** One owner per concern: wire/codec types under `storage.aeron.*`, node
lifecycle orchestration under `node.aeron`. Move `AeronReaderWatermark` next to the
channel that produces/consumes it, or move the channel next to the record type.

---

### S3. `storage.types` is a 27-file god package
**Where.** Contracts, `Crc32c`, `AtomicFileWriter`, `PathSecurity`,
`DistributedStorage` (single main caller `ClusterFoundation:995`), index policy
(`ClusterIndex*`, `StoreIndexReflection`), exceptions, exporter — one package
(`storage/types/package-info.java` even documents the mutual-coupling excuse).

**Fix.** Split: `storage.types` (replication contracts + cursor), `storage.io`
(atomic write, path security, CRC), `storage.index` (Lucene/JVector policy).
Move `DistributedStorage` to `node.store` (or fold into `ClusterFoundation` wiring).
Update package-infos; keep package-private seams where needed by adjusting package
placement rather than widening visibility.

---

### S4. `cluster.node` root mixes unrelated roles
**Where.** `BackupRestorePolicy`, `UserUploadValidator` (backup-only),
`CloseSequencer` (lifecycle), `NodeHousekeeper`, properties provider, role, config,
foundation, two manager interfaces.

**Fix.** `BackupRestorePolicy` + `UserUploadValidator` → `node.backup`;
`CloseSequencer` → `node.lifecycle` (or a small `node.close`); keep foundation /
control / role / housekeeper in the root only if they stay cohesive.

---

### S5. Exceptions scattered; two reseed types; false “exported” claims
**Where.** `node.exceptions` (5), `node.backup.BackupBusyException`,
`node.aeron.ReseedRequiredException`, `storage.types.StorageBinaryData*Exception`,
`storage.aeron.wire.ReplicationWireException`, nested
`BackupArchive.IncompleteArchiveException`. `BackupBusyException:9` and
`WriterFencedException:12` claim “exported contract / exported type” but
`module-info` exports only `peruncs.cluster.api`. README documents
`storageNodeManager()`, `BackupBusyException`, `AeronArchiveReader.New` — none are
reachable outside the module.

**Fix.** Pick one: (a) export thin API exceptions from `cluster.api`
(`BusyBackupException`, `WriterFencedException` facade) and re-export, **or**
(b) stop claiming export and document message-prefix matching as the contract
(weaker — prefer (a)). Unify `ReseedRequiredException` and
`StorageBinaryDataReseedException` under one type (keep the `RESEED_REQUIRED:`
message prefix for logs). Rewrite README “Programmatic control boundary” to the real
`ClusterNode` surface or export the control packages intentionally.

---

### S6. Unnecessary `public` surface (non-exported packages)
Demote to package-private (or document as internal-only if cross-package is required):
`StorageNodeManager` (refs only in `cluster.node`),
`BackupArchiveLimits`, `BackupBusyException` (after S5 decision),
`ReplicationWireException`, `StorageBinaryDataLifecycleException`,
`DistributedStorage` (after move), `CrashHook` (test seam in **main** — see S7),
`StorageFileOperations` / `CloseSequencer` (already apology-javadoc “public only
because packages share it” — fix by package placement, not apology).
Nested `Stage`/`Action` on `CloseSequencer` are public solely for cross-package use.

---

### S7. `CrashHook` is public production API for test seams
**Where.** `storage/aeron/writer/CrashHook.java` (main): invoked from publisher,
coordinator, target, lease, provider. Javadoc says applications must never install a
hook.

**Fix.** Keep the seam but make the type package-private where possible; expose
hooks only through test-support types in `src/test`. If main code must call it,
use a package-private holder in each package or a single internal
`crash.CrashPoints` class in a non-exported package with no public methods.

---

### S8. Interface + nested `Default` + `New()` factory repeated 10×
**Where.** Nested `Default` in `StorageBackupManager`, `StorageBackupTaskExecutor`,
`BackupNodeManager`, `ClusterStorageManager`, `StorageDiskSpaceReader`,
`StorageNodeHealthCheck`, `StorageTaskExecutor`, `StorageNodeManager`,
`StoredReplicationCursorManager`, `StorageBinaryDataMerger`. ~27 `static … New(`
factories in main. `AeronCheckpointCodec` is an interface of statics only
(`AeronCheckpointCodec.java`).

**Fix.** Prefer `final` class + package-private ctor + `of(...)` (rule 18 for
static-only types). Where an interface remains, name the impl by role
(`FixedRoleStorageNode`, `ScheduledBackupService`) or promote it top-level so stack
traces and navigation show a real type name. Rename all `New` → `of` / `create`,
`NoOp()` → `noOp()`.

---

### S9. God records: 14–21 components
**Where.** `NodeConfiguration` (21, `NodeConfiguration.java:46`),
`AeronSettings` (26+ scalars + groups, `AeronSettings.java:67`),
`NodeStatus` (13, duplicates `ReplicationMetrics`),
`AeronReplicationConfiguration` (14), `AeronReplicationCheckpoint` (14).

**Fix.** Nest by concern: `NodeConfiguration` → `StorageCollaborators` /
`ReplicationCollaborators` / `BackupCollaborators` (or delete unreachable nullable
fields — only four builder setters are public; the rest is a service locator).
`AeronSettings` → already has `Channels`/`Directories`/`Auth`; finish the job for
timeouts/identity/archive tuning. Compose `NodeStatus` from `ReplicationMetrics` +
role flags instead of 13 flat fields.

---

### S10. Naming offenders (worst first)
| Current | Suggested | Why |
|---|---|---|
| `ClusterFoundation` | `NodeRuntime` / `NodeAssembly` | Not a “foundation”; owns managers + lifecycle |
| `AeronClusterReplicationTransportProvider` | `AeronTransportProvider` | Three synonyms; 6+ “Provider” types |
| `NodeLibraryPropertiesProvider` | `NodeEnvironment` / `NodeEnv` | It is env/config, not a “library” |
| `NodeLibraryException` / `NodeLibrary*` | `NodeException` / drop “Library” | “Library” is leftover product naming |
| `StorageNodeManager` | `StorageNodeLifecycle` | Fixed role + start/close, not generic manager |
| `StoredReplicationCursorManager` | `CursorFileStore` | README says “cursor” |
| `ClusterStorageManager` | `ClusterStorage` / `StoreAdapter` | 944-line interface + adapters |
| `Crc32c` | `Crc32C` | Constant/method casing |
| `WatermarkFanIn` | `WatermarkCollector` | “FanIn” is jargon |
| `AeronReaderSlot` | `CurrentReader` | Holds one current reader |
| `New()` / `NoOp()` | `of()` / `noOp()` | Java naming (29 sites) |
| Nested `Default` | role name | Meaningless in stack traces |

Also: `StorageBinaryDataClient`/`Distributor`/`Merger`/`Receiver` read as generic IO;
align with README vocabulary (`ReplicationIngest` / `ReplicationPublish` /
`ReplicationApply`) or document the mapping in package-info once.

---

### S11. Duplicate state enums
`api.ReplicationState` vs `ReplicationHealth.State` (internal) — mapped in
`ClusterNode.map` (`ClusterNode.java:95–102`); `DEGRADED` vs `DEGRADED_ARCHIVE`.
**Fix.** Single source of truth (api enum as the public projection, derive internal
from it, or export the internal enum and drop the api copy).

---

## P2 — Robustness & documentation polish

### R1. Retention message-string classification
`AeronArchiveRetention.isReplayInProgressDetach` (L599–602) matches
`failure.getMessage().contains(...)` on `GENERIC`. Pinned by tests to Aeron 1.53.
**Fix.** Prefer error codes; keep the string probe as a last resort with a version
guard comment.

---

### R2. `WriterFencingLease.close` ignores lock failure then `ACTIVE.remove`
`WriterFencingLease.close` (L618–623): lock acquisition failure is logged; registry
entry still removed.
**Fix.** Propagate or retry; do not remove from `ACTIVE` until the lock is proven
released (or record an orphan for reconciler).

---

### R3. Terminal `driverFailure` vs admission
`driverFailure` is checked in some paths (`Provider` L855, L1254) but write
admission/`executeWriteAtomically` can still accept work against a dead driver.
**Fix.** On terminal driver failure, flip a fail-fast flag in admission with the
terminal cause.

---

### R4. `AeronOfferRetryer` interrupt drops original cause
Interrupt path (L84–87) throws without chaining a prior `ArchiveException`.
**Fix.** Preserve cause via suppressed/cause chaining.

---

### R5. `AeronReaderSlot.replace` null window
Dispose old then create new under lock; lock-free observers can see `current == null`.
**Fix.** Publish new before removing old, or require the slot lock for readers.

---

### R6. Retention state file vs epoch bump
`AeronArchiveRetention` state (version 4) does not migrate when `writerEpoch`
increments; runtime rejects mismatched epochs (L591) but the file can stay stale.
**Fix.** Rewrite state when stored epoch ≠ runtime epoch on open.

---

### J1. module-info typos + wrong security story
`module-info.java:19–20`: “authntication”, “a air-gaped”, and “authentication not
required”. A VPN is not an air gap; README itself says the network is the boundary.
**Fix.** “Nodes require a trusted network (VPN/firewall); authentication is out of
scope for cluster frames. Archive control auth is separate.” Correct spelling; state
the invariant positively.

---

### J2. False export / API docs
- `BackupBusyException:9`, `WriterFencedException:12`: “exported” — false until S5.
- `exceptions/package-info` promises types “for the embedding application” while the
  package is not exported.
- Exported `ClusterStore.store/storeAll/storeRoot` have no `@throws` for
  `ReaderWriteRejectedException` / `WriterFencedException` (and those types are not
  exported — callers can only catch `RuntimeException`).
- `ClusterNode` backup APIs do not document busy/conflict.
- `api/package-info` (7 lines) never mentions single-writer constraint or exceptions.

---

### J3. Jargon-first sentences (rewrite first sentence in plain language)
| File | Current first sentence | Suggested |
|---|---|---|
| `StorageNodeHealthCheck` | “Neutral storage + replication readiness gate.” | “Reports whether the Store and replication are ready to serve.” |
| `ReseedRequiredException` | “Typed fail-closed signal for recovery evidence that cannot be reconciled.” | “Thrown when the node cannot recover state and must be reseeded.” |
| `StorageNodeControl` | “Protocol-neutral control and observability view of a node manager.” | “Read-only status surface for one storage node (no close).” |
| `ReplicationPositionProvider` | “Latest-position and provider-readiness contract used by neutral lifecycle code.” | “Supplies the writer’s latest durable sequence and position.” |
| `NodeRole` | “Fixed-topology node role, normalized from the legacy and current settings.” | “The node’s fixed role: writer, reader, or backup-reader.” |
| `storage/package-info` | “…contracts and Aeron replication machinery.” | “Replication contracts and the Aeron implementation.” |
| `storage/types/package-info` | “persistence replication contract and its package-private machinery.” | One purpose paragraph + pointer to ADR. |

---

### J4. Package-infos: design essays and commit messages
- `storage/types/package-info` (~47 lines): visibility manifesto — replace with
  purpose + “see ADR”.
- `storage/aeron/wire/package-info`: ScopedValue history is a commit message —
  keep the one-line “checksum context is explicit, not thread-local” rule.
- `node/aeron/package-info`: env-var selection belongs in README/config reference.

---

### J5. Spelling / markup consistency
- UK/US: `materialisation` vs `materialization` (reader package + receiver);
  `authorisation` in `AeronSettings` javadoc vs US identifiers elsewhere — pick US
  to match code.
- Broken markup: `ReplicationMetrics` `[ #lagTransactions()]` (space breaks link);
  `ClusterFoundation` mixes `{@link}` and markdown `[...]`; `NodeLibraryPropertiesProvider`
  `@since` placement mid-description.
- `DistributedStorage` javadoc embeds a test name + `StackOverflowError` autopsy —
  move to a code comment.

---

### J6. Single-use types / methods to fold
- `UserUploadValidator` — only `ClusterFoundation` → fold into
  `BackupRestorePolicy` or `FilesystemVolumeBackupBackend`.
- `DistributedStorage.configureWriting` — one production caller → private wiring
  in foundation (keep tests via package-private seam).
- Private one-shot methods: `WriterFencingLease.suspendHeartbeatForTest`,
  `WatermarkFanIn.hasChannel`/`channelFailure` helpers, `AeronSettings.clearAuthCredentials`
  — inline or fold.

---

### R7. `NodeConfiguration` mostly inert
Only four public builder setters; remaining collaborators are null-selected service
locator. **Fix (bold simplification):** delete unreachable fields; build runtime from
root supplier + Store foundation + replication properties; keep explicit test seams
only where behavior is truly substituted.

---

### R8. `BackupNodeControl extends StorageNodeControl`
Backup nodes inherit `startStorageChecks` / `readStorageSizeBytes` they may not own.
**Fix.** Composition: `BackupNodeControl { StorageNodeControl storage(); …backup ops }`.

---

## Suggested fix order

1. **C1** half-init writer (smallest change, blocks safe startup).
2. **C2** lease lock vs offer retry (fencing under load).
3. **C4** shared PM close (Store corruption).
4. **C3** checkpoint fsync off `writeLock` + group-commit evaluation (crash matrix).
5. **C5** resolve-before-publish ordering in `flushDeliveries`.
6. **L1–L8** liveness cluster (housekeeper, coordinator guard, retention, watermark,
   reconnect, dispose, close idempotency, reseed witness).
7. **L9–L15** throughput/memory (CRC once, buffer views, recording-id cache, probe
   cadence, `os.name`, admission backoff, envelope clone).
8. **S1–S5** structure + export truth (layering, package split, exceptions/README).
9. **S6–S11, R*, J*** visibility, naming, enums, javadocs.

---

## Coverage / confidence

| Area | Confidence | Notes |
|---|---|---|
| C1–C5, L1–L8, L14 | High | Direct source reads of cited methods |
| L9–L13, L15 | High | Direct reads of CRC/clone/checkpoint paths |
| S1–S5 visibility/naming | High | Grep-verified import and reference sets |
| Full `AeronClusterReplicationTransportProvider` beyond cited regions | Medium | 1664 lines; not every branch line-audited |
| Index `ClusterIndexValidation` L415 gap | N/A | Not in cluster hot path for this review |
| Builds/tests | Not run | Per review-only instructions |

No code was modified; this file is the only deliverable.
