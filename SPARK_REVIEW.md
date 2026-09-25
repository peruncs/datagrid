# SPART Review — Aeron Datagrid Cluster

Review-only static analysis (no builds run, no code changed), per `AGENTS.md` guardrails.
Focus: (1) cluster correctness, (2) performance / liveliness / memory,
(3) package structure and visibility, (4) names, (5) javadocs.
Only problems are listed. Each item has file:line evidence and a concrete fix.
Bold simplifications are marked with **BOLD**. No new features are proposed.

How to read: P0 = fix before any production claim (data loss / split-brain /
unbounded stall). P1 = throughput / availability / structural debt that blocks
operating the cluster. P2/P3 = cleanups to do opportunistically, ordered.

---

## P0 — Correctness: data loss, corruption, split-brain

### C1. Data chunks bypass the fencing lease; only some markers are gated
`storage/aeron/writer/AeronReplicationPublisher.java:544-597,788-820`
`publishDataChunks()` holds `offerLock` across the whole chunk loop and calls
`offerDataChunk()` directly; the commit path has a direct `offerEncoded(..., () -> true)`
bypass next to the `leaseGate` abort path. A writer deposed mid-transaction can
leave a data prefix for sequence N under the old token; the successor's N then
trips the reader's mixed-token guard (`storage/aeron/reader/TransactionAssembler.java:604-606`)
and poisons the reader to FAILED instead of cleanly ignoring the loser.
Fix: route **every** offer (chunks and both markers) through
`WriterLeaseGate.OwnedOffer` with a per-attempt `stillOwner` check; fail
`prepare` fast with `WriterFencedException`. **BOLD:** delete the dual-path
offer API — one `offerFenced(kind, …)` method, no boolean bypass parameter.

### C2. Watermarks carry no fencing token, retention does not check it
`storage/aeron/checkpoint/AeronReaderWatermark.java:29-37,588-592`,
`node/aeron/AeronArchiveRetention.java:359-379`
Watermark = (reader, cluster, generation, epoch, recording, seq, pos). Retention
validates epoch/recording/cluster/generation, never the token. Same-epoch writer
restart mints token+1 (`node/aeron/WriterFencingLease.java:199-210`), but deferred
watermarks (`node/aeron/WatermarkFanIn.java:124-131,160-180`) buffered before the
restart can authorize purging the new generation's history.
Fix: add `fencingToken` to the watermark encode/decode/aggregate/monotonic-progress
path, persist it in retention state, reject `watermark.token != activeLeaseToken`.

### C3. Split-brain with equal tokens when the lease volume is not actually shared
`node/aeron/WriterFencingLease.java:190-192`,
`node/aeron/AeronClusterReplicationTransportProvider.java:973-990`
Absent lease file means `token=1` on every isolated volume. Startup validation
rejects overlap with driver/archive/checkpoint dirs but never proves `BACKUP_PATH`
is one shared filesystem. Two writers on two volumes each mint 1 under the same
epoch; the reader only rejects `token < floor` and adopts on `>` 
(`storage/aeron/reader/TransactionAssembler.java:220-225,446-450`), so equal-token
streams interleave silently.
Fix: fail writer startup unless sharing is proven — require a pre-provisioned lease
file, or an interprocess `FileLock` contention test on `writer-lease.lock`, or add a
node-unique tie-break and reject `token == floor` from a different `holderId`.

### C4. `ARCHIVE_FIRST` is not atomic; crash windows end in manual reseed only
`storage/aeron/writer/AeronStorageBinaryReplicationTarget.java:195-246`,
`storage/aeron/writer/AeronReplicationWriteCoordinator.java:391-412,713-759`
Order is PREPARING fence → data offers → local `Store.write` → commit offer →
Archive await → COMMITTED fence. A crash after data chunks / before local write /
after local write but before commit leaves orphan chunks and/or local data with no
commit. Recovery (`AeronClusterReplicationTransportProvider.java:1087-1104,1147-1165`)
correctly fails closed, but there is no commit-completion or Store-truncation path:
local Store already holds N while the tail fence blocks reuse of N.
Fix: on startup with `PREPARING` + Archive tail == N, either complete the commit
(re-offer marker + await + COMMITTED) or truncate local Store to N-1 before
extending. Never allow `clearEnqueueFence()` to delete `PREPARING` — only a terminal
transition may remove it. **BOLD:** collapse the 5-state fence
(PREPARING/ENQUEUED/COMMITTING_UNCERTAIN/COMMITTED/REJECTED) to 3
(PREPARING/COMMITTED/REJECTED) with the uncertain case represented as
"PREPARING + Archive tail present", removing one crash state entirely.

### C5. `ENQUEUE_THEN_ARCHIVE` local-accept without Archive copy stays consumed
`storage/aeron/writer/AeronStorageBinaryReplicationTarget.java:155-184`,
`storage/aeron/writer/AeronReplicationWriteCoordinator.java:670-703`
Crash after `delegate.write()` before `prepare()` leaves durable ENQUEUED state and
`abandonReservedSequence()` fails closed. If an operator clears the fence in place,
the sequence is reused and double-published.
Fix: make the in-flight fence file non-deletable except by a terminal transition;
document and enforce that writer recovery after this state is "restore Store image
from backup", never "clear fence in place". Add a startup guard that refuses to
start when a fence file was removed without a matching terminal record.

### C6. Reader crash between Store import and cursor durability double-applies
`storage/aeron/reader/TransactionAssembler.java:787-828,860-911`,
`node/aeron/AeronClusterReplicationTransportProvider.java:602-638`
`stage()` applies to the Store, `flushDeliveries()` later publishes the tail cursor.
A crash in between leaves Store applied with a stale cursor; safety depends entirely
on the delivery marker surviving. If the marker fsync fails or is lost, replay
redelivers as new (`nextExpectedSequence = initial+1`) and the merger double-applies.
Fix: make the merger idempotent on (epoch, sequence, crc), or flush synchronously
per transaction when marker persistence is unavailable; treat "Store newer than
cursor with no marker" as RESEED_REQUIRED, never as replayable.

### C7. Fresh reader cannot skip its own cursor terminal
`storage/aeron/reader/TransactionAssembler.java:252-257`
A resumed assembler has `lastResolutionKind == null`. Replaying from
`lastResolvedPosition` (`storage/aeron/reader/AeronArchiveReader.java:652-656`)
that redelivers the already-resolved COMMIT/ABORT throws "terminal witness is
unavailable" instead of skipping idempotently.
Fix: persist the terminal witness (kind, crc, length, chunks) in cursor/checkpoint,
or accept `sequence == lastResolved && kind == COMMIT/ABORT` with matching metadata
as a no-op when no witness is held.

### C8. Retention authorizes purge on an uncheckpointed (volatile) tail
`node/aeron/AeronArchiveRetention.java:445-459,230-277,387-396`
`requireWithinDurableBoundary()` accepts `sequence > checkpoint && position <=
recordedPosition` (live, volatile Archive position). That beyond-checkpoint
aggregate is persisted and `deleteThrough()` computes its boundary from
`min(quorum, requested)`. A crash then loses the only copy of data with no durable
COMMITTED checkpoint.
Fix: gate `deleteThrough` **and** `persistState` strictly at/below the durable
writer boundary (sequence, position); treat beyond-checkpoint watermarks as
not-yet-durable (defer, do not persist).

### C9. `ClusterFoundation.start()` cast hides checked failures
`node/ClusterFoundation.java:800-808` — verified in source: `catch (Throwable)` →
`if (failure instanceof Error) throw error; throw (RuntimeException) failure;`.
Any checked `Throwable` becomes `ClassCastException`, destroying the real cause.
Fix:
```java
if (failure instanceof RuntimeException r) throw r;
if (failure instanceof Error e) throw e;
throw new NodeLibraryException("node start failed", failure);
```

### C10. Checkpoint identity never validates the fencing token
`node/aeron/AeronClusterReplicationTransportProvider.java:1127-1140,1066-1074`
`validateWriterCheckpointIdentity()` checks type/mode/cluster/node/generation/epoch/
recording, not `fencingToken`. A copied checkpoint from a prior lease generation
passes.
Fix: compare `checkpoint.fencingToken()` against the held lease token (monotonic
`>=` rule) after `ensureWriterLease()`.

### C11. Exception hygiene: raw Aeron leaks, mistyped fail-closed signals
- `node/aeron/AeronArchiveReplicationPublisher.java:449-451` rethrows non-
  UNKNOWN_RECORDING `ArchiveException` raw; `:409-411,464-466` timeouts as bare
  `IllegalStateException`.
- `storage/aeron/reader/TransactionAssembler.java:211-212` cluster/epoch mismatch
  as `IllegalArgumentException`, not a reseed/wire type.
- `storage/aeron/reader/AeronArchiveReader.java:692` subscription failure as
  `IllegalStateException("PersistentSubscription failed")`.
- `node/aeron/AeronArchiveRetention.java:282-287` wraps `ArchiveException` in
  `IllegalStateException`, dropping the error code.
Fix: introduce `ReplicationTransportException` / `ArchiveTimeoutException`, map
INVALID_START_POSITION / RECORDING_NOT_FOUND / cluster / epoch / generation
mismatches to `StorageBinaryDataReseedException`, and always preserve
`ArchiveException.errorCode` as a field or cause. **BOLD:** one reseed type —
merge `node/aeron/ReseedRequiredException.java:18` with
`storage/types/StorageBinaryDataReseedException.java:15` (see S2).

### C12. Lease/retention file TOCTOU + in-JVM aliasing
- `node/aeron/WriterFencingLease.java:164-166` vs `:156,226`: `mutexFor()` normalizes
  the path, the `ACTIVE` map key does not — aliased paths bypass the single-holder
  check.
- `:706-752`: symlink check covers the file only; `writeAtomically` does no parent
  `isDirectory`/`NOFOLLOW_LINKS` re-verify (contrast the correct pattern in
  `storage/types/AtomicFileWriter.java:106-113,239-245` and
  `storage/aeron/checkpoint/AeronReplicationCheckpointStore.java:118-137`).
Fix: normalize the `ACTIVE` key identically; reuse `PathSecurity.ensureNoSymbolicLinks`
+ parent re-verify in lease write/renew/release; create the lease temp file with
OWNER_ONLY permissions like `AtomicFileWriter`.

---

## P1 — Performance, liveliness, memory

### L1. `offerLock` held across the full 30 s offer deadline — one slow chunk stalls everything
`storage/aeron/writer/AeronReplicationPublisher.java:558-594,788-820` — verified:
the comment at `:554-557` admits holding `offerLock` across the whole fill/offer
loop because of one shared `envelopeBuffer`; `offerer.offer` parks until
`offerTimeoutNanos` (30 s default). A BACK_PRESSURED/NOT_CONNECTED chunk pins the
buffer and the lock for up to 30 s × chunks; commit/abort markers and the close
abort path (`:977-983`) block behind it.
Fix: stop sharing one buffer across retries. Use `ExclusivePublication.tryClaim()`
and encode directly into the claimed term buffer (zero-copy, no lock across park),
or stage into a per-transaction buffer and scope `offerLock` to fill+encode only.

### L2. Unbounded admission spin, no IdleStrategy, no deadline
`storage/aeron/writer/AeronReplicationWriteCoordinator.java:585-607` — verified:
`while (!writeLock.tryLock()) { parkNanos(100_000L); }` with no idle strategy and no
timeout. If maintenance wedges (each Archive step can take 30 s), writers spin at
~10 kHz forever. Same file `:567-583`: `withWritesPaused()` holds `writeLock`
across `awaitNoCommit()` + maintenance.
Fix: replace the spin with `retryPolicy.idleStrategy().idle(work)` and a deadline
from `offerTimeoutNanos`; throw a retryable error on expiry. Bound maintenance with
one global shutdown/maintenance deadline (~recordedPosition + recordingStop + offer
timeouts).

### L3. Writer checkpoint fsync serializes every commit inside `writeLock`
`storage/aeron/writer/AeronReplicationWriteCoordinator.java:732-758` →
`storage/aeron/checkpoint/AeronReplicationCheckpointStore.java:33-51` →
`storage/types/AtomicFileWriter.java:122-134`: temp file + `force(true)` +
`ATOMIC_MOVE` + directory force **per transaction while holding `writeLock`**.
That is 2× fsync + 1× dir-fsync on the commit critical path; the reader side
amortizes with barriers, the writer has no batching.
Fix: group-commit checkpoints (one forced write per barrier, preserving
COMMITTED-before-ack ordering) or move the forced write off `writeLock` behind a
background flusher.

### L4. Archive await loops poll the shared control session every iteration under lock
`node/aeron/AeronArchiveReplicationPublisher.java:407,429,581`: `pollForErrorResponse()`
(`synchronized(archive)`) runs on every `idle.idle(0)` cycle while the substantive
probe is correctly throttled (`:399,459-461,577-579`). With the fast-phase spin/yield
this hammers the single non-thread-safe client lock and starves co-users.
Fix: gate `pollForErrorResponse` behind the same `archiveProbeDelayNanos` (10 ms)
duty cycle as the probes.

### L5. Two full memcpys per payload byte on the writer
`storage/aeron/writer/AeronReplicationPublisher.java:580-581,817`: fill shared
`envelopeBuffer`, then Aeron copies again into the term. 1 MiB chunk = 2 MiB memcpy;
64 MiB txn = 128 MiB memcpy plus framing.
Fix: `tryClaim(header+chunk)` and encode (+CRC, see L6) directly into the claimed
range; keep the copy path only as fallback when claim fails.

### L6. Four CRC32C passes per byte end-to-end (two per side)
Writer `:578-579` updates both `dataCrc` and `chunkCrc` over the identical segment;
reader `storage/aeron/wire/AeronReplicationEnvelope.java:380-382` verifies the wire
CRC, then `TransactionAssembler.java:651-652,660-665` re-hashes the same bytes into
the assembly CRC.
Fix: single pass per side — derive the full CRC with CRC-combine from chunk CRCs
(or drop per-chunk CRC, keep header + full-transaction CRC); on the reader verify
once from the assembled direct buffer at commit.

### L7. `Crc32c.update` allocates a `ByteBuffer.duplicate()` per CRC call
`storage/types/Crc32c.java:90-91` — verified (`duplicate()` on every call: ~190
short-lived buffers per 64-chunk transaction, plus
`storage/types/StorageBinaryBuffers.java:50,59`,
`storage/types/StorageBinaryDataImporter.java:50,72,140`,
`TransactionAssembler.java:422,687` duplicates on the same hot path).
Fix: save/restore position+limit in place — the pattern already used at
`AeronReplicationPublisher.java:624-634` — no `duplicate()`.

### L8. Assembler pre-growth is 64 KiB → double instead of exact-size
`storage/aeron/reader/TransactionAssembler.java:667-694` (seed 64 KiB at `:672`).
The first chunk already carries `payloadLength`; a 64 MiB transaction pays ~10
allocate-copy-free cycles plus a `duplicate()` per growth (`:687`).
Fix: on first `add()`, `ensureCapacity(payloadLength)` once; keep doubling only for
protocol violations.

### L9. `chunkSize=1 MiB / MTU=1408` ≈ 730 fragments per offer; no MTU coherence
`storage/aeron/config/AeronReplicationConfiguration.java:54-56,132-135` validates
`chunkSize+84 ≤ maxMessageLength` but never relates `chunkSize` to `mtuLength`.
One lost 1408 B fragment stalls a whole 1 MiB message; Archive segment churn and
retransmit amplification scale with `chunkSize/MTU`. Conversely no minimum lets a
64 B chunk turn a 64 MiB txn into ~1 M offers.
Fix: require `chunkSize ≥ max(MTU, 4 KiB)`; warn/fail when `chunkSize > 64×MTU`.
Raise the default MTU to 8–16 KiB where the fabric allows, or lower the default
chunk to 64–256 KiB.

### L10. Reader barrier is count-only (64 txns × 64 MiB = 4 GiB staged); fragments-per-poll too
`storage/aeron/config/AeronReplicationConfiguration.java:73,178`,
`storage/aeron/reader/TransactionAssembler.java:92,816-819`,
`storage/aeron/reader/AeronArchiveReader.java:534` (`controlledPoll(handler, 256)`:
256 × 1 MiB decoded+CRC+copied before idle/stop/reconnect checks run).
Fix: add `readerBarrierMaxBytes` (default ≤ merger `CACHING_BYTES_LIMIT`) and flush
on whichever trips first; bound bytes-per-poll as well as fragments-per-poll, or
break early when the barrier window fills.

### L11. `dataCrc` shared without a consistent lock domain (quiet corruption)
`storage/aeron/writer/AeronReplicationPublisher.java:59,545,599-614,515-523`:
`publishDataChunks` uses `dataCrc` under `offerLock` (outside `synchronized(this)`),
`computeDataCrc`/`transactionMetadata` use it under `synchronized(this)` without
`offerLock`. Concurrent prepare + metadata corrupts both digests.
Fix: one `CRC32C` per path, or guard both with `offerLock`; add a single-writer
assertion test.

### L12. `awaitNoCommit` deadline can wrap; hot knobs untunable in production
- `AeronReplicationWriteCoordinator.java:860-862`: `System.nanoTime() + timeout`
  instead of saturating `ReplicationRetry.deadlineNanos()`. Use
  `deadlineNanos/remainingNanos/expired` everywhere.
- `node/aeron/AeronSettings.java:326-343`: `replication()` binds only term/MTU/chunk/
  maxTx/offer/recording timeouts + durability. `readerFragmentsPerPoll`,
  `readerBarrierMaxTransactions/IdleFlushNanos`, and the whole `retryPolicy`
  (spin/yield/park, jitter, probe delays) are builder-only — production always gets
  `AeronRetryPolicy.Default()`. Add `ECLIPSE_DATAGRID_AERON_{READER_FRAGMENTS,
  READER_BARRIER_MAX, READER_BARRIER_IDLE_NANOS, IDLE_*, JITTER_*, *_PROBE_*}` bindings.
- `AeronSettings.java:426` hardcodes `term-length=16m` in the default live URI while
  `validateFraming:933-945` rejects divergence — changing `termLength` without
  overriding the channel fails startup. Template the default URI from the
  `replication` values; validate `MTU ≤ maxMessage` with operator-visible numbers.

### L13. Per-transaction allocation + unconditional formatting on background paths
- `StorageBinaryBuffers.java:50,59,62,103` (`duplicate`/`slice`/`toArray` per txn),
  `TransactionAssembler.java:790-819` (dictionary `String` + `ChunksWrapper.New` +
  `PendingDelivery` per txn), good counter-example to copy:
  `AeronReplicationWriteCoordinator.java:796-811` (`bufferScratch` reuse). Use
  thread-confined reusable scratch lists, stable duplicate views, pre-sized arrays.
- `node/store/StorageLimitGate.java:101-102`, `StorageDiskSpaceReader.java:97,100`:
  unconditional `.formatted()` on every housekeeper tick even when DEBUG is off.
  Guard with `isLoggable(DEBUG)`.
- `StorageDiskSpaceReader.java:86-117`: full Store tree walk on the gate thread,
  cached 5 s. Move off the write-admission path or invalidate on Store events.
- `storage/types/StorageBinaryDataMerger.java:671,700` →
  `ClusterIndexMaintenance.java:69-96,119-133`: every barrier does a full
  ≤4096-object root scan + `synchronized(map)` per map + Lucene close + per-index
  HNSW probe — an O(store) rebuild per barrier regardless of batch size. Retire the
  Lucene view only if a query opened one since the last barrier; make vector refresh
  incremental or first-query-deferred behind a dirty flag (keep the documented
  no-deadlock invariant).

---

## P2 — Package structure, visibility, naming

### S1. **BOLD:** delete `node/aeron/` — one transport split in two, with a cycle
`node/aeron/AeronClusterReplicationTransportProvider.java:18-25` imports all of
`storage.aeron.{checkpoint,reader,writer}` + `storage.types.*`, while
`storage/aeron/reader/AeronArchiveReader.java:8` imports the provider back.
`node/aeron/AeronDistributionGate.java:1-5` implements
`storage.types.StorageBinaryDataDistributor`;
`node/aeron/WriterFencingLease.java:5-8` imports writer + types;
`node/aeron/AeronSettings.java:13-15` imports config + wire.
Neither half is usable without the other — not two layers.
Fix: move provider + `AeronRuntime` (312 lines), `AeronHealth` (202),
`AeronReaderSlot` (68), `AeronPositionProvider` (91), `AeronArchiveCapacity` (108),
`AeronWatermarkChannel` (431), `WatermarkFanIn` (219), `WriterFencingLease` (753),
`AeronArchiveRetention` (669), `AeronDistributionGate`, `AeronWriterBoundary` into
`storage.aeron.provider/`. Keep `node/replication/ClusterReplicationTransport.java:23`
as the sole neutral port. Break the `AeronArchiveReader:8` back-import by injecting
a callback/config instead of the provider type.

### S2. **BOLD:** one exception hierarchy; two reseed types become one
Today: `node/exceptions/` (5 types) + `node/aeron/ReseedRequiredException.java:18`
(misplaced) + `storage/types/` (`StorageBinaryDataException`,
`StorageBinaryDataLifecycleException`, `StorageBinaryDataReseedException`) +
`storage/aeron/wire/ReplicationWireException.java:6`. Two reseed signals for one
recovery action force callers to catch two trees.
Fix: single hierarchy under `node/exceptions` (or `cluster.api` if operators catch
by name): `NodeLibraryException` + `ReaderWriteRejected` + `WriterFenced` +
`StorageLimitReached` + `ReseedRequired(reason enum)` merging both reseed types.
Demote `ReplicationWireException` to package-private; map wire decode failures to
the single reseed/corrupt cause. Move `ReseedRequiredException` → `node/exceptions/`
immediately. This also fixes the layering inversion in S3.

### S3. End the `storage → node` upward dependency
`storage/types/RejectingPersistenceTarget.java:5` imports
`node.exceptions.ReaderWriteRejectedException`;
`storage/aeron/writer/WriterLeaseGate.java:3`, `AeronOfferRetryer.java:5`,
`AeronReplicationWriteCoordinator.java:4` import `node.exceptions.WriterFencedException` —
while `node` imports `storage` everywhere (`ClusterFoundation.java:20`,
`StorageNodeManager.java:9-10`, `NodeConfiguration.java:12-15`).
Fix: move both exception types to `storage.types` (or shared errors package per S2)
and have `node` import from there. `storage` must never import `node`.

### S4. Consolidate storage adaptation: `Storage*` lives in two roots
`node/store/ClusterStorageManager.java:39` wraps the Eclipse `StorageManager` and
owns `storage.types.StorageGraphCoordinator`, while
`storage/types/DistributedStorage.java:48` mutates the same
`EmbeddedStorageFoundation` and `storage/types/RejectingPersistenceTarget.java:30`
exists only to serve `ClusterStorageManager.java:888`. File safety is triple-owned:
`storage/types/PathSecurity.java:24,45,63` is the implementation,
`node/store/StorageFileOperations.java:83-84,182-183` are one-line delegates,
`storage/types/AtomicFileWriter.java:268` calls `PathSecurity` directly.
Fix: move `DistributedStorage`, `RejectingPersistenceTarget`,
`StorageGraphCoordinator` to `node/store/` (their only consumers are node-side) —
or move `ClusterStorageManager` + `StorageFileOperations` the other way; do not
keep `Storage*` in both. Merge `PathSecurity` + `StorageFileOperations` +
`AtomicFileWriter` guards into one `FileSafety` unit and delete the
`StorageFileOperations.java:83,182` delegates. Move `node/UserUploadValidator.java`
(115 lines, validates backup ZIPs) → `node/backup/` as package-private
`BackupUploadValidator`; move `node/BackupRestorePolicy.java` (216) → `node/backup/`.

### S5. Demote `public` that is only intra-module (module exports only `api`)
`module-info.java:158-177` exports only `peruncs.cluster.api`, yet ~70
types/methods are `public` for intra-module sharing. Concrete, safe demotions:
- `storage/types/PathSecurity.java:16` + `node/store/StorageFileOperations.java:18`
  ("public only because backup and node packages share it") + `node/CloseSequencer.java:27-31`
  → create `node/internal/` (or `node/shared/`), move both there, demote single-package
  methods to package-private.
- `storage/types/DistributedStorage.java:22,48` (single caller `ClusterFoundation.java:995`)
  → move to `node/store/`, package-private, or fold `configureWriting` into
  `ClusterStorageManager.New` as private wiring.
- `storage/types/RejectingPersistenceTarget.java:30,37` (single caller
  `ClusterStorageManager.java:888`) → private nested class of
  `ClusterStorageManager.ReadOnly`.
- `storage/aeron/reader/CursorSnapshot.java:12` (13-line record duplicating
  `ReplicationCursor`/`AeronReplicationCursor`) → package-private or private nested
  record in the reader.
- `storage/aeron/writer/CrashHook.java:19` (75-line test hook) → test scope or
  package-private accessor.
- `storage/aeron/reader/ReaderDeliveryListener.java:16`,
  `storage/aeron/writer/WriterLeaseGate.java:24` → package-private unless retained
  as provider SPI; if retained, move next to `node/replication/ClusterReplicationTransport`.
- `storage/aeron/wire/ReplicationWireException.java:6`,
  `node/replication/ReplicationCursorStore.java:25` impl → package-private
  (keep the `ClusterReplicationTransport` port interfaces public).
- `storage/types/Crc32c.java`, `ReplicationRetry.java`, `AtomicFileWriter.java` stay
  public only because the split forces it; after the S1/S4 consolidation, demote.

### S6. Split the god objects (verified line counts: provider 1664, foundation 1352, merger 1208, publisher 1198, settings 1126, storage-manager 944)
- `node/aeron/AeronClusterReplicationTransportProvider.java:50` — factory + inner
  `Transport` (~1300 lines: distributor + client + position + retention + health +
  cursor + checkpoint + watermark + fencing). Split into `AeronTransportFactory`
  (only `create()` at `:97`), `AeronWriterAssembly`, `AeronReaderAssembly`,
  `AeronCheckpointing`.
- `node/ClusterFoundation.java:38` — interface + `Builder` (21 setters, only 4 used
  by `api/ClusterNode.java:34-37`) + `Node` (20+ `LazyConstant` fields `:181-200`).
  Split `Builder` → top-level `NodeFactory`, `Node` → `NodeLifecycle` +
  `NodeStorageAssembly` + `NodeReplicationAssembly`; delete unused setters.
- `storage/types/StorageBinaryDataMerger.java:56` — interface + 11-component
  `Configuration` + `Default` (`:175`: queueing + import + materialization + index
  refresh + lifecycle + watchdog). Split into `MergerQueue`, `MergerMaterializer`,
  `MergerLifecycle`; put index calls (`ClusterIndexValidation.java:554`,
  `ClusterIndexMaintenance.java:323`) behind one `IndexRefresh` port.
- `storage/aeron/writer/AeronReplicationPublisher.java` (1198) — offer retry +
  framing + CRC + token + ownership + cleaner. Extract `EnvelopeFramer`; use the
  existing `AeronOfferRetryer.java:134` and delete the duplicated retry in the publisher.
- `node/aeron/AeronSettings.java` (1126) — env parsing + validation + credentials +
  channel topology. Split `AeronTopology`, `AeronAuth`, `AeronTimeouts`; keep a thin
  record delegating to parsers.
- `node/store/ClusterStorageManager.java:39` — interface + `Default` + `ReadOnly` +
  callbacks + ~35 `StorageManager` delegate methods (`:226-460`). Extract
  `StorageGate` (limit + read-only rejection) and `StorageLifecycle` (shutdown
  callback).
- `node/StorageNodeManager.java:23` — `Role` enum + `Configuration` + `Default` +
  `CloseFailures` in one interface file; the 10+ `is*/read*/current*/latest*/archive*/
  writer*/applied*` pass-throughs (`:115-221`) belong in `StorageNodeHealthCheck`.

### S7. Fold single-use / 1–2-method entities; fix constructors
Fold: `DistributedStorage.configureWriting` (1 caller) → private wiring in
`ClusterStorageManager`; `RejectingPersistenceTarget.New` (1 caller) → nested class;
`NodeRole.of` (`node/NodeRole.java:98`, delegates to `resolve()`) → inline;
`NodeHousekeeper.New` → package-private constructor; `CloseSequencer.append`
(`node/CloseSequencer.java:120`) → fold into `run/close` or a shared `Errors` util.
Over-5-arg constructors needing a Builder or param object (all verified):
`NodeConfiguration.java:46-67` (21 components; 21 setters on the Builder — group
into `StorageConfig/ReplicationConfig/BackupConfig`),
`StorageNodeManager.Configuration` (`:42-51`, 8, no Builder),
`StorageBinaryDataMerger.Configuration` (`:78-90`, 11 — make canonical ctor private,
force `New`/Builder), `AeronReplicationConfiguration.java:35-50` (14 — same),
`api/NodeStatus.java:18-32` (13 positionals built once at `ClusterNode.java:80-84` —
add `NodeStatus.of(control, metrics)`), `AeronReplicationCheckpoint.java:36-48` (12+),
`BackupMetadata.java:36-46` (10), `AeronReplicationCursor.java:29-38` (8),
`AeronReaderWatermark.java:29-36` (7). No statics-only interfaces exist; but
`NodeLibraryPropertiesProvider.java:62-69` buries role strings + `EnvKeys:358` in an
interface — move role strings to the `NodeRole` enum, `EnvKeys` to a final
`NodeEnvKeys` class. No FQN abuse found — imports are clean.

### S8. Renames so names carry the design (Manager/Control/Provider/Gate/Slot/Boundary/Foundation today do not)
- `ClusterFoundation` → `ClusterNodeLifecycle` / `NodeAssembly` (it is not an
  Eclipse `EmbeddedStorageFoundation`; nested `Builder`+`Node` in an interface
  compounds the misuse — extract to top-level classes).
- `StorageNodeManager` / `BackupNodeManager` / `StorageBackupManager` (own
  lifecycle + `close()`) → `*Lifecycle` or `*Owner`; `StorageNodeControl` /
  `BackupNodeControl` (borrowed views, no `close()`) → `*View` or `*Probe`.
  The Manager-vs-Control distinction is documented but the names do not carry it.
- `NodeLibraryPropertiesProvider` → `NodeConfigSource` / `NodeSettings`.
- `NodeConfiguration` (21 internal collaborators, not user config) → `NodeWiring`.
- `AeronClusterReplicationTransportProvider` → `AeronTransportFactory` (only
  `create()` stays); stateful inner class → `AeronTransport`.
- Three `Gate`s, three concerns: `AeronDistributionGate` → `AeronWriteAdmission`,
  `StorageLimitGate` → `StorageCapacityAdmission`, keep `WriterLeaseGate` (accurate)
  or rename `FencingLease`.
- `AeronReaderSlot` (generic `<T extends Disposable>` holder, not Aeron-specific) →
  `DisposableSlot` in `node/internal/`.
- Five cursor words for one idea: `AeronWriterBoundary` (record) vs
  `AeronReplicationCheckpoint` vs `AeronReplicationCursor` vs `ReplicationCursor`
  vs `CursorSnapshot`. Unify: keep neutral `ReplicationCursor`; merge
  `AeronReplicationCursor` + `CursorSnapshot` into an `AeronCursorCodec`; rename
  `AeronReplicationCheckpoint` → `AeronCheckpoint`; delete `AeronWriterBoundary`
  (fold into checkpoint). See the enforced glossary in D5.
- `DistributedStorage` (one-method foundation decorator, not storage) →
  `DistributingFoundation` or fold into `ClusterStorageManager`.
- `StorageBinaryDataImporter` vs `Materializer` vs `ObjectMaterializer` vs
  `StorageBinaryBuffers` (one pipeline in four helpers) → single `BinaryMaterializer`.
- `ClusterStoreIndexes` vs `ClusterIndexValidation` vs `ClusterIndexMaintenance`
  vs `StoreIndexReflection` → facade `NodeIndexes` + package-private `LucenePolicy`,
  `VectorRefresh`, `UpstreamLayout`. Reflection itself
  (`StoreIndexReflection.java:26` — `getDeclaredFields` + field offsets, cached in
  `ClassValue`s) is correctly contained as the single choke point; keep it in that
  one file, do not spread it, and add a version-pin test that fails the build when
  upstream GigaMap/Lucene/JVector field names change.

---

## P3 — Javadocs: human language, operator-critical content

### D1. Missing operator fields on the exact types operators touch (fix first)
- `api/NodeOptions.java:6` ("Immutable application-owned inputs…") names zero
  `ECLIPSE_DATAGRID_*` variables, defaults, or required-vs-optional. Rewrite: "Two
  values you supply to start one node; everything else comes from ECLIPSE_DATAGRID_*
  variables listed on ClusterNode.open."
- `api/ClusterNode.java:26` ("Opens and starts a node…") omits required env
  (`REPLICATION_ROLE`, `AERON_CLUSTER_ID/NODE_ID/STORE_GENERATION`, `PROD_MODE`),
  the fixed-role consequence, and fail-closed throws (`ReseedRequired`,
  `WriterFenced`, `NodeLibraryException`). Rewrite: "Start one node whose
  writer/reader/backup-reader role is fixed by ECLIPSE_DATAGRID_REPLICATION_ROLE;
  fails closed with RESEED_REQUIRED instead of inventing state."
- `api/ClusterNode.java:60,64,69` (storage checks / scheduled / manual backups) omit
  what the checks are, wrong-role `IllegalStateException`, concurrent-backup
  `BackupBusyException`, and stop/resume side effects.
- `api/ClusterStore.java:32,40,49` ("Persists one changed object…") omits the three
  exceptions callers branch on: `ReaderWriteRejected` (reader), `WriterFenced`
  (lease lost), `StorageLimitReached` (full). Rewrite: "Save one changed object on
  the writer; fails fast on a reader or a fenced writer instead of queuing."
- `api/NodeStatus.java:3` ("Immutable operational snapshot…", 13 positional params
  `:18-32`) documents `-1` but not the action: RESEED_REQUIRED → reseed from
  backup/Store+cursor; FAILED → do not retry writes; lag = `latestSequence -
  appliedSequence`. Rewrite: "One point-in-time answer to 'can this node serve, and
  how far behind the writer is it'."
- `node/replication/ReplicationHealth.java:72-85` (`RESEED_REQUIRED` / `FAILED` /
  `DEGRADED_ARCHIVE`) gives no remediation. Rewrite the stuck state as: "Stuck until
  you reseed it from a compatible backup; restart alone will fail again."
- `node/replication/ReplicationLogRetention.java:9` omits the quorum rule. Rewrite:
  "Delete old Archive segments only after every configured reader confirms it no
  longer needs them."
- `storage/aeron/checkpoint/AeronReaderWatermark.java:13` omits the critical
  exclusion (no fencing token, never fences, epoch-bound, isolated network is the
  trust boundary) — that must be the first sentence. Rewrite: "One reader's 'I have
  durably applied up to here' report; the writer deletes old log segments only
  after every configured reader reports past them."
- `node/aeron/WriterFencingLease.java:29` omits env (`LEASE_PATH`,
  `LEASE_STALENESS_MILLIS`), the NTP/chrony requirement, the shared-volume
  requirement, and stale-steal vs fresh-reject. Rewrite: "The file in the shared
  backup volume that decides which writer may publish; only its holder may write."

### D2. Jargon-heavy / circular first sentences — rewrite batch
- `storage/types/StorageBinaryDataDistributor.java:10` ("Cluster-aware extension of
  the binary distributor.") → "What the writer calls to publish one Store
  transaction to the log."
- `storage/types/StorageBinaryDataClient.java:7` ("Replication reader lifecycle…")
  → "What a reader calls to replay the writer's log in order; do not mark a message
  consumed until the Store has applied it."
- `node/replication/ClusterReplicationTransport.java:15` ("Replication transport for
  one Data Grid cluster instance.") → "How a node publishes, replays, checks health,
  and deletes old log; Aeron is the only production choice, none means unreplicated."
- `node/StorageNodeManager.java:14` ("This manager controls a storage node…") →
  "Run one Store node that is either born a writer and always publishes, or born a
  reader and never publishes."
- `node/backup/BackupNodeManager.java:13` / `StorageBackupManager.java:19` →
  "Run one backup-reader that pauses replay at a safe boundary, zips the Store, then
  resumes." / "Take one ZIP backup at a stopped replay boundary; old backups are
  pruned only after the new ZIP is durable."
- `node/replication/StoredReplicationCursorManager.java:13` → "Remember where a
  reader stopped so a restart resumes there instead of replaying from scratch."
- `node/replication/ReplicationCursorStore.java:24` ("Atomic cursor file used by
  lifecycle implementations.") → "Read and write one small CRC-protected file that
  records where a reader stopped."
- `node/NodeLibraryPropertiesProvider.java:12` ("Configuration contract shared by
  cluster lifecycle code and the Aeron provider.") → "Where a node reads its settings
  from; the default reads ECLIPSE_DATAGRID_* variables, tests may supply values
  directly."
- `node/StorageNodeControl.java:7` / `node/backup/BackupNodeControl.java:7`
  ("Protocol-neutral control and observability view…") → "The handful of health,
  readiness, and sequence numbers your HTTP/MCP boundary should render; you never
  close it."
- `storage/types/ReplicationDurabilityMode.java:3` → "Choose whether a write hits
  the shared log before your local disk (safe default) or your disk first (must
  reseed if the log write then fails)."
- `storage/aeron/writer/WriterLeaseGate.java:9`
  ("Gates terminal-marker offers on continued writer-lease ownership.") → "Let a
  writer finish its commit marker only while it still holds the lease; a writer that
  lost the lease fails with WriterFencedException."
- `node/store/ClusterStorageManager.java:27` ("…adds cluster shutdown and write
  gating to Store.") → "The Store your application code touches; it refuses writes
  on readers, refuses writes past the disk limit, and shuts down exactly once."
- `storage/aeron/checkpoint/AeronReplicationCheckpoint.java:7` ("The restart record
  for one writer or reader.") → "The writer's note-to-self about whether its last
  transaction surely committed, so a restart never reuses a sequence it is unsure about."
- `storage/types/ReplicationCursor.java:7` ("Durable replication position.") →
  "The neutral 'restart here' bookmark a reader saves after every applied commit."
- `storage/aeron/checkpoint/AeronReplicationCursor.java:10` (near-duplicate) →
  "The Aeron spelling of that restart bookmark: which recording, which Archive byte
  offset, and which sequence, with the fencing token that rejects stale writers."

### D3. `module-info.java:1-178` is a design doc in a descriptor (also: typos `authntication`, `air-gaped` at `:19-20`)
178 lines for a module exporting one package (`api`). Keep ~15 (open/close order,
package map, Aeron-only, no HTTP). Move Archive-first + envelope framing to
`storage/aeron/wire/package-info.java`; fencing lease to `node/aeron/` (or its
`storage.aeron.provider/` successor); cursors/checkpoints + reseed to
`storage/aeron/checkpoint/package-info.java`; quorum retention + nonce/channel
separation to the provider package-info + README operator section; backup ZIP +
`user-uploaded-storage.zip` to `node/backup/package-info.java`; index-in-graph to
`storage/types/package-info.java`. Fix the two typos regardless.

### D4. Missing `package-info.java` for the packages that need them most
Present: api, node, node/{aeron,backup,exceptions,replication,store},
storage, storage/{aeron,types}. Missing: `storage/aeron/{writer,reader,wire,
checkpoint,config}` (wire/checkpoint/config have one; writer/reader do not —
verify and add), `cluster/` root has one, `node/aeron` will move per S1.
Every new `storage.aeron.provider/` package from S1 needs a package-info stating
ownership (provider owns its embedded MediaDriver/Archive lifecycle) and the
single-reader-per-stream rule.

### D5. Enforced glossary (use verbatim in first sentences; fixes writer/distributor, reader/client, cursor/checkpoint/watermark/position/sequence drift)
- `writer` — only the node role that may originate Store writes. Never the send path.
- `distributor | publisher | coordinator` — writer-side send path only.
- `reader` — node role that replays; `backup-reader` — reader that also serves
  backups; `client` — only the reader-side subscription object. Never call a node a client.
- `sequence` — logical transaction number only; `position` — only the Aeron Archive
  byte offset. Never swap (`NodeStatus` currently mixes `current/latest/applied/
  writerDurable` of both).
- `cursor` — the only neutral durable restart bookmark; `checkpoint` — Aeron restart
  record with PREPARING/COMMITTED/etc.; `watermark` — volatile reader progress report
  authorizing retention only as a complete quorum, never a restart bookmark, never
  carrying a fencing token.

---

## Appendix — AGENTS.md guardrail sweep (brief)

- Modern Java 26: preview enabled (`pom.xml:32-35,309-319`); pattern matching present
  (envelope `Kind.fromCode`) but `instanceof` chains remain (`ClusterIndexMaintenance.java:79-83`,
  `StorageBinaryBuffers.java:40,165`) — replace with sealed-type switch + record
  patterns where the Store API permits. No `StructuredTaskScope` for driver/Aeron/
  Archive fan-in/out (`AeronArchiveReader.java:473`, `AeronWatermarkChannel.java:73`,
  `AeronRuntime.java:178-229` sequential start, `AeronReaderLifecycle.java:94-100`
  latch+join shutdown) — wrap in `ShutdownOnFailure` with the existing per-step
  timeouts as scope deadlines. No `CachedTimeClock`/Agrona time source despite the
  dependency — replace ad-hoc `nanoTime()+timeout` with `ReplicationRetry`
  deadlines and a cached clock for barrier-idle/disk-TTL/heartbeat checks.
  Production launchers must pass `--enable-preview --add-modules jdk.incubator.vector`
  (surefire/failsafe set it for tests at `pom.xml:254,264`); either document the flags
  as required or drop preview APIs from the hot path.
- OOP/records/Optional: good record usage for wire/cursor/status types; the gap is
  §S7 constructors (13–21 positionals) — Builders/param objects, canonical ctors private.
- Overengineering to remove: dual offer paths (C1), dual reseed hierarchies (S2),
  triple file-guard owners (S4), duplicated offer-retry (S6 publisher vs
  `AeronOfferRetryer`), `StorageFileOperations` delegate pair (S4), unused
  `ClusterFoundation.Builder` setters (S6), `CursorSnapshot` duplicate carrier (S5).
- AutoCloseable/exceptions: closes are idempotent and ordered (keep); fix C9 cast,
  C11 raw leaks, C12 TOCTOU.
- Threading: three-monitor protocol on the reader poll path
  (`synchronized(delivery)` → `synchronized(this)` per fragment at
  `TransactionAssembler.java:175-189`, plus `barrierLock` at `:794-815,863-904`) —
  collapse to one explicit lock with a documented order (the conductor-error
  `failure.compareAndSet` at `:537` is correctly lock-free, keep it). Narrow the
  slow-path critical sections that hold `writeLock`/transport/archive monitors
  across fsync, Store writes, Aeron retry loops, and Archive RPCs (C-writeup §P1-11
  pattern in the working notes). `withWritesPaused`/`lockWriteAdmission` get bounded
  deadlines per L2.
- Retries/network: offer/recording/replay timeouts exist and are coherent; the holes
  are unbounded admission (L2), terminal retention-agent latch on timeout
  (`AeronArchiveRetention.java:150-184` — fail the op, keep the controller usable;
  add timeouts to position probes), fragile string-match replay detection
  (`:46,599-603` — gate on `recordingIsActive()`/stop-position + segment
  completeness, keep the string match only as fallback with a version-pin test),
  and untunable production retry policy (L12).
- Agrona/Store utils: `BackoffIdleStrategy` used correctly in reader/watermark paths
  — extend `idleStrategy().idle(work)` to admission (L2) and use `LockedExecutor`
  on the poll hot path instead of 2–3 monitor enters per chunk.
- 1-writer/N-reader: enforced by role-fixed subscription (no writer path on readers)
  + fencing lease; holes are C1–C3, C10, C12 above — all must close before the
  invariant can be claimed.
- Off-heap discipline: envelope path is allocation-aware in comments but still
  copies twice (L5), CRCs four times (L6), duplicates per call (L7); importer and
  assembler allocate per txn (L13). Apply the `bufferScratch` reuse pattern
  (`AeronReplicationWriteCoordinator.java:796-811`) everywhere on the hot path.
- Lucene/JVector: embedded in-graph policy present and tested via soak
  (`AeronWriterReaderSoakIT` writer adds/updates/removals + reader query threads +
  graph/Lucene/JVector census); the perf gap is eager per-barrier rebuild (L13) —
  gate on a dirty flag.
- Security: fencing/CRC/nonce correctly documented as non-authentication; gaps are
  C12 (lease aliasing/symlink), unproven shared-volume (C3), and maintenance
  operations carrying no auth of their own (README already assigns this to the
  embedder — keep, but add an auth-checklist to `StorageNodeControl`/`BackupNodeControl`
  javadocs per D1).
- Pinned versions: Agrona 2.6.0 forced over JVector's 1.20.0 (`pom.xml:40,77-84`,
  nearest-wins today, explicit pin tomorrow) — verify JVector 4.0.0-rc.9 against
  Agrona 2.6.0 in CI, do not assume.

## Suggested execution order

1. C1, C2, C3 (fencing integrity — the invariant everything else assumes).
2. C9 (one-line crash-reporting fix), C10, C12 (identity + filesystem trust).
3. C4, C5, C6, C7, C8 (crash atomicity + retention safety).
4. C11 + S2 + S3 (one error tree, correct layering) — unblocks all later moves.
5. L1–L4 (liveliness: offer lock, admission, fsync, control-session polling).
6. S1 + S4 + S5 (package collapse, visibility) — mechanical after the error tree.
7. L5–L13 (throughput + allocation).
8. S6–S8 (god splits, builders, renames) + D1–D5 (javadocs + glossary).
