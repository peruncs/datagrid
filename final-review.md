# final-review.md — Consolidated, validated review of `peruncs.cluster`

**Sources assessed (review documents only):** `DS-REVIEW.md`/`ds-review.md`,
`MIMO-REVIEW.md`, `SPARK-REVIEW.md`, `KIMI-REVIEW.md`, and `GLM-REVIEW1.md`
(== `glm-review.md`, identical duplicates — counted once). `ES.md` is a design
document, not a review, and is excluded per instruction.

**Method:** every finding below was checked against the current working tree
(HEAD `cd39892` + uncommitted delta). Status tags: **[verified]** — I read the
cited source this session and confirmed the defect; **[verified-corroborated]** —
two or more independent reports agree and my spot-check confirms;
**[plausible]** — control flow is consistent but the failure was not reproduced
(static review only; no builds/tests run, no code modified, per AGENTS.md).

**Duplicate handling:** the five reports overlap heavily (god classes, long
argument lists, FQN sweep, javadoc indentation, test hooks, envelope
allocations, fencing-token bound, typed exceptions). Overlapping findings are
merged into one entry with all source tags; the strongest mitigation wins.

---

## 0. Already fixed — do not redo (verified in the current tree)

Round 4–6 commits resolved these earlier findings; several reports still
carry them, so they are recorded here to prevent wasted work:

- Coordinator relock interrupt path — re-acquires uninterruptibly, original
  failure suppressed (`AeronReplicationWriteCoordinator:726-757`).
- `backupRunning` set inside the task body (`StorageBackupTaskExecutor:145`).
- Read-side admission latch: `drain()` sets `admissionClosed`, entries
  re-check inside the lock (`StorageGraphCoordinator.drain()/admissionClosed`).
- `NodeLifecycle.start` failure rethrow is instanceof-guarded (no blind cast).
- `GuardedDatabase` cached; `BackupRestorePolicy` param renamed
  `ownAuthoritativeStore`; `AeronWatermarkChannel.available()` reads only
  volatile/atomic state; `BackupArchive` declared-size + dry-run budgets
  pre-extraction; `NodeSettingsSource` duplicated javadoc removed; archive
  control **auth feature** removed (docs remain — see F-15).
- Writer keeps local Store over compatible backups; uncertain local writes
  journal `COMMITTING_UNCERTAIN`; merger disposal no longer interrupts busy
  workers; Store `shutdown()==false` fails the close retryably; persistence
  centralized through `persist()`; upgrade guards on all write paths.

---

## 1. P0 — Data loss / permanent node failure (fix before anything else)

### F-01 Reader disposal interrupts its own durability boundary and permanently fails the shared merger
**[verified-corroborated: DS-01; my read of `AeronReaderLifecycle:110-125`,
`AeronArchiveReader:598-610`, `StorageBinaryDataMerger:658,725` confirms]**

The only stop signal for the polling thread is `thread.interrupt()`
(AeronReaderLifecycle:119). After the loop exits, `flushDeliveries()` still
runs with the interrupt flag set; `TransactionAssembler.flushDeliveries`
(:1016) calls `receiver.awaitApplied()`, whose blocking `Future.get` throws
`InterruptedException` immediately, and the merger's catch treats it as a
**terminal** failure (`recordLifecycleFailure` latches the first cause). Any
later reader on the same node fails every import; the cursor stops advancing.
A clean node close or reader replacement under backlog routine triggers this.

**Mitigation:** clear the interrupt status before the final flush
(`boolean interrupted = Thread.interrupted();` … flush … restore), and make
`StorageBinaryDataMerger.awaitApplied` treat interruption during a
*final drain* as retryable rather than terminal. The stop interrupt must be
provable unable to reach a durability wait. **Required test:** stage a
delivery barrier with data, block the Store import, call `dispose()`, assert
the merger remains usable afterward.

### F-02 Restored backup files are not fsynced before the durable cursor is installed
**[verified: DS-02; my read of `BackupArchive:745-758` confirms extraction uses
`Files.newOutputStream` with no `force`; `AtomicFileWriter.installStorage`
does `ATOMIC_MOVE` + directory fsync only]**

The cursor write is durable (`AtomicFileWriter.write` forces file + parent),
but the restored Store files live in page cache. Power loss between cursor
install and writeback leaves a durable cursor pointing at a Store image the
disk never fully received — precisely the corruption class the
cursor/backup protocol exists to prevent.

**Mitigation:** extract through `FileChannel` + `force(true)` per file (or
walk the staged tree forcing each file), force the staged `storage` directory,
then install + persist the cursor. **Required tests:** crash between install
and cursor force; crash between extraction and install (crash matrix
partially covers the latter).

### F-03 Writer restart still has no Store-vs-checkpoint validation; the missing-Store branch restores an older backup first
**[verified-corroborated: DS-03, GLM-A6/round-5 A6; the
`!storageExists` branch at `BackupRestorePolicy:149-152` runs before the
`ownAuthoritativeStore` guard at `:155-165`]**

Three residual paths: (a) a writer whose Store directory is missing gets
reseeded from the newest *backup* — possibly older than its durable
checkpoint; (b) a writer with a live checkpoint but empty Store silently
creates a fresh root (`mayCreateRoot()` is `role == WRITER`) and resumes
publication at checkpoint+1 over an image missing N+1..N+K; (c) nothing
compares the opened Store image against the checkpoint boundary
(`AeronWriterTransport:365-373` extends the recording unconditionally).

**Mitigation:** backup/restore policy becomes a **no-op for the writer role**
(not just "keep existing files"); add an authoritative-recovery gate: durable
writer checkpoint present + Store missing/empty → `ReseedRequiredException`;
compatible backup cursor older than checkpoint → reject it. Optionally persist
a per-commit boundary marker in the Store root to prove the image.
**Required test:** backup at sequence N, acknowledged writes through N+K,
wipe Store, restart → assert reseed required, never silent resume.

### F-04 `StorageGraphCoordinator.write()` latches a clean admission rejection as graph damage
**[verified: DS-11; my read of current `write()` confirms — the
`ensureAdmission(); ensureValid(); update.run()` sequence sits inside the
invalidating catch]**

After `drain()` sets `admissionClosed`, any late in-flight
replication/integration write that had not yet acquired the lock is rejected
by `ensureAdmission()` — and that clean rejection is latched as
`GraphInvalidatedException("Store graph may be partially updated")` although
no callback ran. A close that fails and is retried then operates on a graph
wrongly poisoned as damaged. `writeExclusive` has the correct shape.

**Mitigation:** perform admission/validity checks *before* entering the
invalidating catch — e.g. acquire, check, then wrap only `update.run()` in the
latch-on-failure region. One structural edit in `write(Runnable)` and
`write(Supplier)`.

### F-05 `drain()` takes the write lock with no deadline — one stuck application section hangs the entire close
**[verified: DS-12; my read confirms `drain()` = `admissionClosed.set(true);
writeLock().lock(); unlock();`]**

`ReentrantReadWriteLock.lock()` is uninterruptible and unbounded. An
application section blocked on external I/O parks the close sequencer's
"graph drain" stage forever; `ClusterNode.close()` hangs with no failure and
no diagnostics.

**Mitigation:** bounded `tryLock(timeout)` in `drain()`, throwing a typed,
retryable close-stage failure ("graph sections did not drain within budget").
Pick the budget from node settings; surface it in close diagnostics.

### F-06 `LazyHolder` initialization flag has a publication race leaking node resources
**[verified: KIMI; my read of `NodeAssembly:251-277` confirms — `get()` sets
`initialized = true` *after* `constant.get()` returns; `isInitialized()` is
read by close-stage preconditions]**

A first `get()` that completed the factory but has not yet stored the flag
races a concurrent close: the close stage for that collaborator is skipped as
"never created" and the just-constructed MediaDriver/Store leaks. Small
window, real failure mode — this is exactly what per-stage close tracking
exists to prevent.

**Mitigation:** make `LazyHolder.get()` and `isInitialized()` `synchronized`
(cold path; monitor cost irrelevant) so the flag store has a happens-before
edge with the close-path read. Three-line fix.

---

## 2. P1 — Trust, fencing, and liveness under failure

### F-07 Orphan-workspace reaper can delete a live lease and then the live workspace
**[verified: DS-06]**
`FilesystemVolumeBackupBackend:787` filters only the export-workspace
prefix — `<workspace>.lease` files match too; `deleteOrphanWorkspace` then
locks the (recreated) lease and deletes a workspace whose export is still
running.
**Mitigation:** filter with `Files.isDirectory(path, NOFOLLOW_LINKS)` and
exclude the lease suffix; pin lease-file identity (file key) before deleting
a workspace. **Test:** reaper listing that reaches `.lease` first.

### F-08 `WriterFencingLease.close()` can write a heartbeat after close returns
**[verified: DS-08]**
`closed = true` is set only *after* the interprocess lock is released
(:691-731); a renew() past its check can then acquire and write a fresh
heartbeat, delaying successor failover up to the staleness bound (30 s) —
the class javadoc claims the opposite ordering.
**Mitigation:** set `closed` under `stateLock` while still holding the file
lock (inside the try-with-resources); keep the final check as defense.
**Test:** slowed-renew interleaving.

### F-09 Lease reacquisition leaks the superseded instance (heartbeat executor + log spam)
**[verified: MIMO F-04; my read of `WriterFencingLease.acquire` confirms —
only `isCurrent()` and `releaseUnproven` are special-cased; every other
non-current entry falls through to `acquireLocked`, which replaces the
`ACTIVE` map entry without closing the old instance]**
**Mitigation:** before `acquireLocked`, treat any non-current active entry
like the `releaseUnproven` branch: fence it under `stateLock`, remove it, shut
its heartbeat down (`active.close()` is idempotent). One cleanup where there
is currently a leak.

### F-10 Write admission uses a rate-limited cached lease check documented as immediate
**[verified: DS-09]**
`AeronWriterTransport:504-512` claims admission bypasses the freshness cache
but returns `lease.isCurrent()`, cached up to `maxStaleness/3` (~10 s). After
a takeover the deposed writer can pass admission and reach
`purgeWithWritesPaused` — stopping/purging the shared recording — before any
authoritative ownership proof.
**Mitigation:** add an uncached ownership read for admission/maintenance/purge
(re-read under the interprocess lock), or amend the javadoc to declare the
cached window an accepted risk and gate *purge* on the per-offer
`executeUnderOwnership` path. Purge must never run on the cached check alone.

### F-11 Durability wait is shorter than the index-refresh budget it must cover
**[verified: DS-10]**
`awaitMaterialization` gives up at `3 × applyTimeoutMs` (≈180 s) while the
apply worker's own index-refresh budget is `10 × 180 s`; a healthy but slow
post-materialization JVector/Lucene refresh is declared a terminal merger
failure.
**Mitigation:** size the durability wait from the worker's total phase budget
(`materializationBudgetMs + indexRefreshBudgetMs`) or make it progress-aware
(watchdog-heartbeat or sequence-advance resets the deadline).

### F-12 Runtime index registration races the merger's scans over non-thread-safe upstream state
**[verified: DS-13]**
`ClusterStoreIndexes` registration is serialized only by its own
`LockedExecutor`; merger scans iterate the same upstream `GigaIndices`
groups without that lock (upstream `BulkList` is not thread-safe). Torn
iteration → terminal merger failure.
**Mitigation:** route registration through the per-Store coordinator write
side (expose via `graphBoundary()`), or enforce and document
registration-before-start and reject runtime registration after startup.

### F-13 `isLeafValue` prunes arbitrary `Number`/`Enum` subclasses — validation fails open
**[verified: DS-14]**
`ClusterIndexValidation:549-565` treats every `Number`/`Enum` subtype as a
leaf, so a user `Number` subclass or field-carrying enum reachable from a
root can hide a `GigaMap`/external index from validation — the class javadoc
promises the opposite ("anything unprovable stays relevant").
**Mitigation:** restrict leaves to the known-final JDK numeric wrappers;
analyze custom subclasses and enums by field walk like any other class.

### F-14 Per-commit writer validation re-walks the whole root graph with no work budget
**[verified-corroborated: DS-15, GLM-A4]**
`AeronStorageBinaryReplicationTarget.prepareWrite` runs
`validateStorageRoots` before every distributed write; a fresh
`ValidationScratch` per call; collection element visits are not charged to
the object budget. The F-13 fix direction (above) widens the walk further.
**Mitigation:** cache per-`GigaMap`/generation validation results and skip
unchanged topology; reuse a writer-confined scratch; charge element visits
to `maxValidatedIndexObjects`. **Benchmark:** constant transaction size,
growing unrelated collection — throughput must not decay proportionally
(the repo has the benchmark infrastructure: `AeronReplayBenchmark`,
`soak` profile).

### F-15 Auth-removal left orphaned javadoc that breaks the doclint gate and misdocuments the security boundary
**[verified-corroborated: KIMI (rule 6/14/24), DS-30; my read of
`AeronSettings:784-826` confirms dangling `///` blocks with a truncated
sentence at :795]**
Also `AeronRuntime:36-42,454-458` still documents challenge/response and
credential erasure that no longer exist. The `deploy` profile runs
`doclint=all, failOnWarnings=true`, so these are build-gate failures, and the
text contradicts the no-auth trust model.
**Mitigation:** delete the orphaned blocks; rewrite the two `AeronRuntime`
passages to state the no-auth boundary in one sentence. Mechanical,
do immediately.

### F-16 User-upload TOCTOU: validation and extraction open the archive twice
**[verified: DS-04]**
`FilesystemVolumeBackupBackend:423-428` validates, then re-opens the path for
extraction; local storage is deleted before restore, so a swapped archive on
the shared volume is extracted without ever being validated.
**Mitigation:** validate and extract from one pinned `ZipFile` handle (or
re-verify file key + size + mtime immediately before extraction); prefer
staging extraction before deleting local storage.

### F-17 `contentDigestOfArchive` inflates without a true-byte budget while holding the publication lock
**[verified: DS-05]**
The digest loop reads declared sizes only (attacker-controlled), with no
byte budget, while every publisher on the shared volume is blocked behind
the publication file lock. `validateUpload` already has the budget pattern.
**Mitigation:** route the digest through `transferBounded` with the same
true-byte budget; count unknown sizes against it.

---

## 3. P2 — Robustness, operations, hot-path performance, API design

### F-18 Writer-restart failure taxonomy: disjoint exception roots at the exported boundary
**[verified: MIMO F-01 (+F-05, SPARK 9.2, KIMI rule 23); my read of
`errors/*` confirms `NodeException` and `ReplicationException` are parallel
`RuntimeException` roots]**
`ReseedRequiredException`/`WriterFencedException` — the signals operators act
on — extend `ReplicationException`, but are thrown out of
`ClusterNode.open()`, whose backup/status javadocs point at `NodeException`;
`ClusterNode.open()` declares no `@throws` at all. Additional inconsistency
found in validation: `StorageLimitReachedException` extends `NodeException`
while its sibling write-gate rejection `ReaderWriteRejectedException` extends
`ReplicationException`. Lease-acquisition failures surface as bare
`IllegalStateException` (F-05/MIMO).
**Mitigation:** single root — `ReplicationException extends NodeException`
(least churn) or a shared `ClusterException`; put the complete typed set in
`ClusterNode.open()`'s `@throws`; map lease-acquisition failures to
`WriterFencedException`/a `NodeException` subtype; make the two write-gate
rejections share one parent. Also covers SPARK 23.1 (config vs I/O
retryability split) as a follow-on.

### F-19 Protocol violations cross the reader boundary as untyped `IllegalStateException`/`IllegalArgumentException`
**[verified-corroborated: KIMI rule 23, SPARK 9.2]**
`TransactionAssembler` throws untyped runtime exceptions for stale token,
regression, gap, duplicate/mismatch terminals (≈12 sites); operators must
string-match to map to the documented RESEED/RETRY contract.
**Mitigation:** `CorruptReplicationDataException` for torn/gap/mismatch,
`ReseedRequiredException` for regression-past-durable-cursor, preserving
messages; one parameterized test asserting the typed mapping per site.

### F-20 `CloseSequencer` drops the failing stage's name from the exception chain
**[verified-corroborated: KIMI rule 23.2, SPARK 23.2]**
Stage names survive only in a DEBUG log; a failed teardown cannot identify
which collaborator failed from the thrown exception.
**Mitigation:** `failure.addSuppressed(new NodeException("close stage '" +
name + "' failed", stageFailure))` — name in the causal chain, not the log.

### F-21 Unbounded fencing-token acceptance and unbounded retention pause on the single-writer path
**[verified-corroborated: KIMI rule 33, SPARK 33.1/33.2]**
(a) `adoptFencingToken` raises the floor to *any* greater token; a runaway
restart loop is indistinguishable from legitimate takeovers. Bind acceptance
to a configurable window `[floor, floor + maxTokenAdvance]`, fail closed with
`ReseedRequiredException` beyond it. (b)
`withWritesPaused(() -> purgeSegmentsWhileWritesPaused(...))` has no
deadline; a wedged purge wedges the sole writer's admission indefinitely.
Bound the pause (`retentionPauseTimeoutNanos`), fail the retention command on
expiry, surface pause duration in `ReplicationMetrics`.

### F-22 Virtual-thread pinning and mixed lock idioms on the writer lease and import paths
**[verified-corroborated: KIMI rule 21/27, SPARK 1.2/21.1/27.1, GLM-B1]**
(a) `WriterFencingLease.renew`/`executeUnderOwnership` perform NFS file I/O
+ fsync inside `synchronized(mutexFor(path))` on a virtual-thread heartbeat —
a slow NFS round-trip pins the carrier. (b) The `storage.binary` import
pipeline mixes `LockedExecutor`, `synchronized(budgetLock)`, and
`ReentrantLock` in one flow. (c) `mutexFor(path) → stateLock` nesting is
convention-only; any future reverse path deadlocks. (d)
`TransactionAssembler` exposes three monitor roles on one object (GLM-B1).
**Mitigation:** per-path `ReentrantLock` or serializer
`StripeLockedExecutor` for the lease mutex; consolidate the import pipeline
onto the existing `LockedExecutor`s; one private final lock object per
concern in `TransactionAssembler` (no `synchronized` methods); document the
global lock order; assert absence of `jdk.VirtualThreadPinned` in the soak's
JFR report (`SoakJfrReport` already exists).

### F-23 Cross-thread `synchronized(archive)` on the shared Aeron Archive client
**[verified-corroborated: KIMI rule 26, SPARK 26.1]**
Aeron's Archive client is single-threaded-by-design; 11+ monitor sites in
`AeronArchiveReplicationPublisher` and `AeronRuntimeOwner` contend the
conductor and serialize writer commits against reader queries.
**Mitigation:** confine each `AeronArchive` to its owning thread (the write
regime already exists via the coordinator's write lock), route cross-thread
queries through a hand-off, or document precisely why each monitor site is
safe against Aeron's `poll()` contract. Cross-check
`$GITHUB_ROOT/aeron-io/aeron` `ArchiveTool`/`ReplayMerge`.

### F-24 Reader-reconnect and stop-budget defects (four small, related)
**[verified: DS-16..19, DS-21, DS-17, DS-18]**
- Suppressed-exception accumulation per failed reconnect attempt
  (`AeronArchiveReader:775-781`) — keep first cause + attempt counter, cap
  suppressed, sample-log the rest.
- `withholdSinceNanos` survives reconnect/dispose — clear it whenever the
  withheld sequence resolves by any path and on `dispose()`.
- Stop-at-latest can report TIMED_OUT although the final flush advanced —
  extend the stop deadline *after* `flushDeliveries()`, or let the flush
  update progress counters first.
- 1 kHz recorded-position refresher swallows every failure — run it only
  while a live terminal marker is actually withheld, back off to tens of ms,
  stop it in `fail()`, attach the last refresh failure as suppressed cause.

### F-25 Dev mode silently ignores configured role/transport/storage path; dev nodes can join prod channels
**[verified-corroborated: DS-20, KIMI 19.3, SPARK 28.1]**
`PROD_MODE` unset → `startDevNode()`: writable, unreplicated, role and
`ECLIPSE_DATAGRID_STORAGE_PATH` ignored. Separately, loopback/wildcard
rejection is prod-mode only, so a dev node pointed at a production
cluster-id's routable channels starts without complaint.
**Mitigation:** make dev mode incompatible with an explicitly configured
transport/role (fail startup demanding `PROD_MODE`); route the dev start
through `prepareEmbeddedStorage`; reject non-loopback channels whenever
`TRUSTED_NETWORK != true` regardless of mode.

### F-26 Backup/ops robustness group
**[verified: DS-07, DS-22, DS-24, DS-26, DS-27, DS-33]**
- Restore staging dirs (`.backup-restore-*`) are never reaped — reap at
  startup with the same age/lock guard as export workspaces.
- Backup-executor close is a 5 s hard failure for a non-interruptible
  minute-scale export — use a configurable budget and expose an
  "export draining, Store close deferred" state.
- Default archive-entry budget (1 << 24 ≈ 16 M) materializes every
  `ZipEntry` before checking — lower the default to tens of thousands and
  validate declared entry counts pre-extraction.
- "Archive maintenance in progress" throws `IllegalStateException` where
  siblings throw retryable `ReplicationUnavailableException` — align the
  type so callers can retry.
- Maintenance tasks can outlive scheduler close into Store shutdown — gate
  the embedded-storage stage on scheduler termination the way the backup
  executor is gated.
- Cold `StorageUsageGauge` serves 0 until first async measurement and the
  limit gate treats 0 as under-limit — fail the gate closed until measured,
  or take one synchronous startup measurement.

### F-27 `-1` unknown sentinel leaks into exported status; `lagTransactions()` turns it into a false "healthy"
**[verified-corroborated: DS-21, MIMO F-08]**
`ReplicationStatus` mixes `OptionalLong` components with raw
`-1`-carrying `currentSequence`/`latestSequence`; `NodeStatus.replication`
uses a `null` sentinel — two absence idioms in one exported package, and
`-1 - x` becomes a lag of exactly 0.
**Mitigation:** one convention: apply `present()` to every metric-derived
component (or a dedicated `UNREPLICATED`/`UNKNOWN` state with primitive
components); fix `lagTransactions()` to report unknown.

### F-28 Settings API: nullable boxed primitives and implementation class in the exported interface
**[verified: MIMO F-07]**
`NodeSettingsSource` returns `Integer`/`Long` boxes where the default is
known (NPE at unboxing; "unset" vs "zero" ambiguous), and nests the concrete
`Env`/`EnvKeys` implementation inside the exported API.
**Mitigation:** resolve defaults during assembly and expose primitives (or a
resolved settings record); move `Env` to an internal package keeping a
`NodeSettingsSource.environment()` factory.

### F-29 `ClusterNode.open()`'s documented runtime flags may be incomplete for consumers
**[verified: MIMO F-10, SPARK 30.1; my read of `pom.xml:254,264,576,628`
confirms `--add-exports java.base/jdk.internal.misc=ALL-UNNAMED` +
`--add-modules jdk.incubator.vector` on all four argLines while README tells
consumers only `--enable-preview`]**
Either a dependency genuinely needs `jdk.internal.misc` — then every
production consumer fails as the tests never did, and README must document
it — or the flags are vestigial and should be deleted (AGENTS rule 30).
**Mitigation:** run the suite once per flag removed to identify the
requiring class; then delete or document (Build *and* Use sections).

### F-30 Heap/direct-buffer allocation on the replication hot paths
**[verified-corroborated: SPARK 20.1/29.1/31.1/DS-25, KIMI rule 29/31; my
read of `AeronReplicationEnvelope.copyToOwned` confirms the per-envelope
`new byte[...]`]**
- Reader: `copyToOwned` allocates a heap array per decoded envelope on the
  polling thread → decode into a caller-supplied pooled
  `ExpandableArrayBuffer`/`UnsafeBuffer` scratch owned by the reader loop;
  extend the existing `perTransactionHeapAllocationStaysWithinBudget` test
  to the decode path.
- Writer: `PreparedWrite` retains `byte[] dictionary` + `ByteBuffer[]` per
  transaction → `DirectBuffer` views over one pooled buffer
  (`EnvelopeFramer.offerDictionaryChunks` already accepts that shape).
- `EnvelopeFramer` allocates and frees one 128 KiB direct buffer per
  transaction → pool one on the publisher, reset per transaction.
- `TransactionAssembler.EMPTY_BUFFER` is a NIO direct buffer in an
  otherwise-Agrona path → Agrona constant.
- `.formatted()` on hot gates → guard with `isLoggable` or precomputed
  constants (exception paths may keep it).

### F-31 Fixed sleeps ignore the configured retry policy; one timeout bounds two different waits
**[verified-corroborated: SPARK 20.3/22.1, KIMI rule 20]**
`Thread.sleep` in `AeronRuntime` driver-retry and `StorageBackupManager`
poll/retry loops bypass `AeronRetryPolicy` (which already exists, backed by
`BackoffIdleStrategy`); `readerStopTimeoutNanos` bounds both live-terminal
withholding and Archive-tail reconnect, so tolerance for a stalled recording
cannot be tuned separately.
**Mitigation:** route the loops through the retry policy; split into
`liveWithholdTimeoutNanos` + `reconnectTimeoutNanos` (+ probe delay) with
their own env keys.

### F-32 No CI pipeline in the repository
**[verified: DS-28; commit `41dd6f0` deleted the only workflow]**
`mvn verify` plus crash-matrix/soak profiles run nowhere automatically.
**Mitigation:** restore a CI definition covering `mvn verify` and scheduling
the `crashmatrix`/`soak` profiles; this is the delivery vehicle for half the
tests this review requires.

### F-33 `AtomicFileWriter.deleteRegularFile` "file reappeared" check is a TOCTOU false positive
**[verified: KIMI 19.2]**
The check fires against any legitimately racing re-create (checkpoint
rewrite racing restore cleanup).
**Mitigation:** compare the new file's `fileKey` and fail only on
same-identity reappearance, with a comment naming the race actually caught —
or delete the check.

### F-34 Per-commit scan budget vs reflection-based index machinery (consensus finding)
**[verified-corroborated: MIMO F-03, SPARK 13.1/30.1/32.2, KIMI rule 30 ledger]**
`StoreIndexReflection` resolves private JVector/Store fields by name and
writes them via `XMemory` byte offsets. It is the sanctioned single
layout-owner and fails closed on layout change, but: it mutates upstream state
the upstream builder machine owns (races its discipline), `graphRebuilt` is
toggled via raw byte write with no memory-semantics statement, and the
`5.0.0-SNAPSHOT` daily-rebuilt pin maximizes the breakage window.
**Mitigation (staged):** (1) now — keep `StoreIndexReflectionTest` in the
*default* verify gate (not profile-gated) so an upstream rename fails the
build; add the volatile/plain-write comment or a `VarHandle` for the boolean;
(2) next — replace the write path with an upstream-supported reset/refresh
hook (the repo tracks snapshot builds, so contributing one is feasible), or
move the mutable builder/graph references into a peruncs-owned holder; keep
the reflective probe as a startup fail-fast assertion only.

### F-35 Long positional parameter lists on framing and wiring paths
**[verified-corroborated: MIMO F-06, SPARK 7.1/16, KIMI rule 16, GLM-B3]**
`AeronReplicationEnvelope.encode` 17-18 params (transposing
`chunkIndex/chunkCount/chunkOffset` is a silent corruption bug),
`AeronArchiveRetention` 14, `TransactionAssembler` ctor 10 and
`Delivery.prepare` 10 (three consecutive `int`s = transposable cursor
witness), `BackupRestorePolicy` 8 mixed callbacks.
**Mitigation:** house pattern for cold paths — `record Configuration` /
`record Inputs` (already proven by `StorageBinaryDataMerger.Configuration`,
`StorageNodeManager.Configuration`); hot path — a scalar-replaceable
`record FrameHeader(clusterId, epoch, fencingToken, wireNonce, sequence,
kind, payloadLength, chunkIndex, chunkCount, chunkOffset, commitCrc32c)`
passed once (escape analysis elides the allocation; zero GC cost). For
`BackupRestorePolicy`, group the callbacks in a `RestoreActions` record so
transposition cannot compile.

### F-36 Residual correctness asymmetries flagged for a decision, not silent carry-over
- **ABORT token handling** is asymmetric with COMMIT
  (`TransactionAssembler:426-430` discards buffered data and adopts the new
  token; COMMIT rejects mixed tokens) — define one rule and test both
  takeover orderings. [verified: DS-34]
- **Writer local-write `Error` path** journals `PREPARING` while
  `RuntimeException` journals `COMMITTING_UNCERTAIN` — same hazard class,
  two terminal states; unify or document both in restart recovery.
  [verified: GLM-A5, DS-02 round]
- **Unresolved writer position is encoded as a "resolved" cursor**
  (`AeronPositionProvider:78-83` emits an empty payload that consumers
  decode unconditionally) — throw
  `ReplicationPositionUnavailableException` instead. [verified: DS-35]
- **Reader replacement validates the uncertainty marker before disposing the
  current reader** — a healthy reader inside its marker-open window fails
  replacement; move the check inside the replacement. [plausible: DS-36]
- **Repeated stop-at-latest resets the overall cap** — anchor the cap to the
  first request. [verified: DS-37]
- **Windows: POSIX-only temp-file attributes** make every cursor/checkpoint
  write fail — probe `supportedFileAttributeViews()` as the backup backend
  already does. [verified: DS-23]

---

## 4. P3 — Structure, style, documentation, tests

### F-37 God classes concentrated on the replication hot path
**[all five reports; sizes re-confirmed: `TransactionAssembler` 1092,
`AeronArchiveReader` 1066, `GuardingStorageManager` 1033,
`AeronReplicationPublisher` 1004, `FilesystemVolumeBackupBackend` 900,
`AeronArchiveReplicationPublisher` 880, `BackupArchive` 867,
`AeronWriterTransport` 852, `WriterFencingLease` 841, `AeronSettings` 826]**
**Mitigation (extract-with-tests, do as scheduled refactors):**
`GuardingStorageManager` → six package-private top-level adapters
(`GuardedDatabase`, `GuardedPersistenceManager`, storer adapters, guarded
target) each unit-testable against a proxy delegate; facade < 400 lines.
`TransactionAssembler` → `FramingState` / `DeliveryBarrier` /
`TransactionBuffers` under a ~300-line coordinator. `AeronSettings` → one
`fromProperties` per nested record; composition record on top. `NodeLifecycle`
→ `NodeStarter` / `NodeCloser` split.

### F-38 Housekeeping sweep (mechanical, one PR)
- **FQN sweep** with a checkstyle/forbiddenapis rule banning inline
  `java.*` in `src/main` (`WriterFencingLease:382`,
  `TransactionAssembler:489,545`, `StoreIndexReflection:137`).
  [MIMO F-15, KIMI rule 15, SPARK 13.2, DS-31]
- **Javadoc indent normalization** to declaration indentation house-wide;
  the `deploy` doclint gate then stays clean
  (`StorageGraphCoordinator`, `NodeAssembly:99`, `AeronArchiveRetention:74`,
  `AtomicFileWriter:32,49,56`, `BackupArchive`, …).
  [MIMO F-14, GLM-B6, KIMI rule 14]
- **`peruncs/package-info.java` missing** — the only package without one;
  add the one-sentence product narrative. [KIMI rule 11, SPARK 11.1]
- **`module-info` wildcard import** (`import peruncs.cluster.api.*;`)
  replaced the explicit export-surface imports — restore them; the wildcard
  hides which types the javadoc links document. [DS-31]
- **Comment debris:** the `TransactionAssembler:115-124` lock-order comment
  contains an unedited self-correction ("… no — even the barrier monitor is
  released") — state one rule; `NodeMaintenanceScheduler:26` ungrammatical
  threshold sentence; "supression" typo. [MIMO F-13, KIMI]
- **`AtomicFileWriter` write-overload chain** — collapse to
  `write(path, encoder, Phase)` + `writeBytes`, derive hook names from
  `Phase`; document or fold the 6-arg private overload. [SPARK 5.3/14.1]

### F-39 Test scaffolding and gating
- **One `TestHooks` class** for the five duplicated `ScopedValue` hook
  copies (`AtomicFileWriter`, `AeronReplicationCheckpointStore`,
  `FilesystemVolumeBackupBackend`, `CrashHook`, `TransactionAssembler`);
  production types keep zero test-shaped public methods
  (`suspendHeartbeatForTest`, `runWithHook`, `runWithTestHook`).
  [KIMI rule 5, MIMO F-12, SPARK 5.1/6.2]
- **Promote the cheapest, most regression-prone crash/admission cells into
  the default `mvn verify` gate** (prepare/local-write/commit-offer
  seams; read-admission/shutdown races); keep duration, not coverage,
  behind `-Pcrashmatrix`/`-Psoak`. Several tests demanded by earlier rounds
  still do not exist at all — see §6. [KIMI rule 25, SPARK 25.1]
- **Zero-assertion probe test** `ColdVectorUpdateProbeTest` under the
  Surefire pattern — assert vectorizer counts and post-refresh query state
  (the class already tracks them) or move it out of `*Test`. [MIMO F-11]
- **`useModulePath=false`** for tests silently permits split-package drift
  between `src/main` and `src/test`; migrate forked fixtures to explicit
  modular launches and re-enable the module path, deleting the
  bytecode-parsing workaround test. [SPARK 25.2]

### F-40 Naming and API-convention alignment
- `NodeAssembly` reads as a factory but is the lifecycle interface;
  consider `ClusterNodeFactory` or folding into `ClusterNode.open`;
  builder naming is split `setX`/`withX`/bare-noun — standardize
  (`withX` for immutable copies, bare nouns for mutable builders).
  [SPARK 2.1/2.2 — partial accept: naming churn is optional; do it only
  alongside F-37]
- `AeronTransportShared`'s `closed()/closing()/closing(boolean)/closed(boolean)`
  boolean pairs → `enum TransportState` + `compareAndSet`; single-use
  `claimStream` triple → inline. [SPARK 2.3/8.1, KIMI rule 8]
- `CrashHook` + `WriterLeaseGate` fold into their sole owners.
  [SPARK 8.2, KIMI rule 8]
- `NodeClose` carries close + `checkOpen` — split the admission check into
  its own input so the callback stays single-responsibility.
  [GLM-B4, KIMI rule 4/7]
- `public` outside `api`/`errors` is an implicit test harness: state the
  convention in the root package-info and demote where feasible
  (`AtomicFileWriter`'s 12 public static helpers → keep
  `write/writeBytes/delete`, demote the rest). [KIMI rule 10, SPARK 10.1/10.2]

### F-41 Modernization (accept partially, not as a blanket rewrite)
- `StructuredTaskScope.ShutdownOnFailure` for genuine fan-out-and-join
  (soak workers, backup pause-snapshot-resume) — not a mandate to rewrite
  every executor. [SPARK 1.1, KIMI rule 1.3 — partial accept]
- Isolate the load-bearing `LazyConstant` preview type behind one internal
  holder so a JEP 531 rename touches one file. [SPARK 1.4, KIMI rule 1.2]
- `ThreadLocalRandom` is *not* a rule-1 violation (reports mischaracterize
  it), but a per-policy seedable `RandomGenerator` buys deterministic soak
  runs (`soak.seed`) — do it for that reason. [SPARK 1.3, KIMI rule 1.1 —
  reframed]
- `NodeOptions`/builder silently replaces a caller-supplied foundation's
  file provider — fail fast with `IllegalArgumentException` on a non-default
  provider and document the replace-list. [SPARK 4.3, KIMI rule 4/7]

---

## 5. Rejected / deferred (with rationale — do not re-file)

| Proposal | Source | Rationale |
|---|---|---|
| Flip `GraphBoundary.write` to auto-invalidate by default | MIMO F-02 | Contradicts the reviewed ES.md design (D4): clean callback failures must not poison; the *caller* reports potentially-dirty state via `invalidate()`. **Accepted remainder:** the obligation is invisible in the README example and API shape — fix the README example, add a prominent "dirty failure contract" paragraph to `GraphBoundary`, and consider a `writeGuarded(...)` convenience that auto-invalidates. |
| `Authorizer` parameter on `NodeOptions` | SPARK 19.3 | Conflicts with the cluster constraint "no node authentication features"; the README already delegates auth to the embedding application. Do not add node-level auth surface. |
| Replace NFS fencing lease with Aeron-cluster election / `WatchService` | SPARK 7.2 | The reviewed design is the NFSv4 lease; a `WatchService` short-term variant may be evaluated, but the election rewrite is a project decision, not a review fix. |
| Reader→writer promotion API | SPARK 4.1 | The module deliberately fixes roles ("role change is a restart"). **Accepted remainder:** ship the freeze-copy-replace-cursor restart runbook (`promote.sh`) as documentation. |
| `launchDriver` nullable-`Path` rework "contrary to the Optional convention" | DS-31 (bullet 3) | Non-finding: `Optional` on an input parameter complied with the convention; the nullable form also complies. Keep whichever reads better (the single-use 3-arg overload can still go — F-38). |
| `parse_partial` at `ClusterIndexValidation:429` | SPARK 32.1 | Tooling artifact of the graph indexer, not a code defect; re-index and ignore. |
| JVector `graphRebuilt` volatile-write concern as a standalone P1 | DS appendix | Folded into F-34's staged mitigation (comment/VarHandle now; upstream hook next). |
| Envelope `FrameHeader` instantiated per frame | MIMO F-06 (as written) | Mitigation refined per KIMI: one scalar-replaceable record argument, escape analysis elides allocation — do not allocate per frame on the hot path. |

---

## 6. Missing tests the consolidated fixes must ship with

From the validated findings above — none of these exist in the tree today:

1. **F-01:** staged barrier + blocked import + `dispose()` → merger stays usable.
2. **F-02:** crash between restore install and cursor force (and extraction→install).
3. **F-03:** writer restart with backup at N, acked writes N+K → reseed, not resume.
4. **F-04/F-05:** late write section after `drain()` rejects without latching; close with a stuck section fails bounded, not hanging.
5. **F-06:** close concurrent with first `LazyHolder.get()` → no leaked resource.
6. **F-07..F-09:** reaper `.lease`-first ordering; slowed-renew/close interleaving; superseded-lease acquisition cleanup.
7. **F-13/F-14:** polymorphic `Number`/enum holder hides an index (fail-closed); constant-transaction/growing-collection scaling benchmark.
8. **F-19:** typed-exception mapping per assembler site.
9. **F-21:** token-advance window rejection; retention-pause deadline.
10. **F-25:** dev node with configured role/transport → startup failure; dev node vs prod channel guard.
11. **F-29:** suite run per removed JVM flag (identifies the requiring class).
12. **F-30:** decode-path allocation budget test (extend the existing per-transaction budget test).

---

## 7. Execution waves

**Wave 1 — ship-blockers (days, surgical):**
F-01, F-02, F-03, F-04, F-05, F-06 (all P0) + F-15 (doclint gate) and F-32
(CI skeleton) — the P0 set is small, self-contained, and each has a required
test in §6.

**Wave 2 — trust and fencing (next):**
F-07, F-08, F-09, F-10 (purge on uncached proof), F-12, F-16, F-17, F-36
(decisions on ABORT/Error-path/unresolved-cursor).

**Wave 3 — liveness and operator surface:**
F-11, F-13, F-14 (with benchmark), F-18/F-19/F-20 (exception taxonomy),
F-21, F-24, F-25, F-27.

**Wave 4 — hot path and platform:**
F-22, F-23, F-29, F-30, F-31, F-26, F-33, F-34 (stage 1), F-35.

**Wave 5 — structure and hygiene (scheduled refactors):**
F-37 (extractions with tests), F-38 (housekeeping sweep), F-39 (test
gating), F-40, F-41, remaining F-36 items, profile-gate promotion of §6 tests.
