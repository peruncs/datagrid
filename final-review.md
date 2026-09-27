# final-review.md — Consolidated, validated review of `peruncs.cluster`

**Historical sources assessed:** `DS-REVIEW.md`/`ds-review.md`,
`MIMO-REVIEW.md`, `SPARK-REVIEW.md`, `KIMI-REVIEW.md`, and `GLM-REVIEW1.md`
(== `glm-review.md`, identical duplicates — counted once). `ES.md` is a design
document, not a review, and is excluded per instruction.

**Original method:** the report was checked against `cd39892` plus its then-
uncommitted delta. That snapshot is stale. This validation is against current
HEAD `f4c908b` plus the working-tree changes listed below.

**Duplicate handling:** the five reports overlap heavily (god classes, long
argument lists, FQN sweep, javadoc indentation, test hooks, envelope
allocations, fencing-token bound, typed exceptions). Overlapping findings are
merged into one entry with all source tags; the strongest mitigation wins.

## Current-tree status (2026-09-27)

- **Already resolved at `f4c908b`:** F-01–03, F-07–10, F-12–13, and F-15–17.
  The report's statements that no shutdown/drain tests exist are stale.
- **Fixed in this working tree:** F-04–06, F-11, F-18–20, F-23–24, F-26–29,
  F-33, and the actionable F-36 cases. The index scan now charges both
  visited objects and collection elements to one work budget.
- **Partly addressed:** F-14 remains an index-relevant graph walk on each
  writer validation; it is reusable and bounded, but safe topology caching
  cannot observe unsupported direct upstream index registration. F-21 rejects
  an arbitrary token-advance cap; caller waits and each Archive control call
  are bounded, though an uninterruptible purge may outlive the caller's wait.
  F-25 validates explicit dev role/transport settings
  and honors an explicit storage path; no dev channel policy was added. F-30
  reuses the writer's direct framing
  buffer; the reader hot path already uses a reusable `EnvelopeView`, so the
  public copying decoder is not on that path. F-34's reflection test is
  already in the default test tree, and its plain-write visibility contract
  is now documented. F-35 groups cold restore callbacks in `RestoreActions`;
  fixed-shape wire methods remain scalar to avoid per-frame objects.
- **Newly completed:** F-31 now gives reader stop, live-marker durability, and
  Archive reconnect independent timeout budgets; the latter two have their
  own environment settings. Existing short retry/poll delays remain fixed
  because they are local pacing intervals, not remote-operation deadlines.
- **Housekeeping/test status:** F-38's cited inline FQNs and module imports
  were cleaned up, package narrative was added, and malformed comments were
  corrected. Repository-wide Javadoc normalization and private file-writer
  overload reshaping remain optional cleanup. F-39's stale
  claim that the listed tests do not exist is corrected below; test-hook
  consolidation and a full module-path test migration remain structural work.
- **Deferred or rejected:** F-21's arbitrary fencing-token window would reject
  valid long-running token histories. F-22's monitor
  pinning claim is stale for this Java 27 project (JEP 491 shipped in JDK 24;
  see the [JDK 24 release notes](https://www.oracle.com/java/technologies/javase/24-relnote-issues.html));
  lock-structure cleanup needs measured contention. F-31's fixed local
  retry delays remain; its reader wait budgets are now independent. F-32 runs
  Maven verify on push/PR and the crash matrix weekly; soak is manual because of its
  runtime cost. F-37, F-39, F-40, and F-41 remain optional structural work,
  not isolated correctness fixes.
- **Network requirement:** PerunCS Cluster neither implements nor requires
  node authentication or transport encryption. The README and module record
  state this explicitly; no authentication or encryption feature was added.

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
  pre-extraction; `NodeSettingsSource` duplicated javadoc removed; Archive
  control authentication support removed. Documentation now explicitly states
  that node authentication and transport encryption are neither implemented
  nor required.
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
**Current status:** both overloads now latch only failures thrown by the
application update callback; late admission rejection leaves graph validity
unchanged.

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
**Current status:** implemented with `ECLIPSE_DATAGRID_GRAPH_DRAIN_TIMEOUT_MILLIS`
and `GraphDrainTimeoutException`.

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
**Current status:** implemented; the lifecycle test blocks initialization
while close observes the same holder.

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
**Current status:** the apply wait now covers the materialization budget plus
the configured index-refresh allowance.

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
**Current status:** scratch is reused and the combined scan budget charges
visited objects and collection elements, with a 65,536-item default. Each
write still scans index-relevant reachability because direct upstream
registrations bypass version tracking; safe topology caching remains open.

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
**Current status:** `ReplicationException` now extends `NodeException`;
writer lease startup failures are wrapped in `NodeException`, and
`ClusterNode.open()` documents its typed startup failures. The sibling write
rejections share the exported `NodeException` root, though not a dedicated
write-rejection subtype.

### F-19 Protocol violations cross the reader boundary as untyped `IllegalStateException`/`IllegalArgumentException`
**[verified-corroborated: KIMI rule 23, SPARK 9.2]**
`TransactionAssembler` throws untyped runtime exceptions for stale token,
regression, gap, duplicate/mismatch terminals (≈12 sites); operators must
string-match to map to the documented RESEED/RETRY contract.
**Mitigation:** `CorruptReplicationDataException` for torn/gap/mismatch,
`ReseedRequiredException` for regression-past-durable-cursor, preserving
messages; one parameterized test asserting the typed mapping per site.
**Current status:** protocol violations now use typed corruption or reseed
exceptions, with tests covering malformed terminals and fencing regressions.

### F-20 `CloseSequencer` drops the failing stage's name from the exception chain
**[verified-corroborated: KIMI rule 23.2, SPARK 23.2]**
Stage names survive only in a DEBUG log; a failed teardown cannot identify
which collaborator failed from the thrown exception.
**Mitigation:** `failure.addSuppressed(new NodeException("close stage '" +
name + "' failed", stageFailure))` — name in the causal chain, not the log.
**Current status:** nonfatal failures name the stage in the thrown exception;
fatal errors retain a named suppressed stage marker.

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
**Current status:** an arbitrary token-advance cap was rejected because valid
writer history can advance by any number of takeovers. Retention callers and
Archive control requests have finite waits, but a purge already executing may
outlive the caller; an interrupting deadline is unsafe during the Archive
stop/purge/extend sequence.

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

### F-23 Redundant cross-thread intrinsic locking around the shared Aeron Archive client
**[verified-corroborated: KIMI rule 26, SPARK 26.1]**
The prior report called the Archive client single-threaded, but upstream
`AeronArchive` source documents thread-safe calls protected by its
configured `Context` lock. The project keeps the default `ReentrantLock`; the
extra intrinsic monitor at 14 sites only layered a second lock around control
requests.
**Mitigation:** remove the extra `synchronized(archive)` wrappers and use
AeronArchive's own lock. Upstream `AeronArchive` documents that the client is
thread-safe; its default `Context` uses a `ReentrantLock`, and this project
does not replace it with `NoOpLock`. A second intrinsic lock only added
contention around the same control calls.
**Current status:** removed the redundant monitor wrappers from the publisher
and runtime owner. The publisher's separate state locks remain.

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
**Current status:** all four changes are implemented; the refresher runs only
while withholding a live terminal and retains its last failure.

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
**Current status:** the configured role/transport and storage-path defects are
fixed. The proposed dev channel policy was not added; no authentication or
transport-encryption feature is part of this work.

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
**Current status:** restore workspaces use a lease-checked stale-workspace
reaper; backup close time is settings-driven; entry bounds and scheduler close
gating were already present; admission rejects maintenance contention with a
retryable exception; startup now measures storage synchronously before writer
admission opens, then refreshes the usage on the maintenance schedule.

### F-27 `-1` unknown sentinel leaks into exported status; `lagTransactions()` turns it into a false "healthy"
**[verified-corroborated: DS-21, MIMO F-08]**
`ReplicationStatus` mixes `OptionalLong` components with raw
`-1`-carrying `currentSequence`/`latestSequence`; `NodeStatus.replication`
uses a `null` sentinel — two absence idioms in one exported package, and
`-1 - x` becomes a lag of exactly 0.
**Mitigation:** one convention: apply `present()` to every metric-derived
component (or a dedicated `UNREPLICATED`/`UNKNOWN` state with primitive
components); fix `lagTransactions()` to report unknown.
**Current status:** missing replication is `NOT_CONFIGURED`, sequence and
position boundaries use `OptionalLong`, and unknown lag is empty.

### F-28 Settings API: nullable boxed primitives and implementation class in the exported interface
**[verified: MIMO F-07]**
`NodeSettingsSource` returns `Integer`/`Long` boxes where the default is
known (NPE at unboxing; "unset" vs "zero" ambiguous), and nests the concrete
`Env`/`EnvKeys` implementation inside the exported API.
**Mitigation:** keep the environment parser behind `NodeSettingsSource.env()`
factories and retain `EnvKeys` as the public names for supported variables.
The nullable numbers remain the smallest way to distinguish “unset” from an
explicit zero while settings defaults are resolved by assembly. `null` is now
documented on each nullable return; replacing these values with primitives
would silently change configuration semantics, and Optional returns conflict
with the API's no-Optional-return convention.
**Current status:** the concrete parser is package-private, process and
map-backed sources are created through the interface, and environment keys
remain public without exporting the implementation class.

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
**Current status:** README now documents preview, vector-module, and Serializer
JDK-export flags for classpath and named-module launches.

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
**Current status:** the reader hot path uses `decodeView` with a reused view;
the public allocating decoder is not used there. The writer now owns and
reuses its direct frame buffer. The remaining dictionary-buffer and cold
message allocations need allocation profiling before further pooling.

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
**Current status:** reader stop, live-marker durability, and reconnect now
have independent positive budgets; the latter two have separate environment
keys. The fixed local retry/poll delays remain short pacing intervals; the
configured Aeron policy governs Aeron poll/offer loops, not the independent
backup manager or startup loop.

### F-32 No CI pipeline in the repository
**[verified: DS-28; commit `41dd6f0` deleted the only workflow]**
`mvn verify` plus crash-matrix/soak profiles run nowhere automatically.
**Mitigation:** add a GitHub Actions verification workflow. Current status:
pushes and pull requests run `mvn verify`; the crash matrix runs weekly, and
the soak profile is manually selectable to avoid imposing a long scheduled
run on every repository cycle.

### F-33 `AtomicFileWriter.deleteRegularFile` "file reappeared" check is a TOCTOU false positive
**[verified: KIMI 19.2]**
The check fires against any legitimately racing re-create (checkpoint
rewrite racing restore cleanup).
**Mitigation:** compare the new file's `fileKey` and fail only on
same-identity reappearance, with a comment naming the race actually caught —
or delete the check.
**Current status:** deletion now compares file identity, and the test covers a
new file created after the original was removed.

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
build; the current code documents why the plain write is visible through the
graph-lock handoff;
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
transposition cannot compile. The restore callbacks now use that record;
other cold constructor groupings remain optional.

### F-36 Residual correctness asymmetries flagged for a decision, not silent carry-over
- **ABORT token handling** is asymmetric with COMMIT
  (`TransactionAssembler:426-430` discards buffered data and adopts the new
  token; COMMIT rejects mixed tokens) — define one rule and test both
  takeover orderings. [verified: DS-34; current status: documented and covered
  by fencing tests — newer-token ABORT may discard an older partial write]
- **Writer local-write `Error` path** journals `PREPARING` while
  `RuntimeException` journals `COMMITTING_UNCERTAIN` — same hazard class,
  two terminal states; unify or document both in restart recovery.
  [verified: GLM-A5, DS-02 round; current status: fatal errors fail closed
  without follow-up checkpoint I/O]
- **Unresolved writer position is encoded as a "resolved" cursor**
  (`AeronPositionProvider:78-83` emits an empty payload that consumers
  decode unconditionally) — throw
  `ReplicationPositionUnavailableException` instead. [verified: DS-35]
  Current status: missing writer recording/position now throws that type.
- **Reader replacement validates the uncertainty marker before disposing the
  current reader** — a healthy reader inside its marker-open window fails
  replacement; move the check inside the replacement. [plausible: DS-36;
  current status: validation now runs after disposing the old reader]
- **Repeated stop-at-latest resets the overall cap** — anchor the cap to the
  first request. [verified: DS-37; current status: first request anchors the
  overall deadline]
- **Windows: POSIX-only temp-file attributes** make every cursor/checkpoint
  write fail — probe `supportedFileAttributeViews()` as the backup backend
  already does. [verified: DS-23; current status: POSIX attributes are used
  when supported, with a portable default otherwise]

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
  `java.*` in `src/main`. The cited instances were replaced by imports; a
  repository-wide ban remains optional.
  [MIMO F-15, KIMI rule 15, SPARK 13.2, DS-31]
- **Javadoc indent normalization** to declaration indentation house-wide;
  a repository-wide sweep remains optional. The cited malformed blocks
  were corrected along with the current module/package narratives.
  [MIMO F-14, GLM-B6, KIMI rule 14]
- **Package narrative, module imports, and comment debris:** `peruncs/package-info.java`
  now exists, module imports are explicit, and the cited malformed comments
  and typo were corrected. [KIMI rule 11/15, SPARK 11.1/13.2, MIMO F-13]
- **`AtomicFileWriter` write-overload chain** — its public API already has
  `write(path, encoder, Phase)` and `writeBytes`; the private overload
  carries precomputed crash-hook names so normal writes allocate none.
  Further folding is unnecessary. [SPARK 5.3/14.1]

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
  behind `-Pcrashmatrix`/`-Psoak`. The earlier report's claim that the
  shutdown, drain, backup cleanup, reader disposal, writer recovery, index
  reflection, and index refresh tests are absent was stale; see §6.
  [KIMI rule 25, SPARK 25.1]
- **Zero-assertion probe test:** `ColdVectorUpdateProbeTest` now asserts a
  post-refresh vector search returns results. [MIMO F-11]
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
- `public` outside `api`/`errors` is an implicit test harness: the module
  exports only `api` and `errors`, so implementation classes are not JPMS
  consumer API. Keep public visibility only where cross-package implementation
  callers need it; the `AtomicFileWriter` macOS alias helpers are now private.
  A blanket visibility reduction would break those internal package boundaries.
  [KIMI rule 10, SPARK 10.1/10.2]

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
  file provider — **not a current finding**: `NodeOptions` and its builder
  already document that the live provider comes from the configured storage
  path, and `NodeAssemblyLifecycleTest` verifies that Store tuning survives
  this deliberate replacement. [SPARK 4.3, KIMI rule 4/7]

---

## 5. Rejected / deferred (with rationale — do not re-file)

| Proposal | Source | Rationale |
|---|---|---|
| Flip `GraphBoundary.write` to auto-invalidate by default | MIMO F-02 | Contradicts the reviewed ES.md design (D4): clean callback failures must not poison; the *caller* reports potentially-dirty state via `invalidate()`. The README example and `GraphBoundary` Javadoc now state the dirty-failure contract; an auto-invalidating convenience remains optional. |
| `Authorizer` parameter on `NodeOptions` | SPARK 19.3 | Conflicts with the cluster constraint "no node authentication features"; the README already delegates auth to the embedding application. Do not add node-level auth surface. |
| Replace NFS fencing lease with Aeron-cluster election / `WatchService` | SPARK 7.2 | The reviewed design is the NFSv4 lease; a `WatchService` short-term variant may be evaluated, but the election rewrite is a project decision, not a review fix. |
| Reader→writer promotion API | SPARK 4.1 | The module deliberately fixes roles ("role change is a restart"). Promotion remains a deployment runbook: safe steps depend on which Store and Archive image the operator elects as authoritative. |
| `launchDriver` nullable-`Path` rework "contrary to the Optional convention" | DS-31 (bullet 3) | Non-finding: `Optional` on an input parameter complied with the convention; the nullable form also complies. Keep whichever reads better (the single-use 3-arg overload can still go — F-38). |
| `parse_partial` at `ClusterIndexValidation:429` | SPARK 32.1 | Tooling artifact of the graph indexer, not a code defect; re-index and ignore. |
| JVector `graphRebuilt` volatile-write concern as a standalone P1 | DS appendix | Folded into F-34's staged mitigation (comment/VarHandle now; upstream hook next). |
| Envelope `FrameHeader` instantiated per frame | MIMO F-06 (as written) | Mitigation refined per KIMI: one scalar-replaceable record argument, escape analysis elides allocation — do not allocate per frame on the hot path. |

---

## 6. Test status

The original “none exist” claim was stale. The tree has coverage for
shutdown, bounded drain, backup workspace cleanup, reader disposal, writer
recovery, index reflection, and index refresh. The focused run below also
covers the changed admission, close, lifecycle, index-budget, storage-limit,
backup, typed-exception, and reader-timeout behavior.

This focused command passed:

```text
mvn -o -q -Dtest=StorageGraphCoordinatorTest,CloseSequencerTest,NodeAssemblyLifecycleTest,NodeLibraryEnvKeysTest,StorageLimitGateTest,TransactionAssemblerFailureTest,TransactionAssemblerFencingTest,TransactionAssemblerDurabilityTest,ClusterIndexProbeScratchTest,AtomicFileWriterTest,ColdVectorUpdateProbeTest,CurrentReaderTest,FilesystemVolumeBackupBackendTest,StorageBackupTaskExecutorTest,BackupRestorePolicyTest,AeronReplicationPublisherTest,AeronSettingsTest,ClusterStoreIndexesTest,BackupRestoreCompatibilityTest,ReaderSeedBootstrapTest,ReplicationApplierAeronTest,AeronReplicationMonitoringTest#writerWithoutSharedLeaseDirectoryFailsAtStartup test
```

The latest settings, role, lifecycle, publisher, and lease sweep also passed
test compilation and this focused run:

```text
mvn -o -q -DskipTests test-compile
mvn -o -q -Dtest=NodeLibraryEnvKeysTest,NodeRoleTest,NodeAssemblyLifecycleTest,ReaderSeedBootstrapTest,AeronReplicationPublisherTest,WriterFencingLeaseTest test
```

The initial sandboxed full verify could not bind Aeron UDP sockets and failed
with “Operation not permitted.” Re-running outside the restricted sandbox
passed all requested gates on 2026-09-27:

```text
mvn -o verify                 — 751 unit tests, 12 default integration tests
mvn -o -Pintegration verify   — 751 unit tests, 10 integration tests
mvn -o -Psoak verify          — 751 unit tests, 1 soak test (30 seconds)
mvn -o -Pcrashmatrix verify   — 751 unit tests, 69 crash/Store integration tests
```

The GitHub Actions workflow runs verify on push and pull request, runs the
crash matrix weekly, and makes integration, soak, and crashmatrix profiles
manually selectable (F-32).

## 7. Remaining work

- **Needs profiling:** F-22 lock-structure cleanup and F-30's remaining
  dictionary-buffer/cold-message allocations.
  JDK 27 does not have the synchronized-monitor pinning defect described by
  the old F-22 report.
- **Needs a design decision:** F-21 cancellation of an in-progress Archive
  purge; any fencing-token acceptance window must account for valid
  long-running histories. The suggested F-25 dev-channel policy was not added;
  the documented network boundary remains unchanged.
- **Optional structural work:** remaining cold constructor grouping in F-35, god-class
  extraction in F-37, repository-wide doclint cleanup in F-38, test-hook
  consolidation/module-path migration in F-39, naming cleanup in F-40, and
  selective preview-feature adoption in F-41.
