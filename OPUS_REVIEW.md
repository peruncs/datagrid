# PerunCS Cluster — Review and Implementation Spec (Opus), revision 7

**Baseline:** commit `9975d95`. Revision 6 was a static review; revision 7 records implementation
follow-up and remaining benchmark gates.

## 0. Scope, conventions, revision history

### What PerunCS Cluster is (governs every decision below)

PerunCS Cluster is an **embedded, Eclipse Store-based clustering library**: one writer node and N
reader nodes, replicated over Aeron.

- **Its API is Eclipse Store's API.** An Eclipse Store application switches to a cluster by
  changing how it obtains its `StorageManager`, not how it uses it.
- **It is not a framework and not a feature port.** Eclipse Data Grid was supplied as a
  *reference only*: no Data Grid feature, name or SPI is adopted unless it independently serves
  the embedded library.
- **Deployments are examples, not requirements.** Helidon behind Caddy / a Cloudflare tunnel is
  one example deployment. No decision may depend on a web framework, proxy or configuration
  system, and configuration must be pluggable from any source.

### Conventions

- **Paths.** `M/` means `src/main/java/peruncs/cluster/`, `T/` means `src/test/java/peruncs/cluster/`,
  and `$GITHUB_ROOT/…` are upstream checkouts. A path starting with `src/` is repo-relative.
- **Severity.** P0 = correctness/data loss/availability; P1 = major performance, robustness or API
  contract; P2 = simplification or rule violation; P3 = cosmetic.
- **Compatibility.** `AGENTS.md` requires no backward compatibility: format changes mean "reseed
  on upgrade" and no migration code.
- **Decision provenance.** "Default decision" = chosen by this spec because nothing settled it;
  applied unless the owner objects.

### Revision history

- **rev 1:** the review.
- **rev 2:** owner decisions (no failover, drop-in API, mark in the Store).
- **rev 3:** the spec.
- **rev 4:** deployment neutrality, PR #832.
- **rev 6 (this revision):**
  - The old AGENTS.md rule 31 ("prefer Agrona/Serializer memory utilities") is deleted by the
    owner. Old rules 32–34 are now 31–33.
  - Added the limited FFM adoption policy (D-29) with two spec blocks: **F1**, bounds-checked
    parsing of replicated bytes, and **F2**, an arena-backed buffer pool spike folded into N1.
  - Added N3 (retention never triggered), found while writing the rev-5 documentation.
- **rev 7 (implementation sweep):**
  - Implemented C9 backup outcome reporting, including separate post-publication maintenance
    failures; implemented writer-driven N3 retention; removed the D-17 pause/resume control API.
  - Kept S1's same-name archive checks after tracing the crash-retry and conflicting-content
    cases: removing them can overwrite a durable backup. Digest inspection stays outside the
    publication lock and rechecks the destination stamp before publishing.
  - Confirmed the protocol has no node authentication or transport encryption and needs neither.
    P1-1/F1 code and parser checks are present; their remaining performance benchmark gate is
    not claimed complete.
- **Current review follow-up (2026-09-28):**
  - Recorded-ABORT rejection now applies to coordinator and low-level publisher preparation. The
    original failure remains in the cause chain and the abort position reaches the checkpoint
    journal. Cause cycles, excessive depth, errors, and unrelated replication failures fail closed.
  - Application drain uses `NodeClose.awaitAppIdle(Duration)` and the settings timeout.
    `AppSections` uses a lock-protected count; lifecycle admission under that lock is the close
    gate, so a second permanent `closing` flag is unnecessary.
  - Starter backup waits are bounded. Retention caps requests at the complete reader-quorum
    watermark, preserves history for incomplete or lagging readers, and has a configurable cadence.
  - C5 deliberately keeps `LazyHolder` synchronization: close must wait for a resource factory
    already in progress before it can skip disposal; `volatile initialized` alone loses that race.
- **rev 5:**
  - Data Grid-derived items removed: the `ClusterFoundation` naming, the exported
    `ObjectGraphUpdateHandler` SPI and the provider SPI. A9 is now purely "Eclipse Store API
    compatibility".
  - C10 corrected: the rev-3 fix would have wedged close.
  - C1 precedence and default stated; C2 admission mechanics stated.
  - A1: spike S-0 now answered from the Aeron source; ordered recovery table; definitive
    delete/edit lists; external-Archive decision.
  - A8 rewritten: discovery already exists.
  - C7 rows completed; C9 type specified.
  - Per-step acceptance criteria, test-to-step mapping and ID tombstones added.
  - The "run a formatter" instruction dropped.

---

## 1. Decision log

| # | Topic | Decision | Source |
|---|-------|----------|--------|
| D-00 | Scope | As in §0. | Owner |
| D-01 | Roles and failover | Fixed roles (a product invariant, `module-info` "Fixed roles"). The library chooses no writer and performs no failover. It guarantees that one Store cannot have two writers, and readers fail closed on stale writers. | Owner |
| D-02 | API | Eclipse Store API compatibility: `ClusterStorageManager<T> extends StorageManager` (exists, `M/api/ClusterStorageManager.java:41`) plus a `ClusterStorage` entry point mirroring `EmbeddedStorage` (A9). No Data Grid SPI or names. | Owner |
| D-03 | A1 | Do it, gated by the benchmark (D-25). | Owner |
| D-04 | Writer exclusivity | Local `writer.lock` + a token in the Store mark (A2). The NFS lease is deleted. Two hosts sharing one Store path is unsupported operator error, documented. | Owner (D-01) |
| D-05 | C1 | An exported `WriteRejectedException` is thrown at named sites. The classifier default is **latch**; uncertain wins over clean. No message matching. | Default |
| D-06 | C2 | Facade-level `AppSections` counter (outermost section per thread). The admission close reuses the lifecycle's existing closing flag. It is permanent, not interim. | Default |
| D-07 | C3 | `CompletableFuture<BackupInfo> createBackup(BackupSlot)`; the pause happens at task start. | Default |
| D-08 | Mark placement | Named root `"peruncs.replication"`; fallback per spike S-1. | Default |
| D-09 | COMMIT offer fails after a local commit | `ReplicationPendingException`, graph valid, admission suspended until re-offered. | Default |
| D-10 | Entry point | `ClusterStorage` (static) + `ClusterStorageFoundation<T>` (builder). Delete `ClusterNode.open(NodeOptions)` and `NodeOptions`. | Default |
| D-11 | Graph coordination API | `GraphBoundary` is the only coordination API. **No exported update-handler SPI.** The internal `ObjectGraphUpdateHandler` interface is folded into `StorageGraphCoordinator` (single implementation, D2). | Default (rev-5 scope) |
| D-13 | Framework integrations | Not part of the library. The API is shaped so that any integration is a few lines (§5 integration guidance, docs only). | Owner |
| D-14 | Unsectioned reader traversal | Documented rule: on readers, traverse inside `graphBoundary().read(...)`. | Default |
| D-15 | Environment prefix | `PERUNCS_`. | Default |
| D-16 | Optional values in public records | AGENTS rule 3: primitive + `-1`/`""` sentinel + `has…()` accessors. | AGENTS.md |
| D-17 | Pause/resume replication API | Deleted (`BackupNodeControl.stopReadingAtLatestMessage/resumeReading/isReading`); backup work owns its own boundary pause. | Default |
| D-18 | Standalone `status()` | Returns `NOT_CONFIGURED`, never `WrongRoleException`. | README contract |
| D-19 | Roles | Exported `enum NodeRole { STANDALONE, WRITER, READER, BACKUP_READER }`. `NodeStatus.writer` becomes `NodeStatus.role()`. | Default |
| D-20 | P1-4 | Resolved by J1-a (PR #832): an off-lock warm-up. | Upstream |
| D-21 | A8 | Discovery **already exists**. The remaining work is an epoch in the alias, verification against the mark, and README fixes. Needed only for A2b. | Code evidence |
| D-22 | S1 | Keep same-name identity checks: they prevent a conflicting retry from overwriting a durable archive. Retain off-lock digest inspection with a destination-stamp recheck to keep large-archive hashing outside the shared lock. | Code evidence; data-loss prevention |
| D-23 | Group commit | Out of scope. | Spec |
| D-24 | Apply thread | Platform daemon `peruncs-apply`, landing in the same change as N1. | Default |
| D-25 | Benchmark gate | Writer p99 commit ≤ baseline × 1.10; writer commits/s ≥ baseline × 0.95; reader apply p99 ≤ baseline × 1.10; live end-to-end p99 ≤ baseline. | Default |
| D-26 | External Archive mode (`ECLIPSE_DATAGRID_AERON_EXTERNAL_ARCHIVE`) | **Removed.** A1's durability premise holds only for the embedded Archive, whose `fileSyncLevel` PerunCS controls (§3.A1 S-0). This also deletes the control-session fallback in `awaitRecorded`. | Default |
| D-27 | Buffer pool size classes (N1) | Keep the existing power-of-two classes (`M/storage/binary/NativeBufferPool.java`). No new scheme. | Default |
| D-29 | Off-heap memory policy (the old AGENTS rule 31 is deleted) | **Limited FFM (`java.lang.foreign`) adoption.** Agrona stays at the Aeron boundary: envelope encode/decode and offers work zero-copy on Aeron's `DirectBuffer`s. Serializer `ByteBuffer`/`Binary` stays at the Store boundary. FFM is used for exactly two things: parsing untrusted replicated bytes (F1) and native memory PerunCS allocates and frees itself (F2). FFM is final since Java 22, so no preview flag is needed. Only `MemorySegment.ofBuffer`, `Arena` allocation and `ValueLayout` access are allowed: **no restricted methods** (`ofAddress`, `reinterpret`), so `--enable-native-access` is never required. The `--add-exports java.base/jdk.internal.misc` requirement comes from Serializer's `XMemory` and is out of PerunCS's reach; migrating `XMemory` to FFM is proposed upstream (§4.J1-b). | Owner + default |
| D-28 | Formatting | No repo-wide formatter run (there is no formatter plugin in `pom.xml`). Fix indentation only in touched files. | Default |

**Retired IDs** (kept so references resolve):
- **A10** → C7;
- **C11** → subsumed by A2;
- **C13** → merged into C1;
- **P1-2** → resolved by A1 + A2;
- **P1-3** → subsumed by A2;
- **P1-5** → resolved by A1 (reader durability gate deleted);
- **P1-9** → deleted by A1 (no reader cursor file);
- **D-12** → dropped (no provider SPI).

---

## 2. Linearized plan

| Step | Work | Needs | Done when |
|------|------|-------|-----------|
| 0 | **Baseline benchmark** on `9975d95` (harness in §3.A1 tests). | – | `bench/results/9975d95.json` committed. |
| 1 | Batch 1: **C1**, **C2**, **C3**, **C5**, **C6**, **C10**, **A4**. | 0 | `mvn verify` green; all §9 rows tagged step 1 pass. |
| 2 | **A1 spike** S-1…S-5 (S-0 is already answered). | 1 | Each spike criterion documented as pass/fail; D-08 or its fallback confirmed. |
| 3 | **A1** (+D-26, +C1 re-verify on the reworked publisher). | 2 | `mvn verify -Pintegration -Pcrashmatrix` green with the new oracle; §9 rows tagged step 3 pass. |
| 4 | **Benchmark compare** against step 0. | 3 | D-25 thresholds met. Otherwise stop and report to the owner. |
| 5 | **A2** (delete the NFS lease; `writer.lock`; bootstrap commit). | 3 | §9 A2 rows pass; README network section updated. |
| 6 | **P1-1 + F1** (per-commit type-id pre-filter on an FFM-based, bounds-checked entity-header scanner; temporary bound key). | 1 | §9 P1-1/F1 rows pass. |
| 7 | **P1-7 + N1 + F2** (apply thread; reader-owned pool, arena-backed if the F2 spike passes; receiver ownership API deleted). | 3, 6 | §9 P1-7/N1/F2 rows pass; soak green; F2 benchmark within D-25. |
| 8 | **C4** re-measure (relax the lock only on evidence). | 4, 5 | Read-latency metric recorded; decision noted. |
| 9 | **A3** (typed position; delete `replicationStreamName`, `ReplicationCursor`, `AeronReplicationCursor`) → **A9** (`ClusterStorage`) → **D1** (`NodeConfig`, `PERUNCS_` keys; temporary keys from steps 6–7 migrate). | 3 | §9 A9/D1 rows pass. |
| 10 | **C7**, **A5/S4**, **A6**, **A7**, **D9** (packages + `module-info` exports). | 9 | §9 C7 rows pass. |
| 11 | **J1-a + P1-4** (PR #832). | PR #832 in the eclipse-store snapshot | §9 J1-a rows pass. |
| 12 | **N3** (writer-driven retention; P1, may be pulled forward), cleanup: C8, C9, C12, S1, S3, S5, S6, D2–D8, D10, J1-b, J2–J6, §6 security, §7 docs. Optional: **A2b**, **A8**. | 10 | Remaining §9 rows pass. |

**Current gate status.** This table is the acceptance plan, not a completion ledger. The
2026-09-28 follow-up addresses the confirmed code-review defects in C1, C2, C3, C6, C10, A4,
F1, and N3. Verification is green: `mvn verify` passed 793 unit tests (1 skipped) and 13
integration tests; `mvn verify -Pintegration` passed 793 unit tests (1 skipped) and 10
integration tests; `mvn verify -Pcrashmatrix` passed 793 unit tests (1 skipped) and all 69 crash
and integration tests; and `mvn verify -Psoak` passed 793 unit tests (1 skipped) and the soak.
The final soak completed 326 transactions, 56,778 queries, 44,593 verified reads, zero torn
transactions, and successful reader/index convergence. Its 49-second JFR recording passed
`SoakJfrReportTest` with failure gating enabled: maximum GC pause 24 ms, with no GC pause over
100 ms, monitor blocking, or virtual-thread pinning. A separate 60-second soak repeat completed
945 transactions; its live `jcmd` sample found no deadlock. An Archive-resume regression test
now verifies that retention can purge a segment and the writer can publish again through a fresh
publication.

Step 0/4 benchmark evidence is still open. F1's JMH threshold and equivalence against the
upstream iterator over real Serializer output remain unclaimed, as do its fuzz cases. The N3
scheduler-specific multi-reader integration case remains open; current integration coverage
drives the retention controller directly. C5 deliberately keeps `LazyHolder` synchronization for
the in-flight-factory close race described above. C1 covers writer recovery and reader resolution
through sequence S+2. C2 exercises an active write during close,
timeout with the Store left open, and a successful retry through the facade; the full
`NodeLifecycle` close graph remains an integration gap. C3 covers bounded running-backup close,
and C10 covers submission rejection returning the executor to IDLE; starter-upload preservation
still needs an executable test. No step-0 baseline or step-4 performance comparison is available.

---

## 3. Specifications — step 1 and A1

### C1 — Clean pre-publication rejections must not latch the writer graph (P0)

**Problem (verified).**
- `M/node/store/GuardingStorageManager.java:430-456` (`persist`): the outer `validateState()` runs
  outside the try. Those rejections (closed, graph invalid, storage limit, reader role) are clean
  and correct.
- Inside the try, `delegate.store(...)` reaches
  `M/storage/aeron/writer/AeronStorageBinaryReplicationTarget.java:102-196`
  (`validateWriterState()` `:135`, `coordinator.prepare()` `:149`), then
  `M/storage/aeron/writer/AeronReplicationWriteCoordinator.java:423-438` (`ensureWriteAdmitted`)
  and `:496-539` (`lockWriteAdmission`).
- Every rejection there happens before any byte is published or written locally, yet `persist`
  latches it.

**Decision.** D-05.

**API/format changes.**
1. **New exported type.** `peruncs.cluster.errors.WriteRejectedException extends
   ReplicationException` (final), with constructors `(String)` and `(String, Throwable)`.
2. **Throw sites** (the throw site owns the classification; `persist` infers nothing):

   | Site | Today | Becomes |
   |------|-------|---------|
   | `ensureWriteAdmitted` | capacity: `ReplicationUnavailableException` | `WriteRejectedException` |
   | `ensureWriteAdmitted:432` | `maxTransactionBytes`: `IllegalArgumentException` | `WriteRejectedException` |
   | `lockWriteAdmission` | admission timeout / interrupt | `WriteRejectedException` (interrupt flag restored) |
   | `AeronStorageBinaryReplicationTarget.prepareWrite` | a `RuntimeException` from `validateWriterState()` | wrapped as the cause of a `WriteRejectedException` |
   | `AeronReplicationPublisher.prepareWithRecovery` (`M/storage/aeron/writer/AeronReplicationPublisher.java:324-357`) | prepare failure | A prepare failure whose ABORT was successfully **awaited recorded** becomes `WriteRejectedException` in coordinator and low-level paths. It carries the durable ABORT position and preserves the original cause. If ABORT cannot be offered or recorded, it keeps today's behaviour (fail closed, latch). |

3. **Classifier.** A package-private static `boolean isCleanRejection(Throwable)` in
   `GuardingStorageManager`:
   - walk `t, t.getCause(), …` (max depth 16, allocation-free cycle detection);
   - return **true iff** a `WriteRejectedException` is found and no `Error` or unrelated
     `ReplicationException` subtype appears. One `ReplicationUnavailableException` cause is allowed
     only below a `WriteRejectedException` carrying a recorded ABORT position (uncertain wins);
   - otherwise return **false** (latch: the fail-closed default).

   `persist`'s catch invalidates only when `!isCleanRejection(failure)`. The walk covers Eclipse
   Store's wrapping (e.g. `PersistenceExceptionTransfer`) automatically.
4. **Publisher return-to-ready** (the recorded-ABORT case), under the publisher monitor:
   - `pendingTransaction = null`, `reservedSequence = -1`;
   - `nextSequence = aborted + 1` (**sequences are consumed, never reused**);
   - `preparing = false`, `failed = false`;
   - the coordinator (`finishCommit`/`abort` paths) clears `activeWrite`, clears the buffer
     scratch, and restores `retryDictionary` from the aborted write.
   - **Checkpoint:** the existing `onAbort` callback writes `REJECTED` (valid pre-A1; deleted by A1).
5. **`WriterFencedException`** remains latching until A2 deletes it (§4.A2).
6. **Re-verification.** Step 3 (A1) reworks `AeronReplicationPublisher`; re-run the C1 tests there.

**Files.**
- new `M/errors/WriteRejectedException.java`
- `M/node/store/GuardingStorageManager.java`
- `M/storage/aeron/writer/AeronReplicationWriteCoordinator.java`
- `M/storage/aeron/writer/AeronStorageBinaryReplicationTarget.java`
- `M/storage/aeron/writer/AeronReplicationPublisher.java`
- `M/errors/package-info.java`

**Invariants.**
1. A clean rejection is only raised before `delegate.write` starts.
2. No string matching.
3. Sequences are monotonic, and ABORT consumes a sequence.

**Tests** (fault injection through the existing `CrashHook` / `ScopedValue` seams and the
coordinator's `LongPredicate writeAdmission`):
- capacity → clean, and the next write succeeds after capacity returns;
- oversize → clean;
- validator rejection → clean;
- admission timeout → clean;
- prepare back-pressure with recorded ABORT → clean, the next sequence is S+2, and readers resolve
  S+1 as aborted;
- `WriterFencedException` → latched;
- failure at `AFTER_PREPARE_BEFORE_LOCAL_WRITE` → latched;
- a synthetic chain `WriteRejectedException` caused by `CorruptReplicationDataException` →
  latched (precedence).

### C2 — Drain application sections before stopping replication (P1)

**Decision.** D-06. The counter is permanent: even after A1, the Store must not close under an
application section.

**API/format changes.**
- **Counter.** `AppSections` is a static nested class with an `int active` protected by its
  `ReentrantLock` and an `idle` condition. `enter()` checks `NodeClose.checkOpen()` under that
  lock before incrementing; `exit()` decrements in `finally` and signals when the count reaches
  zero. `awaitIdle(Duration)` waits to the caller's deadline. The lifecycle's admission state is
  the only permanent close gate; a second `closing` flag would duplicate it.
- **Single helper.** Every facade method classified **WRITE** or **READ** in C7 runs through
  `<R> R appSection(Supplier<R>)`. That includes `graphBoundary().read/write`, `persist`,
  `Database.getObject`, `exportAdjacencyData`, and (after C7) `exportTypes`, `issueFullBackup`,
  `exportChannels` and the PM READ methods.
- **Count only the outermost section:** `enter`/`exit` run only if
  `!graphCoordinator.isHeldByCurrentThread()` at entry.
- **The hook lives at the facade layer, not in `StorageGraphCoordinator`.** The replication merger
  enters the coordinator directly and is therefore never counted.
- **Admission.** `NodeClose.awaitAppIdle(Duration)` delegates the wait to the guarded manager.
  New outer entries check `NodeClose.checkOpen()` while holding the section-count lock, closing
  the gap between admission and increment. **Do not call `StorageGraphCoordinator.drain()` for
  this.** `drain()` takes the
  write lock and would contend with a live merger. The coordinator's own admission stays open
  until the replication-side drain at stage 10.
- **Close stages** (`M/node/NodeLifecycle.java:647-764`), in order:
  1. (implicit) the `closing` flag is set by `closeNode`;
  2. **`app drain`**: `appSections.awaitIdle(graphDrainTimeout)`;
  3. maintenance scheduler;
  4. backup executor;
  5. storage executor;
  6. replication transport;
  7. position provider;
  8. retention;
  9. managers and collaborators;
  10. `graphCoordinator.drain()` (replication side, unchanged);
  11. embedded storage.
- **Timeout.** `ECLIPSE_DATAGRID_GRAPH_DRAIN_TIMEOUT_MILLIS` (default 5,000 ms; renamed `PERUNCS_…`
  in D1). On timeout, the app-drain stage throws `GraphDrainTimeoutException`; close stages 3–5
  still stop maintenance and executors, while stages 6–11 remain not ready, keeping transport,
  managers, graph drain, and Store open. `close()` reports the failure; a later call retries.
- **The same behaviour holds pre-A1 and post-A1.** The close never proceeds past a live
  application section.

**Files.** `M/node/store/GuardingStorageManager.java`, `M/node/store/NodeClose.java`
(`void awaitAppIdle(Duration)`), `M/node/NodeLifecycle.java`.

**Invariants.**
1. No application section runs when the transport stops.
2. The merger never counts.
3. A close from inside a section is still rejected (`NodeLifecycle.java:600-607`).

**Tests.**
- Close from thread B while thread A is inside `graphBoundary().write` at the
  `AFTER_PREPARE_BEFORE_LOCAL_WRITE` hook → A completes, B's close returns after A, and restart is
  `LIVE` without reseed.
- A section held past the timeout → `GraphDrainTimeoutException`, the transport is still open, and
  a second close after release succeeds.
- An exception thrown inside a section still decrements (the counter returns to 0).

### C3 — Backup API returns a future (P1)

**Decision.** D-07.

**API/format changes.**
- **`api` types:**
  - `enum BackupSlot { SCHEDULED, MANUAL }`;
  - `record BackupInfo(UUID id, Instant createdAt, long sequence, boolean manual)`, where
    `sequence` is -1 when unknown (D-16).
- **`ClusterNode.createBackup(BackupSlot)`** returns `CompletableFuture<BackupInfo>` and replaces
  `createScheduledBackup`/`createManualBackup`.
  - **Busy:** a future completed exceptionally with `BackupBusyException` (never thrown).
  - **Wrong role:** a synchronously thrown `WrongRoleException` (a programming error).
- **`StorageBackupTaskExecutor.runBackup(BackupSlot)`** returns the future, and the task body
  completes it. The reader stop-at-latest runs **at task start**.
- **Starter backup.** `NodeLifecycle.startBackupNode` calls
  `createBackup(SCHEDULED).get(backupCloseTimeout)` before `backend.deleteUserUploadedStorage()`.
  Failure or timeout fails startup, and the upload is kept.
- **`StorageBackupManager.createStorageBackup`** returns `BackupInfo`.

**Files.**
- `M/api/ClusterNode.java`
- new `M/api/BackupSlot.java`, `M/api/BackupInfo.java`
- `M/node/backup/{BackupNodeManager,BackupNodeControl,StorageBackupTaskExecutor,StorageBackupManager}.java`
- `M/node/NodeLifecycle.java`

**Tests.**
- Success yields `BackupInfo`.
- A concurrent second request → `BackupBusyException` future.
- A failing backend → exceptional future.
- A failing starter backup → startup fails and the upload is still present.

### C5 — `LazyHolder.get()` synchronized (P2)

`LazyConstant` serializes value creation, but its thread safety does not coordinate the separate
`isInitialized()` probe used by close. Keep `get()` and `isInitialized()` synchronized so close
waits for an in-flight factory and then sees the created resource before deciding whether its
stage can be skipped. The volatile-only variant fails this close race. The class is deleted in
step 10 (A5).

### C6 — Reader-side registry mutators (P1)

**Decision.** In `BinaryPersistenceManagerAdapter` (`M/node/store/GuardingStorageManager.java:623-817`),
on read-only roles only:

| Method | Behaviour |
|--------|-----------|
| `ensureObjectId(Object)`, `ensureObjectId(U, requestor, handler)`, `ensureObjectIdGuaranteedRegister(...)` | Return `delegate.lookupObjectId(object)` if the object is registered; otherwise throw `ReaderWriteRejectedException`. |
| `mergeEntries`, `registerLocalRegistry`, `consolidate` | Throw `ReaderWriteRejectedException`. |
| `createRegisterer()`, `objectRegistry()` | Throw `ReaderWriteRejectedException` on readers. |
| Writer | All listed operations delegate unchanged, preserving Eclipse Store behavior. |

**Why internal loading is unaffected (verified):**
- Store loading never uses this adapter. `Lazy` binds to the `ObjectSwizzling` loader captured at
  load time (`$GITHUB_ROOT/eclipse-serializer/serializer/…/reference/Lazy.java:257-264`).
- GigaMap main sources do not call `persistenceManager()`, `ensureObjectId` or `createRegisterer`
  (grep of `$GITHUB_ROOT/eclipse-store/store/gigamap/*/src/main`).

**Tests.**
- Reader: each row's rejection; `ensureObjectId` of a loaded object returns its id.
- Regression: lazy traversal inside `graphBoundary().read` still works.

### C10 — Backup "running" state without wedging close (P2)

**Correction.** The rev-3 fix ("set the flag before `submit`, clear it in `finally`") is
**wrong**: a task cancelled while queued (`cancel(false)`) never runs its `finally`, so the flag
would stay set and the Store-close stage would never proceed. The existing comment at
`M/node/backup/StorageBackupTaskExecutor.java:154-157` exists for that reason. It also contradicts
its own code ("out of the body" versus the assignment inside the body, `:158`).

**Decision.** A three-state machine under the executor monitor:
`enum BackupPhase { IDLE, QUEUED, RUNNING }`, one field `phase`.
The monitor keeps the phase, queued task and result future as one transition; a second atomic state
would not remove the lock needed by close and cancellation.

| Transition | Where |
|------------|-------|
| `IDLE → QUEUED` | `runBackup`, under the monitor, immediately before `submit`. If `submit` throws, restore `IDLE` and complete the returned future exceptionally. |
| `QUEUED → RUNNING` | First statement of the task body, under the monitor. If `phase != QUEUED` (the close path already reset it), return without running. |
| `RUNNING → IDLE` | Task `finally`, under the monitor. |
| `QUEUED → IDLE` | `close()`, under the monitor: `backupTask.cancel(false)`, then `if (phase == QUEUED) phase = IDLE`. |

`isRunningBackup()` returns `phase != IDLE` (the busy check) and `isBackupExecuting()` returns
`phase == RUNNING`. The Store-close gate in `NodeLifecycle` uses `isBackupExecuting()`.

**Tests.**
- Cancel while queued → the phase is `IDLE`, the close completes, and the body never runs.
- Close while running → close waits (bounded) and the phase ends `IDLE`.
- A submit rejection → the returned future fails and the phase is `IDLE`.

### A4 — Export the index registration facade (P1)

**Decision.**
- **Surface.** Exactly three methods are exported. The other public methods of
  `ClusterStoreIndexes` (`withRegistrationRead`×2, `validate*`, `validateStorageRoots`,
  `writerValidator`, `registerVector`) remain internal.
- **Module requirements.** The GigaMap, Lucene, JVector, and Lucene Core modules become
  `requires transitive`, because their types appear in exported signatures.

**API/format changes.** `peruncs.cluster.api.ClusterIndexes` (final, private constructor):

```java
public static <E> LuceneContext<E> embeddedLuceneContext(DocumentPopulator<E> populator); // → ClusterStoreIndexes.embeddedLuceneContext (:76)
public static <E> LuceneIndex<E>   registerLucene(GigaMap<E> map, DocumentPopulator<E> populator); // → registerLucene (:92)
public static <E> VectorIndex<E>   addVector(/* exact parameter list of ClusterStoreIndexes.addVector, :151 */); // → addVector (:151)
```

`src/main/java/module-info.java`: change `requires org.eclipse.store.gigamap`,
`requires org.eclipse.store.gigamap.lucene`, `requires org.eclipes.store.gigamap.jvector`,
`requires jvector` and `requires org.apache.lucene.core` to `requires transitive`. Keep the
upstream misspelling `org.eclipes`.

**Tests.**
- `T/probe/ModulePathRuntimeProbeTest.java`: no `--add-exports`; its named consumer compiles with
  only `requires peruncs.cluster` while using Lucene/JVector types from the facade.
- `T/probe/ModulePathProbeMain.java`: use only `peruncs.cluster.api.ClusterIndexes`.
- `AeronStoreIntegrationIT.aeronReplicatesEmbeddedLuceneAndVectorStateToAReader`: register both
  indexes through the facade on a writer, replicate, and query on a reader.

---

### A1 — Replication mark inside the Store commit (P1; decided)

#### A1.1 Decision

D-03, D-08, D-26. The writer's local commit and its replication position become one atomic Store
transaction. Recovery decides COMMIT or ABORT deterministically.

#### A1.2 S-0: prepare durability (answered from the Aeron source)

`awaitRecorded` (`M/storage/aeron/writer/AeronArchiveReplicationPublisher.java:279-345`) returns on
three paths:
- **(a)** the local `RecordingPos` counter;
- **(b)** `getStopPosition` when the recording is inactive;
- **(c)** a control-session `getRecordingPosition` when the counter is not visible (the external
  Archive).

For **(a)**, Aeron's `RecordingWriter.onBlock` writes the block and, when `fileSyncLevel > 0`,
calls `recordingFileChannel.force(fileSyncLevel > 1)` **before returning**
(`$GITHUB_ROOT/aeron-io/aeron/aeron-archive/src/main/java/io/aeron/archive/RecordingWriter.java:132-141`,
`forceWrites = ctx.fileSyncLevel() > 0`, `:88`). `RecordingSession.record()` publishes the counter
with `position.setRelease(...)` only **after** `image.blockPoll(recordingWriter, …)` returns
(`RecordingSession.java:236-241`). Segment rollover forces the archive directory as well
(`RecordingWriter.java:221-223`).

So for the embedded Archive with `fileSyncLevel ≥ 1`, `RecordingPos ≥ X` means bytes up to `X` are
on disk. Path **(b)** reads the stop position of a recording whose writes completed through the
same writer. Path **(c)** depends on a foreign Archive's configuration, so D-26 removes the
external mode and path (c).

`fileSyncLevel ≥ 1` in production is **already enforced** (`M/node/aeron/AeronSettings.java:326-333`);
only a test is added.

#### A1.3 Mark schema and identity

```java
final class ReplicationMark {           // internal; package M/storage/aeron/mark until D9
    UUID clusterId; UUID storeGeneration;
    long epoch; long recordingId; long fencingToken;
    long sequence;              // last sequence whose data is committed in this Store; -1 = none
    long prepareStartPosition;  // recording position where prepare(sequence) begins; recording start position when sequence = -1
}
```

- **Registration call site.** `NodeLifecycle.prepareEmbeddedStorage`
  (`M/node/NodeLifecycle.java:455-483`) is the Store bootstrap shared by every replicated role.
  After `setConfiguration`, it calls
  `foundation.getConnectionFoundation().getRootResolverProvider().registerRoot("peruncs.replication", mark)`
  (`$GITHUB_ROOT/eclipse-serializer/…/PersistenceRootResolverProvider.java:92`). WRITER, READER
  and BACKUP_READER register it; STANDALONE does not (no mark exists there).
- **Identity.** There is exactly one instance per node, never replaced.
  - **Writer.** It mutates the fields in place under coordinator admission and stores the instance
    in every commit. One object id, a new version per commit; Store GC reclaims old versions.
  - **Reader.** Store loads the persisted state into the registered instance at start. Each import
    carries a new version of the same object id, and the existing materializer
    (`M/storage/binary/StorageBinaryDataMaterializer.java`) updates **the same instance** in place,
    like any replicated object. The reader never stores it.
- **Thread safety.**
  - Writer: written and serialized by the thread holding coordinator admission; recovery runs
    before admission opens.
  - Reader: written by the materializer under the coordinator write side; others read a volatile
    `ReplicationPosition` snapshot published after each barrier.
- **Reserved identifier.** If the application registered `"peruncs.replication"`, startup throws
  `NodeException`.
- **Facade.**
  - `viewRoots()` omits the entry (both `iterateEntries` and identifier lookups).
  - `Database.getObject(markOid)` is readable (documented read-only).
  - `issueFullBackup` includes the mark (intended).
- **Existing Store without the root:**

  | Case | Result |
  |------|--------|
  | Writer with an empty recording | Initialize `sequence=-1`, `prepareStartPosition=recordingStartPosition`, `fencingToken=0`, persisted by the bootstrap commit (A2) |
  | Writer with a non-empty recording | `ReseedRequiredException` |
  | Reader or backup-reader | `ReseedRequiredException` |

#### A1.4 Writer protocol

This runs inside `persist` (under `writeExclusive`) and coordinator admission.

1. **Reserve.**
   - `S' = coordinator.nextSequence`; it is `mark.sequence + 1` unless an ABORT consumed sequences.
   - `start = exclusivePublication.position()`. With one publisher on one thread, the next frame
     begins there.
   - Set `mark.sequence = S'`, `mark.prepareStartPosition = start`, `mark.fencingToken = current`.
2. **Serialize.** One storer: `store(application objects…)`, `store(mark)`, `commit()`. The target
   receives one `Binary` containing both.
3. **Mark check.** In the target, the binary must contain an entity with the mark's object id,
   checked with the entity-header scanner shared with P1-1. Otherwise throw
   `WriteRejectedException("replicated commit without replication mark")`.
4. **Prepare.**
   - Offer the dictionary and data chunks, then `awaitRecorded(lastChunkEndPosition)` (path (a)).
   - **Timeout or failure:** offer `ABORT(S')`, then throw `WriteRejectedException`. The sequence
     is consumed, and the in-memory mark fields are overwritten by the next reservation.
5. **Local write** (`delegate.write`).
   - **Clean pre-write rejection:** ABORT + `WriteRejectedException`.
   - **Anything else:** latch. Recovery resolves it at the next start.
6. **COMMIT.** Offer `COMMIT(S')`. **Do not await** its recording.
   - **Offer failure after bounded retry** (`offerTimeoutNanos`): throw `ReplicationPendingException`
     (new, exported). The graph is **not** latched, and the writer enters `REPLICATION_SUSPENDED`:
     admission throws `WriteRejectedException("replication suspended: COMMIT(S') pending")`.
   - A retry task re-offers every `offerTimeoutNanos` and reopens admission on success. A restart
     also resolves it (A1.5).

**Which facade methods carry the mark:**

| Method | Mark |
|--------|------|
| `store`, `storeAll(Object...)`, `storeAll(Iterable)`, PM `store`/`storeAll` | One storer with the mark. |
| `Storer.commit()` (application storers) | The mark is added to that storer just before `commit()`. |
| `storeRoot()` | A storer stores `rootReference`, `root` (if non-null) and the mark. This mirrors upstream `storeAll(rootReference, root)`, `$GITHUB_ROOT/eclipse-store/…/EmbeddedStorageManager.java:238-251`; S-3 verifies it. |
| `setRoot` | None (in memory only). |
| PM `updateMetadata`, PM `updateCurrentObjectId`, `persistenceManager().target().write` | **REJECT** (`UnsupportedOperationException`) when `role ∈ {WRITER, READER, BACKUP_READER}`. STANDALONE keeps today's behaviour. The raw target is reachable only through `persistenceManager().target()`. |

Store-internal work (housekeeping, GC, dictionary export) does not write through the persistence
target. Step 3 catches any missed path.

#### A1.5 Writer recovery (before admission opens)

- **Inputs:** mark `M`, `p = prepareStartPosition`, epoch, recording, token, and recording
  `stopPosition`.
- **Window:** `W = p + 2 × (maxTransactionBytes + ceil(maxTransactionBytes / chunkSize) × 84) + 4 × 84`.
- **Replay** `[p, stopPosition)` once, locally. Evaluate the rules **top-down; the first match
  wins**.

| # | Condition | Action |
|---|-----------|--------|
| 1 | `stopPosition < p` | RESEED ("mark ahead of Archive") |
| 2 | `stopPosition > W` | RESEED ("tail exceeds recovery window") |
| 3 | Any frame whose cluster, epoch or recording differs from the mark, or whose token is > the mark's token | RESEED |
| 4 | `M ≥ 0` and frames of M are not a **complete prepare** | RESEED (unreachable in a healthy run: the prepare is awaited before the local write) |
| 5 | Terminal of M is `ABORT` | RESEED (contradiction) |
| 6 | Frames of M+1 present **and** terminal of M absent | RESEED (impossible ordering) |
| 7 | `COMMIT(M+1)` present | RESEED (a local write must precede COMMIT) |
| 8 | Any frame with a sequence > M+1 | RESEED |
| 9 | Otherwise | (a) if `M ≥ 0` and the terminal of M is absent → append `COMMIT(M)`; (b) if frames of M+1 exist without a terminal → append `ABORT(M+1)`; (c) `nextSequence` = M+2 if M+1 appeared, else M+1 |

**Complete prepare(n)** means:
- zero or more `TYPE_DICTIONARY` chunks with contiguous `chunkIndex` 0..k-1 and consistent
  `chunkCount`;
- then `STORE_BINARY` chunks 0..c-1, contiguous, with consistent `chunkCount`;
- every chunk passes the header checksum and payload CRC;
- the summed payload equals the declared `payloadLength`;
- all of it lies at positions ≤ `stopPosition`.

**Appending:** through the existing `extendRecording` path
(`M/storage/aeron/writer/AeronArchiveReplicationPublisher.java:223, 479-486`), awaited recorded.

**Availability:** readers wait at the tail until the writer restarts. Today the same crash requires
a reseed.

#### A1.6 Reader protocol

- **Start:** `initialSequence = mark.sequence`, `initialPosition = mark.prepareStartPosition`,
  fencing floor = `mark.fencingToken`.
- **Duplicates.** The assembler **skips without validation** every frame with
  `sequence ≤ initialSequence`, including the re-delivered prepare chunks and the `COMMIT` of
  `mark.sequence`. It resumes normal processing at the first frame with
  `sequence > initialSequence`. Existing witness validation
  (`M/storage/aeron/reader/TransactionAssembler.java:377-402`) applies only after a locally
  resolved sequence.
- **Aborts.** As today, including ABORT after a partial prepare
  (`AeronReplicationPublisher.java:343-345`).
- **Live COMMIT.** Applied immediately: COMMIT(n) is offered only after prepare(n) is recorded and
  n is committed locally, and recovery (A1.5 rule 9a) completes such a sequence, never aborts it.
- **Delete (reader):**
  - `CommitDurabilityGate` and its call (`TransactionAssembler.java:239-261, 297-335`);
  - the recorded-position refresher (`M/storage/aeron/reader/AeronArchiveReader.java:332-345, 550-593`);
  - `ECLIPSE_DATAGRID_AERON_LIVE_WITHHOLD_TIMEOUT_NANOS`;
  - `ReaderDeliveryListener` and the `.reader-inflight` uncertainty record
    (`M/node/aeron/AeronReaderTransport.java:278-300`, `readerUncertaintyPath`,
    `rejectUncertainReaderImport`);
  - `DurableCursorFile`, the `offset` file, and `NodeLifecycle.requireStoredCursorForExistingStore`.
- **Epoch.** Readers keep rejecting frames of another epoch (rule unchanged in A1). Only A2b
  relaxes it, and that edit belongs to A2b.

#### A1.7 Invariants

1. Local Store commit only after its prepare is recorded (path (a)).
2. COMMIT(n) only after the local commit of n.
3. The mark and data are in one Store commit.
4. Sequences are strictly increasing; ABORT consumes one.
5. `fileSyncLevel ≥ 1` in production (existing check, now tested).
6. One transaction in flight (D-23).
7. Embedded Archive only (D-26).

#### A1.8 Files

**Add:**
- `M/storage/aeron/mark/ReplicationMark.java`
- `M/errors/ReplicationPendingException.java`

**Delete:**

| File | Why |
|------|-----|
| `M/storage/aeron/checkpoint/AeronReplicationCheckpoint.java` | Every writer state and the `READER_CURSOR` record type are replaced by the mark |
| `M/storage/aeron/checkpoint/AeronReplicationCheckpointStore.java` | |
| `M/storage/aeron/checkpoint/AeronCheckpointCodec.java` | |
| `M/node/replication/DurableCursorFile.java` | |
| `M/node/replication/ReplicationCursorStore.java` | |
| `M/node/replication/CommitAppliedListener.java` | |
| `M/storage/aeron/reader/ReaderDeliveryListener.java` | |

**Edit** (every current importer of the deleted types, verified by grep):

| File | Edit |
|------|------|
| `M/node/aeron/AeronWriterTransport.java` | Checkpoint reconciliation (`:628-730`) → A1.5 recovery |
| `M/node/aeron/AeronReaderTransport.java` | Resume from the mark; remove the uncertainty record |
| `M/node/aeron/AeronTransport.java` | Checkpoint-path wiring and metadata-storage probe for checkpoints |
| `M/node/NodeAssembly.java` | Remove the `DurableCursorFile` holder, the applied listener and the `offset` path |
| `M/node/NodeLifecycle.java` | Mark registration; remove the cursor gates |
| `M/node/replication/ClusterReplicationTransport.java` | Remove `CommitAppliedListener` / cursor parameters |
| `M/node/backup/BackupRestorePolicy.java` | Keep: identity/compatibility selection and "writer never restores over existing local state". Delete: `cursorManager`, `closeCursorManager`, `deleteOffsetFile`, and the local-cursor comparison (`:188-224`); a restore installs the Store, whose mark is its cursor |
| `M/node/backup/FilesystemVolumeBackupBackend.java` | Remove the `manifest` read/write (`ReplicationCursorStore.encode/decode`); `getCursorForBackup` reads the mark from the extracted Store's metadata instead (or is deleted if identity comes from the backup identity sidecar, S-1) |
| `M/node/backup/BackupArchive.java` | Remove `MANIFEST_ENTRY` requirements |
| `M/node/backup/BackupMetadata.java` | `requireConsistentWithCursor` deleted |
| `M/storage/aeron/checkpoint/AeronReaderWatermark.java` | Drop checkpoint-type references; keep the watermark format |
| `M/storage/aeron/reader/{AeronArchiveReader,TransactionAssembler}.java` | Per A1.6 |
| `M/storage/aeron/writer/{AeronArchiveReplicationPublisher,AeronReplicationWriteCoordinator,AeronReplicationPublisher,AeronStorageBinaryReplicationTarget}.java` | Per A1.4, D-26; remove `CheckpointWriter` and the PREPARING/COMMITTED/COMMITTING_UNCERTAIN/REJECTED notifications |
| `M/storage/package-info.java` | Update the reference to `AeronReplicationCheckpoint` |
| `M/node/aeron/AeronSettings.java` | Remove `EXTERNAL_ARCHIVE`, `CHECKPOINT_PATH`, `LIVE_WITHHOLD_TIMEOUT_NANOS` |

`AeronReplicationCursor` and `ReplicationCursor` stay until A3 (step 9).

**Docs:** README ("Store binary transport", crash-matrix oracle, seeding, remove checkpoint-path
settings) and the `module-info` "Durable cursors and checkpoints" / "Seeding and reseed" sections.

#### A1.9 Spike (step 2)

- **S-1 (named root on the reader path).**
  1. Writer Store W registers the root, stores the mark and one object.
  2. Copy W to reader Store R.
  3. R registers an instance and starts.
  4. W commits 100 transactions, each imported and materialized in R.
  5. Assert that R's fields equal W's and that the instance identity is unchanged.
  - **Fail** → fallback: an internal default root `ClusterRoot{Lazy<T> user; ReplicationMark mark}`,
    hidden by the facade (`root()` returns `user`).
- **S-2 (atomicity).** A forked child writes 4 MiB of objects plus the mark in one
  `Storer.commit()`, and is killed (`kill -9`) at a uniform 0–50 ms delay inside `commit()`, 200
  trials. After restart, assert (mark = n and all of n present) or (mark = n-1 and none of n).
- **S-3 (`storeRoot`).** The custom `storeRoot` produces the same persisted roots as upstream, on a
  fresh and on an existing Store.
- **S-4 (dictionary).** The mark type is merged into the reader dictionary before its first import.
- **S-5 (recovery).** For each A1.5 rule, synthesize the tail with the existing crash hooks and
  assert the action, with replay ≤ `W − p` bytes.

#### A1.10 Tests

- **Crash matrix:** new oracle `CONTINUE`, except rules 1–8 → `RESEED_REQUIRED`; checkpoint cells
  deleted; one cell per A1.5 rule.
- **Reader resume:** in-flight S+1 later committed / later aborted → correct Store; duplicates
  ≤ the mark are skipped.
- **`ReplicationPendingException`:** block COMMIT offers → exception, graph valid, suspended;
  release → resumes; restart instead → COMMIT appended.
- **`fileSyncLevel=0` in production** → startup fails (existing check, new test).
- **Benchmark harness** (steps 0 and 4):
  - **Setup:** Linux, local NVMe, `fileSyncLevel=1`, 1 writer + 3 readers on loopback, payloads of
    1 KiB and 64 KiB, 4 writer threads, 60 s warm-up, 5 × 60 s measured.
  - **Metrics:** writer commit p50/p99 and commits/s; reader apply p99 (import start → mark
    visible); live end-to-end p99 (`store()` return → reader mark ≥ n); and, for C4,
    `graphBoundary().read` p99 under write load.
  - **Tooling:** a JFR collector (existing `SoakJfrReport` pattern) writes
    `bench/results/<commit>.json`; `bench/compare` enforces D-25; profile `-Pbench`, not in the
    default gate.

---

## 4. Specifications — steps 5–11

### A2 — Delete the NFS fencing lease (P1)

**Decision.** D-04.

**Changes.**
- **`writer.lock`.** `<storagePath>/writer.lock`, opened (`CREATE, WRITE`, owner-only) and locked
  with `FileChannel.tryLock()` **before** Store or Aeron start.
  - `null` or `OverlappingFileLockException` → `NodeException("another writer process holds <path>")`,
    and nothing has started.
  - It is released after the Store close stage.
- **Bootstrap commit.** After A1 recovery and before admission:
  - one replicated transaction (sequence `nextSequence`) storing only the mark, with
    `fencingToken = previous + 1` (1 on a fresh cluster);
  - the **epoch is unchanged**. The epoch changes only with a new recording (a new cluster, or
    A2b);
  - readers raise their token floor when they apply it.
- **Admission after A2:** only publisher state (`isFailed`, `REPLICATION_SUSPENDED`), the Archive
  capacity predicate and the maintenance flag. Edit `ensureWriteAdmitted` (`M/storage/aeron/writer/AeronReplicationWriteCoordinator.java:424`,
  remove `leaseGate.isValid()`), `performCommit` (`:549-588`), `withWritesPaused` (`:460-494`),
  `EnvelopeFramer` (the lease-gated offer path), and `AeronOfferRetryer.offerGated`.
- **`WriterFencedException`:** deleted from `errors`. A reader seeing a lower token throws
  `CorruptReplicationDataException` (reader FAILED).
- **Delete:**
  - `M/node/aeron/WriterFencingLease.java`, `M/storage/aeron/writer/WriterLeaseGate.java`;
  - lease-directory validation (`M/node/aeron/AeronTransport.java:103-140`);
  - settings `…_LEASE_PATH`, `…_LEASE_STALENESS_MILLIS`, `…_LEASE_LOCK_TIMEOUT_MILLIS`,
    `…_SHARED_LEASE_FILESYSTEM`;
  - the NFSv4 check;
  - the README "Network boundary" NFS paragraphs.
- **Documented residual risk.** `writer.lock` protects one Store path on one host. Two hosts
  writing to the same cluster from different Store paths is an operator error that the library
  cannot detect beyond the reader token check.

**Tests.**
- A second writer on the same storage fails at startup, and the first is unaffected.
- The token equals N after N starts.
- A reader rejects a synthesized lower-token frame.

### A2b — Manual promotion without reseed (optional, P3; needs A8)

**Admin API:** `ClusterNode.promoteToWriter(long newEpoch)`, allowed only for READER when stopped
at the latest boundary.

**Procedure:**
1. Stop the old writer.
2. Wait until the chosen reader's mark equals the last COMMIT of the old recording.
3. Restart it as WRITER with `epoch = old + 1`: it creates a new recording (alias per A8), starts
   at `mark.sequence + 1`, and runs the bootstrap commit.
4. Redirecting application writes is a deployment concern.
5. Other readers follow when `reader.mark.epoch == old` and
   `reader.mark.sequence == newWriter.startSequence − 1`; otherwise reseed.

**This item owns the edit** that relaxes the reader's epoch check for exactly that condition.

### P1-1 — Per-commit index validation pre-filter (P1)

**Problem (cited).**
- `ClusterIndexValidation.countScanWork` throws beyond `DEFAULT_MAX_VALIDATED_OBJECTS = 65_536`.
- The writer validator hard-codes that default (`M/storage/index/ClusterStoreIndexes.java:297-307`),
  and the error names the merger's setting, which the writer does not read.
- Opaque stateful `java.*` holders also throw (`M/storage/index/ClusterIndexValidation.java:446-456`).

**Changes.**
- **Shared scanner (built per F1).** Extract the bounds-checked entity-header scanner from
  `M/storage/index/ClusterIndexMaintenance.java:34-70` into `M/storage/index/EntityHeaders.java`,
  usable for a writer-held `Binary` (iterate `binary.iterateChannelChunks` → buffers) and for
  imported buffers. The class and cross-package `validateFraming` hook are public for use by
  `storage.binary.StorageBinaryDataMaterializer`; the visitor and parser details remain
  package-private. It uses Serializer's entity header layout (length, typeId, objectId), as today.
- **Pre-filter.** `ClusterIndexValidation.commitTouchesIndexes(Binary, PersistenceTypeHandlerManager)`:
  for each entity type id → handler → `type()` → the existing `INDEX_RELEVANT` `ClassValue`.
- **Gate.** `AeronStorageBinaryReplicationTarget.prepareWrite` runs the full validation only when
  the pre-filter returns true. A rejection is a `WriteRejectedException` (C1).
- **Temporary key.** `ECLIPSE_DATAGRID_INDEX_VALIDATION_MAX_OBJECTS` (default 65,536), read through
  the existing `NodeSettingsSource.replicationProperty` by both the writer and the merger. The
  error message names it. Step 9 renames it `PERUNCS_INDEX_VALIDATION_MAX_OBJECTS`.
- **Fallback if the proof test fails:** keep the per-commit full scan, with the configurable bound
  and the corrected message.

**Tests.**
- **Proof:** register an external-directory Lucene index directly on an already-stored GigaMap,
  then `storage.store(map)` (the commit carrying the new index group) → `WriteRejectedException`
  caused by `IllegalArgumentException(EXTERNAL_LUCENE_MESSAGE)`; the graph stays valid; the
  application must remove the registration (documented).
- 10k plain entities → the full scan is not invoked (spy).
- Over the bound → a clean rejection naming the key.

### F1 — Bounds-checked parsing of replicated bytes with FFM (P1; step 6, with P1-1)

**Problem.** Replicated Store binaries can be malformed by corruption or software bugs. The
cluster requirement is no node authentication and no transport encryption; framing validation
handles malformed bytes and does not authenticate or authorize publishers. Binaries were walked
with raw native address arithmetic through Serializer helpers:
- the entity-header walk in `M/storage/index/ClusterIndexMaintenance.java:34-70`, which fails on
  a "truncated imported entity header";
- the materializer's `BinaryEntityRawDataIterator.iterateEntityRawData(address, address + limit, …)`
  over `getDirectByteBufferAddress(buffer)` in `M/storage/binary/StorageBinaryDataMaterializer.java`.

A corrupt entity length can drive these reads out of bounds, and such a native read is unchecked:
it yields a crash or garbage, not an exception.

**Decision.** D-29.
- **PerunCS-owned parsing** of replicated bytes reads through a `MemorySegment` view of the same
  direct buffer. Every read is bounds-checked, and a violation surfaces as
  `IndexOutOfBoundsException`, which is mapped to `CorruptReplicationDataException`.
- **No copy, no restricted method.**
- **Serializer's own `BinaryEntityRawDataIterator`,** used inside the materializer, is upstream
  code and stays as is. PerunCS validates the entity framing with F1 *before* handing the buffer to
  it, so the iterator never sees a length that runs past the buffer.

**API/format changes.**
- **New scanner.** `M/storage/index/EntityHeaders.java` exposes the cross-package framing hook to
  `storage.binary.StorageBinaryDataMaterializer`; only the class and `validateFraming` are public,
  while visitor and implementation details remain package-private:
  ```java
  // Serializer writes entity headers in native order through XMemory.
  private static final ValueLayout.OfLong LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.nativeOrder());
  static void forEach(ByteBuffer directBuffer, EntityVisitor visitor);   // MemorySegment.ofBuffer(directBuffer); loop: length = seg.get(LONG, off) …
  static void forEach(Binary binary, EntityVisitor visitor);             // iterateChannelChunks → forEach(buffer)
  public static void validateFraming(ByteBuffer directBuffer);           // every entity: length ≥ header size, off + length ≤ segment size
  @FunctionalInterface interface EntityVisitor { void entity(long typeId, long objectId, long offset, long length); }
  ```
- **Format:** the scanner uses Serializer's native-order entity header layout, including 8-byte
  length, type-id and object-id fields. The old PerunCS byte walk has been removed; equivalence with
  `BinaryEntityRawDataIterator` over real Serializer output remains an explicit open gate.
- **Users:**
  - P1-1 `commitTouchesIndexes` (writer `Binary`);
  - A1.4 step 3 mark check (writer `Binary`);
  - reader index maintenance (replaces the raw walk in `ClusterIndexMaintenance`);
  - `StorageBinaryDataMaterializer`, which calls `validateFraming` on every buffer before the
    Serializer iterator.
- **Failure mapping:** an `IndexOutOfBoundsException` or a framing violation →
  `CorruptReplicationDataException("replicated entity framing is invalid at offset …")`, which fails
  the reader closed (existing semantics for corrupt data).

**Files.** New `M/storage/index/EntityHeaders.java`, `M/storage/index/ClusterIndexMaintenance.java`,
`M/storage/binary/StorageBinaryDataMaterializer.java`, and the P1-1/A1 call sites.

**Invariants.**
1. No PerunCS code reads replicated bytes by raw address.
2. No restricted FFM method is used.
3. There is no heap copy of payload bytes.

**Tests.**
- **Fuzz:** truncated buffers, a length past the end, a zero or negative length, and a header
  split across the end, each → `CorruptReplicationDataException`, never a JVM crash. 10k seeded
  random corrupt length fields are rejected by both framing validation and header scanning.
- **Equivalence:** for 10k valid binaries, `EntityHeaders.forEach` yields exactly the entities of
  the current parser (run both during migration).
- **Micro-benchmark** (JMH, `-Pbench`): compare FFM scans of 64 KiB and 1 MiB Serializer binaries
  against the upstream raw iterator. The 5% gate remains open and is not claimed complete.

**Out of scope.** Aeron envelope decode (`AeronReplicationEnvelope`), which stays on Agrona
`DirectBuffer` (D-29): Aeron hands us its term buffer as a `DirectBuffer`, and its reads are already
bounds-checked by Agrona when `agrona.disable.bounds.checks` is not set. Keep that property unset.

### P1-7 + N1 — One apply thread, reader-owned buffer pool (P1)

**Decision.** D-24, D-27.

**Changes.**
- **`Barrier`.** `record Barrier(ByteBuffer[] buffers, int bufferCount, long bytes, long firstSequence,
  long lastSequence, long lastPosition, String dictionary /* "" = none */, boolean hasData,
  long[] generations /* checked mode only, else empty */)`.
- **Producer side (Aeron poll thread, the sole producer).**
  - The fragment handler keeps returning `Action.BREAK` when the assembler's barrier is full
    (existing).
  - After each `controlledPoll`, the poll thread `offer`s the completed barrier to an Agrona
    `OneToOneConcurrentArrayQueue<Barrier>` (capacity 64). SPSC is correct because the poll thread
    is the only producer and the apply thread the only consumer.
  - There is an additional byte bound: `queuedBytes` (`AtomicLong`) ≤ merger `cachedBytesLimit`
    (existing `ECLIPSE_DATAGRID_DATA_MERGER_LIMIT`, default 64 MiB).
- **Queue full.** The poll thread keeps the barrier (it still owns the buffers) and **does not call
  `controlledPoll`** until the offer succeeds (duty-cycle idle). The subscription then stops
  consuming: Aeron flow control back-pressures the live image, and replay pauses. No drop, no
  failure, no blocking inside a fragment callback.
- **Consumer (apply thread `peruncs-apply`), per barrier:**
  1. dictionary merge;
  2. import (coordinator write side);
  3. materialize;
  4. retire Lucene views / JVector graphs (J1-a);
  5. release the write side;
  6. vector warm-up under a read section (after J1-a);
  7. publish `ReplicationPosition`;
  8. release buffers to the pool;
  9. `queuedBytes -= bytes`.

  It stamps `volatile Phase phase` (`IDLE, IMPORT, MATERIALIZE, INDEX_RETIRE, WARMUP`) and
  `volatile long phaseStartedNanos`.
- **Budget.** The poll thread checks, once per duty cycle, that
  `phase ∈ {IMPORT, MATERIALIZE, INDEX_RETIRE}` has not exceeded `materializationBudgetMs`, and
  `WARMUP` not the refresh budget. On expiry it latches the reader failure
  `ReplicationUnavailableException("apply phase <phase> exceeded <n> ms")`, and the node reports
  FAILED. The apply thread is not interrupted (the same as today's watchdog).
- **Synchronous "wait until applied"** (used by backup stop-at-latest):
  `ReaderComponent.stopAtLatest()` completes when the poll thread has reached the live tail and
  stopped polling, **and** the queue is empty, **and** `phase == IDLE`. `stopResult()` then
  reports `RESOLVED_BOUNDARY` with the last published `ReplicationPosition`.
- **Close:** stop polling → the apply thread drains the queue within `graphDrainTimeout` → the
  remaining barriers' buffers are released → join.
- **Pool (N1).**
  - The reader component owns one `NativeBufferPool` and passes it by constructor to the assembler
    and the apply thread.
  - Make it mandatory: delete every `pool == null` branch in `M/storage/binary/StorageBinaryDataImporter.java`.
  - The retention cap becomes a temporary key `ECLIPSE_DATAGRID_POOL_MAX_RETAINED_BYTES` (default
    32 MiB; D1 later).
  - Remove the `// ponytail:` comment.
  - Pool storage is arena-backed if F2 passes (below); otherwise it keeps `XMemory`
    allocate/free.
- **Checked mode** (system property `peruncs.pool.checked=true`, set by surefire/failsafe; off in
  production):
  - an `IdentityHashMap<ByteBuffer, Long>` of generations guarded by the pool's lock;
  - `release` increments the generation and poisons (capacity ≤ 4 KiB → all bytes `0xDE`; else
    the first and last 64 bytes);
  - the apply thread compares the barrier's recorded generations at import entry and at
    materialize entry;
  - a mismatch → `CorruptReplicationDataException("native buffer reused while owned")` → the
    reader latches.
- **Delete:**
  - `M/storage/binary/{ApplyQueue,ApplyWorker,MergerLifecycle}.java`;
  - the watchdog executor and the coalescing timer in `StorageBinaryDataMerger`;
  - `StorageBinaryDataReceiver.{canReceiveDataOwned,receiveDataOwned,awaitApplied,allocateNativeBuffer,releaseNativeBuffer}`
    and the `ReceiverAdapter` overrides (`M/node/aeron/AeronReaderTransport.java:389-414`).

**Tests.**
- **Stress:** max retained = one buffer, checked mode, 10k transactions of random 1 B–1 MiB. A
  writer-side test CRC32C per entity payload is compared with the reader's materialized objects →
  zero mismatches and no corruption exception.
- **Budget:** a blocking Store stub → FAILED after the budget, and the poll loop stays alive.
- **Back-pressure:** a slow apply stub → the queue fills, polling stops, no loss, then catch-up.
- **Backup:** stop-at-latest waits for the queue to drain.

### F2 — Arena-backed native memory for PerunCS-owned buffers (P2; spike inside step 7)

**Scope.** Only native memory PerunCS allocates and frees itself:
- the assembler's transaction buffers (`M/storage/aeron/reader/TransactionAssembler.java:833-857`);
- `NativeBufferPool`;
- the merger/importer copies (`M/storage/binary/StorageBinaryDataImporter.java`);
- the writer staging buffer (`M/storage/aeron/writer/AeronReplicationPublisher.java:153`,
  `framingStorage`).

Today all of them use `XMemory.allocateDirectNative` / `XMemory.deallocateDirectByteBuffer`
(Serializer, Unsafe-backed) or `ByteBuffer.allocateDirect`.

**Decision.** D-29. Spike first; adopt only if both gates pass.

**Design.**
- **Arena.** One `Arena.ofShared()` per reader component (and one per writer publisher), created at
  component start and closed at component close. A shared arena is required because buffers cross
  from the poll thread to the apply thread (P1-7).
- **Pool on top of the arena.** Size-class free lists of `MemorySegment`s carved from the arena.
  Keep the D-27 power-of-two classes.
  - Segments are allocated with `arena.allocate(size, 64)`.
  - They are handed out as `segment.asByteBuffer()` (a direct buffer view, accepted by Aeron's
    `UnsafeBuffer.wrap` and by Store `importData`).
  - They are returned to the free list, never freed individually.
  - The arena close at component close releases everything deterministically.
- **No per-transaction arenas.** Closing a shared arena makes the JVM synchronize with every
  thread, which is too expensive per barrier. Confined arenas cannot cross the poll → apply
  hand-off.
- **Temporal safety is not gained inside the pool** (memory is recycled), so the N1 checked mode
  (generation plus poisoning) stays. Accessing a buffer view after its arena closed does throw,
  which covers close-time bugs.
- **Replace** `XMemory.allocateDirectNative`/`deallocateDirectByteBuffer` in the four sites above.
  PerunCS must **never** call `XMemory.deallocateDirectByteBuffer` on an arena-backed buffer.
  Enforce this with one allocation facade, `NativeMemory` (package-private), as the only
  allocator/releaser in PerunCS.
- **Writer staging buffer.** The writer's single staging buffer becomes a segment from the
  publisher's arena. If P1-6 (gathering offer) lands first, the staging buffer disappears and this
  item shrinks.

**Spike gates (both required).**
1. **Store never frees our buffers.** Verify in `$GITHUB_ROOT/eclipse-store` and
   `$GITHUB_ROOT/eclipse-serializer` that the `importData(XGettingEnum<ByteBuffer>)` path and the
   `ChunksWrapper`/`Binary` paths used by PerunCS never call
   `XMemory.deallocateDirectByteBuffer` (or a buffer cleaner) on the caller's buffers. Add a test:
   import arena-backed buffers, close the Store, then close the arena, with no crash and no
   double-free.
2. **No throughput loss.** The benchmark (§3.A1 harness, reader apply p99, plus a JMH
   allocate/release loop) is within D-25 of the `XMemory` variant.

**Fail** → keep `XMemory` allocation behind the same `NativeMemory` facade, so a later switch
touches one class.

**Files.** New `M/storage/binary/NativeMemory.java`, `NativeBufferPool.java`,
`StorageBinaryDataImporter.java`, `M/storage/aeron/reader/TransactionAssembler.java`,
`M/storage/aeron/writer/AeronReplicationPublisher.java`.

**Invariants.**
1. All PerunCS native allocations go through `NativeMemory`.
2. No individual free of arena memory.
3. The arena is closed exactly once, at component close, after the apply thread has joined.

**Tests.**
- Spike gate 1.
- Leak test: native memory tracking (`-XX:NativeMemoryTracking=summary` in a forked test) returns
  to baseline after node close.
- The N1 stress test passes on the arena-backed pool.

**Out of scope.**
- Aeron's own term buffers (owned by Aeron).
- Store-internal buffers.
- The small metadata codecs (cursor, checkpoint, lease, watermark): not hot, and mostly deleted by
  A1/A2.

### C4 — Writer graph write-lock hold (P1; step 8)

**Decision.** Keep `writeExclusive` for writer `persist`. After A1 + A2 the hold is: serialize +
prepare + prepare recorded-wait + local fsync + COMMIT offer. Measure `graphBoundary().read` p99
under write load (§3.A1 harness).

Only if it exceeds 2 × the idle read p99: release the lock after `delegate.write` and before the
COMMIT offer (the offer needs only coordinator admission).

### J1-a — Adopt `VectorIndex.invalidateGraph()` (eclipse-store PR #832) (P1; with P1-4)

**Upstream.** PR #832 (open) adds `VectorIndex.invalidateGraph()`:
- it takes `builderLock.writeLock()` and no parent-map monitor;
- it discards `deferredBuilderOps`;
- it closes and clears the builder and graph;
- it clears `graphRebuilt` last, so the next search/mutation/`optimize` rebuilds lazily;
- **it throws `IllegalStateException` in incremental on-disk mode.**

It confirms that Lucene needs nothing new, because `LuceneIndex.close()` is public and reusable.
PerunCS already uses it (`M/storage/index/ClusterIndexMaintenance.java:429-455`).

**Changes.**
1. **Retire.** Replace the field surgery in `ClusterIndexMaintenance` (`:345-375`) with
   `index.invalidateGraph()`, called inside the materialization write section.
2. **Delete** `StoreIndexReflection.VectorGraphFields` / `vectorGraphFields` and the
   `write`/`writeBoolean`/`readBoolean` helpers. After this, no write into upstream private state
   remains.
3. **P1-4.** Replace `ensureVectorSearchGraphs` (`ClusterIndexMaintenance.java:392-419`: an eager
   probe search inside the write section, then a reflective `graphRebuilt` check) with the apply
   thread's `WARMUP` step: a top-1 probe search per dirty index, **after** releasing the write
   side, inside `graphCoordinator.read(...)`.
   - This is safe because upstream's lazy rebuild is designed for concurrent search: #832
     serializes it on `builderLock`.
   - Queries arriving before the warm-up rebuild lazily themselves.
   - Delete the reflective guard check (it is now upstream's tested contract,
     `VectorIndexInvalidateGraphTest`).
4. **On-disk mode.** `ClusterIndexValidation` already rejects on-disk/external vector indexes, so
   `invalidateGraph()`'s `IllegalStateException` is unreachable. If it is ever thrown, latch the
   reader (`CorruptReplicationDataException`).

**Interim (before #832 merges).** Route the current field writes through one method,
`UpstreamIndexAccess.invalidateVectorGraph(VectorIndex<?>)` (new, `M/storage/index/`), so the
switch is a one-line change.

**Tests.**
- `T/node/aeron/ReaderLiveIndexFreshnessTest` and the soak JVector checks still pass.
- The write-section duration does not scale with vector count (1k vs 100k, bounded ratio).
- An on-disk vector index is rejected at validation.

### J1-b — Remaining read-only reflection: upstream follow-ups (P2; step 12)

| Read | Where | Proposed upstream method (small PR in #832's style) |
|------|-------|-----------------------------------------------------|
| `GigaIndices.Default.indexGroups` | `ClusterIndexValidation.collectIndexGroups` (`:210-235`) | `GigaIndices.iterateGroups(Consumer<? super IndexGroup<E>>)` |
| `LuceneIndex.Default.context` | `ClusterIndexValidation:365` via `StoreIndexReflection.luceneContextField` | `LuceneIndex.context()` |
| (upstream, not a PerunCS read) Serializer `XMemory` built on `jdk.internal.misc.Unsafe` | Every consumer JVM needs `--add-exports java.base/jdk.internal.misc` | Propose moving `XMemory` off-heap access to FFM upstream. That would remove the flag for all Eclipse Store users (D-29). |
| The reachable-fields walk | `ClusterIndexValidation.java:440-465` | None. It is inherent to the check, rare after P1-1, and uses Serializer's public `XMemory` field reads on heap objects. These are not off-heap access, so D-29 does not apply. |

---

## 5. Specifications — steps 9–10

### A9 — Eclipse Store API compatibility (P1)

**Decision.** D-02, D-10, D-11, D-14. An Eclipse Store application changes only how it obtains the
manager.

**Porting map** (README):

| Plain Eclipse Store | PerunCS Cluster |
|---------------------|-----------------|
| `EmbeddedStorage.start(root, path)` | `ClusterStorage.start(() -> root)` (config from the environment) or `ClusterStorage.start(() -> root, NodeConfig)` |
| `EmbeddedStorage.Foundation(path)…start()` | `ClusterStorage.Foundation()…start()` |
| `EmbeddedStorageFoundation` tuning | `setEmbeddedStorageFoundation(f)`: preserved, except the live file provider (from `NodeConfig.storage().root()`) |
| `root()`, `setRoot`, `storeRoot`, `store`, `createStorer`, `shutdown`, try-with-resources | Unchanged API, with the deviations listed below |

**API** (`peruncs.cluster.api`):

```java
public final class ClusterStorage {                       // mirrors EmbeddedStorage; private constructor (rule 18)
    public static <T> ClusterStorageManager<T> start(Supplier<? extends T> rootSupplier);
    public static <T> ClusterStorageManager<T> start(Supplier<? extends T> rootSupplier, NodeConfig config);
    public static <T> ClusterStorageFoundation<T> Foundation();
}
public final class ClusterStorageFoundation<T> {          // mirrors EmbeddedStorageFoundation naming; immutable result, fluent setters
    public ClusterStorageFoundation<T> setRootSupplier(Supplier<? extends T> supplier);          // required
    public ClusterStorageFoundation<T> setEmbeddedStorageFoundation(EmbeddedStorageFoundation<?> f);
    public ClusterStorageFoundation<T> setNodeConfig(NodeConfig config);                         // default NodeConfig.fromEnvironment(), resolved at start
    public ClusterStorageManager<T> start();       // starts Store, Aeron runtime, replication, maintenance; manager.shutdown() closes all
    public ClusterNode<T> startNode();             // same start; returns the control handle (status, backups); node.storageManager()
}
```

**Root contract.** The supplier may return `T`. `NodeLifecycle.initializeRoot` stores it wrapped in
`Lazy.Reference` (`M/node/NodeLifecycle.java:510`), and `root()` returns `Lazy<T>`. A supplier
returning a `Lazy` is used as is.

**Coordination (D-11).** `GraphBoundary` is the only coordination API. The internal
`M/storage/binary/ObjectGraphUpdateHandler.java` is folded into `StorageGraphCoordinator` (its only
implementation is `coordinator::write`).

**Deviations from Eclipse Store** (a method-level list in the `ClusterStorageManager` Javadoc and
README):
- `importData`/`importFiles` → `UnsupportedOperationException` (all roles);
- `persistenceManager().target().write`, `updateMetadata`, `updateCurrentObjectId` →
  `UnsupportedOperationException` (replicated roles);
- on readers: every C7 WRITE-class method and the C6 mutators → `ReaderWriteRejectedException`;
- on readers: traversal must run inside `graphBoundary().read(...)` (D-14), because an unsectioned
  traversal may observe a half-applied batch;
- `viewRoots()` omits `"peruncs.replication"`;
- `setRoot` requires a `Lazy`;
- `shutdown()` closes the whole node.

**Deletions.** `ClusterNode.open(NodeOptions)`, `M/api/NodeOptions.java`,
`NodeSettingsSource.env(...)` (replaced in D1).

**Files.** New `M/api/{ClusterStorage,ClusterStorageFoundation}.java`, `M/api/ClusterNode.java`,
`M/node/{NodeAssembly,NodeLifecycle}.java`, `src/main/java/module-info.java`.

**Tests.**
- The porting snippets compile and run.
- Per role, `ClusterStorage.start` reaches `status().ready()`.
- A two-field invariant written in one transaction is never observed half-applied inside
  `graphBoundary().read` on a reader (stress).

### D1 — `NodeConfig`: pluggable configuration (P1; after A3/A9)

**Decision.** Delete `NodeSettingsSource`/`EnvironmentNodeSettingsSource` and all legacy keys. Any
configuration system plugs in through a map or the builder.

```java
public record NodeConfig(NodeRole role, StorageConfig storage, BackupConfig backup,
                         Timeouts timeouts, Limits limits, AeronConfig aeron) {
    public static NodeConfig fromEnvironment();                 // PERUNCS_* process environment
    public static NodeConfig fromMap(Map<String,String> values); // any source: MicroProfile Config, Spring Environment, properties files, …
    public static Builder builder();                            // programmatic
    public record StorageConfig(Path root, long limitBytes, Duration limitCheckInterval, Duration gcInterval) {}
    public record BackupConfig(Path volume, int kept, Duration interval, Duration closeTimeout) {}
    public record Timeouts(Duration graphDrain, Duration applyBudget, Duration refreshBudget, Duration offer,
                           Duration recordedPosition, Duration readerStop, Duration reconnect, Duration archiveControl) {}
    public record Limits(int maxValidatedIndexObjects, long bufferPoolRetainedBytes, long applyQueueBytes,
                         int maxTransactionBytes, int chunkSize) {}
    public record AeronConfig(/* the existing AeronSettings records: Topology, Channels, ArchivePolicy, Directories, StorageIdentity */) {}
}
```

- All keys live in one `enum Setting { key, default, parser }`, and the README table is generated
  from it.
- The temporary keys from P1-1 and P1-7 are renamed.
- **Dependency:** A3 first (it removes `replicationStreamName`, which is threaded through
  `M/node/NodeAssembly.java:394,521,585` and `M/node/NodeLifecycle.java:364`).

### Integration guidance (docs only; D-13)

README section "Using PerunCS inside a container". It is framework-neutral; Helidon, Spring and
others are examples.

1. **Lifecycle.** Start with `ClusterStorage.Foundation()…startNode()` on container start. Close
   `ClusterNode` (idempotent, equivalent to `storageManager().shutdown()`) in the container's
   disposal hook, not a JVM shutdown hook.
2. **Expose** `ClusterStorageManager<T>` as the application's `StorageManager`.
3. **Configure** with `NodeConfig.fromMap(<your framework's config as a map>)` or the builder.
4. **Health**, a recommended mapping:
   - readiness = `status.ready()`;
   - liveness is down on `FAILED` or `RESEED_REQUIRED`;
   - `DEGRADED` (replication) or a failed last backup (C9) are reported but stay live;
   - include `status.role()`.

### C7 — Gate classification of every delegated method (P1; step 10)

**Classes:**
- **WRITE:** `persist`, i.e. admission + exclusive + latch-on-uncertain;
- **READ:** a coordinator read section inside `appSection`;
- **ADMIN:** explicitly ungated;
- **HANDLE:** returns a handle whose use is classified separately;
- **LIFECYCLE;**
- **REJECT.**

| Method(s) | Class | Note |
|-----------|-------|------|
| `store`, `storeAll`×2, `storeRoot`, `setRoot`, `Storer.commit`, PM `store`/`storeAll` | WRITE | Readers: REJECT |
| PM `updateMetadata`, `updateCurrentObjectId`, `GatedPersistenceTarget.write` | REJECT (replicated), WRITE (standalone) | A1.4 |
| `Storer.store`/`storeAll` (buffering), `Storer.clear`/`skip*`/capacity methods | ADMIN | Buffer only; the commit is WRITE |
| `exportAdjacencyData`, `exportTypes`, `issueFullBackup`, `exportChannels`, `Database.getObject`, PM `lookupObject`/`getObject`/`get`/`collect`×2/`createLoader`/`typeDictionary`, `typeDictionary()`, `viewRoots()` | READ | |
| `root()`, `persistenceManager()`, `database()`, `createStorer`/`createLazyStorer`/`createEagerStorer`, PM `createStorer`×4, `createConnection()`, PM `source()`, PM `target()` | HANDLE | Validity check only; each returned object's methods are classified in their own rows. `createConnection()` returns the facade itself. |
| PM `ensureObjectId*`, `mergeEntries`, `registerLocalRegistry`, `consolidate`, `createRegisterer` | ADMIN (writer, unchanged per C6) / REJECT (readers) | Direct writer delegation preserves Store behavior; these calls do not enter cluster persistence gating. |
| PM `objectRegistry()` | Writer delegates unchanged / REJECT (readers, per C6) | Preserves Eclipse Store's writer behavior. |
| `issueGarbageCollection`, `issueCacheCheck`, `issueFileCheck`, `issueIntegrityCheck`, `issueTransactionsLogCleanup`, `issueStorageFlush`, `createStorageStatistics`, `configuration`, `initializationTime`, `operationModeTime`, `isRunning`/`isActive`/`isAcceptingTasks`/`isShuttingDown`/`isStartingUp`, `checkAcceptingTasks`, PM `getTargetByteOrder`/`currentObjectId`/`objectRegistryMonitor` | ADMIN | Store-internal housekeeping or read-only probes |
| `accessUsageMarks`, `markUsedFor`, `unmarkUsedFor`, `markUnused`, `isUsed` | ADMIN | These mutate only the in-memory *usage-mark* set Eclipse Store uses to decide whether a manager is still in use. The writer's `objectRegistry()` preserves Store behavior; readers reject it. |
| `start()`, `shutdown()`, PM `close()` | LIFECYCLE | `start` = idempotent admission check; `shutdown` = node close (C2); PM `close` = no-op |
| `importData`, `importFiles`, `Database.setStorage`, `Database.guaranteeNoActiveStorage` | REJECT | |

**Test spec.**
- **Location:** `T/node/store/GateClassificationTest.java`, with the map in
  `T/node/store/GateClassification.java`.
- **Key:** `FirstDeclaringInterfaceSimpleName#method(ParamSimpleName,…)`, for example
  `Storer#store(Object,long)`. The interface order for "first declaring" is `StorageManager`,
  `StorageConnection`, `Database`, `PersistenceManager`, `Storer`, `PersistenceStorer`,
  `PersistenceRegisterer`.
- **Enumeration:** public, non-static, non-bridge, non-synthetic methods including defaults,
  excluding `Object`'s. Every key must be mapped and every map key must exist.
- **READ check (deterministic, bounded):**
  1. Inject a real coordinator.
  2. Thread A takes the write side.
  3. Thread B invokes the method.
  4. Poll `ReentrantReadWriteLock.hasQueuedThread(B)` (through a package-private accessor) until
     true, **timeout 1 s → fail**.
  5. Release, then join B with **timeout 5 s → fail**.
- **WRITE/REJECT on readers:** assert the exception type.

### Other step-9/10 items

- **A3 (P2).** Replace `ReplicationCursor` and `AeronReplicationCursor` with
  `record ReplicationPosition(UUID clusterId, UUID storeGeneration, long epoch, long recordingId,
  long sequence, long prepareStartPosition, long fencingToken)`, the in-memory twin of the mark.
  Delete the transport SPI `NoOp`s, the `"aeron"/"none"` string checks
  (`M/node/NodeAssembly.java:383,668`, `M/node/backup/BackupRestorePolicy.java:104`,
  `M/node/backup/BackupMetadata.java:95,165,231`) and `replicationStreamName` (it selects nothing:
  it is only compared in `AeronTransportShared.claimStream`).
- **A5 / S4 (P1).** Role classes replace the 21 `LazyHolder` fields (`M/node/NodeAssembly.java`).
  The role is parsed once (today it is re-parsed at `M/node/NodeLifecycle.java:337, 357, 394, 439`).
  One `CloseSequencer` is built in reverse construction order (its retry semantics are kept).
  Remove the duplicate reseed gate (`:334-338` / `:354-358`) and the field-by-field
  `StorageConfiguration` copy (`:455-483`).
- **A6 (P2).** `record ReplicationSnapshot(ReplicationState state, NodeRole role, long appliedSequence,
  long latestKnownSequence, long appliedPosition, long archiveUsableBytes, long lastApplyEpochMillis)`
  (`-1` = unknown), published through one volatile field. Delete the `StorageNodeHealthCheck` and
  `StorageNodeControl` metric defaults and `ReplicationMetrics`.
- **A7 (P2).** `api` becomes a leaf; `ClusterStorage` is the single entry into `node`.
- **D9 (P2).** Packages:
  - `api`, `errors` (exported);
  - `node`;
  - `store` (manager, graph lock, limits, indexes);
  - `replication` (runtime, writer, reader, wire, retention, mark);
  - `backup`.

  Update `module-info` exports. A3 and A9 must land first.

---

## 6. Remaining findings (condensed; step 12 unless stated)

- **A8 (optional; for A2b).** Recording discovery **already exists**:
  `AeronReaderTransport.discoverReaderRecordingId` (`M/node/aeron/AeronReaderTransport.java:260-276`)
  lists recordings by `alias=` on the live channel and requires exactly one. The recording id
  setting already defaults to `-1`. The remaining work:
  1. the writer includes the epoch in the alias (`alias=<configured>-e<epoch>`), because A2b
     creates a second recording;
  2. readers select the alias for their mark's epoch;
  3. verify the first replayed envelope's `clusterId`/`epoch` against the mark;
  4. fix `README.md:126-129`, which tells operators to set the recording id.

  Old recordings without the epoch suffix need a reseed (no compatibility required).
- **C8 (P3).** The dictionary parse monitor is moot after P1-7.
- **C9 (P2).** `NodeStatus` gains `BackupStatus backup`: `record BackupStatus(boolean lastFailed,
  String lastFailureMessage /* "" when none */, long lastSuccessEpochMillis /* -1 */)`.
  - It is set by the backup task and cleared (`lastFailed=false`, message `""`) by the next
    successful backup.
  - `ready`/`healthy` no longer depend on backup failure (`M/node/backup/BackupNodeManager.java:210-214`).
  - `ReplicationState.DEGRADED` stays replication-scoped.
  - **Status:** implemented. `BackupStatus` also reports post-publication maintenance failure
    separately; a durable backup remains successful and readiness is unchanged.
- **C12 (P2).** Hard-coded retries move into `NodeConfig`:
  - `StorageBackupManager.java:128-131`
  - `FilesystemVolumeBackupBackend.java:533`
  - `NodeMaintenanceScheduler.java:27-28`
  - `StorageTaskExecutor.java:50`
  - `NativeBufferPool.java:14`
- **P1-6 (P2).** Drop the fence CRC (gone with A1), derive or drop the transaction CRC, and use a
  gathering `DirectBufferVector[]` offer (`M/storage/aeron/writer/EnvelopeFramer.java:117-161`).
- **P1-10 (P1).** Backups: single-pass CRC32C, 1 MiB direct buffers or `transferTo`, consider
  directory backups; no manifest after A1.
- **P1-11 (P3).** `StorageUsageGauge` virtual-thread spawning (`M/node/store/StorageUsageGauge.java:79-87`)
  → measure in the limit task, or use `createStorageStatistics()`.
- **P1-12 (P2).** Threads per reader after A1 + P1-7: poll, apply, watermark, maintenance, storage
  checks, retention. Compose poll + watermark + budget as Agrona agents on one `AgentRunner`.
- **S1 (P2; D-22).** Retain same-name publication checks in
  `M/node/backup/FilesystemVolumeBackupBackend.java`: a crash retry is idempotent only for the
  same archive identity, and a conflicting ID must not replace a durable backup. The ZIP digest
  runs outside `.publish.lock`; a file-stamp check retries if the destination changes before the
  locked publish. Removing this path would reintroduce data loss or hold the shared lock while
  hashing a potentially large archive.
- **S3, S5, S6.**
  - S3: one `section(lock, invalidateOnFailure, supplier)` helper in `StorageGraphCoordinator`
    (`:119-264`).
  - S5: a shared versioned-record codec (or SBE) for the formats left after A1/A2 (envelope,
    watermark, retention state, backup identity).
  - S6: an `AeronArchiveReader` state machine.
- **D2–D8, D10.**
  - Single-implementation interfaces become final classes.
  - Fold static-only types.
  - D4: sealed `NodeException`, final leaves, `outcome()` derived from the types
    (`WriteRejectedException`, `ReplicationPendingException`, …), move
    `ReplicationPositionUnavailableException` internal, remove the unread
    `ReplicationUnavailableException.errorCode()`; constructors stay public.
  - D5–D8 decided (D-16..D-19, D-15).
  - D10: records for the >5-parameter constructors (`TransactionAssembler` 10,
    `EnvelopeFramer` 11, `AeronReplicationPublisher` 9, `Delivery.prepare` 9).
- **J2 (P2).** 157 lines with `synchronized` in `src/main/java`. Single-owner state needs no locks;
  lifecycle → `AtomicReference<State>` + `CompletableFuture`; the rest → `LockedExecutor` /
  `StripeLockedExecutor`.
- **J3–J6.**
  - FQNs.
  - One `CloseSequencer`.
  - One `FaultInjection` seam.
  - `sealed` roles/states; `StructuredTaskScope` for startup/close fan-out. (No `ScopedValue`
    counter; withdrawn.)

### N3 — Archive retention is never triggered in a deployed cluster (P1; found in the rev-5 documentation pass)

**Original evidence (before this change).**
- `ReplicationLogRetention.deleteThrough` has exactly one production caller:
  `StorageBackupManager` (`M/node/backup/StorageBackupManager.java:478-482`).
- `StorageBackupManager` is wired only for the backup-reader (`M/node/NodeAssembly.java`,
  `ensureStorageBackupTaskExecutor` and `NodeLifecycle.startBackupNode`).
- On a backup-reader, `AeronRetentionOwner` returns the "unsupported" retention
  (`M/node/aeron/AeronRetentionOwner.java:43-61`), because retention requires an embedded writer
  with configured retention readers. `advanceRetention` therefore only logs "preserving Archive
  history".
- The writer, which owns the Archive and collects the watermarks, did not call `deleteThrough`;
  only tests and the soak did.

**Consequence.** The writer's Archive grows without bound in production. The quorum and watermark
machinery is inert.

**Fix.** The writer runs retention on its maintenance scheduler. Each pass requests its latest
durable boundary; the retention controller caps deletion at the minimum durable quorum watermark
and complete Archive segments. Incomplete quorums and unavailable retention preserve history.

**Current status.** `NodeLifecycle` schedules Aeron writer retention at the configurable
`ECLIPSE_DATAGRID_AERON_RETENTION_INTERVAL_MINUTES` cadence (default one minute). Each pass asks
for the latest durable writer position; `AeronArchiveRetention` caps that request at the complete
reader-quorum watermark and purges only complete durable segments. An incomplete quorum or a
quorum that has not crossed a segment preserves history. The backup-reader path remains
unsupported and no control-message protocol was added. Controller quorum-capping tests exist;
the scheduler-specific writer-plus-two-readers test remains open.

**After A1,** watermarks report the reader's mark (`sequence`, `prepareStartPosition`).

**Remaining test.** A writer plus 2 retention readers with a small segment length. Drive the
writer's scheduled pass: after both readers pass two segments, it deletes the older segment; while
one reader has not crossed a complete segment, the pass preserves history and maintenance health
stays healthy.

## 7. Security and documentation

- **Protocol constraint.** PerunCS does not provide node authentication or transport encryption;
  neither is required or permitted. No trusted-network acknowledgement setting is part of the
  configuration.

- **SEC1.** Default Aeron directory (`M/node/aeron/AeronSettings.java:251-258`) →
  `<storage>/aeron`; reject `/tmp` in production.
- **SEC2.** Backup permissions `0600`/`0700`.
- **SEC3.** Legacy keys removed by D1.
- **SEC4.** `M/node/aeron/AeronArchiveFailures.java:38-40` → `errorCode()`. For
  `M/node/aeron/AeronRuntime.java:45` (driver/mark-file message text, which has no error code):
  keep the match, isolated in one method with a comment naming the Aeron version it was verified
  against and a test pinning the message. That is the accepted fallback.
- **SEC5.** JVM-global statics → per-node instances.
- **Docs:**
  - rewrite the `module-info` essay (shrink it) after A1/A2;
  - README: porting map, deviation list, integration guidance, generated key table, network
    section without NFS;
  - keep `requires org.eclipes.store.gigamap.jvector`;
  - no repo-wide formatter (D-28).

---

## 8. Reference corrections (cumulative)

| Earlier claim | Corrected |
|---------------|-----------|
| C10 fix "set before submit, clear in finally" (rev 3) | Wedges close on cancelled-queued tasks; see the §3.C10 state machine |
| A8 "readers must configure the recording id" | Discovery exists (`AeronReaderTransport.java:260-276`); only the README is stale |
| A9 `ClusterFoundation<F>` / exported `ObjectGraphUpdateHandler` (rev 3/4) | Withdrawn (not Data Grid features, D-02/D-11) |
| `NativeBufferPool.java:456` | `:14` |
| `WriterFencingLease` ≈960 LOC, `executeUnderOwnership` 596–657 | 913 LOC; 602–666 |
| 22 lazy holders / ~180 `synchronized` | 21 / 157 lines |
| `fileSyncLevel=0` rejection as new work | Existing (`AeronSettings.java:326-333`); test only |
| A1 "last COMMIT position stored alongside" | Impossible (unknown when the mark is written); `prepareStartPosition` + duplicate skip |
| J6 `ScopedValue` for the C2 counter | Withdrawn |
| "Run a formatter" | Dropped (D-28) |

---

## 9. Test-to-step map

| Step | Tests (defined in the spec blocks) |
|------|------------------------------------|
| 0 | Benchmark baseline |
| 1 | C1 (8 cases), C2 (3), C3 (4), C5 (existing suite), C6 (2), C10 (3), A4 (probe without `--add-exports` + integration) |
| 2 | Spikes S-1…S-5 |
| 3 | A1 crash matrix (new oracle), reader resume, `ReplicationPendingException`, `fileSyncLevel=0` test, C1 re-run |
| 4 | Benchmark compare (D-25) |
| 5 | A2 (3) |
| 6 | P1-1 (3), F1 (fuzz, equivalence, micro-benchmark) |
| 7 | P1-7/N1 (4), F2 (spike gates 1–2, NMT leak test, N1 stress on arena pool) |
| 8 | C4 read-latency metric |
| 9 | A9 (3), D1 (a parameterized test per `Setting`), A3 (existing suites adapted) |
| 10 | C7 classification test, A5/A6 existing suites adapted |
| 11 | J1-a (3) |
| 12 | C9 (status fields), S1 (backup suite), A2b/A8 if taken; a `ClusterStorage` contract test per role (open/close idempotence, standalone `NOT_CONFIGURED`) |
