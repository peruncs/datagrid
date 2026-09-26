# KIMI-REVIEW.md — Complete code review of `peruncs.cluster`

Static analysis only, per AGENTS.md review policy: no builds, no JUnit runs, no
code modifications; only problems are reported, each with a concrete
recommendation.

**Reviewed state:** working tree = HEAD `cd39892` ("Review round 6") plus a
trivial uncommitted import-reorder delta (`NodeAssembly`, `NodeLifecycle`,
`NodeMaintenanceScheduler`, `AeronTransport`, `module-info` — verified: no logic
change). Source counted at 31k lines main / 130 files, 173 test files.
**Method:** direct source reads of all central files (every file cited below was
read or grepped in the current tree; line numbers are current). The
codebase-memory graph reports these paths as `not_tracked` (generation
2026-09-25), so every claim below is source-verified, not graph-derived.

## Verification ledger — previously filed findings that are NOW FIXED

So stale findings are not re-raised, these were re-checked in the current tree
and are **resolved**: coordinator relock interrupt path
(`AeronReplicationWriteCoordinator.notifyStateOutsideAdmission:726-757` now
re-acquires uninterruptibly and suppresses the original failure);
`backupRunning` is set inside the task body
(`StorageBackupTaskExecutor.java:145`); read-side admission is observable
(`StorageGraphCoordinator.drain()/admissionClosed:278-293` with `ensureAdmission`
inside the read/write lock) — a late reader that passes the outer check now
fails inside the lock; `NodeLifecycle.start` failure rethrow is instanceof-guarded
(`:169-171`); `GuardedDatabase` is cached (`GuardingStorageManager.java:69-70`);
`BackupRestorePolicy` parameter is now `ownAuthoritativeStore`
(`BackupRestorePolicy.java:49`); `AeronWatermarkChannel.available()` reads only
volatile/atomic state (`:186-188`); `BackupArchive` enforces declared-size and
dry-run inflation budgets pre-extraction (`BackupArchive.java:90,540-544,557-582`);
`NodeSettingsSource` duplicated javadoc fixed. The pre-existing
`KIMI-REVIEW.md` shipped by the earlier session was written against a pre-round-6
tree and is superseded by this document.

---

## Findings

### Rule 6/14/24 — [HIGH] Incomplete removal of Archive-control authentication

Commit `0d70c4f` ("Remove archive control auth entirely") removed the auth
feature but left its documentation behind:

- `node/aeron/AeronSettings.java:786-826` — three orphaned `///` doc blocks
  (for `authenticatorSupplier()`, the authorization service, and
  `credentialsSupplier()`, each ending in `@return` tags) with **no member
  declarations following them** — these are dangling documentation comments.
  One sentence is truncated mid-phrase (`:795": …and has no"`).
- `node/aeron/AeronRuntime.java:36-42` — the class javadoc still documents the
  deleted behavior: "When [AeronSettings] enables Aeron authentication, the
  embedded Archive challenges every control session…".
- `node/aeron/AeronRuntime.java:454-458` — `closeAllQuietly` javadoc still
  describes erasing "settings-held auth credentials"; no such code exists.

Defects this causes: (1) the `deploy` profile runs javadoc with
`doclint=all, failOnWarnings=true` (`pom.xml:362-364`), and dangling doc
comments fail that gate; (2) the docs actively contradict AGENTS.md rule 28
("no node authentication features") and the module's own "Trusted network
boundary" section, misdirecting the next maintainer about the security model.
**Fix:** delete the orphaned blocks in `AeronSettings` and rewrite the two
`AeronRuntime` javadoc passages to state the no-auth boundary in one sentence
("Archive control carries no authentication; the isolated network was and
remains the only boundary"). Add no replacement API.

### Rule 21 — [HIGH] `LazyHolder` initialization flag has a publication race

`node/NodeAssembly.java:267-276`: `get()` returns `constant.get()` and *then*
sets `initialized = true`; `isInitialized()` is an unsynchronized volatile read
from the close path (`NodeLifecycle.java:631-634`, stage preconditions). A first
`get()` that has completed the factory call but not yet stored `initialized`
races with a concurrent close: the close stage for that collaborator is skipped
as "never created", permanently leaking the resource that was just constructed.
The window is small but the failure mode (leaked MediaDriver/Store on a
close-during-startup race) is exactly what the per-stage tracking exists to
prevent.
**Fix:** give `LazyHolder` one monitor: make `get()` and `isInitialized()`
`synchronized` (holder is cold-path only, so monitor cost is irrelevant), which
provides the happens-before edge the two half-operations currently lack. If you
keep `LazyConstant` inside for the memoization, still wrap both methods.

### Rule 1 — ThreadLocal ban violated; preview features unused

1. `storage/ReplicationRetry.java:3,111` uses `ThreadLocalRandom.current()` for
   jitter. AGENTS.md bans `ThreadLocal`-family state.
   **Fix:** hold one `java.util.random.RandomGenerator` (e.g.
   `RandomGeneratorFactory.of("L64X128MixRandom").create(seed)`) per retry
   policy instance; as a bonus the jitter becomes seedable for the deterministic
   soak (`soak.seed`).
2. The module already requires Java 27 preview for `LazyConstant`; that preview
   type is load-bearing in four packages (`node/NodeAssembly.java:256-261`,
   `node/store/GuardingStorageManager.java:44,64`,
   `node/backup/BackupArchive.java:38`,
   `storage/aeron/writer/EnvelopeFramer.java:26`) with no isolation seam — a
   JEP 531 rename/removal touches four packages.
   **Fix:** one tiny internal `Lazy` holder (the `NodeCollaborators.LazyHolder`
   shape, minus the `LazyConstant` delegate) used by all four sites; then the
   preview dependency is confined to one file or removed outright.
3. `StructuredTaskScope` is used nowhere; `NodeMaintenanceScheduler.java:30-31`
   pairs a `ScheduledThreadPoolExecutor` with
   `Executors.newVirtualThreadPerTaskExecutor()` and re-implements
   skip-if-running with a per-task `AtomicBoolean` (`:136`). Where the pattern
   is truly fan-out-and-join (the soak/chaos workers in tests are the natural
   candidates), prefer `StructuredTaskScope.ShutdownOnFailure` over hand-rolled
   failure latching (`fatalFailure`/`degradedFailures` maps at `:32-34`).

### Rule 15 — Inline fully-qualified names

- `node/aeron/WriterFencingLease.java:382-383` — `java.util.concurrent.atomic.AtomicReference` written out twice.
- `storage/aeron/reader/TransactionAssembler.java:489` — `java.util.concurrent.Callable`; `:545` — `java.nio.charset.CharacterCodingException`.
- `storage/index/StoreIndexReflection.java:137` — `new java.util.ArrayList<>()` twice in one expression.

**Fix:** plain imports; then enforce with a checkstyle/forbiddenapis rule
banning `java\.[a-z].*` inline in `src/main`.

### Rule 12/5 — God classes concentrated on the replication hot path

Current sizes (lines): `TransactionAssembler` 1092, `AeronArchiveReader` 1066,
`GuardingStorageManager` 1033, `AeronReplicationPublisher` 1004,
`FilesystemVolumeBackupBackend` 900, `AeronArchiveReplicationPublisher` 880,
`BackupArchive` 867, `AeronWriterTransport` 852, `WriterFencingLease` 841,
`AeronReplicationWriteCoordinator` 838, `AeronSettings` 826,
`NodeAssembly`/`NodeCollaborators` 797, `NodeLifecycle` 770.

Worst offenders with a clear split:

- `GuardingStorageManager` is facade + six cohesive adapters
  (`GuardedDatabase`, `BinaryPersistenceManagerAdapter`,
  `ClusterPersistenceRegistererAdapter`, `ClusterPersistenceStorerAdapter`,
  `ClusterStorerAdapter`, `GatedPersistenceTarget`, plus the `GraphBoundary`
  factory at `:565-613`). **Fix:** extract each adapter to a package-private
  top-level class taking `(GuardingStorageManager, delegate)`; each becomes
  independently unit-testable against a proxy delegate, and the facade drops
  under 400 lines.
- `TransactionAssembler` mixes the chunk state machine, the delivery barrier
  (`Delivery`, `PendingDelivery`, flush at `:1002-1062`), native buffer
  ownership (`Transaction.ensureCapacity/dispose`, `:805-861`), and the
  crash-test hook plumbing (`:472-497`). The comment at `:115-124` even contains
  an unedited self-correction ("…parks on the barrier monitor… **no** — even
  the barrier monitor is released during the await"), which is evidence of how
  hard the lock choreography is to keep straight. **Fix:** three package-private
  types — `FramingState` (accept/commit validation), `DeliveryBarrier`
  (stage/flush), `TransactionBuffers` (native ownership) — coordinated by a
  ~300-line `TransactionAssembler`.
- `AeronSettings` has nested records (`ArchivePolicy:135`, `Timeouts:160`,
  `Channels:182`, `Directories:220`) but keeps ~500 lines of
  parse/validate/`fromEnvironment`/loopback-skew helpers in the one file.
  **Fix:** give each nested record its own `fromProperties` in its own file and
  make `AeronSettings` a 6-field composition.

### Rules 21/27 — synchronized/`LockedExecutor` mixing; pinning on virtual threads

1. `storage/binary/ApplyWorker.java` holds a serializer `LockedExecutor`
   (`:29`) *and* six `synchronized(this.budgetLock)` sites (`:156,220,244,282,292,311`);
   `storage/binary/StorageBinaryDataMerger.java` likewise mixes
   `LockedExecutor` (`:186`) with `synchronized(dictionaryParseLock)` (`:503`),
   while `ApplyQueue` uses `ReentrantLock` (`:33`). Three lock idioms guard the
   same import pipeline.
   **Fix:** consolidate the whole import path onto the existing
   `LockedExecutor` instances (they already implement `XThreads`-style
   ownership probes) and delete `budgetLock`/`dictionaryParseLock`.
2. `node/aeron/WriterFencingLease.renew():497-531` runs
   `openLockChannel` + bounded `lockFile` retry (`LockSupport.parkNanos`) +
   file read + `writeAtomically` (fsync on NFS) **inside `synchronized(mutexFor(path))`,
   on a virtual-thread heartbeat** (`:397-398`). A slow NFS round-trip pins the
   carrier thread for the whole critical section; the same is true of
   `executeUnderOwnership():589-650` (fsync under `synchronized` while a
   commit offer runs).
   **Fix:** switch the per-path mutex to a `ReentrantLock` (or the serializer
   `StripeLockedExecutor` keyed by canonical path) so virtual threads park
   instead of pinning; assert the absence of `jdk.VirtualThreadPinned` events
   in the soak's JFR report (`SoakJfrReport` exists for exactly this).
3. Lock discipline is by convention only: `mutexFor(path)` → `stateLock` is
   nested at `:175/:194`, `:499-500`, `:591-592`, while `close()` is
   `synchronized(this)` on the lease itself plus `stateLock` (`:692-693`).
   Any future `stateLock` → `mutexFor` path deadlocks.
   **Fix:** one private final lock object per role, documented order in the
   class javadoc, no `synchronized` methods.

### Rule 23/9 — Protocol violations cross the boundary as untyped runtime exceptions

`TransactionAssembler` throws `IllegalStateException`/`IllegalArgumentException`
for wire-level protocol violations: stale fencing token (`:355`), sequence
regression (`:360`), replayed-data mismatch (`:372`), missing terminal witness
(`:388`), duplicate-terminal mismatch (`:391,:395,:399`), sequence gap (`:412`),
abort-with-checksum (`:424`), non-data envelope (`:438`), interleaving
(`:446,:739-763` region), bare commit (`:520`), commit/payload mismatch (`:534`).
Meanwhile the module exports a typed failure taxonomy
(`errors.CorruptReplicationDataException`, `errors.ReseedRequiredException`)
that the operator contract (`README.md` "Programmatic control boundary")
expects to map onto HTTP/retry behavior. Untyped `IllegalStateException`s force
callers to string-match.
**Fix:** throw `CorruptReplicationDataException` for gap/regression/mismatch and
`ReseedRequiredException` for regression-past-durable-cursor, preserving the
formatted messages; add one parameterized test asserting the typed mapping for
each site.

Separately, `node/CloseSequencer.java:84-102` aggregates per-stage failures by
suppression but drops the stage **name** from the exception chain (it survives
only in a DEBUG log at `:98-99`).
**Fix:** wrap each stage failure: `failure.addSuppressed(new NodeException("close stage '" + stage.name() + "' failed", stageFailure))` — the name must be
in the causal chain, not only in the log.

### Rule 16/17 — Long argument lists on worth-fixing paths

- `storage/aeron/reader/TransactionAssembler` constructor: 10 positional params
  (`:144-155`).
- `TransactionAssembler.Delivery.prepare(...)`: 10 positional params
  (`:894-897`) with three consecutive `int`s
  (`resolutionDataLength, resolutionDataChunkCount, resolutionCrc32c`) — one
  transposition is a silent cursor-witness corruption.
- `node/aeron/AeronArchiveRetention` constructor: 14 positional params
  (`:84-98`) — two `UUID`s, four lambdas, two `IntSupplier`s, a `long`, a
  `Path`, a `long` — as the unreadable test call sites prove.
- `storage/aeron/wire/AeronReplicationEnvelope.encode(...)`: 17 params (`:101`),
  `encodeWithPayloadCrc`: 18 (`:150`), `validate(...)`: 15 (`:203`).

**Fix:** package the cold-path constructors as `record Configuration(...)`/
`record Inputs(...)` (the house pattern already proven by
`StorageBinaryDataMerger.Configuration` and `StorageNodeManager.Configuration`).
For the envelope methods (hot path, allocation-sensitive): bundle the fixed
header fields into a small `record FrameHeader(clusterId, epoch, fencingToken,
wireNonce, sequence, kind, payloadLength, chunkIndex, chunkCount, chunkOffset,
commitCrc32c)` — records of scalars are scalar-replaceable and do not allocate
under escape analysis, so this removes the transposition hazard at zero GC cost.
`AeronReplicationConfiguration` itself (13 components,
`config/AeronReplicationConfiguration.java:35-49`) keeps its Builder but should
gain `withX` copy methods so tests stop rebuilding via full builder chains.

### Rule 26 — Cross-thread monitor on the shared Aeron Archive client

`storage/aeron/writer/AeronArchiveReplicationPublisher.java` synchronizes on the
shared `archive` object in 11 places (`:110,203,231,265,471,477,483,489,499,511`…),
and `node/aeron/AeronRuntimeOwner.java` adds more (`:198,205,212,228,236`).
Aeron's Archive client is designed for single-threaded use with an idle
strategy; cross-thread `synchronized(archive)` both contends with the conductor
and serializes writer commits against reader `listRecordings` calls.
**Fix:** confine each `AeronArchive` to its owning thread (the write regime
already exists via `AeronReplicationWriteCoordinator.writeLock`) and route any
cross-thread queries through a hand-off queue, or document precisely why each
monitor site cannot observe Aeron's `poll()` contract. Cross-check against
`$GITHUB_ROOT/aeron-io/aeron` `ArchiveTool`/`ReplayMerge` discipline.

### Rule 20/22 — Fixed sleeps and single-valued budgets

1. `Thread.sleep` polling: `node/aeron/AeronRuntime.java:172`
   (`STALE_DRIVER_RETRY_DELAY_MILLIS = 100`), `node/backup/StorageBackupManager.java:439`
   and `:492`. These are fixed delays in driver-recovery and replicate-wait
   loops that ignore the configured retry policy.
   **Fix:** route through `storage/aeron/config/AeronRetryPolicy` (the
   `BackoffIdleStrategy`-backed policy that already exists) so deployment knobs
   actually cover these waits.
2. One `readerStopTimeoutNanos` (default 30 s, `config/AeronReplicationConfiguration.java:73,86`)
   bounds both live-terminal withholding (`TransactionAssembler.withholdTerminalMarker:306-321`)
   and Archive-tail reconnect in `AeronArchiveReader`. A long tail-reconnect
   tolerance cannot be had without also tolerating a stalled recording on the
   live tail.
   **Fix:** split into `liveWithholdTimeoutNanos` and `reconnectTimeoutNanos`
   with separate `ECLIPSE_DATAGRID_AERON_*` keys.

### Rule 29/31 — Heap allocation and buffer-abstraction mismatches

1. `storage/aeron/wire/AeronReplicationEnvelope.copyToOwned:267-272` allocates
   `new byte[view.payloadLengthOnWire]` per decoded envelope — on the reader
   polling thread, for every owned copy.
   **Fix:** decode into a caller-supplied pooled `ExpandableArrayBuffer`/
   `UnsafeBuffer` scratch (the pattern already used for `ChecksumContext`),
   owned by the reader loop.
2. `storage/aeron/writer/AeronReplicationWriteCoordinator.PreparedWrite:94`
   retains `byte[] dictionary` + `ByteBuffer[] buffers` per transaction.
   **Fix:** `DirectBuffer` views over one pooled
   `ExpandableDirectByteBuffer`; `EnvelopeFramer.offerDictionaryChunks(DirectBuffer,…)`
   already accepts that shape.
3. `storage/aeron/reader/TransactionAssembler.EMPTY_BUFFER:34` uses
   `ByteBuffer.allocateDirect(0)` where the read path otherwise speaks Agrona
   `DirectBuffer/UnsafeBuffer`.
   **Fix:** Agrona's `UnsafeBuffer.EMPTY_BUFFER`/constant, one abstraction.

### Rule 33 — Fencing token acceptance has no upper bound; retention pause has no deadline

1. `TransactionAssembler.adoptFencingToken:578-582` raises the accepted floor to
   *any* greater token, and `WriterFencingLease.nextToken:269-278` increments
   without bound. A writer in a restart loop (crash–restart–crash) mints an
   unbounded token series that every reader accepts as legitimate fencing; the
   runaway case is indistinguishable from many small legitimate takeovers.
   **Fix:** bind acceptance to a sliding window
   (`[floor, floor + maxTokenAdvance]`, configurable) and fail closed with
   `ReseedRequiredException` beyond it.
2. `AeronWriterTransport.java:560` runs `purgeSegmentsWhileWritesPaused` inside
   `withWritesPaused(...)` with no deadline; `AeronArchiveRetention` has a
   60 s *command-queue* bound (`DEFAULT_OPERATION_TIMEOUT_MILLIS:75`) but the
   pause itself is unbounded — a wedged purge wedges the single writer's
   admission indefinitely.
   **Fix:** bound the pause (dedicated `retentionPauseTimeoutNanos`), fail the
   retention command on expiry, and surface in `ReplicationMetrics`.

### Rule 19 — Residual security/hardening gaps

1. `writeAtomically`-style lease reads validate the lease path is not a symlink
   (`WriterFencingLease.readForAcquire:773-776`) but the mount lacks an
   explicit `nosuid`/`noexec` recommendation anywhere; the README requires NFSv4
   but not mount options. **Fix:** document `nosuid,nodev,noexec` and probe
   them in the same startup validation as the filesystem-type check.
2. `AtomicFileWriter.deleteRegularFile:444` throws "File reappeared" if the
   path exists after delete — that is a TOCTOU false positive against any
   legitimately racing re-create (e.g. a checkpoint rewrite racing restore
   cleanup). **Fix:** delete the reappearance check, or compare the new file's
   `fileKey` and only fail on *same-identity* reappearance with a comment
   stating which race is actually being caught.
3. Loopback/wildcard rejection is prod-mode only (`AeronSettings.java:462-466`);
   a dev-mode node pointed at real routable endpoints of a production cluster
   id starts without complaint. **Fix:** add a cluster-id-scoped guard:
   whenever `ECLIPSE_DATAGRID_AERON_TRUSTED_NETWORK != true`, refuse
   non-loopback channels regardless of `isProdMode`.

### Rule 10 — `public` in unexported packages is the implicit test harness

~60 types in `node/**` and `storage/**` are `public` while `module-info.java:201-202`
exports only `peruncs.cluster.api` and `peruncs.cluster.errors`. Encapsulation
therefore relies on JPMS at runtime while tests run with
`useModulePath=false` (`pom.xml:253`) — `public` is load-bearing for tests, not
for consumers, which is an accidental contract.
**Fix:** either demote internal types to package-private and patch the test
sources onto the module (`--patch-module peruncs.cluster=...`), or state the
convention ("every `public` outside `api`/`errors` exists for the test
classpath") in the root `package-info.java` and guard it with a bytecode-level
test that no exported-package type imports an internal one.

### Rule 11 — Missing root package docs

`peruncs/package-info.java` does not exist (the only package without one;
`peruncs/cluster/package-info.java` exists). **Fix:** add it with the
one-sentence product narrative so javadoc covers the root package.

### Rule 14 — Javadoc hygiene

1. Truncated/dangling doc comments: see the auth-removal finding above
   (`AeronSettings.java:795` truncated sentence; orphaned blocks).
2. Inconsistent markdown-javadoc indentation house-wide: many files mix
   column-4 and column-8 `///` blocks (e.g. `StorageGraphCoordinator.java:97,117,135,162,188,196,228`,
   `NodeAssembly.java:99`, `AeronArchiveRetention.java:74`,
   `AtomicFileWriter.java:32,49,56`, `TransactionAssembler.java:296,642`,
   `WriterFencingLease.java:99,132,315,326`). Rendering tolerates leading
   whitespace, but the inconsistency signals un-formatted drift and makes
   every subsequent edit noisier.
   **Fix:** one mechanical pass normalizing to the declaration's indentation;
   the `deploy` doclint gate then stays clean.

### Rule 8 — Single-use ceremony to fold

- `node/aeron/AeronTransportShared.java:57-95`: `claimStream/clearStreamClaim/claimedStream`
  are each called once (`AeronTransport.java` teardown and one error message),
  alongside a `closed()/closing()` boolean pair that is a hand-rolled state
  machine (`closing(boolean)`/`closed(boolean)` setters).
  **Fix:** inline the claim into the constructor (final `distributorStream`) and
  replace the booleans with `enum State { OPEN, CLOSING, CLOSED }` +
  `compareAndSet`, deleting ~25 lines and four methods.
- `storage/aeron/writer/CrashHook.java` (1 record + 4 statics, used only by
  `AeronReplicationWriteCoordinator`/`WriterFencingLease`) and
  `storage/aeron/writer/WriterLeaseGate.java` (1 interface, 1 implementor)
  can nest into their owners.
- `AtomicFileWriter.rejectSymbolicLinks:282-284` is a four-line private wrapper
  around public `ensureNoSymbolicLinks` — inline one of them.

### Rule 5 — Duplicated ScopedValue test-hook scaffolding

Five independent copies of the same `ScopedValue.newInstance()` +
`isBound() ? get() : null` + `inheritCurrent` pattern:
`AtomicFileWriter.java:43-70`, `AeronReplicationCheckpointStore.java:41-47`,
`FilesystemVolumeBackupBackend.java:38-53`, `CrashHook.java:20-72`,
`TransactionAssembler.java:472-497` (plus hook *binding* sites in
`AeronTransportShared.java:18`, `AeronWriterTransport.java:47`).
**Fix:** one internal `TestHooks` final class (`<T> ScopedValue<T>`, `runWith`,
`inheritCurrent`, `fire`) in `storage.io`; each owner keeps only its typed
constant. ~90 lines of copy-paste idioms disappear and the crash-test contract
gets one documented home.

### Rule 4/7 — Architectural notes

- **Tester-friendly asymmetry:** `AeronTransportShared`'s stream-claim + boolean
  lifecycle, the `NodeCollaborators.LazyHolder` cycle-breaking, and
  `NodeClose`'s two-method contract (`NodeClose.java:7-24`, close + `checkOpen`)
  all encode the same root cause: the wiring graph is circular
  (`NodeAssembly.java:39-41,168-172` admits it). Constructor injection with
  `Supplier<T>` for forward references (only the Store connection genuinely
  needs it) would delete the whole LazyHolder layer and its race (above).
- `NodeOptions`/`NodeAssembly.Builder` silently replace the file provider of a
  caller-supplied `EmbeddedStorageFoundation` (`NodeAssembly.java:76-86`,
  `ClusterNode.java:53-58`; now documented, but) a caller that tuned the
  provider gets a *different* Store layout than requested with no error.
  **Fix:** fail with `IllegalArgumentException` when the supplied foundation
  carries a non-default `StorageLiveFileProvider`; document the replace-list
  explicitly.

### Rule 25 — Test gating

The default `mvn verify` runs only two ITs (`pom.xml:267-270`); the crash
matrix, Store integration, and soak are all profile-gated. Several previously
*required* tests for race/crash fixes (interrupt-during-relock, blocked import
across disposal, late-reader-admission) were demanded by earlier review rounds;
the current tree's headline fixes (e.g. read-side admission latch) are only
covered if their tests actually run in the default gate — verify by running and
by listing which of those tests are under `src/test` (surefire) vs. profile-only
failsafe.
**Fix:** promote the cheapest, most regression-prone cells — prepare/local-write/
commit-offer checkpoint seams and the read-admission/shutdown race — into the
default surefire set; keep duration, not coverage, behind profiles.

### Rules 3, 18, 30, 32 — Verified as non-issues (no action)

- Rule 3: entities are records/immutable where they should be; the only
  `Optional` pattern-match is `ClusterIndexValidation.java:405`
  (`case Optional<?>`), which is validation of *foreign* graphs — acceptable,
  though persisting-entity `Optional` fields deserve one
  `ClusterStoreIndexes`-registration rejection test.
- Rule 18: no static-only interfaces remain.
- Rule 30: `XMemory` offset access is confined to
  `storage/index/StoreIndexReflection.java` (+ two address helpers), which is
  the sanctioned "single place that knows the layout" and is rule-31 compliant;
  one residual: `readBoolean/writeBoolean` (`:227-238`) mutate an upstream
  `boolean` via raw byte access — if that field is not volatile in Store, add a
  comment explaining the memory-safety argument (single rebuild thread) or use
  a `VarHandle`.
- Rule 32: Lucene/JVector embeddings exist and are reflectively guarded with a
  fail-closed resolver; keep `StoreIndexReflectionTest` in the *default* gate so
  an upstream Store snapshot rename breaks the build, not the first deployment.

---

## Priority order

1. **Auth-removal cleanup** (`AeronSettings` orphaned javadoc + `AeronRuntime`
   docs) — breaks the `deploy` javadoc gate and misdocuments the security
   boundary; mechanical, high value.
2. **`LazyHolder` initialization race** — real resource-leak window on the only
   racy close path; one-line-scope fix (`synchronized`).
3. **Fencing-token upper bound** + **unbounded retention pause** — the two
   remaining unbounded behaviors on the single-writer correctness path.
4. **Virtual-thread pinning through `synchronized` file I/O** in
   `WriterFencingLease` + lock consolidation in `storage.binary` — measurable
   liveness risk under NFS stall.
5. **Typed replication failure taxonomy** (`TransactionAssembler`) +
   `CloseSequencer` stage names — operator-facing correctness.
6. **God-class splits** (`TransactionAssembler`, `GuardingStorageManager`,
   `AeronSettings`) — schedule as refactors with extracted unit tests.
7. FQN/javadoc/style sweep, `TestHooks` deduplication, `NodeOptions`
   fail-fast provider check, default-gate test promotion.
