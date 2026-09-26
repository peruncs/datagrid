# MIMO-REVIEW — datagrid (peruncs-cluster)

Static code review against `AGENTS.md`. No builds, no tests, no source modifications
other than this document (per the review-only rules).

**Scope examined:** `src/main/java` (all 137 files grepped; deep reads of
`peruncs.cluster.api`, `peruncs.cluster.errors`, node lifecycle, writer fencing,
Aeron wire/reader/writer paths, backup/restore, index maintenance, settings),
`src/test/java` (structure, assertion density, scratch/probe tests), `pom.xml`,
`README.md`, `module-info.java`, and the installed Eclipse Store jar descriptors.

**Not covered:** build/test execution, runtime profiling, dependency CVE scanning,
exhaustive deadlock/liveness proofing of every monitor (findings below name only
the concrete defects located, not the absence of others).

**Method notes:** every finding below was verified against the current working
tree; line numbers are as of 2026-09-25. Where a suspicion did not survive
verification (e.g., the `org.eclipes.store.gigamap.jvector` module name in
`module-info.java` is a correct match to a typo'd upstream descriptor, already
commented in place), it is not reported.

---

## HIGH

### F-01 — Two disjoint exception roots at the exported boundary; `ClusterNode.open()` declares no `@throws`

- `peruncs.cluster.errors.NodeException extends RuntimeException`
  (`errors/NodeException.java:8`) and
  `peruncs.cluster.errors.ReplicationException extends RuntimeException`
  (`errors/ReplicationException.java:8`) are parallel roots. `ReseedRequiredException`,
  `GraphInvalidatedException`, `WriterFencedException`, `ReplicationUnavailableException`,
  `CorruptReplicationDataException`, `ReaderWriteRejectedException` hang off the
  *replication* root, although several are lifecycle/operational failures:
  `ReseedRequiredException` is thrown from `NodeLifecycle.java:230,339,359,499,506,529`
  and `AeronArchiveReader.java:714`, i.e. out of `startStorageManager()` /
  `ClusterNode.open()`.
- `ClusterNode.open()` (`api/ClusterNode.java:47-87`) has **no `@throws` javadoc tag
  at all**, so the exported contract of the primary entry point does not state which
  typed failures an embedder must catch.
- Impact: an embedder that catches `NodeException` (the natural "node failed"
  type, and the one used by `createScheduledBackup()`/`createManualBackup()`
  javadoc at `api/ClusterNode.java:119-121,131-132`) silently misses every
  `ReplicationException` — including the terminal `RESEED_REQUIRED` signal that
  `NodeStatus`/README tell operators to act on. The split also forces callers to
  catch two roots for one lifecycle call.

**Recommendation:** give both roots one parent. Make `ReplicationException extends
NodeException` (least churn — every `catch (NodeException)` starts covering
replication failures), or introduce `ClusterException` as the single root of both.
Then document the complete typed set on `ClusterNode.open()` with `@throws`
(`NodeException`, `ReseedRequiredException`, `BackupBusyException`, `WrongRoleException`,
lease failures — see F-05), matching the level of detail already used on
`createScheduledBackup()`.

### F-02 — Application writes through `GraphBoundary` do not auto-latch graph invalidation; a partially mutated graph keeps serving reads

- Replication-driven writes auto-invalidate: `StorageGraphCoordinator.write(...)`
  (`storage/StorageGraphCoordinator.java:144-160`) latches invalidity when the
  supplied action throws.
- The exported application path does **not**: `GuardingStorageManager` maps
  `GraphBoundary.write(...)` to the coordinator's `writeExclusive(...)`
  (`node/store/GuardingStorageManager.java:583-603`), which deliberately skips
  auto-invalidate and instead documents that *every embedder* must call
  `invalidate()` itself on application failure (`api/GraphBoundary.java:44-55`).
- README's usage example never calls `invalidate()`. Nothing in the API shape
  makes the obligation visible — the failure mode is silent: an application
  exception thrown mid-mutation leaves a half-written graph that subsequent reads
  serve normally until some unrelated path happens to latch invalidity.
- This is the correctness inverse of the conservative default the same class
  already applies elsewhere: `persist()` latches on *any* exception precisely
  because a failed section "must not be left ambiguous"
  (`GuardingStorageManager` javadoc).

**Recommendation:** flip the default. `GraphBoundary.write(...)` should
auto-invalidate on a throwing action (same rule as replication sections), and
non-latching sections should require an explicit, differently named opt-out —
`writeClean(Supplier)` / `validate(...)` — used only for pre-mutation validation
that is guaranteed side-effect free. Keep `invalidate()` for nested sections that
catch and rethrow. Enforce it again in the README example.

### F-03 — Index rebuild writes private fields of upstream JVector/Store classes by reflection

- `storage/index/StoreIndexReflection.java:36-113,218-238` resolves private fields
  (`builder`, `index`, `graphRebuilt`, `deferredBuilderOps`) of
  `org.eclipse.store.gigamap.jvector` internals reflectively and writes them
  through `org.eclipse.serializer.memory.XMemory` at byte offsets.
- Consequences:
  - Breaks the moment the upstream snapshot changes field names/layout — the
    failure mode is a runtime `NoSuchFieldException`/corrupted state on a rebuild
    path, with only a version probe (`"unsupported Store version"`) standing
    between the two.
  - Bypasses upstream invariants: `graphRebuilt`/`deferredBuilderOps` are state
    the upstream builder machine mutates under its own locking; writing them from
    outside that discipline (AGENTS rule 13) can race the upstream code that owns
    them.
  - The repo pins `5.0.0-SNAPSHOT` jars rebuilt daily (README), which is exactly
    the condition under which private-field coupling bites.
- Note: use of `XMemory` itself is fine (AGENTS rule 31); the problem is the
  *target* — private state of a foreign class — not the tool.

**Recommendation:** make reflection a compatibility *probe*, not the write path.
Either (a) contribute a small reset/refresh hook to `gigamap-jvector` (the repo
already tracks snapshot builds) and call it through the supported API, or (b) own
the mutable wrapper locally: keep the builder/graph reference in a
`peruncs.cluster`-owned holder that upstream code cannot silently reallocate, so
the fields being written are ours. Keep the current reflective check as a
fail-fast assertion at startup, not as mutation during rebuild.

---

## MEDIUM

### F-04 — Writer lease reacquisition leaks the superseded instance's heartbeat executor and logs a fenced warning forever

- `WriterFencingLease.acquire()` (`node/aeron/WriterFencingLease.java:176-199`)
  handles only two stale-`ACTIVE` states: `isCurrent()` (throws, line 177-178) and
  `releaseUnproven` (cool-down then terminal fence, lines 179-199). Any other
  non-current instance — notably one whose lease file was **stolen by another
  host** or whose renewal repeatedly fails — falls through to
  `acquireLocked` → `ACTIVE.put(path, lease)` (line 264), which *replaces* the map
  entry without closing the old instance.
- The old instance's periodic task then runs forever: `renew()` throws
  `WriterFencedException` on `matchesHolder` mismatch (lines 495-497), the
  heartbeat's `catch (RuntimeException)` logs `WARNING` each period
  (lines 407-412), and `close()` — the only thing that calls
  `heartbeat.shutdownNow()` (line 696) — is never invoked. Result: a leaked
  `ScheduledThreadPoolExecutor` plus unbounded log spam per check interval on a
  writer that already fenced itself.
- Verified that `isCurrent()` (lines 455-472) returns false in exactly these
  states (`closed`, `releaseUnproven`, `heartbeatFailure`, stolen file, stale
  `ownLeaseFresh`), so the gap is reachable, not theoretical.

**Recommendation:** before `acquireLocked`, treat any non-current `active` entry
exactly like the `releaseUnproven` branch already does: fence it under its
`stateLock`, remove it from `ACTIVE`, and shut its heartbeat down — or simply call
`active.close()` (idempotent, removes only its own entry, bounded await). One
line of cleanup where there is currently a leak.

### F-05 — Lease acquisition failures surface as bare `IllegalStateException` from `ClusterNode.open()`

- `WriterFencingLease.acquire()` documents and throws `IllegalStateException` for
  *every* failure class — peer holds a fresh lease, corrupt lease file, lock
  contention, token-series exhaustion (`WriterFencingLease.java:117-121`, thrown at
  172, 178, 191-193, 250-253, 255-257).
- The caller chains it straight up: `AeronWriterTransport.java:466-469` → node
  assembly → `ClusterNode.open()`. An embedder therefore cannot distinguish
  "another writer already owns this store" (an operational, expected condition —
  map to conflict/refused) from an internal invariant violation, even though the
  repo exports exactly the right type for it: `WriterFencedException`.
- This compounds F-01: the primary entry point documents no throws at all.

**Recommendation:** throw the exported `WriterFencedException` (or a
`NodeException` subtype such as `WriterLeaseHeldException`) for peer-held,
foreign, and corrupt-lease conditions; reserve `IllegalStateException` for true
invariant violations inside the class. List the typed lease failures in
`ClusterNode.open()`'s `@throws`.

### F-06 — Parameter lists far past the 5-argument rule, with no staging types

Verified offenders (AGENTS rule 16, violation by construction):

| Signature | Params | Location |
|---|---|---|
| `AeronReplicationEnvelope.encodeWithPayloadCrc(...)` | 18 | `storage/aeron/wire/AeronReplicationEnvelope.java:150` |
| `AeronReplicationEnvelope.validate(...)` | 14 | same file |
| `AeronArchiveRetention` constructor | 14 | `node/aeron/AeronArchiveRetention.java:82` |
| `TransactionAssembler` constructor | 10 | `storage/aeron/reader/TransactionAssembler.java:144` |
| `BackupRestorePolicy` constructor | 8 (six are mixed `Supplier`/`Consumer`/`Runnable`) | `node/backup/BackupRestorePolicy.java:42` |
| `AeronReaderWatermark.validateFields(...)` | 6 | checkpoint code |

`BackupRestorePolicy` is the worst readability case: six unlabelled callback
parameters (`deleteDirectory`, `closeCursorManager`, `deleteOffsetFile`, …) can be
transposed at any call site without a compile error.

**Recommendation:**
- Envelope: a record `FrameHeader(clusterId, epoch, fencingToken, wireNonce,
  sequence, kind, payloadLength, chunkIndex, chunkCount, chunkOffset, commitCrc32c,
  payloadLength...)` — but instantiate it **once** and reuse it as a mutable
  framer-owned staging object (aggregated or preallocated), not per frame, to
  respect rule 29 (no per-message heap allocation on the hot path).
- `RetentionContext` record grouping the 14 constructor inputs (most are immutable
  collaborators/ids).
- `RestoreActions` record grouping `deleteDirectory`/`closeCursorManager`/
  `deleteOffsetFile`/`storageParentPath` — turns 8 mixed params into
  `transport + positionProvider + cursorSuppliers + RestoreActions + flag`.

### F-07 — Implementation class and nullable boxed values leak into the exported settings API

- `api/NodeSettingsSource.java` (exported package): `class Env implements
  NodeSettingsSource` at line 144 (with `EnvKeys` at line 346) puts the concrete
  environment reader — legacy key maps, env logging — inside the API interface.
  Consumers of `peruncs.cluster.api` see implementation detail by construction.
- The contract returns nullable boxes where the value is a primitive with a known
  default: `default Integer gcIntervalMinutes()` (line 90),
  `default Integer backupIntervalMinutes()` (line 97),
  `Long dataMergerTimeoutMs()` / `dataMergerCachedDataLimit()` /
  `dataMergerApplyTimeoutMs()` (lines 128-139), `Long writerLeaseStalenessMillis()`
  (line 141). Two failure modes: NPE at every unboxing call site, and a caller
  that cannot tell "unset → use default" from "null means zero" without reading
  the javadoc for each method.

**Recommendation:** resolve defaults once during assembly and expose primitives on
the API (`int gcIntervalMinutes()` with the default baked in, or a settings record
already carrying resolved values). Move `Env`/`EnvKeys` to an internal package
(e.g. `peruncs.cluster.node.config`) and keep only a factory
(`NodeSettingsSource.environment()`) plus, if genuinely needed for advanced
embedding, the raw key constants — re-exported from the API type only as data.

### F-08 — Absence convention collides inside the exported status model: `null` vs `OptionalLong`

- `NodeStatus.replication` is `null` for unreplicated nodes
  (`api/NodeStatus.java:26-34`, produced at `api/ClusterNode.java:163-169`) —
  documented, but it is a `null` escaping an exported record.
- Its sibling `ReplicationStatus` uses `OptionalLong` *components*
  (`api/ReplicationStatus.java:31-33,51-52`) and its own class doc declares
  "Absence is the single documented sentinel: an [OptionalLong]" (lines 9-13) —
  which is then contradicted by the adjacent record's null sentinel.
- Two problems: (1) AGENTS rule 3 reserves `Optional` for method *input* params —
  as record components they push container handling onto every consumer; (2) two
  exported records in one package teach embedders two different absence idioms,
  one of which is a null (rule 3's spirit: no null/Optional ambiguity at API
  boundaries).

**Recommendation:** one convention, both records. Give `ReplicationStatus` an
explicit non-null sentinel — `ReplicationState.UNREPLICATED` with primitive
components and no `OptionalLong` — and have `NodeStatus.replication` always return
it instead of `null`. Consumers get a single non-null record, no Optional
components, no null checks.

### F-09 — Periodic housekeeping task can be silently cancelled by an escaped exception

- `NodeMaintenanceScheduler` schedules each task via `scheduleWithFixedDelay` with
  a wrapper that **rethrows** `RejectedExecutionException` when not closing
  (`node/NodeMaintenanceScheduler.java:145-147`). An exception escaping a
  `scheduleWithFixedDelay`/`scheduleAtFixedRate` runnable permanently cancels that
  periodic task — the JVM documents this, and the repo itself already knows it:
  `WriterFencingLease.java:404-406` and 417-423 wrap their periodic heartbeat for
  exactly this reason.
- Today the rethrow is unreachable only by an *implicit* invariant: `workers` is
  shut down strictly after `closing = true` (`close()`, lines 166-176), and the
  wrapper checks `!this.closing` before rethrowing. Any future shutdown path, a
  second executor, or a refactor that flips the order turns a transient rejection
  into a housekeeping task that never runs again — with no error beyond one logged
  exception.

**Recommendation:** in the wrapper, log and return instead of rethrowing (a
rejected tick during teardown is meaningless); the guarded `runGuarded` already
handles per-task failures. This removes an entire class of silent-schedule-death
for the cost of one branch.

### F-10 — Test/soak JVMs run with flags the README never tells consumers about

- `pom.xml` surefire/failsafe/soak argLines add
  `--add-modules jdk.incubator.vector --add-exports java.base/jdk.internal.misc=ALL-UNNAMED`
  at lines 254, 264, 576, 628.
- `README.md` Build/Use sections (lines 16-20 and the run instructions) state that
  every consumer JVM must enable only `--enable-preview`.
- If a dependency (most plausibly Eclipse Serializer's off-heap path) genuinely
  needs `jdk.internal.misc`, production consumers following the README will fail
  at runtime in ways the test suite never exercises. If the flags are vestigial
  from an earlier dependency version, they are dead weight that also masks which
  API the code actually depends on — the `--add-exports` in particular is a
  standing invitation to use JDK-internal APIs (AGENTS rule 30).

**Recommendation:** run the suite once with each flag removed to identify the
requiring class. If required: document the exact runtime flags in README's Build
*and* Use sections (they are deployment configuration, not test detail). If not:
delete them from all four argLines.

---

## LOW

### F-11 — Zero-assertion scratch test shipped under the Surefire pattern

`src/test/java/peruncs/cluster/storage/index/ColdVectorUpdateProbeTest.java`
(73 lines): class javadoc says "Scratch probe", **0 assertions**, `System.out`
reporting. It exercises repeated remove+add vector churn but asserts nothing — a
regression that drops every update still "passes".

**Recommendation:** convert to a real test — assert the `CountingVectorizer`
invocation count per refresh and that queries observe the post-refresh state
(both are already tracked in the class), or move it out of the `*Test` pattern if
it is genuinely a manual probe. The sibling `ClusterIndexProbeScratchTest`
(same package) does assert (6) and can serve as the template.

### F-12 — Test-only APIs living in production types

- `WriterFencingLease.suspendHeartbeatForTest()` (`node/aeron/WriterFencingLease.java:441-443`)
- `CrashHook.runWithHook(...)` (`storage/aeron/writer/CrashHook.java:29`, public)
- `AtomicFileWriter.runWithTestHook(...)` (`storage/io/AtomicFileWriter.java:50,58`)
- `FilesystemVolumeBackupBackend.runWithTestHook(...)` (`node/backup/FilesystemVolumeBackupBackend.java:47`)

The `ScopedValue` hook pattern is sound, but each production class carries its own
public/package test entry point, and `CrashHook` is reachable from any code on the
module path that can see the package.

**Recommendation:** consolidate into one package-private `TestHooks` type (or a
`CrashPoints` collaborator injected only by test fixtures) so production classes
keep zero test-shaped methods; document that hooks are inert when unbound.

### F-13 — Drafting debris and typos in shipped comments

- `storage/aeron/reader/TransactionAssembler.java:115-123` — the lock-order
  rationale contains a visible self-correction:
  *"the flush only parks on the barrier monitor… no — even the barrier monitor is
  released during the await."* This comment is the documented invariant behind
  `TransactionAssemblerFailureTest`; it must state one rule, not an abandoned edit.
  Rewrite to the final rule (which monitor, if any, is held across the park).
- `node/NodeMaintenanceScheduler.java:26` — *"Runs a task must fail consecutively
  before health degrades."* (ungrammatical; should state the threshold it
  documents, e.g. "A task must fail [FAILURE_THRESHOLD] consecutive runs before
  health degrades").
- `node/aeron/WriterFencingLease.java:417` — *"supression"* → *"suppression"*.

### F-14 — Formatting inconsistencies

- `storage/binary/StorageBinaryDataMerger.java:216` — the private constructor is
  emitted at **column 0** (`private StorageBinaryDataMerger(final Configuration
  configuration) {`), unindented relative to every other member.
- Mixed `///` doc-comment indentation across files: `BackupArchive.java` has 18
  doc lines starting at 8 spaces vs 124 at 4; `NodeSettingsSource.java` 41 vs 46;
  `AeronArchiveRetention.java:74-75` starts one block at 8 spaces and continues at
  4 (the same block switches indent mid-comment).

**Recommendation:** one formatter pass over `src/main` and a rule in the build
(style check) so doc indent drift cannot re-enter.

### F-15 — Fully qualified name in a field declaration (AGENTS rule 15)

`node/aeron/WriterFencingLease.java:382-383`:
`private final java.util.concurrent.atomic.AtomicReference<RuntimeException> heartbeatFailure`.
The file already imports `AtomicBoolean`/`AtomicLong`-family types elsewhere;
import `AtomicReference` and drop the FQN.

---

## Review-only checklist coverage

- Rule 1 (modern Java): virtual threads, `ScopedValue`, records, pattern matching,
  sealed-ish switch usage present; no `ThreadLocal` in `src/main`
  (only `ThreadLocalRandom`). No finding.
- Rule 19/21/22 (security, races, retries): zip extraction budgets/digest/path
  handling in `BackupArchive` verified sound (traversal-safe, declared-size +
  dry-run budgets, digest-order enforcement); bounded retries/deadlines present in
  reader reconnect and offer paths; no finding beyond F-04/F-09.
- Rule 28 (cluster constraints): single-writer enforced by `WriterFencingLease`
  + fencing token floor in `TransactionAssembler`; no auth/transport-encryption
  features present (as required). No finding.
- Rule 29 (hot-path allocations): writer framing and reader assembly operate on
  direct buffers with a reused envelope view (`decodeView`,
  `TransactionAssembler.java:238`); `.formatted(...)` appears only on error paths.
  No finding.
- Rule 32 (Lucene/JVector coverage): external-directory rejection enforced and
  tested (`ClusterIndexValidation`; `ClusterStoreIndexesTest` 70 assertions,
  `StoreIndexReflectionTest`, probe tests); soak asserts index queries. No finding
  beyond F-03.
- Test suite: 98 unit tests + 11 ITs, no `@Disabled`; assertion density is healthy
  everywhere except F-11.
