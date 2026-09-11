# Aeron crash-matrix testing plan

Status: Tier-1 state-machine checks, the fork/kill atomic-file check, the
provider-level Archive/MediaDriver process matrix, the independent Store
fixture, diagnostics collector, directory layout, and frame-level Archive
inspector, reader-process cells, and the external-Archive process cell are
implemented. A provider cell is green only when the controller captures a
CRC-validated milestone, kills the child, restarts the same directories, and
verifies the restart policy plus checkpoint and Store-fixture evidence. The
current provider matrix contains twenty-two embedded-Archive writer cells
(nineteen RESEED_REQUIRED cells and three CONTINUE cells), including three
ENQUEUE_THEN_ARCHIVE cells, one deterministic double-crash recovery cell, and
two external-Archive cells.
Version-checked catalog/recording-corruption cells, final-frame truncation,
stale-active-catalog refusal, and atomic production MessageInfo replacement
are implemented. The provider subscriber currently acts only as a transport
sink; a full writer-child/reader-child orphan-tail observation is deferred and
must not be described as a green end-to-end cell. A seeded process-kill soak is
also available through the crashmatrix profile and is configurable for larger
nightly runs.

This is an implementation plan, not a claim that the complete matrix is green.
Only cells explicitly marked implemented in the tables below are green. Every
other cell remains `PLANNED`, `BLOCKED`, or `RED` and must stay visible in CI
reporting; it must not be converted into a passing smoke test.

This document defines the crash tests required before the Aeron integration can
claim loss-free restart behavior. The invariant is strict:

> After a crash, every committed sequence is replayed exactly once, or startup
> fails closed with `RESEED_REQUIRED`. A restart must never silently skip,
> duplicate, or republish a sequence with different data.

The document is the implementation contract. Every section names the concrete
classes, methods, files, and behaviors a coding agent must produce or assert.
Do not weaken the invariant; if a section names a behavior that the current code
cannot satisfy, implement the feature listed under "Features required before the
matrix can be green" first.

## Scope and topology

The current provider starts `ArchivingMediaDriver` with the writer. Therefore
the default process-crash topology is:

1. controller/test JVM;
2. writer JVM containing the provider, Archive, and Media Driver;
3. optional reader JVM.

Consequence: killing the writer also kills the embedded Archive and Media
Driver. The Archive is recovered from its catalog on the next writer start.
Cells that kill the Archive while the writer keeps running use the separate
external-Archive fixture; they must never simulate that topology by killing an
embedded runtime.

The process topology is intentionally two-tiered:

* **Tier A (shipped topology):** one writer JVM owns the Media Driver and
  Archive; the controller kills that JVM and starts a new writer against the
  same directories.
* **Tier B (external Archive, F4):** an Archive/Media Driver JVM is started
  separately and the writer connects through configured control channels. The
  writer uses `SourceLocation.REMOTE` for recording and extension because a
  separate Archive cannot use the local-driver spy subscription. This is the
  only valid topology for testing Archive loss while the writer stays alive.
  Never share a Media Driver directory between JVMs; only Aeron control, replay,
  and live channels are shared.

Tier B is selected by `ECLIPSE_DATAGRID_AERON_EXTERNAL_ARCHIVE=true`. Its
process fixture owns separate Archive and Media Driver directories. The checked
in `ExternalArchiveCrashIT` is the first Tier-B row; additional external cells
must retain the same topology and source-location rules.

The external fixture launch contract is:

```text
ArchiveProcessMain:
  -Ddg.archive.base=<work>
  -Ddg.archive.controlChannel=aeron:udp?endpoint=localhost:<archive-control-port>
  -Ddg.archive.replayChannel=aeron:udp?endpoint=localhost:0
  -Ddg.archive.reseed=<false|true>

ProviderCrashChildMain:
  -Ddg.crash.externalArchive=true
  -Ddg.crash.controlPort=<archive-control-port>
  -Ddg.crash.livePort=<writer-live-port>
```

The writer uses `<work>/writer-aeron` and the Archive uses
`<work>/archive-aeron`; neither process may launch an `ArchivingMediaDriver`
against the other's directory. `reseed=true` deliberately creates a fresh
Archive catalog after a hard Archive kill. That cell proves the writer refuses
to silently bind a replacement recording. The stale-catalog cell preserves the
original catalog and requires an active Archive mark file to be surfaced as a
machine-readable `RESEED_REQUIRED`; it does not claim transparent continuation
of an active recording.

Every test uses fresh, isolated directories (one per test cell):

- `<work>/aeron/` — Media Driver state;
- `<work>/archive/` — Archive catalog and recordings;
- `<work>/checkpoint/` — writer checkpoints;
- `<work>/store/` — local Store data;
- `<work>/cursors/` — reader cursor/offset files;
- `<work>/control/` — milestone + release files (see "Control protocol").

Tests must not depend on execution order. Two tests never share a `<work>` dir.

The writer checkpoint is a 108-byte fixed record. In addition to identity,
sequence, recording position, and CRC32C it stores `dataLength` and
`dataChunkCount` for the represented transaction. During PREPARING/ENQUEUED
transitions the same format is written to `<checkpoint>.inflight`; the terminal
COMMITTED/REJECTED record is written to the main path and then the in-flight
file is deleted and its parent directory forced. On startup an in-flight file
without a covering terminal record is `RESEED_REQUIRED`; a stale in-flight file
covered by a newer terminal record is removed. This is the durable local-store
fence used by the F2 cells.

A local Store rejection that occurs before Aeron reserves a sequence is not a
replication transaction. It deletes only the in-flight fence and does not write
a synthetic `REJECTED` checkpoint; advancing the terminal sequence in that case
would create a reader-visible gap.

## Test source layout and Maven gating

The test support is split by artifact so package-private seams never become
cross-module API. The provider process child/controller, reader process
child/controller, and independent Archive/Store/diagnostic helpers are checked
in. Live-reader orphan-tail coverage is in the reader unit suite; a full
writer-child/reader-child overlap remains optional topology coverage:

```text
storage/distributed/aeron/src/test/java/
  org/eclipse/datagrid/storage/distributed/aeron/crashtest/
    CrashPoint.java
    CrashBarrier.java              // shared blocking barrier used by process cells
    ChildMilestone.java              // CRC-protected binary milestone value
    CrashOutcome.java                // strict typed outcome parser
    RecordingInspector.java          // implemented independent frame scanner
    ArchiveArtifactMutator.java      // version-checked segment/catalog mutations
    AeronCrashChildMain.java
    AeronCrashMatrixIT.java
    ReaderMilestone.java             // CRC-protected reader barrier value
    ReaderCrashChildMain.java
    AeronReaderCrashMatrixIT.java    // production Archive-reader process cells

cluster/nodelibrary/aeron/src/test/java/
  org/eclipse/datagrid/cluster/nodelibrary/types/crashtest/
    DirectoryLayout.java             // isolated roots and reserved ports
    StoreFixture.java                // CRC-protected Store oracle
    DiagnosticCollector.java         // failure artifact collector
    RecoveryPolicy.java              // provider-child recovery outcome policy
    ProviderCrashChildMain.java      // real provider child
    ProviderCrashMatrixIT.java       // real controller
    ArchiveProcessMain.java           // external Archive child
    ExternalArchiveCrashIT.java       // writer-alive/Archive-killed cell
```

`AeronCrashBoundaryTest` exercises publisher/coordinator/distributor seams
directly. `AeronCrashChildMain`/`AeronCrashMatrixIT` prove the forced-temp/rename
filesystem boundary. `ProviderCrashChildMain`/`ProviderCrashMatrixIT` construct
the real `Transport`, embedded Archive and Media Driver, then exercise provider
restart and health/reseed behavior. The provider controller currently verifies
the fixed milestone, restart outcome, checkpoint fields emitted by the child,
and the CRC-protected Store fixture. `RecordingInspector` is exercised by
`AeronArchiveReplicationIT`; `StoreFixture`, `DirectoryLayout`, and
`DiagnosticCollector` are used by the provider process matrix. Neither child
widens production visibility:
the provider child installs package-private Aeron hooks reflectively from the
test class path. Tier-1 tests run under Surefire; real provider process tests
run under Failsafe with the dedicated `crashmatrix` Maven profile and are not
part of ordinary CI.

The Aeron `wire` package is intentionally module-internal and is not exported
by `storage/distributed/aeron`; its public-for-same-module codec types are not
an external API. Keep frame decoding in `RecordingInspector` inside the test
module. Export the wire package only if a separately versioned diagnostic or
replay tool becomes a real requirement.

`AeronReaderCrashMatrixIT` launches `ReaderCrashChildMain` against the parent
Archive/Media Driver and covers `REPLAY_BEFORE_FIRST_IMPORT`,
`DURING_STORE_IMPORT`, and `AFTER_STORE_IMPORT_BEFORE_CURSOR_WRITE`. The child
uses `ReaderDeliveryListener`, persists a CRC-protected cursor, and leaves the
reader uncertainty marker in place when the parent kills it. These are process
cells, not assembler-only tests.

The provider `crashmatrix` profile includes `ProviderCrashMatrixIT` and
`ExternalArchiveCrashIT`, sets a 5-second
forked-process exit timeout, and passes overridable budgets as system
properties. Current commands (the checked-in tests) are:

```text
mvn -pl storage/distributed/aeron,cluster/nodelibrary/aeron -am test
mvn -pl storage/distributed/aeron -am verify
```

The provider matrix is run with:

```text
mvn -pl cluster/nodelibrary/aeron -am verify -Pcrashmatrix
```

That profile must include exactly `ProviderCrashMatrixIT` and
`ExternalArchiveCrashIT`, disable parallel forks, and must not be active in the
ordinary `test` lifecycle. The reader process matrix remains in the Aeron
module's normal integration-test set because it owns the parent Archive/driver
fixture; it is not silently pulled into the provider profile.

The checked-in profile is the executable version of that contract: it uses
Failsafe `integration-test`/`verify`, includes only those two provider classes,
sets `forkedProcessExitTimeoutInSeconds=5`, sets `<parallel>none</parallel>`,
and passes the five `crash.*` system properties listed below. Do not move these
tests into Surefire or enable parallel forks without first giving every child a
fully isolated Media Driver, Archive, checkpoint, Store, and port allocation.
The child JVM inherits the test class path and the profile's `argLine`; it is
not launched as a JPMS module because `cluster/nodelibrary/aeron` has no
`module-info.java`.

`-Dcrash.matrix.seed`, `-Dcrash.budget.startup`,
`-Dcrash.budget.milestone`, `-Dcrash.budget.archiveStop`, and
`-Dcrash.budget.cell` override the defaults. Crash tests run one fork per cell;
parallel forks are disabled because each child owns loopback ports and native
Aeron resources.

The seeded soak is controlled independently with
`-Dcrash.matrix.random.iterations` and `-Dcrash.matrix.random.seeds`. The
profile defaults to a three-iteration smoke sample; a nightly invocation can
use `-Dcrash.matrix.random.iterations=100 -Dcrash.matrix.random.seeds=3`.

## Recovery policies

```java
enum RecoveryPolicy
{
    CONTINUE,            // restart runs normally; no duplicates, no gaps
    REPLAY_FROM_ARCHIVE, // restart re-applies from the Archive tail (target only)
    RESEED_REQUIRED,     // startup fails closed with a diagnostic; no writes
    FAIL_CLOSED,         // any other terminal failure that refuses to continue
    HARNESS_ERROR        // the armed barrier was never reached; the cell is invalid
}
```

The provider child reports exactly one `OUTCOME=<RecoveryPolicy>` after
startup. The controller asserts that outcome for the cell. The existing
atomic-file smoke child reports `OUTCOME=baseline` only and is not a policy
result.

The policy is selected from observed durable state, not from the requested
crash point. Before asserting an outcome, the controller reads the checkpoint,
`.inflight` marker, recording stop position, and (when present) the reader
uncertainty marker. The following rules are mandatory:

* prepared dictionary/data frames without a terminal marker are not an
  ignorable tail; the current implementation requires `RESEED_REQUIRED`;
* a COMMIT offer whose recorded position was not acknowledged is ambiguous and
  must never reuse its sequence;
* a local Store write without a terminal Archive record is fenced by
  `.inflight` and requires `RESEED_REQUIRED`;
* a reader import that completed before its cursor became durable is uncertain;
  `.reader-inflight` therefore requires `RESEED_REQUIRED`. The current design
  makes no exactly-once claim for this window; an idempotent Store ledger is a
  future feature, not an implicit test assumption.

Target table (what the matrix asserts once green):

| Condition | Expected result |
|---|---|
| Crash before any publication or local write | `CONTINUE` |
| Valid committed checkpoint | `CONTINUE` |
| Incomplete prepared tail in the recording | `RESEED_REQUIRED` (the current stop-position fence rejects it; no automatic tail replay is promised) |
| Commit offered but Archive durability is unknown | `RESEED_REQUIRED` |
| `COMMITTING_UNCERTAIN` checkpoint | `RESEED_REQUIRED` |
| Torn checkpoint/cursor file | previous valid file is retained, or `RESEED_REQUIRED` |
| Archive-ahead-of-checkpoint | `RESEED_REQUIRED` (the startup stop-position fence rejects it) |
| Archive-confirmed commit with missing checkpoint | `RESEED_REQUIRED` until a validated tail scanner/replay feature is implemented |

`RESEED_REQUIRED` is a machine-readable contract, not a prose label. A writer
restart must expose `ReplicationHealth.State.RESEED_REQUIRED` and an
`IllegalStateException` whose message starts with `RESEED_REQUIRED:`. A child
restart reports the same value in `OUTCOME=RESEED_REQUIRED`; the controller
asserts all three where the provider is available. A malformed checkpoint,
identity mismatch, archive-tail fence, or uncertain reader marker must not
fall back to a generic `FAILED` state.

## Baseline vs. target (cell gating)

The table below is the current-code truth (`HEAD`, as of this document) so a
coder does not chase failing cells. "Blocked by" names a feature from the
"Features required" section. Cells not listed are expected to pass today.

| Cell / crash point | Current behavior | Target | Blocked by |
|---|---|---|---|
| Torn writer checkpoint (corrupt bytes) | read CRC fails → `RESEED_REQUIRED` | `RESEED_REQUIRED` | — |
| Torn cursor file | read CRC fails → rejected | previous retained or rejected | — |
| Restart with `COMMITTING_UNCERTAIN` | `RESEED_REQUIRED` (load rejects non-COMMITTED/REJECTED) | `RESEED_REQUIRED` | — |
| Extend identity mismatch (stream/term/MTU/recording) | throws → `FAIL_CLOSED` | `FAIL_CLOSED` | — |
| Archive stop position behind checkpoint | `RESEED_REQUIRED` (canonical check exists) | `RESEED_REQUIRED` | — |
| Crash before publication connects (`BEFORE_PUBLICATION_CONNECTED`) | no sequence or checkpoint exists; phase 2 may start a fresh recording | `CONTINUE` | implemented |
| Crash before publication prepare (`BEFORE_PREPARE`) | the coordinator has already persisted a PREPARING fence; phase 2 refuses to guess | `RESEED_REQUIRED` | implemented |
| Crash after data chunks (`AFTER_DATA_CHUNKS`) | restart compares Archive stop with the terminal checkpoint and rejects any orphan tail | `RESEED_REQUIRED` | implemented |
| Crash after dictionary chunks (`AFTER_DICTIONARY_CHUNKS`) | restart compares Archive stop with the terminal checkpoint and rejects any orphan tail | `RESEED_REQUIRED` | implemented |
| Crash after commit offer, ack unknown (`AFTER_COMMIT_OFFER`) | restart rejects an Archive tail beyond the terminal checkpoint | `RESEED_REQUIRED` | implemented |
| Crash after archive-recorded commit, before checkpoint (`AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT`) | restart rejects the archive-ahead/checkpoint-behind state | `RESEED_REQUIRED` | implemented |
| Crash after local write before commit (`AFTER_LOCAL_WRITE_BEFORE_COMMIT`) | durable `.inflight` fence remains; startup refuses to guess | `RESEED_REQUIRED` | implemented |
| Crash after local enqueue before prepare (`AFTER_ENQUEUE_BEFORE_PREPARE`) | durable `.inflight` fence remains before publication reserves a sequence | `RESEED_REQUIRED` | implemented |
| Crash after local rejection abort offer (`AFTER_ABORT_OFFERED`) | Archive has an abort boundary but the rejected checkpoint is not known durable | `RESEED_REQUIRED` | implemented |
| Failed prepare after publisher ABORT (`AFTER_PREPARE_FAILURE_ABORT_OFFERED`) | the partial publication is terminally aborted when possible; the failed prepare metadata is retained and the ENQUEUE path records `COMMITTING_UNCERTAIN` | `RESEED_REQUIRED` | implemented |
| Checkpoint temp write/rename phase (`DURING_CHECKPOINT_FILE_WRITE` and `AFTER_*CHECKPOINT_*`) | previous complete checkpoint or the newly renamed complete checkpoint remains; a partial temp file is never selected | previous valid record or `RESEED_REQUIRED` | implemented in Tier 1 |
| Reader: import done, cursor not persisted (`AFTER_STORE_IMPORT_BEFORE_CURSOR_WRITE`) | uncertain reader marker survives; restart refuses to guess | `RESEED_REQUIRED` | implemented |
| Reader import failure (`DURING_STORE_IMPORT_FAILURE`) | the injected import fails after the uncertainty marker is durable; cursor does not advance and restart refuses to guess | `RESEED_REQUIRED` | implemented in reader process child |
| Archive killed during commit wait | `ExternalArchiveCrashIT` kills the separate Archive after the commit offer; writer fails closed and recovery opens a fresh catalog | `RESEED_REQUIRED` | implemented |

The four reader rows above are now backed by `AeronReaderCrashMatrixIT`; the
test deliberately expects `RESEED_REQUIRED` rather than claiming idempotent
Store replay. The external Archive row is backed by `ExternalArchiveCrashIT`.
The independent catalog/recording mutation rows are backed by the storage
Archive IT and are green only when `ArchiveArtifactMutator` recognizes the
running Aeron artifact layout. They are not provider-process restart cells:
the provider controller cannot inspect the killed writer's embedded Archive
after SIGKILL.

## Test tiers

### Tier 1: deterministic in-process tests

Package-private test seams at existing boundaries. These run in normal CI under
surefire and must not fork.

Modules:

- `storage/distributed/aeron/src/test/java/.../crashtest/` — publisher, coordinator,
  distributor, `AtomicFileStore`, and reader-assembler windows.
- `cluster/nodelibrary/aeron/src/test/java/.../crashtest/` — provider restart,
  checkpoint/cursor identity, `ensureWriter`/`loadWriterCheckpoint` behavior.

The Tier-1 enum is shared with Tier 2, but the checked-in tests cover a focused
subset: prepared-tail abort, ambiguous commit, enqueue fencing, and
recorded-before-checkpoint uncertainty. The remaining publisher/coordinator/
checkpoint/assembler points are explicit expansion work. The barrier runs in
`THROW` mode
(throws the dedicated `CrashBarrier.SimulatedCrash` runtime exception), and the test asserts the resulting
state transition and cleanup: `failed`/`terminal` flags, no orphan artifacts,
correct checkpoint/cursor bytes, no history mutation. A thrown exception is not
a process kill; Tier 1 does not claim durability.

Existing tests to keep/extend: `AeronReplicationWriteCoordinatorTest`
(uncertain-marker dimensions + CRC), `AeronReplicationCheckpointStoreTest`
(torn file), `AeronReplicationPublisherTest` (prepare failure), plus new ones
named `*CrashTest` for the windows above.

### Tier 2: real process-crash tests

- Module: `cluster/nodelibrary/aeron` with Maven Failsafe (the selected
  `*IT.java` include); target profile `verify -Pcrashmatrix`.
- Parent (controller): `ProviderCrashMatrixIT`.
- Child main (test sources of the same module, launched via
  `System.getProperty("java.class.path")`):
  `org.eclipse.datagrid.cluster.nodelibrary.types.crashtest.ProviderCrashChildMain`.

`cluster/nodelibrary/aeron` is currently an unnamed/class-path Maven artifact;
it has no `module-info.java`. The provider child must therefore be launched
with the test class path, as shown above, not with `--module-path`. The Aeron
storage artifact is the named module; its exports do not imply that the
provider artifact is modular. Adding a provider module descriptor is a
separate migration and is not a prerequisite for this harness.

Child launch (per cell, two invocations):

```
<JAVA_HOME>/bin/java \
  -cp <System.getProperty("java.class.path")> \
  -Ddg.crash.barrier=<CrashPoint> \
  -Ddg.crash.mode=<phase1|phase2> \
  -Ddg.crash.role=<writer|reader> \
  -Ddg.crash.base=<workDir> \
  org.eclipse.datagrid.cluster.nodelibrary.types.crashtest.ProviderCrashChildMain
```

Child lifecycle:

- phase1: start provider, run the scenario (see "Determinism"), block at the
  first barrier hit for the *target* transaction.
- controller observes the milestone for the target transaction and calls
  `Process.destroyForcibly()` (SIGKILL). Normal cleanup APIs must never be
  called on the path under test.
- controller then waits (bounded) until the killed Archive reports a stop
  position for the killed recording (see "Restart rule").
- phase2: restart the child in the same `<work>`, with no barrier
  (`-Ddg.crash.mode=phase2`, barrier unset). It must either agree with the
  cell's expected `OUTCOME` or run the post-scenario proof (below) and stop.

The child must never self-terminate. Do not use stdout as synchronization;
capture stdout/stderr only as diagnostics. `Runtime.halt` is reserved for a
disposable, explicitly separate platform experiment; it is not used by the
checked-in child. Tier 2 uses parent `Process.destroyForcibly()`/SIGKILL so the
child cannot mask the crash with shutdown hooks or self-termination.
The provider child currently writes `control/milestone.reached` (one fixed
record at a time) and waits for `control/release`, although the controller
uses the deterministic kill path and does not release it. Future cells that
need to continue past a barrier must use that same release file; they must not
introduce a second protocol.

The parent-side kill protocol is normative: wait for a CRC-valid milestone,
call `Process.destroyForcibly()`, wait for process exit, and only then start
recovery. Checked-in children must not call `Runtime.halt()` or
`System.exit()`; those actions model a different failure and can mask a
controller bug. A self-halt experiment, if added later, is separate evidence
and cannot make a matrix cell green.

Budgets (overridable via `-Dcrash.budget.*`, milliseconds):

- startup (driver+archive+store): 120_000
- milestone arrival after scenario start: 60_000
- archive-stop poll after kill: 30_000 (see "Restart rule")
- total per cell: 600_000

On any timeout: dump child stdout/stderr, milestone file, `<work>` directory
tree, and fail the cell with the diagnostics path in the message.

`ProviderCrashMatrixIT` implements the first part of that controller sequence:
create and clean `<work>`, allocate loopback ports, start `ProcessBuilder`, redirect
stdout/stderr to diagnostic files, wait for `ready`, wait for the exact
milestone, kill and await exit, retry phase2 while the embedded driver is still
active, and parse `OUTCOME` plus the child proof fields. If any wait expires,
the controller kills the child if necessary, collects evidence, and fails with
the last milestone and artifact directory in the assertion message. The
provider child intentionally reports only the structural Store proof; the
independent frame scanner runs in the storage Archive IT because the provider
artifact is not a named module and does not export the wire package.

## Determinism

- One cell = two transactions minimum: txn #1 commits normally (creates the
  baseline checkpoint), txn #2 reaches the target `CrashPoint` and is killed.
  This ensures every writer cell exercises sequence continuation and the
  archive-tail boundary.
- Payload is a pure function of the sequence: 64 bytes = the repeated SHA-256
  digest bytes of the UTF-8 string `dg-crash:<sequence>` used by the checked-in
  provider child. The
  controller recomputes the expected payload and CRC32C without a plan file.
- The child scenario is: write txn #1, then txn #2. After `OUTCOME` on phase2,
  write one follow-up txn and stop cleanly.
- Type-dictionary cells insert a dictionary update before txn #2.
- Ports are reserved by the controller with loopback `ServerSocket(0)` handoff
  and recorded in the child configuration; never derive ports from the test
  index without checking availability. The Linux process-kill path is the
  required CI target; other operating systems may run Tier 1 only until their
  process/file-lock semantics are verified.

## Control protocol

`<work>/control/` files:

- `ready` — child startup marker;
- `milestone.reached` — fixed-width binary `ChildMilestone` containing
  magic/version, crash-point ordinal, sequence, monotonic timestamp, and
  CRC32C. Archive identity and position are reported separately in the
  typed outcome/checkpoint evidence;
- `milestone.reached.tmp-*` — ignored orphan temp files;
- `release` — reserved for future cells that must survive a barrier; Tier 2
  currently kills the child instead.

The child writes the milestone inline on the executing thread using a bounded
buffer, `force(true)`, and atomic rename. The controller deletes stale
milestones before launch, polls every 10 ms, validates the CRC and expected
point/sequence, and waits at most 60 seconds for the exact marker. A missing or
unexpected marker is a test failure with diagnostics, not an implicit kill.

The controller protocol is deliberately a blocking barrier, not a progress
log. The child must stop at the hook until either the parent creates `release`
or the parent kills it. A child must never call `Runtime.halt`, `System.exit`,
or throw a simulated crash to terminate itself in a Tier-2 test; those paths
run cleanup code or make the kill boundary ambiguous. The parent is the sole
owner of `destroyForcibly()`. Exit codes are diagnostic only and are never used
as the recovery assertion.

The required controller sequence is:

```text
delete stale ready/milestone/release/outcome files
allocate loopback ports and pass them to the child; the child must fail fast
if binding is unavailable
start child phase 1
await ready (startup budget)
await exact CRC-valid milestone (milestone budget)
snapshot checkpoint, inflight marker, cursor, and directory listing
destroyForcibly child; await exit (exit status is diagnostic)
start recovery using the same identity/directories and no barrier
await outcome (startup budget)
assert policy + health prefix + Store/archive evidence
retain artifacts on failure; remove the work directory only after success
```

The checked-in provider controller implements the phase-1/phase-2 process
sequence and the child implements the ready, barrier, release, and outcome
files. The child installs package-private `BiConsumer<String,Long>` hooks by
reflection. The writer seams, provider recovery seam, and checkpoint file seam
delegate to package-private,
thread-confined `CrashHook` registry; hooks are never inherited by another
thread and are explicitly cleared by the child. This keeps fault injection out
of the public API and prevents parallel tests from sharing mutable callbacks.
New hooks should first add a Tier-1 seam in the owning package and only then be
wired to the process protocol. Do not add a public crash-test API or make the
provider test depend on Aeron test classes across modules.

The provider-matrix implementation uses this milestone schema:

```text
magic:u32, version:u16, point:u16, sequence:i64,
timestampNanos:i64, crc32c:u32
```

The provider hook observes only the point and sequence. The authoritative
recording identity and position are read from the checkpoint and Archive
catalog during evidence collection.

The reader child uses a separate `ReaderMilestone` record that adds the
recording position to the same CRC-protected, forced-temp/rename protocol. It
is intentionally not decoded as a provider `ChildMilestone`.

The checked-in provider child writes
`<control>/milestone.reached.tmp-*`, forces the file and its parent directory,
atomically renames it to `<control>/milestone.reached`, then parks until
`<control>/release` appears. The parent removes stale files before launch,
polls the exact expected marker every 10 ms for the milestone budget, validates
magic/version, sequence/point and CRC, and then calls `destroyForcibly()`.
The child never self-kills. A milestone timeout kills the child, collects
evidence, and fails the cell; it is never treated as a normal recovery outcome.
The per-point filename form is a future multi-barrier extension only; it must
retain this fixed record schema and atomic-write protocol.

The checked-in `AeronCrashMatrixIT` kills the child between the forced
temporary-file write and rename and verifies that the previous value is still
selected. The provider child uses the fixed-size binary `ChildMilestone`
record with magic/version/point/sequence/timestamp/CRC32C and the same
forced-temp/rename/parent-force protocol. Recording identity and position are
not milestone fields; they are read from the validated checkpoint and Archive
evidence after restart. The production `AtomicFileStore`
path is covered separately by `AtomicFileStoreTest`.

The child receives `-Ddg.crash.barrier=<CrashPoint>`,
`-Ddg.crash.mode=<phase1|phase2>`, and `-Ddg.crash.base=<work>`. The parent
waits at most 5 seconds for forced process exit and 30 seconds for Archive
stop-position recovery. Test offer/catch-up timeouts are overridden to 2–5
seconds so a broken cell does not consume the production 30-second timeout.

The controller contract is concrete rather than an informal callback. The
checked-in `ProviderCrashMatrixIT` keeps these operations inline; extraction
into the following package-private helper is optional:

```java
final class CrashController implements AutoCloseable
{
    ChildMilestone await(Path controlDir, CrashPoint expected, Duration timeout);
    void kill(Process child);                         // destroyForcibly only after exact marker
    void awaitExit(Process child, Duration timeout);
    CrashTestCaseResult collect(Path work, Process child);
}
```

`ProviderCrashChildMain` reads the crash point, mode, base directory, stream
and port properties from system properties. In phase 1 it starts the real
provider, writes `ready`, executes the deterministic two-transaction scenario,
and blocks at the one armed point. In phase 2 it starts without a barrier,
emits `OUTCOME`, `SEQUENCE`, recording/checkpoint fields, and `PROOF_*` lines,
runs the independent verification callback, emits `HEALTH`, and exits zero
only for a clean outcome. A startup
refusal is a normal phase-2 result when its machine-readable policy is
`RESEED_REQUIRED`; a child that hangs or emits no outcome is a test failure.

Barrier semantics (both tiers):

1. perform the side effect of the crash point;
2. write + fsync the fixed milestone for that point;
3. block: Tier 1 throws `CrashBarrier.SimulatedCrash`; Tier 2 parks on the current
   thread until the parent kills the child.

The milestone must be written inline on the executing thread, immediately
between the side effect and the block — never from a reporter thread, never
before the side effect.

### Required barrier seams

The implementation must install one package-private callback per production
package. The callback is a no-op when unset and must not allocate or lock in
the normal path. The writer seams are:

* `AeronReplicationPublisher.prepareTransaction`: before prepare, after
  dictionary chunks, after data chunks, and after the publisher's failed-
  prepare ABORT offer. The last point is the `prepareTransaction` catch path:
  `markEnqueueWithoutArchive()` does not publish an ABORT; it persists
  `COMMITTING_UNCERTAIN` after the publisher has already attempted that ABORT;
* `AeronReplicationPublisher.commit`: after COMMIT offer and after the
  recorded-position wait (the internal Tier-1 name is
  `AFTER_COMMIT_RECORDED`; the provider-level name is
  `AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT`);
* `AeronReplicationWriteCoordinator.commit`/`abort`:
  recorded-before-checkpoint and after ABORT-before-REJECTED-checkpoint;
* `AeronStorageBinaryTargetDistributing.write`: after prepare-before-local
  write, after `delegate.write(data)` but before `markEnqueued`/commit, and
  after local enqueue-before-prepare. The exact order is
  `prepare -> delegate.write -> markEnqueued -> commit` for `ARCHIVE_FIRST`,
  and `delegate.write -> prepare` for `ENQUEUE_THEN_ARCHIVE`;
 * provider checkpoint persistence: before temp write, after temp force before
   rename, after rename before directory force, and (Tier 1 only) after the
   durable checkpoint write before the in-memory `writerCommittedSequence`
   assignment;
 * `AeronClusterReplicationTransportProvider.ensureRuntime`: before the
   driver/Archive launch (`BEFORE_PUBLICATION_CONNECTED`), and after
   checkpoint validation before recording extension
   (`AFTER_RECOVERY_CHECKPOINT_READ`);
 * `TransactionAssembler.Delivery.run`: before import, during import, and
  after import before cursor advancement.

`AtomicFileStore` is shared by writer checkpoints and the neutral
`ReplicationCursorStore` utility, but not by the production
`StoredMessageInfoManager` MessageInfo path. It provides a
`write(Path, Encoder, String)` overload that accepts a phase name
(`"CHECKPOINT"` or `"CURSOR"`), which selects the corresponding hook
names (`BEFORE_CHECKPOINT_TEMP_WRITE`, `DURING_CHECKPOINT_FILE_WRITE`,
etc. and `BEFORE_CURSOR_TEMP_WRITE`, `DURING_CURSOR_FILE_WRITE`, etc.).
The original `write(Path, Encoder)` overload uses generic names.
The checkpoint store passes `"CHECKPOINT"` and the cursor store passes
`"CURSOR"`. Install only the single armed hook for a test run. It
must execute on the file-writing thread, after the named side effect
and before the next operation. A process kill is not a substitute for
this hook because syscall timing cannot reliably target a rename or
directory-force boundary.

The filesystem phases map one-to-one to crash points. `BEFORE_TEMP_WRITE` is
`BEFORE_CHECKPOINT_TEMP_WRITE`/`BEFORE_CURSOR_TEMP_WRITE`;
`DURING_FILE_WRITE` is `DURING_CHECKPOINT_FILE_WRITE`/`DURING_CURSOR_FILE_WRITE`;
`AFTER_TEMP_WRITE_BEFORE_RENAME` is the corresponding
`AFTER_*_TEMP_WRITE_BEFORE_RENAME`; and `AFTER_RENAME_BEFORE_DIRECTORY_SYNC`
is the corresponding `AFTER_*_RENAME_BEFORE_DIRECTORY_SYNC`. Install only the
single armed hook for a test run. It must execute on the file-writing thread,
after the named side effect and before the next operation. A process kill is
not a substitute for this hook because syscall timing cannot reliably target a
rename or directory-force boundary.

## Crash points and hook sites

The Aeron module owns the canonical `CrashPoint.java` test enum. The provider
module cannot compile against Aeron test sources, so its process protocol uses
the same names as strings and `ChildMilestone` validates its own supported
subset. Any name added to one module must be added to the other in the same
change, or the provider profile must fail with an explicit unsupported-point
diagnostic rather than silently skipping the barrier. Add or extend the enum
in the `crashtest` test-support package; do not export it from production:

```java
enum CrashPoint
{
    BEFORE_PUBLICATION_CONNECTED,          // Tier-1 no-subscriber/startup readiness check
    BEFORE_PREPARE,                        // coordinator.prepare(data) entry
    AFTER_DICTIONARY_CHUNKS,               // publisher.prepareTransaction: after TYPE_DICTIONARY chunks
    AFTER_DATA_CHUNKS,                     // publisher.prepareTransaction: after publishDataChunks, before PreparedTransaction
    AFTER_PREPARE_BEFORE_LOCAL_WRITE,      // distributor.write (ARCHIVE_FIRST): after prepare, before delegate.write
    AFTER_LOCAL_WRITE_BEFORE_COMMIT,       // ARCHIVE_FIRST: after delegate.write, before markEnqueued/commit
    AFTER_ENQUEUE_BEFORE_PREPARE,          // distributor.write (ENQUEUE_THEN_ARCHIVE): after delegate.write, before prepare
    AFTER_PREPARE_FAILURE_ABORT_OFFERED,   // prepare catch: after publisher ABORT offer; not markEnqueueWithoutArchive
    AFTER_COMMIT_OFFER,                    // publisher.commit: after COMMIT offerMarker, before awaitRecorded
    AFTER_COMMIT_RECORDED,                 // publisher.commit: after awaitRecorded, before return
    AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT,// coordinator.commit: after publisher.commit returns, before COMMITTED listener
    AFTER_ABORT_OFFERED,                    // coordinator.abort: after ABORT offer, before REJECTED checkpoint
    DURING_COMMITTING_UNCERTAIN_WRITE,     // coordinator.markCommittingUncertain: before listener (checkpoint write)
    DURING_CHECKPOINT_FILE_WRITE,           // partial temp-file write
    BEFORE_CHECKPOINT_TEMP_WRITE,           // checkpoint store before temp creation
    AFTER_CHECKPOINT_TEMP_WRITE_BEFORE_RENAME, // checkpoint after force, before move
    AFTER_CHECKPOINT_RENAME_BEFORE_DIRECTORY_SYNC, // checkpoint after move
    AFTER_CHECKPOINT_WRITE_BEFORE_COMMITTED_SEQUENCE_UPDATE, // durable file; in-memory derived state is stale
    REPLAY_BEFORE_FIRST_IMPORT,            // reader assembler: terminal accepted, before receiver import
    DURING_STORE_IMPORT,                   // reader Delivery.run: around receiver.receiveData
    DURING_STORE_IMPORT_FAILURE,           // injected Store import exception
    AFTER_STORE_IMPORT_BEFORE_CURSOR_WRITE, // reader: after import returns, before cursor advance/persist
    DURING_CURSOR_FILE_WRITE,              // partial cursor temp write
    AFTER_CURSOR_TEMP_WRITE_BEFORE_RENAME, // cursor after force, before move
    AFTER_CURSOR_RENAME_BEFORE_DIRECTORY_SYNC,
    AFTER_RECOVERY_CHECKPOINT_READ        // provider-child recovery seam
}
```

Current Tier-2 coverage is intentionally smaller than this full enum. The
checked-in provider child schema can encode a subset, and the checked-in
provider tests exercise:
`BEFORE_PUBLICATION_CONNECTED`, `BEFORE_PREPARE`, `AFTER_DICTIONARY_CHUNKS`, `AFTER_DATA_CHUNKS`,
`AFTER_PREPARE_BEFORE_LOCAL_WRITE`,
`AFTER_LOCAL_WRITE_BEFORE_COMMIT`, `AFTER_ENQUEUE_BEFORE_PREPARE`,
`AFTER_PREPARE_FAILURE_ABORT_OFFERED`, `AFTER_COMMIT_OFFER`,
`AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT`, `AFTER_ABORT_OFFERED`, and
`DURING_COMMITTING_UNCERTAIN_WRITE`. The additional commit boundaries
`AFTER_PREPARE`, `BEFORE_COMMIT_OFFER`, and `AFTER_COMMIT_RECORDED` are also
covered by the provider matrix. Checkpoint write phases are now wired to
the provider child through the neutral `AtomicFileStore` seam and have
dedicated terminal-checkpoint cells (the `.inflight` fence is intentionally
excluded), and
`AFTER_RECOVERY_CHECKPOINT_READ` is emitted after checkpoint validation. The
reader child also exercises `DURING_STORE_IMPORT_FAILURE`. Cursor sub-phases
remain fixture-only until the reader uses the production cursor store in this
process harness; adding an enum constant alone does not make a cell
implemented. Every unsupported provider point must fail before the child
starts its scenario.

Hook SPI for new seams (package-private; do not touch public API). The current
writer seams use the shared `CrashHook` implementation below. It is
thread-confined deliberately: a test must install and clear it on the same
thread that executes the hooked Store operation.

```java
// package-private production seam, one adapter per production package
final class CrashHook
{
    private static final ThreadLocal<BiConsumer<String, Long>> CURRENT = new ThreadLocal<>();

    static void install(final BiConsumer<String, Long> callback)
    {
        if (callback == null) CURRENT.remove();
        else CURRENT.set(callback);
    }

    static void clear() { CURRENT.remove(); }

    static void invoke(final String point, final long sequence)
    {
        final BiConsumer<String, Long> callback = CURRENT.get();
        if (callback != null) callback.accept(point, sequence);
    }
}
```

Production additions are limited to `CrashHook.invoke(P, seq)` at each hook
site; with no callback this is a thread-local null lookup and has no I/O or
allocation. Tier-1 tests install a throwing callback. The provider child
installs the milestone/park callback reflectively, while the Aeron-module
child uses the same file protocol directly. If an end-to-end provider cell
needs an Aeron micro-window, the crash profile runs on the class path with an
explicit test-only `--add-exports java.base/jdk.internal.misc=ALL-UNNAMED`
bootstrap; no production export is added.

Hook sites (class -> method -> point):

| Class | Method | Point |
|---|---|---|
| `AeronReplicationPublisher` | `prepareTransaction` | `BEFORE_PREPARE`, `AFTER_DICTIONARY_CHUNKS`, `AFTER_DATA_CHUNKS`, `AFTER_PREPARE_FAILURE_ABORT_OFFERED` |
| `AeronReplicationPublisher` | `commit` | `AFTER_COMMIT_OFFER`, `AFTER_COMMIT_RECORDED` |
| `AeronReplicationWriteCoordinator` | `commit` | `AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT` |
| `AeronReplicationWriteCoordinator` | `abort` | `AFTER_ABORT_OFFERED` |
| `AeronReplicationWriteCoordinator` | `markCommittingUncertain` | `DURING_COMMITTING_UNCERTAIN_WRITE` |
| `AeronStorageBinaryTargetDistributing` | `write` | `AFTER_PREPARE_BEFORE_LOCAL_WRITE`, `AFTER_LOCAL_WRITE_BEFORE_COMMIT`, `AFTER_ENQUEUE_BEFORE_PREPARE` |
| `AeronReplicationCheckpointStore` / `ReplicationCursorStore` | `write` | `CHECKPOINT`/`CURSOR` phase names passed to `AtomicFileStore.write`; generates `BEFORE_CHECKPOINT_TEMP_WRITE`, `DURING_CHECKPOINT_FILE_WRITE`, etc. and `BEFORE_CURSOR_TEMP_WRITE`, `DURING_CURSOR_FILE_WRITE`, etc. The provider child ignores `.inflight` paths for terminal-checkpoint cells. |
| `TransactionAssembler` | `Delivery.run` | `REPLAY_BEFORE_FIRST_IMPORT`, `DURING_STORE_IMPORT`, `DURING_STORE_IMPORT_FAILURE`, `AFTER_STORE_IMPORT_BEFORE_CURSOR_WRITE` (via `ReaderDeliveryListener`) |
| provider (nodelibrary) | `ensureRuntime` | `BEFORE_PUBLICATION_CONNECTED` (before driver launch), `AFTER_RECOVERY_CHECKPOINT_READ` |

Notes:

- The checkpoint store and the neutral cursor store share the same
  atomic-write phase contract; the milestone's sequence plus target path
  identify which store was writing. Production
  `StoredMessageInfoManager.NewAtomic` uses the generic phase because the file
  stores neutral MessageInfo rather than an Aeron cursor. The reader child also has a deliberately small
  fixture cursor for process-level replay tests; those fixture barriers do not
  by themselves prove production MessageInfo persistence.
  The provider child deliberately ignores generic atomic-file phases, so a
  MessageInfo write cannot be mistaken for a terminal writer-checkpoint cell.
- `DURING_CHECKPOINT_FILE_WRITE` and `DURING_CURSOR_FILE_WRITE` are Tier-1
  synthetic partial-write tests. The temp file must never replace the previous
  valid file. Rename-before-directory-sync is retained as a power-loss tier,
  because a SIGKILL process test cannot reliably stop between those syscalls.
  The reader matrix also arms all three cursor-phase barriers around its
  CRC-protected fixture cursor. Those process cells prove the uncertainty
  marker survives each cursor window; they do not replace a future cell that
  routes the provider's production cursor through the same boundary.
- The archive reader shares the same assembler as the live reader; hooking the
  assembler covers both.

The production `AtomicFileStore` may keep its callback-free API; Tier-1 tests
must inject a partial encoder failure or a package-private store delegate at
the same boundary. Tier-2 uses the child’s forced-temp/rename sequence. A
parent kill cannot reliably target a filesystem instruction boundary, so
process cells assert the atomic contract (old or new complete file, never a
partially replaced destination) rather than a specific instruction interleave.

The Aeron module and provider module each own their package-private hook
adapter. In the current implementation the provider child reaches the Aeron
writer hooks only through reflective test-classpath access; this is test-only
and is not a module export. The preferred end state is a package-private
adapter in each production package, with the same callback shape and no
reflection in the child. Publisher/assembler micro-windows are tested by the
Aeron child, while provider restart/health windows are tested by the provider
child.

Barrier rules are normative:

1. Invoke the hook immediately after the named side effect and before the next
   state mutation (`AFTER_COMMIT_OFFER` is before `awaitRecorded`, and
   `AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT` is before the COMMITTED listener).
   `BEFORE_PREPARE` is reached after the coordinator has written its PREPARING
   fence; it is therefore a conservative restart boundary, not a promise that
   no local state exists.
2. Write and force the milestone on the executing thread, then block on the
   release file. Never report from a helper thread or use sleeps to hit a
   window.
3. The unarmed production path is one volatile null check: no allocation,
   locking, logging, or file I/O.
4. Each hook has a Tier-1 throwing test and a Tier-2 blocking process test, or
   is explicitly marked `TIER1_ONLY` when an OS process boundary cannot observe
   it.
5. A missing or malformed milestone is a harness failure, not a recovery
   outcome; collect evidence before killing the child.

Do not add a separate process-recovery cell between the terminal checkpoint
write and the assignment to `writerCommittedSequence`: that value is in-memory
derived state and is reconstructed from the checkpoint on restart. A Tier-1
unit test may assert assignment order, but no crash policy depends on that
instruction; the enum value above documents this no-op boundary rather than
requiring a provider process test.

## Restart rule (mandatory)

After `destroyForcibly()`, the recovered Archive may still consider the killed
recording "active" until it re-reads the catalog. Before starting phase2:

```java
// poll, bounded by crash.budget.archiveStop (30s)
assertUntil(deadline,
    () -> archive.getStopPosition(recordingId) >= 0,
    "recording " + recordingId + " not stopped after writer kill");
```

Only then start the child. Without this the writer's `Extend` may throw
"recording is still active" nondeterministically. The recordingId is read from
the phase-1 checkpoint (see `loadWriterCheckpoint`) or, when absent, from the
Archive catalog by stream/session.

The current `ProviderCrashMatrixIT` cannot query the killed child's embedded
Archive control endpoint after SIGKILL; it retries phase 2 when Aeron reports
an active driver and treats successful startup as the stop-position recovery
signal. The external `ExternalArchiveCrashIT` can kill the Archive independently,
then deliberately starts a fresh Archive catalog (`deleteArchiveOnStart=true`)
and asserts that the writer's existing recording identity produces
`RESEED_REQUIRED`; it does not claim transparent Archive recovery. The
independent `RecordingInspector` runs where an Archive client remains alive
(the storage Archive IT).

### Restart verification protocol

The controller, not the killed child, owns the restart. It must:

1. wait for the killed process to exit (exit status is diagnostic only; a
   forced-kill status is platform dependent);
2. retain the phase-1 milestone, checkpoint bytes, `.inflight` bytes, cursor
   bytes, recording id and stop position before launching phase 2;
3. launch a fresh child with the same directory roots, identity, stream and
   ports, but with no crash barrier;
4. parse the child's machine-readable `OUTCOME`, `HEALTH`, `SEQUENCE`,
   recording/checkpoint fields, and `PROOF_*` records; and
5. independently inspect the Archive and Store. The storage Archive IT now
   runs `RecordingInspector`; the provider suite uses `StoreFixture` and the
   child proof. Provider cells still do not claim frame-level Archive evidence
   until an external-Archive controller can inspect the killed recording. A
   child saying `CONTINUE` is never sufficient evidence for a no-data-loss
   claim.

Each failure is collected by `DiagnosticCollector`, which copies the control
files, child logs, and directory listing into a durable evidence directory:

```text
testName, crashPoint, sequence, phase1Milestone,
checkpointBytes, inflightBytes, cursorBytes,
recordingId, checkpointPosition, archiveStopPosition,
terminalBySequence, payloadCrcBySequence,
storeRecords, childStdout, childStderr, directoryListing
```

The current representation is text plus copied binary inputs rather than a
`CrashTestCaseResult` JSON object. It remains independent of Aeron so a failed
process can be diagnosed without loading the driver:

```json
{
  "testName": "archiveRecordedBeforeCheckpointRequiresReseed",
  "crashPoint": "AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT",
  "sequence": 1,
  "phase1Milestone": { "point": "...", "sequence": 1,
    "recordingId": -1, "recordingPosition": -1, "crc32c": "..." },
  "checkpoint": { "path": "...", "sha256": "...", "state": "COMMITTED" },
  "inflight": { "path": "...", "exists": true, "sha256": "..." },
  "archive": { "recordingId": 7, "stopPosition": 1234,
    "terminalBySequence": { "0": "COMMIT" },
    "payloadCrcBySequence": { "0": "..." }, "orphanTailLength": 0 },
  "store": { "records": [ { "sequence": 0, "length": 64, "crc32c": "..." } ] },
  "phase2": { "outcome": "RESEED_REQUIRED", "health": "RESEED_REQUIRED",
    "errorPrefix": "RESEED_REQUIRED:" },
  "artifacts": { "stdout": "phase1-stdout.log", "stderr": "phase1-stderr.log" }
}
```

`sha256` fields are diagnostic fingerprints only; correctness uses decoded
checkpoint/cursor fields, frame-level CRCs, and Store payload bytes. The
controller must write this result before deleting a successful work directory
when a cell fails, and must include the absolute artifact path in the JUnit
failure message.

The controller compares payload bytes and CRC32C against an oracle captured
before the kill. It never compares raw Store files, which are not a stable
format across restart. A failure message must include the last milestone and
the artifact directory.

## Writer matrix

### `ARCHIVE_FIRST`

1. `BEFORE_PREPARE`: the coordinator reserves the sequence before writing the
   PREPARING fence, so the fence and any subsequent publication always identify
   the same transaction. The fence is durable before the publisher seam;
   phase2 must report `RESEED_REQUIRED` rather than guessing whether a local
   write can be replayed.
2. `AFTER_DICTIONARY_CHUNKS` / `AFTER_DATA_CHUNKS`: phase2 must report
   `RESEED_REQUIRED`; the stop-position fence rejects the orphan tail.
3. `AFTER_LOCAL_WRITE_BEFORE_COMMIT`: phase2 must report `RESEED_REQUIRED`
   because the durable `.inflight` fence survives the local-ahead window.
4. `AFTER_COMMIT_OFFER`: phase2 must report `RESEED_REQUIRED`; the commit
   ambiguity is represented by the terminal/uncertain checkpoint and tail fence.
5. `AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT`: the Archive contains committed
   txn #2 while the checkpoint is still baseline; phase2 must report
   `RESEED_REQUIRED` until validated tail replay is implemented.
   Keep a second fixture with no checkpoint at all; it is the same crash phase
   but verifies that “missing” and “stale” checkpoint diagnostics are both
   machine-readable and never silently treated as a clean empty store.
6. `BEFORE_CHECKPOINT_TEMP_WRITE` /
   `AFTER_CHECKPOINT_TEMP_WRITE_BEFORE_RENAME` (checkpoint write for txn #2):
   the destination is either the old or new complete record; a partial temp
   file is ignored. Phase 2 must continue from the old terminal checkpoint or
   reject with `RESEED_REQUIRED`; it must not infer `REPLAY_FROM_ARCHIVE`
   without a validated tail scanner.
7. `AFTER_ABORT_OFFERED` after local Store rejection: the ABORT may be in the
   Archive while the REJECTED checkpoint is absent. The tail fence prevents
   sequence reuse and phase2 must report `RESEED_REQUIRED`.
8. Torn checkpoint: corrupt bytes after a clean stop; phase2
   `RESEED_REQUIRED`.

### `ENQUEUE_THEN_ARCHIVE`

1. `AFTER_ENQUEUE_BEFORE_PREPARE`: the `.inflight` fence is written before the
   Store write. Phase 2 must report `RESEED_REQUIRED`; this cell is green only
   when the restarted provider proves that it read the fence, rather than just
   failing for an unrelated reason.
2. Prepare failure (injected, see below): assert the `COMMITTING_UNCERTAIN`
   checkpoint carries the real sequence, data length, chunk count, **and
   CRC32C**. This is a Tier-1 assertion (already covered by
   `enqueuePrepareFailureRecordsTheFailedTransactionDimensions`); Tier 2
   reproduces the failure with a child knob
   `-Ddg.crash.failOfferAfter=<n>` that makes the injected `Offerer` return
   `Publication.CLOSED` on the n-th offer. The cell then kills
   `DURING_COMMITTING_UNCERTAIN_WRITE`.
3. Restart persisting that marker (`DURING_COMMITTING_UNCERTAIN_WRITE`): the
   checkpoint is either baseline or the uncertain marker; phase2 outcome is
   `CONTINUE` (baseline retained) or `RESEED_REQUIRED` (marker survives). Do not
   assert a specific one — assert it is one of the two and never a third.
4. Restart with `COMMITTING_UNCERTAIN`: phase2 `RESEED_REQUIRED`; health must
   report `RESEED_REQUIRED` and include the checkpoint path and sequence in the
   message.

`AFTER_PREPARE_FAILURE_ABORT_OFFERED` (the ENQUEUE prepare-failure cell,
injected between the publisher's ABORT offer and `failed=true`): the recording
contains a terminated ABORT for the failed sequence when the offer succeeds.
`markEnqueueWithoutArchive()` then records the non-terminal
`COMMITTING_UNCERTAIN` checkpoint; it is not another ABORT operation. Phase 2
must apply the stop-position fence and return `RESEED_REQUIRED`. The cell must
assert that the uncertainty checkpoint retains the real length, chunk count,
and CRC32C. If the ABORT offer itself fails, the test records that suppressed
failure and still requires the same fail-closed outcome.

Additional writer cells required before claiming the matrix complete:

* `BEFORE_PUBLICATION_CONNECTED`: no sequence is reserved, no checkpoint or
  Store record is created, and a clean restart continues from the previous
  terminal sequence;
* dictionary-only and data-only partial tails: the next transaction must not
  consume the orphaned sequence; phase2 must report `RESEED_REQUIRED`;
* crash after local enqueue but before `markEnqueued`: the local Store record
  plus `.inflight` marker must be visible and phase2 must refuse startup;
* checkpoint failure/read-only directory: the original terminal checkpoint is
  retained, the failed write is surfaced as `FAIL_CLOSED`, and no sequence is
  reused;
* first-ever transaction (no baseline checkpoint): a missing recording id or
  unavailable `RecordingPos` must not produce a restartable checkpoint with
  `recordingId=-1`; startup either waits for a valid recording identity or
  returns `RESEED_REQUIRED`.

## Reader and cursor matrix

Reader cells run txn #1 on the writer, then crash the reader at the listed
point while the writer stays healthy, then restart the reader. Cursor-write
cells must run the backup-reader path with `commitPosition` enabled: the
durable cursor is the neutral MessageInfo/offset file written by the caller's
`transactionResolved -> cursorListener.onChange -> StoredMessageInfoManager`
path. `StoredMessageInfoManager.NewAtomic` now writes that file through
`AtomicFileStore`; the checked-in reader process also uses a CRC-protected
fixture to validate the uncertainty protocol. The Aeron provider itself owns only the
`.reader-inflight` uncertainty marker; it does not secretly create a second
cursor store. `AeronReplicationCursor` is the cross-module value object; when
durable cursor persistence is enabled it is persisted through the neutral
MessageInfo/`ReplicationCursorStore` boundary, not by an Aeron-specific store.
A plain live reader does not create a cursor file and cannot exercise cursor
atomicity.

Reader process fixture requirements:

* `ReaderCrashChildMain` must connect to an already-recorded stream; it must
  never create a second writer or Archive. It receives the recording id,
  replay/live channels, starting cursor, and cursor path as properties.
* Its receiver writes deterministic records through `StoreFixture` and reports
  `STORE_IMPORT_STARTED`, `STORE_IMPORT_RETURNED`, `CURSOR_PERSISTED`, and
  `OUTCOME` in the same forced binary control protocol as the writer child.
* The production `StorageBinaryDataClientAeronArchive.New(...,
  ReaderDeliveryListener)` listener is the only injection seam. Do not put
  test-only sleeps in `TransactionAssembler`; use the listener before import,
  after import, and immediately before the durable cursor callback.
* On restart, the child reads the same cursor and `.reader-inflight` paths. A
  surviving uncertainty marker must produce `RESEED_REQUIRED` before opening a
  Store, while a complete cursor resumes at `sequence + 1`.
* A tracking receiver/importer records native buffers handed to
  `StorageBinaryDataImporter`; every buffer must be released on import failure,
  and no cursor callback may run after a failed import.

- `REPLAY_BEFORE_FIRST_IMPORT` / `DURING_STORE_IMPORT`: incomplete or
  mid-apply transactions are never durably applied. Assert the reader's
  post-restart cursor equals the last fully imported sequence and the local
  Store contains no partial artifact. Exactly-once assertable today **only if**
  the crash leaves the cursor un-advanced; the double-apply window is
  `AFTER_STORE_IMPORT_BEFORE_CURSOR_WRITE`.
- `DURING_STORE_IMPORT_FAILURE`: the reader child injects a Store/import
  exception after
  least one owned native buffer has been allocated. Assert
  `StorageBinaryDataImporter.release()` runs for every owned buffer, the
  assembler drops the in-flight transaction, the cursor remains at the prior
  terminal sequence, and the reader reports a terminal failure rather than
  advancing past the failed transaction. Native allocation counters remain a
  future Tier-1 enhancement; the process cell proves the durable uncertainty
  marker and fail-closed restart policy.
- `AFTER_STORE_IMPORT_BEFORE_CURSOR_WRITE`: the reader writes a
  `.reader-inflight` checkpoint before Store materialisation and deletes it
  only after import returns. A surviving marker makes the next `client()` call
  fail with `RESEED_REQUIRED`; the reader never silently re-imports an
  uncertain transaction.
- Torn cursor file: corrupt bytes; restart rejects it (`RESEED_REQUIRED`);
  assert the previous valid cursor bytes are still readable (atomic replace).
- Cursor ahead of Archive stop: seed `<work>/cursors/<n>.cur` with a
  `ReplicationCursor` whose logical sequence and position exceed the recording
  stop; restart must fail closed, never silently rewind.
- Live reader observing a writer crash: writer is killed at `AFTER_COMMIT_OFFER`
  (orphan tail). The reader must not apply the un-terminated tail, and must
  either continue at the writer's restart or fail closed with a
  duplicate-sequence diagnostic — never apply the orphan's data. Until a
  coordinated tail-abort/reseed protocol exists, assert the exact current
  assembler diagnostics: `interleaved replication transaction`, `unexpected
  chunk index`, `replication sequence gap: expected ...`, or `replayed data for
  an already resolved sequence ...`. Exact expectation after F1: reader
  continues only once the writer reseeds or emits a terminal abort; before that
  coordination exists, the reader's fail-closed behavior is the guard.

Assertions (all reader cells):

- incomplete transactions are never delivered (`REPLAY_BEFORE_FIRST_IMPORT` and
  `DURING_STORE_IMPORT`);
- cursor state does not advance before successful Store import (couple this to
  the `Delivery.run` ordering: import → cursor advance under the assembler
  monitor → cursor persist);
- torn cursor files are rejected or the previous valid cursor survives;
- replay after import-before-cursor is explicitly fail-closed through the
  reader `.reader-inflight` marker; no exactly-once claim is made without a
  Store-level transaction ledger;
- a materialization/Store “poison pill” failure leaves the cursor unchanged,
  records the failed sequence and cause, and restarts as `FAIL_CLOSED` or
  `RESEED_REQUIRED`; it must not retry forever while claiming healthy status;
- empty transactions (commit with `payloadLength == 0` and `chunkCount == 0`)
  are delivered as a genuinely empty binary (`position() == 0`, `limit() == 0`),
  never as a one-byte `allocateDirect(1)` view. The guard is the
  `dataStorage == null` branch in `TransactionAssembler.commit`; keep a Tier-1
  assertion on it.

## Archive matrix

Doable in the current topology:

The checked-in provider suite covers restart of the embedded Archive through
the writer cells above and two external-Archive cells (loss/reseed and stale
active-catalog refusal). The
following artifact rows are executable through `ArchiveArtifactMutator`; they
are independent Archive-integrity tests and are not part of the provider
process-cell count.

- restart with a recording recovered from a killed writer (assert stop position
  becomes >= 0 and equals the last synced frame — see "Restart rule");
- corrupt recording frame: `AeronArchiveReplicationIT` flips a payload byte in
  the version-checked segment and `RecordingInspector` rejects the recording;
- truncated Archive catalog: `AeronArchiveReplicationIT` truncates the
  catalog descriptor and restart either fails or cannot expose the old
  recording, so no replacement can silently reuse its identity;
- wrong stream, term, MTU, or recording identity on `Extend`: assert
  `FAIL_CLOSED` (the framing/stream identity checks already exist — extend the
  existing unit coverage);
- Archive stop behind checkpoint: assert `RESEED_REQUIRED` (already implemented).

Synthetic Archive-state cells must name their fabricator instead of relying on
hand-edited directories:

* `CursorAheadFabricator` is planned, not currently checked in. It will
  construct a valid `ReplicationCursor` whose logical sequence/recording
  position exceeds the recovered stop and pass it through `client(...)`.
  Production cursor persistence is now atomic through
  `StoredMessageInfoManager.NewAtomic`; the fabricator still must not pretend
  to seed a production cursor file.
* `ArchiveArtifactMutator.segment` locates the sole recording segment through
  the Archive catalog, validates the `<recordingId>-<segment>.rec` layout, and
  applies only a bounded, forced byte mutation after the fixture has recorded
  the original path. The checked-in mutations flip the first envelope payload byte
  and truncate only the recorded bytes of the final frame; they are separate
  version-checked methods. If the Aeron version changes its segment
  naming or catalog format, the test must fail with “unsupported artifact
  layout” and be moved to the nightly/manual suite rather than guessing a
  path.
* `ArchiveArtifactMutator.truncateCatalog` makes a byte-level mutation,
  restarts the writer, and asserts a diagnostic `FAIL_CLOSED`/`RESEED_REQUIRED`
  outcome—not a null pointer or silent new recording.
* `IdentityMismatchFixture` is planned, not currently checked in. It will
  restart with one changed cluster id, epoch, store generation, stream id,
  term length, or MTU and assert the normalized `RESEED_REQUIRED:` diagnostic.
  Each variant is a separate cell.

External-Archive rows (F4) now have the first executable cell:

- `ExternalArchiveCrashIT.archiveLossDuringCommitWaitRequiresReseed` launches
  `ArchiveProcessMain` with separate `archive-aeron/` and `archive/` roots,
  launches the writer with `externalArchive=true`, waits for the writer's
  `AFTER_COMMIT_OFFER` milestone, kills only the Archive process, releases the
  writer, and asserts the writer's fail-closed outcome. Recovery starts a fresh
  Archive catalog and asserts `OUTCOME=RESEED_REQUIRED` rather than silently
  creating a replacement recording.
- Archive-restart-without-writer and transparent recovery of an active catalog
  are intentionally not claimed. `ExternalArchiveCrashIT.
  staleArchiveCatalogIsNeverExtendedSilently` covers the safe refusal path:
  Aeron's active mark file is retained, Archive restart reports
  `OUTCOME=RESEED_REQUIRED`, and no writer is allowed to bind a replacement
  recording. Transparent continuation would require an Archive-side orphan
  recording policy that is not part of this release.

The Archive-ahead case is mandatory in the writer matrix via F1: startup must
never reuse a sequence that already exists in the recording.

Archive recovery must be tested in two layers:

1. **Control-plane checks:** wrong stream, term, MTU, recording id, active
   recording, stop-behind-checkpoint, and archive-ahead-of-checkpoint. These
   are deterministic unit/provider tests and assert the canonical
   `RESEED_REQUIRED:` or `FAIL_CLOSED` diagnostic.
2. **Artifact checks:** a stopped recording tail and catalog are copied to the
   evidence directory, mutated, forced, and reopened. The mutator must first
   validate the Aeron version, catalog layout, segment naming, and target
   bounds. On an unknown layout it throws `UnsupportedArtifactLayoutException`
   and the test is reported `BLOCKED`, never “passes” by guessing a path.

The artifact tests must verify the recording itself, not only provider startup:
the independent scanner must reject a bad frame CRC, a data-after-terminal
frame, a duplicate terminal, a chunk gap/overlap, and a commit CRC that does
not match the assembled data. A catalog truncation must fail before a new
recording is created.

## Verification contract

`StoreFixture` owns a deterministic append-only oracle file, not a hash of
Store implementation files. Each record is encoded as
`length:u32, payloadCrc32c:u32, payload[length]` in big-endian order. The
payload is the deterministic transaction bytes for one sequence; the controller
retains `sequence -> payload/CRC32C` before killing the child. Verification
reopens the fixture, validates every record boundary and CRC, and compares
fields and payload bytes. It never compares raw Store files, whose layout may
legitimately change across restarts. The reader child additionally materialises
the payload with `ChunksWrapper.New(...)` and reads it back so a valid
envelope/CRC cannot mask a corrupt chunk graph.

`RecordingInspector` exposes one shared inspection path for every cell:

```java
RecordingEvidence inspect(long recordingId, long startPosition, long stopPosition);
// RecordingEvidence.terminalBySequence()
// RecordingEvidence.payloadCrcBySequence()
// RecordingEvidence.orphanTailLength()
```

It replays the recording, decodes envelopes, groups them by sequence, and
asserts that each committed sequence has exactly one terminal COMMIT/ABORT.
Cells must compare the inspector's CRC to the controller's pre-crash oracle,
not only to the reader's own calculation.

For every cell, after phase2, verify:

- exact sequence continuity (no gap, no reuse);
- one terminal envelope per committed sequence — a `RecordingScanner` helper
  replays the recording from `archive.replay(...)`, decodes envelopes, and
  returns `{ perSequenceTerminal, txnCrcBySeq, orphanTailLength }`; assert
  exactly one terminal marker per sequence, reject two terminals for one
  sequence, and reject a terminal marker whose CRC differs from its chunks;
- payload bytes and CRC32C match the deterministic payload of the sequence;
- checkpoint fields: sequence, `resolutionCrc32c`, recording ID, recorded
  position — parse via `AeronReplicationCheckpointStore.read` and assert
  `recordedPosition ==` the position the commit actually returned;
- Archive stop position is not behind the checkpoint's recorded position; when
  it is ahead, the inspector must account for the exact orphan-tail byte range
  rather than accepting an arbitrary archive-ahead distance. Report both
  `lastTerminalEndPosition` and `orphanTailLength = stopPosition -
  lastTerminalEndPosition`; never use `stopPosition - checkpointPosition` as a
  substitute because the checkpoint position is the beginning/end boundary of
  a terminal frame, not a generic recording-tail length;
- `Extend` preserves the recording ID across restart;
- the imported Store binary on the reader is a valid `ChunksWrapper` (child
  proof line `STORE_VALID=<bool>`);
- cursor identity matches cluster, node, generation, epoch, and recording
  (parse the neutral MessageInfo/StoredMessageInfoManager provider-position
  record; there is no Aeron-specific cursor-store class);
- no sequence is silently skipped or republished with different data
  (compare txn #2's payload CRC from pre-crash milestones against the replayed
  recording).

The Store assertion is semantic, not a file hash: reopen the fixture, validate
each length/CRC record, compare the complete payload bytes, and verify the
record count and sequence order. For reader cells also materialise the binary
through `ChunksWrapper` and read it back; a valid envelope/CRC is insufficient
if Store materialisation corrupts its chunk graph. An “exactly once” assertion
is permitted only when the durable cursor or an idempotency ledger proves it.
Otherwise the required safe outcome is `RESEED_REQUIRED`.

Failure capture (always, on any assertion or timeout):

- child stdout/stderr;
- `<work>` directory listing (recursive);
- `control/milestone` contents and the full milestone timeline if archived;
- checkpoint and cursor bytes;
- Archive recording ID + stop position;
- Store evidence: field-level fixture contents and CRCs after the child's clean
  close (a directory hash may be retained only as a diagnostic, never as a
  correctness assertion);
- the `OUTCOME=` line printed by the child in phase2.

## Provider-child artifact specification

`ProviderCrashChildMain` must print, phase-agnostic, on stdout (and mirror the
same contents to `control/outcome`) these key/value lines. The current
controller parses the file; stdout is diagnostic only:

```text
ROLE=<writer|reader>
PID=<pid>
OUTCOME=<RecoveryPolicy>
PROOF_COMMITTED=<sequence csv>          // writer, phase2
PROOF_CRC_TXN0=<unsigned>                // writer, CRC32C of txn #0 payload
PROOF_CRC_TXN1=<unsigned>                // writer, CRC32C of txn #1 payload
PROOF_CRC_TXN2=<unsigned>                // writer, CRC32C of txn #2 payload
PROOF_STORE_VALID=<true|false>           // CRC-structure Store fixture proof
STORE_VALID=<true|false>                 // reader, when a reader child exists
OUTCOME must be the last line before a clean exit.
```

The controller asserts `OUTCOME` and recorded evidence, not a crash-process exit
code: `destroyForcibly()` exit values are platform-dependent. Phase-2 clean
exit must be zero; startup refusal is asserted through the machine-readable
outcome and diagnostic prefix.

`RESEED_REQUIRED` is recognized only when the child error starts with the exact
`RESEED_REQUIRED:` prefix. Other failures that mention reseeding remain
`FAIL_CLOSED`; this prevents an unrelated diagnostic sentence from becoming a
false recovery success. The embedded provider child normally starts a live
subscriber so MDC publication can make progress. Set
`-Dcrash.matrix.subscriber=false` to test a genuinely reader-less writer; the
external-Archive child always disables that local subscriber.

`ProviderCrashChildMain` writes the machine-readable outcome file and the
controller captures stdout/stderr as diagnostics. The provider proof is a
structural CRC check of the append-only Store fixture plus checkpoint fields;
the storage Archive IT supplies the frame-level `RecordingInspector` proof.
The smaller
`AeronCrashChildMain` still emits only `OUTCOME=baseline`; that line is not a
provider recovery result.

## Initial implementation order

1. F1 archive stop-position reconciliation and F2 local `.inflight` fencing
   are implemented; their unit policy and boundary tests pass.
2. Tier-1 state-machine coverage for publisher/coordinator/distributor failure
   points is implemented and runs under Surefire.
3. Tier-2 harness skeleton is implemented for the forced-temp/rename cell in
   `AeronCrashMatrixIT`; it runs under Failsafe and uses a real child kill.
4. Run the checked-in provider profile and keep all twenty-two deterministic writer
   cells green: before-prepare, dictionary/data orphan tails, prepared-before-
   local-write, ambiguous commit offer, recorded-before-checkpoint, local-
   write-ahead fence, enqueue-before-prepare, local rejection/ABORT,
   `COMMITTING_UNCERTAIN`, and failed-prepare ABORT boundary. The same profile
   also runs the deterministic double-crash recovery cell and the separate
   external-Archive loss cell.
5. Add the synthetic Archive/catalog/cursor fabricators, including
   `DURING_STORE_IMPORT_FAILURE` and poison-pill tests. The four reader
   process cells are implemented; their policy is fail-closed, not exactly-once.
6. Extend F3 coverage with the reader process marker and the tracking importer;
   the production MessageInfo path now uses `AtomicFileStore`, while the
   reader uncertainty marker remains the explicit recovery fence.
7. Keep the live-reader orphan-tail check green. It asserts the current
   fail-closed policy and must be upgraded only when tail abort/reseed
   coordination is implemented.
8. Run the seeded random-kill soak after all deterministic cells are green.

### Coder checklist for each new cell

Implement a cell as one self-contained test method and one deterministic child
scenario. The method must:

1. create a fresh `DirectoryLayout` and reserve all ports;
2. write the cell name, durability mode, crash point, and PRNG seed into the
   evidence directory before launching a child;
3. launch phase 1 with exactly one armed barrier and wait for `ready`;
4. validate the milestone magic/version/CRC, point, and sequence before
   killing the child;
5. call `destroyForcibly()`, wait for exit, and snapshot checkpoint,
   `.inflight`, cursor, Store, and directory listings;
6. launch phase 2 with the same identity and no barrier, then parse
   `OUTCOME`, `HEALTH`, `ERROR`, recording/checkpoint fields, and proof fields
   from the control file;
7. apply the cell's `RecoveryPolicy`, not a generic “process restarted” check;
8. run independent Store and recording assertions; and
9. retain evidence on failure and delete the temporary directory only after a
   successful assertion.

The implementation order is a dependency graph, not an execution-order
requirement: checkpoint/cursor corruption tests depend only on
`AtomicFileStore`; writer process cells depend on `ChildMilestone` and
`StoreFixture`; reader cells depend on `ReaderDeliveryListener` and a durable
cursor fixture; Archive/catalog cells depend on a version-checked mutator;
random soak depends on every deterministic cell being green. A coding agent
must not enable a dependent profile until its prerequisites have executable
tests.

## Features required before the matrix can be green

Concrete work items with hook sites, so they can be implemented and tested in
dependency order. Items marked **implemented** are still required evidence
cells; code presence alone does not make the matrix green.

- **F1 — archive-tail reconciliation and ambiguous ARCHIVE_FIRST commits
  (writer pre-flight; implemented, evidence still required).** `ensureWriter()` now compares the recovered recording
  stop position with the last terminal checkpoint before `Extend`: a stopped
  recording that is behind or ahead of that checkpoint is rejected with
  `RESEED_REQUIRED`, so the writer cannot reuse a sequence hidden in an
  uncheckpointed tail. This is intentionally a conservative stop-position
  fence, not a frame scanner or automatic tail replay. The
  An in-process `ARCHIVE_FIRST` commit-timeout path also persists
  `COMMITTING_UNCERTAIN` through the coordinator's common failure path; a hard
  kill after the offer has no opportunity to write that marker and is fenced
  by the archive/checkpoint comparison. A future tail-replay feature
  may replace the fence with a validated frame scan, but the current matrix
  must assert reseed rather than infer safety from a partial recording.
  Hooks: same points as the `AFTER_DATA_CHUNKS`/`AFTER_COMMIT_*` crash points.
- **F2 — local-ahead detection (implemented).** Persist a lightweight
  in-flight checkpoint (state `PREPARING`/`ENQUEUED`) through
  `AtomicFileStore` *before* `delegate.write` in both durability branches of
  `AeronStorageBinaryTargetDistributing.write`. On restart, any in-flight record
  whose sequence is not terminal in the recording ⇒ `RESEED_REQUIRED`.
  The implementation writes the marker at the correct pre-write boundary;
  F6 must prove the process-kill/restart behavior rather than merely inspect
  the file; the provider process matrix asserts the restart refusal.
- **F3 — reader uncertain-import marker (implemented).** The archive reader
  receives a `ReaderDeliveryListener`. Before `receiveData`, the provider
  atomically writes `<checkpoint>.reader-inflight` as a
  `READER_CURSOR/COMMITTING_UNCERTAIN` checkpoint containing recording,
  sequence, position, length, chunk count, and CRC. After successful Store
  import and a successful resolved-cursor callback it deletes that marker.
  Startup rejects any surviving or malformed marker with the canonical
  `RESEED_REQUIRED:` diagnostic. This deliberately chooses fail-closed
  recovery over a Store-level idempotency ledger.
- **F4 — external Archive topology (implemented).**
  `AeronSettings.externalArchive` selects an `ensureRuntime` branch that
  launches only the writer's Media Driver and connects `AeronArchive.Context`
  to the configured control request/response channels. `ArchiveProcessMain`
  owns a separate Media Driver and Archive directory and writes an atomic ready
  marker. `AeronArchiveReplicationPublisher.NewRemote` and `ExtendRemote` use
  `SourceLocation.REMOTE`; using the local spy source would bind the Archive's
  subscription to the wrong Media Driver. `ExternalArchiveCrashIT` kills only
  the Archive after `AFTER_COMMIT_OFFER`, releases the writer, starts a fresh
  catalog, and asserts `RESEED_REQUIRED`. The cell also asserts that the writer
  never launches an embedded Archive in external mode.
  `ExternalArchiveCrashIT.staleArchiveCatalogIsNeverExtendedSilently` preserves
  the original Archive directory and verifies that Aeron's active mark file is
  surfaced as `OUTCOME=RESEED_REQUIRED`; no replacement recording is accepted.
- **F5 — machine-readable reseed diagnostics (implemented; matrix evidence
  still required).** Normalize every unrecoverable
  writer/cursor restart path to the `RESEED_REQUIRED:` prefix and map it to
  `ReplicationHealth.State.RESEED_REQUIRED`. The harness asserts the health
  state, outcome, and prefix together; it must not depend on inconsistent
  lowercase message text or a generic `IllegalStateException`.
- **F6 — provider process harness (implemented).**
  `ProviderCrashChildMain` and `ProviderCrashMatrixIT` start the real provider
  (writer + embedded Archive + Media Driver), emit `ready`, block at one
  configured seam, and emit machine-readable phase-2 outcome records. The
  controller reserves loopback ports, drains child output, kills only after
  the exact CRC-validated milestone, retries active-driver recovery, and
restarts with the same identity/directories. The checked-in controller verifies
checkpoint fields and the child’s CRC-protected Store fixture. The storage
  Archive IT independently runs `RecordingInspector`; the provider controller
  cannot do so after SIGKILL because its Archive is embedded in the killed
  writer. `ExternalArchiveCrashIT` supplies the separate-Archive controller
  boundary and deliberately validates fresh-catalog reseed after Archive loss;
  it does not claim transparent continuation of a stale Archive mark file;
  the stale-catalog refusal cell is the explicit safe outcome.
- **F7 — Archive artifact corruption (implemented).**
  `ArchiveArtifactMutator` validates the Aeron 1.53 segment-name layout before
  mutating a recording payload or truncating `archive.catalog`. The Archive IT
  reopens each mutated artifact and requires either startup failure or an
  inspector/recovery failure; a corrupted recording may not be accepted and a
  truncated catalog may not expose the old recording as a valid replacement.
- **F8 — atomic metadata phase hooks (implemented).** `AtomicFileStore` exposes
  package-private test hooks for `BEFORE_TEMP_WRITE`, `DURING_FILE_WRITE`,
  `AFTER_TEMP_WRITE_BEFORE_RENAME`, and
  `AFTER_RENAME_BEFORE_DIRECTORY_SYNC`. Checkpoint and neutral cursor writers
  share this utility, so the same unit tests cover both metadata classes. The
  hook is unset by default and adds no production synchronization or I/O.

## Required synthetic corruption tests

These are Tier-1 tests and do not require a process crash:

- write a valid checkpoint, truncate it below the encoded length, restart, and
  assert a length/CRC validation failure;
- flip a checkpoint byte and assert CRC rejection;
- repeat the truncation/bit-flip checks for the CRC-protected reader cursor
  fixture used by `ReaderCrashChildMain`; the production MessageInfo file now
  uses the same forced replacement primitive through
  `StoredMessageInfoManager.NewAtomic`;
- inject a partial `AtomicFileStore` temp write and assert the previous valid
  file remains selected;
- truncate the catalog through the implemented version-aware
  `ArchiveArtifactMutator.truncateCatalog`, and truncate only the recorded
  bytes of the final segment through `truncateFinalFrame(segment, start, stop)`;
  never truncate preallocated unused segment capacity because that does not
  exercise Archive replay validation;
- inject a Store import failure and assert native ownership is released,
  assembler state is cleared, and the cursor remains unchanged;
- inject a materialization (“poison pill”) failure and assert reader health
  becomes terminal with no cursor advance. This case is a required reader test,
  not an implied side effect of the crash matrix.

## Worked cell: recorded commit before checkpoint

Every matrix cell follows this template: Setup, Arm, Run, Kill, Restart,
Assert, Evidence. The first fully specified process cell is
`AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT`:

1. **Setup:** create a fresh `DirectoryLayout`, reserve loopback UDP ports,
   configure a 2-second offer timeout, and start the writer child with
   `ARCHIVE_FIRST`.
2. **Arm:** launch with
   `-Ddg.crash.barrier=AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT` and wait for
   `ready`.
3. **Run:** child commits transaction 1 normally, then writes transaction 2.
   The hook is entered only after `RecordingPos` acknowledges transaction 2 and
   before the COMMITTED checkpoint write.
4. **Kill:** validate the milestone's point/sequence, then use the checkpoint
   and Archive evidence for recording position before calling
   `destroyForcibly()` and wait at most 5 seconds for exit.
5. **Restart:** wait up to 30 seconds for the recording stop position, launch
   phase2 with the same directories and no barrier, and read `OUTCOME`.
6. **Assert:** with the current conservative F1 fence, expect
   `RESEED_REQUIRED` and no sequence reuse. Do not accept
   `REPLAY_FROM_ARCHIVE` until a validated tail scanner/replay feature replaces
   the fence. Inspect the recording and checkpoint with
   `RecordingInspector`/`AeronReplicationCheckpointStore`.
7. **Evidence:** retain the milestone, child logs, checkpoint bytes, recording
   ID/stop position, terminal map, and Store fixture proof.

All other cells must fill the same seven fields; a one-line intention is not an
implementable test.

### First implementation slice (copyable cell definitions)

These are the first five cells a coding agent should implement or keep green.
Each uses a fresh `DirectoryLayout`, transaction 1 as the committed baseline,
and transaction 2 as the armed transaction. The controller captures the
transaction-2 payload/CRC before killing the child.

| Cell | Child/mode | Armed point | Kill boundary | Phase-2 assertion |
|---|---|---|---|---|
| orphan data tail | provider writer, `ARCHIVE_FIRST` | `AFTER_DATA_CHUNKS` | after CRC-valid milestone for seq 1 | `OUTCOME=RESEED_REQUIRED`; no seq-1 terminal may be reused or invented |
| ambiguous commit | provider writer, `ARCHIVE_FIRST` | `AFTER_COMMIT_OFFER` | after COMMIT offer, before recorded-position wait | `OUTCOME=RESEED_REQUIRED`; archive-ahead/checkpoint fence rejects seq 1 |
| recorded but uncheckpointed | provider writer, `ARCHIVE_FIRST` | `AFTER_COMMIT_RECORDED_BEFORE_CHECKPOINT` | after `RecordingPos >= commitPosition`, before checkpoint callback | `OUTCOME=RESEED_REQUIRED` until tail replay exists; checkpoint remains seq 0 |
| local enqueue uncertainty | provider writer, `ENQUEUE_THEN_ARCHIVE` | `DURING_COMMITTING_UNCERTAIN_WRITE` | before the uncertainty checkpoint callback returns | valid `COMMITTING_UNCERTAIN` metadata and `OUTCOME=RESEED_REQUIRED` on restart |
| reader import uncertainty | reader child, backup mode with `commitPosition` | `AFTER_STORE_IMPORT_BEFORE_CURSOR_WRITE` | after Store fixture append, before cursor callback | `.reader-inflight` survives; restart reports `RESEED_REQUIRED`; no exactly-once claim |

For the first three writer cells the phase-2 child must start with the same
cluster/node/store-generation/epoch/recording identity and no barrier. It must
read the checkpoint and `.inflight` files before attempting a write. For the
reader cell the writer remains healthy, the reader receives the same recording
id and cursor path, and phase 2 must inspect the uncertainty marker before
opening the Store. A cell is not green because the process exits: it is green
only when the policy, checkpoint/marker bytes, recording evidence, and Store
fixture all satisfy the table.

The provider child maps the four checkpoint phases to the shared
`AtomicFileStore` test seam. Cursor phase names remain reader-fixture phases;
they are not silently reported by the provider child. `AFTER_RECOVERY_CHECKPOINT_READ`
is emitted by the provider immediately after `loadWriterCheckpoint()` returns,
not merely after transport construction.

The provider process matrix does not arm every enum value. In particular,
cursor-file phases are fixture-only, `AFTER_CHECKPOINT_WRITE_BEFORE_COMMITTED_SEQUENCE_UPDATE`
is an in-memory ordering seam, and `BEFORE_PUBLICATION_CONNECTED` is a startup
readiness seam rather than a transaction cell. An enum entry or hook mapping is
not coverage; a point is green only when a controller arms it and verifies the
resulting milestone and restart evidence.

## Random-kill soak

### Double-crash recovery cell

Use a fresh directory, kill phase 1 at `AFTER_DATA_CHUNKS`, start phase 2 with
the same identity, and arm a second barrier at
`AFTER_RECOVERY_CHECKPOINT_READ`, after checkpoint validation and before
recording extension. Kill it again, then start phase 3 without a barrier. Phase 3 must produce the same
`RESEED_REQUIRED` policy and the same checkpoint/archive evidence as phase 2;
it must not append a new recording, reuse the uncertain sequence, or delete the
diagnostic `.inflight` file. This cell proves that recovery itself is
idempotent and is separate from the random-kill soak.

After all deterministic cells are green, run a seeded soak with a fixed seed
recorded in the test name, 100 iterations per seed, and at least three seeds.
Each iteration chooses a barrier and transaction count from the same PRNG,
retains its seed and evidence directory, and calls one shared
`assertSafeOutcome(RecordingEvidence, StoreFixture, RecoveryPolicy)` helper.
Random tests may not weaken the deterministic assertions or discard failed
artifacts. The checked-in `crashmatrix` profile runs a small smoke sample;
nightly CI should override `crash.matrix.random.iterations=100` and
`crash.matrix.random.seeds=3`.

Add a second deterministic recovery pass before enabling random selection:

* **double crash:** kill phase 1 at `AFTER_COMMIT_OFFER`, restart with
  `AFTER_RECOVERY_CHECKPOINT_READ`, kill again, then restart phase 3 without a
  barrier. Phase 2 and phase 3 must expose the same policy, checkpoint
  identity, and recording id; neither may append or reuse the uncertain
  sequence;
* **writer/reader overlap:** keep a reader replaying while the writer is
  killed. The reader may finish only committed frames, or fail closed with a
  concrete diagnostic; it must never apply the orphan tail;
* **random selection:** choose a barrier and transaction count from a recorded
  seed, but retain the same per-cell evidence and call the shared
  `assertSafeOutcome` helper. A random pass is invalid if any fixed cell is
  still `PLANNED`, `BLOCKED`, or `RED`.

## Decisions required before declaring the matrix green

The following provisional defaults are used until an explicit product decision
changes them:

1. Incomplete tails require `RESEED_REQUIRED`; automatic replay is not assumed.
2. Local Store data ahead of the Archive requires an in-flight marker or
   `RESEED_REQUIRED`; silent continuation is forbidden.
   A first-ever recording with no configured recording id has no durable
   checkpoint to compare; the provider starts a new recording. A first-write
   orphan cell is therefore required before claiming recovery of that case,
   and the child supports `-Ddg.crash.writes=1` for that purpose.
3. Reader import-before-cursor uses an uncertain cursor marker and fails closed
   on restart. Transparent exactly-once continuation is intentionally not part
   of this release; a future idempotency ledger may replace the fence.
4. The provider embeds Archive by default; the external-Archive mode is used
   only by the separate process fixture. Transparent restart of a stale
   external Archive catalog is not promised; the tested policy is explicit
   `RESEED_REQUIRED` refusal when Aeron's active mark file is still present.
5. `ARCHIVE_FIRST` has two ambiguity paths: an in-process await failure uses
   the common `COMMITTING_UNCERTAIN` marker, while a hard kill after the COMMIT
   offer can leave no marker. The current green policy for the latter is the
   archive-ahead/stop-position fence and `RESEED_REQUIRED`; it is not automatic
   tail replay. If that fence is ever relaxed, a validated tail scanner or a
   durable ambiguity marker must be implemented first.
6. Durable cursor assertions run only for backup readers with `commitPosition`
   enabled; plain live readers do not create a cursor file.
7. No recovery path may reuse a sequence that is present in the Archive but
   absent from the terminal checkpoint. The assertion is independent of the
   implementation mechanism: inspect the checkpoint, stop position, and
   terminal map, then require `RESEED_REQUIRED` unless a future replay feature
   proves the sequence and payload are identical.
   The current provider implementation performs the stop-position boundary
   check only; `RecordingInspector` frame scanning remains the follow-up needed
   before any tail can be considered replayable.
8. The backup reader owns the durable MessageInfo/cursor file. A live reader
   without `commitPosition` is tested only for in-memory delivery and restart,
   not cursor atomicity.
9. A live reader that observes an orphaned writer tail is expected to fail
   closed with the concrete assembler diagnostic currently emitted (for
   example “interleaved replication transaction” or “unexpected chunk index”)
   until tail abort/reseed coordination is implemented. It must never apply
   the orphan payload.
10. The crash oracle may include a generation UUID in each deterministic
    payload, in addition to sequence and CRC32C. This is diagnostic metadata
    for detecting stale replay; it does not replace cluster/epoch/store-
    generation checks and is not a wire-format requirement.
11. Checkpoint and cursor format changes are not backward-compatibility tests
    for this feature. Unknown versions and malformed records must be rejected
    as `RESEED_REQUIRED`; accepting an older format is out of scope.
12. SIGKILL proves process-crash behavior only. It does not prove power-loss
    ordering, disk-full behavior, or native-OOM recovery; those are separate
    fault models and must not be reported as covered by this matrix.

## Deferred scope

The first harness does not attempt to prove power-loss durability (directory
fsync requires a power-loss or VM-reset model, not SIGKILL), disk-full/read-only
checkpoint behavior, native OOM recovery, transparent recovery of a stale
external Archive catalog, shared-memory milestones, or multi-reader simultaneous
crashes. The poison-pill
Store/materialization failure is **not** silently deferred: it belongs to the
Tier-1 reader tests and must produce a terminal `FAIL_CLOSED`/`RESEED_REQUIRED`
outcome with no cursor advance. Disk-full and native OOM remain separate fault
models because they require controlled resource exhaustion rather than a crash
barrier. Random-kill soak is gated on the deterministic cells and is enabled by
the crashmatrix profile once those cells pass.
The deterministic double-crash recovery cell is implemented by
`ProviderCrashMatrixIT`; it is not part of the random soak. Live-reader overlap
is covered at the assembler boundary with the documented fail-closed outcome;
a full writer-child/reader-child overlap test remains a separate topology
exercise, not an unstated correctness claim.

For avoidance of doubt, these are the explicit coverage policies and optional
extensions, not implicit green coverage:

| Item | Required deliverable | Green criterion |
|---|---|---|
| Live-reader overlap | assembler-boundary orphan-tail cell; full child overlap is optional topology coverage | live reader reports the concrete gap/interleaving failure and never delivers the orphan payload |
| Production MessageInfo cursor atomicity | `StoredMessageInfoManager.NewAtomic` backed by `AtomicFileStore` plus round-trip test | a replacement is either the previous complete record or the next complete record; no temp file is selected |
| Reader exactly-once | explicit uncertainty marker and fail-closed restart policy | import-before-cursor crash leaves `.reader-inflight`; restart reports `RESEED_REQUIRED`; transparent duplicate-free continuation is not claimed |
| Random-kill soak | seeded selection, retained diagnostics, shared safe-outcome oracle | every selected cell satisfies the same safe-outcome oracle; smoke profile is green and nightly count is configurable |

Do not move an item into the implemented tables merely because its enum value,
hook, or unit test exists. The process controller, restart assertion, and
independent evidence must all be present for a cell to become green.
