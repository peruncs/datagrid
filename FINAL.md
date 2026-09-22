# Final Aeron Data Grid review

This report consolidates and revalidates `SOL_REVIEW.md`, `SPARK_REVIEW.md`,
`MIMO_REVIEW.md`, and `KIMI_REVIEW.md` against the current source. It is a static
review: no build, test, benchmark, or mutation of production code was performed.

Priorities mean:

- **P0** — a current path can corrupt protocol state, admit work through an invalid
  owner, publish an unsafe durability boundary, or leak a live cluster resource.
- **P1** — correctness hardening or a bounded-liveness problem that should be fixed
  before calling the cluster production-ready.
- **P2** — hot-path throughput, allocation, and I/O work.
- **P3** — package ownership, API surface, naming, and simplification.
- **P4** — Javadocs and operator documentation.

The report deliberately prefers deletion and consolidation over adding modes,
extension points, configuration keys, or recovery states. Backward compatibility is
not a constraint.

## Priority summary

| Priority | Recommendation | Main risk removed |
|---|---|---|
| P0-1 | Repair merger byte accounting and wake-up | False cache exhaustion and 10-second apply stalls |
| P0-2 | Give one object atomic ownership of a complete writer transaction | Dictionary theft, concurrent prepare, shared-CRC corruption |
| P0-3 | Fence every frame, not only terminal markers | A deposed writer poisoning the next writer's transaction |
| P0-4 | Publish a writer only after recovery is complete | Reuse of a half-initialized publisher |
| P0-5 | Stop maintenance and backup work before dependencies | Shutdown-time races and leaked Aeron reader |
| P0-6 | Prevent borrowed Store adapters from closing the shared manager | Live Store use-after-close |
| P0-7 | Publish resolved reader state only after its durability callback | Cursor/status ahead of the durable Store boundary |
| P1 | Harden health, recovery, filesystem fencing, indexes, and failure taxonomy | False health, unsafe identity acceptance, brittle recovery |
| P2 | Remove serialized fsync/RPC/copy/allocation work from hot paths | Low write throughput, replay latency, native/heap churn |
| P3 | Redraw package ownership and delete excess abstractions/public surface | Cycles and unauditable state ownership |
| P4 | Keep design in Javadocs and operational instructions in README | Contradictory or jargon-heavy contracts |

## P0 — correctness and resource ownership

### P0-1. Repair the merger's queue-byte invariant and signal before waiting

**Evidence.** In
`src/main/java/peruncs/datagrid/cluster/storage/types/StorageBinaryDataMerger.java`,
`scheduleMaterialization` computes `projectedBytes` as `cachedBytes +
inFlightBytes + incomingBytes`, then stores that total back into `cachedBytes`.
When batch A is in flight and batch B is queued, A is counted in both fields and
is never fully removed. The over-limit branch then calls
`awaitQueueDrainedBelowLimit()` without setting `flushRequested` or signalling
`flushCondition`, although the worker can be asleep in its ten-second coalescing
wait.

**Change.** Define and assert one invariant:

```text
residentBytes = queuedBytes + inFlightBytes
```

Update `queuedBytes` with queued buffers only; move bytes from queued to in-flight
exactly once when the worker takes a batch; subtract in-flight exactly once on
completion. Admission may compare `queuedBytes + inFlightBytes + incomingBytes`
against the limit, but it must never assign that total to either component. Before
the producer waits for capacity, set `flushRequested = true` and signal the worker
under `queueLock`.

**Verification.** Add a deterministic latch test with A held in materialization and
B queued. Assert the counters after enqueue, handoff, completion, failure, and
dispose; also assert that an over-limit producer wakes the worker immediately rather
than after the coalescing timeout.

### P0-2. Make admission, dictionary transfer, prepare, and commit ownership one transaction

**Evidence.** `AeronStorageBinaryReplicationTarget.write` runs
`executeWriteAtomically(prepareWrite)` and releases `writeLock` before
`commitOrMarkUncertain`. During `prepareWrite`, the staged dictionary is consumed
from `AeronDistributionGate` and put into the coordinator. A second writer can enter
after the first prepare, replace `pendingDictionary`, then fail because the publisher
still has a pending transaction. The first commit subsequently clears state that the
second writer supplied. The same gap permits `transactionMetadata` and
`publishDataChunks` to use the publisher's shared `dataCrc` under different locks.

**Change.** Replace coordinator-global pending transaction state with an immutable,
single-owner `PreparedWrite` containing sequence, dictionary bytes, buffer views,
metadata, fencing token, and terminal state. Admission must reserve this owner and
must not admit another Store write until that owner reaches COMMITTED, REJECTED, or
UNCERTAIN. Do not put slow Archive waits under `writeLock`; instead use a condition
for ownership release and let monitoring remain lock-free. Move CRC accumulators
into the owner or a strictly single-writer framer.

This should also remove `pendingDictionary`, `commitInProgress`, and the split lock
rules that currently describe one logical transaction in several mutable fields.

**Verification.** Race two Store writes with distinct dictionaries and blocked
Archive acknowledgement. Each published transaction must contain its own dictionary
and CRC, and the second must wait without spinning or modifying the first owner.

### P0-3. Enforce lease ownership for dictionary, data, commit, and abort frames

**Evidence.** `AeronReplicationPublisher.publishDictionaryChunks` and
`publishDataChunks` ultimately offer with an unconditional owner predicate. Commit
is gated by the coordinator and abort is gated inside `offerMarker`, but the payload
prefix is not. `TransactionAssembler.Transaction.add` rejects a transaction that
mixes fencing tokens. A writer deposed after publishing a prefix can therefore leave
sequence N under the old token; a successor publishing N with a new token turns that
prefix into a permanent reader failure instead of a clean stale-writer rejection.

**Change.** Delete the unconditional offer overload. Every offer attempt must receive
the same `WriterLeaseGate.OwnedOffer` check, including zero-length payloads,
dictionaries, and all data chunks. A prepared transaction captures one token and
cannot change it. A failed ownership check aborts locally with
`WriterFencedException`; it must not try to publish an old-token terminal after loss
of ownership.

**Verification.** Steal the lease between two data chunks and again between the last
chunk and COMMIT. The old writer must stop offering; the successor must either
reconcile the orphan tail or fail explicitly with `ReseedRequiredException` before
publishing; readers must not observe a mixed-token transaction.

### P0-4. Install a writer only after every recovery step succeeds

**Evidence.** `AeronClusterReplicationTransportProvider.ensureWriterLocked`
assigns `this.writer` before recording discovery and boundary validation finish. Its
failure path records `writerRecoveryState` but neither closes nor clears that writer.
The next fast path returns the non-null, partially recovered instance.

**Change.** Construct into a local variable, complete recording discovery,
checkpoint identity validation, tail validation, fencing-token claim, and sequence
synchronization, then publish `this.writer` as the last step. Close the local object
on any failure. The fast path must reject a non-null `writerRecoveryState`; it must
never return an object merely because the field is non-null.

**Verification.** Inject failure after publisher creation and at every subsequent
recovery milestone. A second call must either perform a clean recovery or return the
same typed terminal recovery failure, never the discarded publisher.

### P0-5. Make shutdown follow dependency order and make backup-reader disposal complete

**Evidence.** `ClusterFoundation.close` closes backup/storage task executors before
the `ClusterStorageManager`; the latter invokes a callback that finally closes the
housekeeper and replication. Scheduled maintenance can therefore call already-closed
collaborators. `BackupNodeManager.close` deliberately defers `dataClient.dispose()`
while a backup is active and asks a later close to retry, but `ClusterFoundation`
sets `closed = true` unconditionally, so that retry is unreachable.

**Change.** Make the close graph explicit and owned in one place:

1. stop the housekeeper so no new maintenance starts;
2. cancel or boundedly await the backup operation;
3. stop the replication reader/publisher;
4. close managers and their task executors;
5. close Store and low-level transport resources.

Remove housekeeper ownership from the Store shutdown callback. Do not mark the
foundation fully closed while a required client remains undisposed; retain per-stage
completion and allow a close retry to run only unfinished stages. If the intended
policy is never to wait for a backup, cancel it explicitly and still dispose the
reader in the same close.

**Verification.** Close during a blocked backup and during every housekeeper task.
Assert no task starts after stage 1, the Aeron polling thread terminates, every close
is attempted, and a first-stage failure remains retryable.

### P0-6. Make the borrowed `PersistenceManager` adapter non-owning

**Evidence.** `ClusterStorageManager.BinaryPersistenceManagerAdapter.close()` calls
`delegate.close()` on the Store's shared live `PersistenceManager`.

**Change.** Make adapter `close()` a no-op and state that it is borrowed. Only the
owning Store lifecycle may close the delegate. If an API genuinely needs an owned
manager, expose a separately created manager whose ownership is unambiguous; do not
reference-count this singleton unless multiple owners actually exist.

**Verification.** Close the adapter, then read and store through the live
`ClusterStorageManager`. The Store must remain usable and the delegate must close
exactly once when the Store shuts down.

### P0-7. Publish the reader's resolved cursor only after durability succeeds

**Evidence.** `TransactionAssembler.flushDeliveries` updates
`lastResolvedSequence`, `lastResolvedPosition`, and the terminal witness before
running `transactionResolved`. `cursorSnapshot()` can therefore expose the new
boundary while the Store force/cursor callback has not completed or has failed.

**Change.** Build an immutable candidate boundary while draining. Pass that candidate
to a callback that durably forces the Store and persists the cursor. Publish the
volatile/atomic `lastResolved*` state only after the callback succeeds. Keep the
delivery marker until both operations finish. This makes the order visible in the
types instead of relying on comments around mutable fields.

**Verification.** Block and fail the callback while another thread reads status and
`cursorSnapshot()`. Both must continue to report the previous durable boundary.

## P1 — correctness hardening and bounded liveness

### P1-1. Stop discovering the role by catching `IllegalStateException`

`ClusterNode.control()` calls `storageNodeManager()` and treats every
`IllegalStateException` as proof that it should use `backupNodeManager()`. Startup,
closed-state, and real storage-manager failures are thereby misclassified. Capture
the normalized `NodeRole` once in `ClusterNode` and select the control view with a
switch. Capture the same value in `ensureDataMessageAppliedListener`; its current
per-commit `props.nodeRole()` call rereads and reparses the environment. Wrong-role
access should use a dedicated exception or never be attempted.

### P1-2. Track maintenance degradation per task

`NodeHousekeeper.runGuarded` clears the node-wide `degradedFailure` whenever any
task succeeds. Keep a failure/streak record per task name, clear only that task on
success, and derive the aggregate failure from all active records. Fatal `Error`
stays sticky. A test should interleave a permanently failing backup task with a
successful GC task and prove health remains degraded.

### P1-3. Do not block watermark ingestion behind retention maintenance

`AeronArchiveRetention.onAgent` queues all work onto one executor and waits up to
60 seconds; timeout permanently latches `terminalFailure`. A reader watermark can
therefore block behind purge/stop/extend work. `WatermarkFanIn.drainDeferred` then
drops the deferred watermark on every `RuntimeException`, including transient
agent/I/O failures.

Use a bounded, non-blocking enqueue for watermark updates and expose completion
through the retention failure/status surface. Keep explicit operator maintenance
operations awaitable. Drop a watermark only for permanent identity/policy rejection;
retain the newest per reader after transient failure. Do not invent several new
executors: one serial owner plus a bounded mailbox is enough.

### P1-4. Make file fencing assumptions executable

The lease mutex normalizes a path, but `ACTIVE.get/put/remove` use the unnormalized
path; aliases can bypass the in-JVM holder check. Lease writes validate the target
symlink but do not revalidate every parent like `AtomicFileWriter`. `close()` removes
the `ACTIVE` entry even when it failed to prove the interprocess lock was released.
Writer startup also cannot prove that two hosts use the same lease volume; isolated
volumes both mint token 1.

Canonicalize one path once and use it for the mutex, registry, reads, writes, and
removal. Reuse one file-safety implementation for parent checks, owner-only temp
files, atomic replacement, and directory force. Keep the registry entry in an
orphan/failed-close state until lock release is proven. At deployment validation,
require a pre-provisioned shared lease namespace and document/test the supported
filesystem and lock semantics; fail closed on local or unknown filesystems in
production rather than claiming cross-host fencing.

`validateWriterCheckpointIdentity` must also validate a positive fencing token that
is not newer than the currently held token. Equality is wrong after a legitimate
restart because the new lease token is greater than the previous terminal
checkpoint; the valid relationship is `0 < checkpointToken <= heldToken`, followed
by the existing recording/tail reconciliation.

### P1-5. Bound lease-held terminal offers without weakening ownership

`WriterFencingLease.executeUnderOwnership` holds the interprocess `FileLock` across
the complete Aeron offer retry. Contrary to one source review, this does prevent a
successor from stealing during the offer; it does **not** create the claimed
self-fencing window. It does, however, block heartbeat, close, and takeover for the
full offer deadline.

Keep the atomic ownership guarantee, but make it cheap: use short per-attempt lease
claims, refresh/check ownership, perform one non-blocking offer, then recheck after
success. Bound the whole terminal-offer budget well below lease staleness. Do not
simply move the current multi-second retry outside the lock with a single precheck;
that creates the race the lock currently prevents.

### P1-6. Make index validation fail closed and stop locking application objects

Three current index hazards belong in one repair:

- `ClusterIndexValidation.validateLuceneIndex` accepts a null context, although a
  missing context cannot prove embedded storage.
- `StoreIndexReflection` selects the first assignable/name-matching field. An
  upstream layout with two candidates silently binds the wrong one.
- `ClusterIndexMaintenance` synchronizes on the user-visible `GigaMap` and claims
  queries use that monitor. Application code may lock the same object, and the
  upstream implementation does not make that monitor a supported contract.

Reject a null Lucene context. Resolve reflection layouts by collecting candidates
and requiring exactly one expected field of each name/type; pin the supported Store,
Lucene, and JVector layouts with compatibility tests. Use the existing
`StorageGraphCoordinator` write section plus a private index-maintenance lock, never
an application-visible object monitor.

### P1-7. Close small lifecycle races

- `StorageBackupManager.stopDataClient()` returns on `RESOLVED_BOUNDARY` before
  rechecking `dataClient.failure()`. Read failure, then stop result, then failure
  again, or return one atomic `StopResult` containing both outcome and cause.
- `AeronArchiveReader.pollSubscription()` reconnects unconditionally when the
  subscription is null. Return immediately when inactive or `disposeRequested`.
- `AeronReaderSlot.replace()` clears the volatile slot between disposal and factory
  completion while its Javadoc promises observers never see that window. Do not
  publish a second live reader before disposing the old one; instead represent
  STARTING explicitly or require observers that need a stable state to take the
  lifecycle lock, and fix the Javadoc.

### P1-8. Fail writer admission immediately after terminal driver failure

`recordDriverFailure` fails a current reader and updates health, but an already
constructed `AeronStorageBinaryReplicationTarget` can keep calling its coordinator.
Put the terminal cause in the write-admission predicate used by every prepare and
maintenance entry point. Throw one domain exception carrying that original cause;
do not let the next publication fail later with an unrelated Aeron error.

### P1-9. Replace incidental exception classes with one small domain taxonomy

`StorageBinaryDataException`, `StorageBinaryDataLifecycleException`, and
`StorageBinaryDataReseedException` all extend `IllegalStateException`; transport
code also throws raw `ArchiveException`, `IllegalArgumentException`, and plain
`IllegalStateException` for recovery decisions. There are two reseed types in
different packages.

Use one dependency-free `cluster.errors` leaf package (exporting only the failures
an embedding application can act on), rather than making storage import node:

- `ReplicationException` — base runtime failure;
- `CorruptReplicationDataException` — invalid frame/transaction;
- `ReplicationUnavailableException` — timeout/disconnect with preserved cause and
  Aeron error code;
- `ReseedRequiredException` — durable local state cannot be reconciled;
- `WriterFencedException` and `ReaderWriteRejectedException` — admission results.

Map at package boundaries and preserve causes/error codes. This also fixes the
`storage -> node.exceptions` dependency. In `ClusterFoundation.start`, rethrow
`RuntimeException` and `Error`, and wrap any other `Throwable`; never cast a general
`Throwable` to `RuntimeException`. `ClusterStorageManager.shutdown` must likewise
wrap and propagate a checked failure instead of silently returning. Isolate
`AeronArchiveRetention`'s replay-in-progress message substring check in one
version-pinned Aeron adapter; prefer an error code or explicit recording/replay state
when Aeron exposes it, and never spread message parsing through policy code.

### P1-10. Keep one durability mode and one recovery rule

`ENQUEUE_THEN_ARCHIVE` adds an entire local-acceptance fence/state machine yet still
requires reseed when the process dies after the Store write and before Archive
publication. `ARCHIVE_FIRST` also deliberately fails closed when an orphan Archive
tail cannot be reconciled. The current code explicitly says tail replay/truncation is
not implemented; this is a documented limitation, not silent corruption.

Delete `ENQUEUE_THEN_ARCHIVE` and its `ENQUEUED` branches. Keep archive-first as the
only write order. For an in-flight PREPARING fence, adopt one simple policy now:
reseed unless a matching terminal checkpoint already covers it. Do not add more
recovery states. Later deterministic tail truncation/completion should only replace
that policy if it removes reseeds without expanding the state machine.

### P1-11. Require an explicit deployment nonce and state the unauthenticated boundary

`AeronSettings` and the reader builder derive a wire nonce from the cluster UUID,
while `AeronReplicationEnvelope.defaultWireNonce` says derivation is for fixtures,
not deployments. Require a non-zero explicit nonce in production and confine the
derived helper to tests/development. A nonce, CRC, cluster id, fencing token, and
watermark are not authentication. Production must require a private/firewalled
replication and watermark network; disable quorum deletion when that boundary is not
present. Do not add per-frame cryptography unless the product's threat model changes.

## P2 — throughput, memory, and I/O

### P2-1. Replace rename-per-transition checkpoints with a fixed journal

Every PREPARING and terminal checkpoint currently writes a temp file, forces it,
renames it, and forces the directory. Some transitions run while `writeLock` is
held. This makes write throughput proportional to metadata fsync latency.

Use a pre-created, fixed-size two-slot journal with generation, state, payload
metadata, and CRC. Write the next generation into the inactive slot and force it;
startup selects the newest valid slot. This removes per-transaction create/rename/
directory-fsync churn while retaining torn-write detection. Move all blocking file
I/O outside `writeLock`; the in-memory transaction owner remains reserved until the
forced record completes. Measure before considering group commit, because group
commit changes acknowledgement latency and is not needed to obtain the first large
gain.

### P2-2. Remove the publisher-wide buffer lock from retry waits

`publishDataChunks` holds `offerLock` for the complete chunk loop, and each offer can
retry to the publication deadline. A single backpressured chunk blocks terminal
markers and close. Stop sharing one mutable envelope buffer across transactions.
Use a transaction-owned direct staging buffer, or use `ExclusivePublication.tryClaim`
only for frames at or below Aeron's claimable payload size and encode directly into
the term buffer. A 1 MiB fragmented message is not a valid blanket `tryClaim`
solution; chunk sizing must first be coherent with MTU/max payload.

One offer attempt should be non-blocking. Retry/backoff belongs outside the short
encode/claim critical section and must check close, deadline, driver failure, and
lease ownership on every attempt.

### P2-3. Stop polling the Archive control session on every idle spin

The publisher's recorded/start/stop waits throttle the substantive position probe
but call `pollForErrorResponse()` every iteration under the shared non-thread-safe
Archive monitor. Poll errors on the same `archiveProbeDelayNanos` cadence. Cache the
recording id after successful creation/extension; `newWriterCheckpoint` must not run
a synchronized catalog lookup for every state transition.

### P2-4. Bound reader staging by bytes, not only transaction count

The assembler flushes at `readerBarrierMaxTransactions`; 64 legal 64 MiB
transactions can stage roughly 4 GiB of direct memory. Track staged bytes using the
existing transaction-size/cache limit and flush when either count or bytes reaches
its bound. Also stop `controlledPoll` once the barrier byte budget is full so 256
large fragments cannot postpone stop/reconnect checks. Prefer deriving this bound
from an existing memory limit over adding another environment variable.

### P2-5. Allocate assembler storage once from declared payload length

The first validated chunk already carries the complete payload length, but
`TransactionAssembler` starts at at most 64 KiB and repeatedly allocate/copy/frees
until it reaches a large transaction. Allocate the exact validated direct capacity
on the first chunk. Retain growth only as a defensive protocol-error path that is
expected never to run.

### P2-6. Remove per-transaction arrays, duplicates, loaders, and wrapper graphs

The current import/materialization path allocates:

- duplicate `ByteBuffer` views and a new exact-size array in
  `StorageBinaryDataImporter.importDirect`;
- `Arrays.copyOfRange`, `ObjectMaterializer`, `ImportedBinarySource`, anonymous
  `PersistenceSource`, `LoadItemsChain`, and `BinaryLoader` in
  `StorageBinaryDataMaterializer.materialize`;
- direct buffers for copied imports, including a separately allocated empty direct
  buffer per empty slot.

Put reusable, worker-confined scratch in the merger worker: stable buffer-view
arrays, loader/source state, and materializer state whose upstream lifecycle permits
reuse. Pass `(array, offset, length)` rather than copying slices. Preserve the clear
ownership rule—exactly one native owner and exactly one release on every exit. Add
allocation/JFR assertions to the existing merger allocation tests rather than only
functional assertions.

### P2-7. Reduce CRC and copy passes without deleting distinct integrity checks blindly

The writer computes a full transaction CRC for the PREPARING fence, then walks the
payload again while framing; the reader verifies each envelope payload and walks it
again for the terminal transaction CRC. `Crc32c.update` also creates a
`ByteBuffer.duplicate()` for direct Agrona buffers.

First remove certain waste: reusable direct `ByteBuffer` views in checksum context,
caller-owned CRC state, and no accessor clone on internal-only envelope paths. Then
benchmark two protocol-preserving options: combine per-chunk CRCs into the terminal
CRC, or compute the full CRC once while copying into transaction-owned staging.
Per-frame corruption detection and end-to-end transaction identity are different
properties; do not drop either without changing the documented protocol. Use chunk
sizes near the actual network/Aeron payload regime (for example 64–256 KiB), not the
current 1 MiB over a 1,408-byte MTU, unless measurements on the target fabric prove
that fragmentation is superior.

### P2-8. Replace per-barrier root scans and unconditional index rebuilds with dirty ownership

Every apply barrier scans the index-relevant root graph, closes Lucene views, resets
JVector state, and probes vector indexes. The relevance cache bounds reflection work
but does not make a repeated graph scan or HNSW rebuild cheap.

Create one `IndexMaintenance` owner registered with the known `GigaMap` indexes at
startup. Mark an index dirty when imported entities that feed it change; retire or
rebuild each dirty index once per apply batch. If the upstream API cannot expose a
safe incremental signal, at minimum cache the validated map/index set for a stable
root generation and invalidate it only when roots/index registrations change. Keep
Lucene/JVector integration and soak coverage; do not solve this by disabling either
index.

### P2-9. Keep slow monitoring work off admission and avoid log allocation storms

`StorageDiskSpaceReader` recursively walks the full Store directory under one
monitor, making all concurrent scrapes wait. Use a single-flight refresh and serve
the previous measurement while one virtual task scans. `StorageLimitGate` logs a
warning every interval while full and eagerly formats DEBUG messages. Log on the
threshold edge plus a throttled reminder, and guard formatting with `isLoggable`.
Cache OS-family checks used by file forcing as static constants.

### P2-10. Match thread type to work

Use dedicated platform threads with Agrona `AgentRunner`/`IdleStrategy` for Aeron
polling and conductor-like loops; virtual-thread migration would add scheduler
jitter to latency-sensitive non-blocking polls. Use virtual threads for blocking
backup, directory scan, and Store-maintenance tasks, with the platform scheduler
only triggering work and preventing overlap. This reconciles the two source-review
suggestions: platform agents for Aeron; virtual tasks for blocking maintenance.

`StructuredTaskScope` is not a replacement for these long-lived agents. Use it only
for a bounded start/stop fan-out where sibling cancellation has a clear owner; do
not add it merely because Java 26 offers it.

### P2-11. Replace spin admission and ad hoc deadlines with one retry clock

`lockWriteAdmission` parks for a fixed 100 microseconds until `tryLock` succeeds and
has no deadline. Use the coordinator condition to await ownership release with a
saturating deadline from `ReplicationRetry`; propagate interruption and return a
typed retryable/unavailable failure on timeout. Use the same deadline helpers for
`awaitNoCommit`. Expose only knobs operators can act on—transaction/chunk size,
memory budget, and operation deadlines—rather than adding environment variables for
every spin/yield/probe constant. Build the default channel URI from the validated
term/MTU configuration so changing one does not make defaults self-contradictory.

## P3 — simplification, packages, visibility, and names

### P3-1. Establish one dependency direction

The desired direction is:

```text
cluster.api -> node assembly -> replication/storage ports -> Aeron + Store details
```

Today `storage` imports `node.exceptions`, and
`storage.aeron.reader.AeronArchiveReader` imports the 1,664-line node-side Aeron
provider while that provider imports all storage Aeron packages. Move the common
errors and narrow SPIs below `node`. Keep orchestration in `node.aeron`; keep codecs,
reader, writer, checkpoint, and transport implementation in `storage.aeron`; inject
the small callback the reader needs instead of the provider class. Do not move every
Aeron class into one giant package—the important result is one-way dependencies and
one owner per state machine.

### P3-2. Split `storage.types` by concern and remove duplicate file utilities

`storage.types` currently mixes replication contracts, native-buffer machinery,
index policy, generic file safety, checksums, Store wiring, and exceptions. Use:

- `storage.binary` — distributor/client/receiver/merger/import/materialization;
- `storage.index` — index facade, validation, maintenance, upstream layout pin;
- `storage.io` — atomic metadata file and path safety, only if more than one owner
  remains;
- `node.store` — `DistributedStorage`, `StorageGraphCoordinator`, read-only target,
  and Store adapter wiring.

Fold `StorageFileOperations` and `PathSecurity` into one implementation; remove
one-line delegates. Move `BackupRestorePolicy` and the backup-only upload validation
to `node.backup`. `UserUploadValidator` currently has no production caller according
to both source search and the code graph: either wire one authoritative validation
path into restore or delete it—do not keep a second dead validator.

### P3-3. Delete unused modes and inert wiring

- Remove asynchronous distribution from `NodeOptions`, `NodeConfiguration`, and
  `ClusterFoundation`; Aeron already rejects it and no second transport implements
  it.
- Delete the 21-component `NodeConfiguration` service-locator record. The public
  builder exposes only a few real inputs while the rest are nullable internal/test
  seams. Construct owned collaborators in focused assembly methods; inject only the
  small ports a test genuinely substitutes.
- Remove no-op close stages, unused overloads (`WriterLeaseGate.of` where confirmed
  unreferenced), duplicate `isCurrent` names, and single-call wrapper helpers.
- Replace the `ScopedValue<ArrayList<ByteBuffer>>` in `StorageBinaryBuffers` with a
  worker-owned scratch list or simple local two-pass collection. The current call
  deliberately recurses only to install the value and still allocates a list for
  each outer call; it is modern syntax without a useful lifetime benefit.

### P3-4. Split only where state ownership changes

The six largest classes are 944–1,664 lines. Do not mechanically make dozens of
stateless helpers. Extract owners with independent invariants:

- provider -> `AeronRuntimeOwner`, `AeronWriterTransport`,
  `AeronReaderTransport`, `AeronRetentionOwner`;
- merger -> `ApplyQueue` (bytes/conditions), `ApplyWorker`
  (import/materialization/index barrier), `MergerLifecycle`;
- foundation -> `NodeAssembly` and `NodeLifecycle`;
- publisher -> transaction-owned `EnvelopeFramer`/offer state;
- settings -> immutable `Topology`, `ArchivePolicy`, `Timeouts`, `Authentication`,
  with one environment parser;
- Store manager -> guarded Store facade plus lifecycle owner, rather than dozens of
  forwarding methods in one interface file.

This is the minimum split that makes 1-writer/N-reader, memory ownership, and close
order separately testable.

### P3-5. Shrink visibility after the package move

The module exports only `peruncs.datagrid.cluster.api`, yet roughly 75 top-level main
types are declared public. Public is still meaningful inside a non-exported module:
it permits accidental cross-package coupling and makes later moves harder. After
P3-1/P3-2, make implementations, codecs, file helpers, `CrashHook`, reader delivery
listeners, lease gates, `CloseSequencer`, and nested implementations package-private.
Keep only the exported facade and deliberately narrow internal ports public.

Move crash injection behind one package-private internal seam. Do not place test
hooks in public production API, but keep the hook implementation in main where forked
crash tests need the bytecode.

### P3-6. Normalize Java construction patterns

`AeronCheckpointCodec` is an interface containing static methods only; make it a
final utility with a private constructor. For the repeated `interface + nested
Default + New()` pattern, choose one of two forms:

- a final class with a constructor/`of` factory when there is one implementation;
- a small port plus a named top-level implementation when substitution is real.

Rename `New`/`NoOp` to Java-style `of`/`create`/`noOp`. Group high-arity records by
real concern, but do not allocate request records on hot paths solely to satisfy an
argument-count rule. `NodeStatus`, settings, and wiring benefit from nested records;
per-chunk offers do not.

### P3-7. Make the exported API match role and failure semantics

`BackupNodeControl extends StorageNodeControl`, inheriting operations a backup role
may not own. Prefer composition (`BackupNodeControl.storage()`) or a common minimal
`NodeStatusView`. `NodeStatus` has 13 flat components and several `-1` sentinels;
compose it from role/readiness plus one replication metrics record and document
absence once. Consolidate `api.ReplicationState` and internal
`ReplicationHealth.State` so mapping cannot drift. Use an enum (or omit the field)
instead of the free-form transport string if Aeron remains the only transport.

### P3-8. Rename by responsibility, after ownership is fixed

Apply renames in the same change as package/owner moves so there is one mechanical
break:

| Current | Recommended | Reason |
|---|---|---|
| `ClusterFoundation` | `ClusterNodeRuntime` or `NodeAssembly` | Owns assembly and lifecycle, not an Eclipse foundation |
| nested `ClusterFoundation.Node` | `NodeLifecycle` | Stateful running owner |
| `NodeConfiguration` | delete; otherwise `NodeWiring` | Contains collaborators, not user configuration |
| `NodeLibraryPropertiesProvider` | `NodeSettingsSource` | Reads settings; “Library” adds no meaning |
| `NodeLibraryException` | `NodeException` | Same cleanup |
| `AeronClusterReplicationTransportProvider` | split; facade `AeronTransport` | “Cluster/Replication/Transport/Provider” hides several owners |
| `StorageBinaryDataClient` | `ReplicationApplier` | Replays and applies writer transactions |
| `StorageBinaryDataDistributor` | `ReplicationPublisher` | Writer-side publication port |
| `DataMessageAppliedListener` | `CommitAppliedListener` | Called after a resolved commit boundary |
| `StoredReplicationCursorManager` | `DurableCursorFile` | One file/codec owner, not a manager |
| `NodeHousekeeper` | `NodeMaintenanceScheduler` | States the work |
| `StorageDiskSpaceReader` | `StorageUsageGauge` | Measures one directory |
| `WatermarkFanIn` | `WatermarkCollector` | Removes topology jargon |
| `AeronReaderSlot` | `CurrentReader` | Holds one lifecycle-owned reader |
| `Crc32c` | `Crc32C` | Java acronym casing |

Use one glossary in names and Javadocs: **writer/reader/backup-reader** are roles;
**publisher/applier** are data paths; **sequence** is a logical transaction number;
**position** is an Aeron byte position; **cursor** is a reader restart bookmark;
**checkpoint** is writer recovery evidence; **watermark** authorizes retention and
is never a restart bookmark.

## P4 — Javadocs and README

### P4-1. Keep architecture in Javadocs; make README operational

The architectural-documentation policy is intentional and should be strengthened,
not reversed:

- `module-info.java` is the canonical system design: roles, ownership, write/apply
  ordering, fencing, durability, recovery, trust boundaries, and package map.
- each `package-info.java` explains that package's responsibility, dependency
  direction, and invariants;
- entity/method Javadocs explain the local human-visible contract;
- `README.md` contains build/run/deployment/configuration/monitoring/recovery
  procedures and links to the generated module Javadocs for design rationale.

The current README repeats design explanations for archive-first ordering, fencing,
watermark semantics, and BCE layering. Move those explanations into the relevant
module/package/entity Javadocs and leave the operator actions and environment keys in
README. Conversely, keep the rich module design instead of reducing it to 15 lines.
Partition detail out of `module-info.java` only when an owning package/type can state
it more precisely; link the sections.

### P4-2. Correct contradictions and document operator decisions at the API

Fix `authntication` and `air-gaped` in `module-info.java`, and replace the heading
claiming authentication is not required with the actual contract: live replication
and watermarks require an isolated trusted network; Archive control authentication
is separate. The module says “exported packages” and errors used at boundaries even
though only `cluster.api` is exported; either expose typed API failures there or stop
claiming callers can name internal exceptions.

Document at the exported methods:

- `ClusterNode.open`: required role/identity settings and fail-closed startup;
- backup methods: correct role, concurrency/busy behavior, stop/resume effects;
- `ClusterStore.store*`: reader rejection, fenced writer, capacity failure, and
  whether an uncertain commit is safe to retry;
- `ClusterStore.read`: the callback runs under the graph read boundary and returned
  live graph objects must not escape/use that boundary; consider renaming to
  `withRootRead` if this remains a discipline rather than an enforceable type;
- `NodeStatus`: what operators do for FAILED, DEGRADED, and RESEED_REQUIRED and how
  lag is calculated.

### P4-3. Rewrite first sentences in plain language and remove implementation history

Start with what the thing does, then the mechanism. Examples:

- `StorageNodeHealthCheck`: “Reports whether the Store and replication can serve
  requests.”
- `ReseedRequiredException`: “Thrown when this node cannot resume and must be
  restored from a compatible seed.”
- `ReplicationLogRetention`: “Deletes old Archive segments only after every
  configured reader has durably passed them.”
- `WriterFencingLease`: “Chooses the only writer allowed to publish for this Store
  generation.”
- `StorageBinaryDataClient`: “Replays writer transactions in order and applies them
  to this Store.”

Remove commit-history explanations (“used to”, removed HTTP transport, why a prior
implementation chose `ScopedValue`) from public Javadocs. Keep current constraints,
failure behavior, ownership, thread safety, and recovery action. Fix broken links,
US/UK spelling drift, false export claims, and the `NodeHousekeeper` grammar error.
All current source packages already have `package-info.java`; the earlier claim that
writer/reader packages are missing them is stale. Rewrite weak package docs rather
than adding duplicates.

## Recommended implementation order and acceptance gates

1. **P0-1 through P0-4.** Run focused concurrency tests plus writer takeover crash
   cells. No later performance work is meaningful until one transaction has one
   owner and every frame is fenced.
2. **P0-5 through P0-7 and P1-1 through P1-9.** Run close-race, backup-stop,
   callback-failure, driver-failure, index compatibility, and health-latch tests.
3. **P1-10 and P2-1.** Delete enqueue-first, replace the checkpoint writer, then run
   the complete writer/reader/backup crash matrix. Expected results must be only
   clean continuation or an explicit `ReseedRequiredException`; never state reuse.
4. **P2-2 through P2-7.** Use JMH/full-path benchmarks and JFR allocation summaries.
   Record throughput, p50/p99 commit latency, replay throughput, heap allocation per
   transaction, direct-memory high-water mark, and Archive-control lock time before
   and after each change.
5. **P2-8 through P2-11.** Run Lucene/JVector add/update/remove convergence tests,
   live query races, slow-disk simulations, and no-data reconnect/stop tests.
6. **P3/P4.** Perform package moves, visibility reductions, renames, and Javadocs in
   one compile-breaking refactor after behavior is stable. Update module/package/
   entity Javadocs and trim README architecture duplication in the same change.

Release claims should require:

- a forked two-writer takeover test on the exact production filesystem;
- crash injection at every PREPARING/data/local-write/terminal/checkpoint boundary;
- a slow/backpressured Aeron test proving deadlines and close remain bounded;
- allocation budgets for 64 MiB write and replay paths;
- Lucene and JVector correctness during concurrent import/query;
- a shutdown matrix for writer, reader, backup-reader, active backup, and failed
  startup;
- module-path tests proving only the intended `cluster.api` surface is reachable.

## SOL review coverage

Every SOL item is retained; several are combined with stronger corroborating
findings from the other reviews.

| SOL item | Final location |
|---|---|
| 1. apply-queue byte accounting | P0-1 |
| 2. atomic writer/dictionary ownership | P0-2 |
| 3. role discovery via `IllegalStateException` | P1-1 |
| 4. exact assembler allocation | P2-5 |
| 5. Store import heap churn | P2-6 |
| 6. per-barrier index scan/rebuild | P2-8 |
| 7. fixed checkpoint journal | P2-1 |
| 8. filesystem fencing constraints | P1-4/P1-5 |
| 9. unauthenticated live channels | P1-11/P4-2 |
| 10. remove async distribution | P3-3 |
| 11. delete inert `NodeConfiguration` | P3-3 |
| 12. package ownership | P3-1/P3-2 |
| 13. names and state-owner splits | P3-4/P3-8 |
| 14. platform Aeron agents | P2-10 |
| 15. `Throwable` handling | P1-9 |
| 16. dead utilities/factory normalization | P3-2/P3-3/P3-6 |
| 17. human Javadocs | P4-1 through P4-3 |

## Validation ledger for the additional reviews

This ledger records merged, corrected, and rejected claims so stale findings do not
survive merely because they appeared in more than one source.

### SPARK review

| Finding | Disposition |
|---|---|
| C1 data chunks bypass fencing | Accepted -> P0-3 |
| C2 watermarks need a fencing token | Rejected as a correctness fix. A watermark is reader progress for epoch/recording/generation; writer tokens do not authenticate it and old progress remains conservative. The security boundary is P1-11. |
| C3 equal tokens on isolated volumes | Accepted as deployment/filesystem failure -> P1-4 |
| C4/C5 archive-first and enqueue-first crash gaps | Corrected. Current code fails closed rather than silently continuing. Delete enqueue-first and keep one explicit reseed rule -> P1-10. |
| C6 crash between Store apply and cursor | Rejected as stated. The delivery marker exists before import and remains until cursor durability; loss of a successfully forced marker is outside the promised filesystem model. P0-7 still fixes premature in-memory publication. |
| C7 resumed terminal off by one | Rejected. Aeron `Header.position()` is the post-frame position persisted as the cursor, so replay resumes after the terminal. Keep a crash-cycle regression test. |
| C8 retention uses a volatile tail | Rejected for production. Production requires Archive file sync > 0, Aeron forces the recording block before advancing recording position, and retention checks that position. |
| C9 startup cast | Accepted -> P1-9 |
| C10 checkpoint token validation | Accepted with monotonic, not equality, rule -> P1-4 |
| C11 exception hygiene | Accepted/merged -> P1-9 |
| C12 lease aliases/TOCTOU | Accepted -> P1-4 |
| L1 offer lock | Accepted -> P2-2 |
| L2 admission spin | Accepted -> P2-11 |
| L3 checkpoint fsync | Accepted -> P2-1 |
| L4 Archive error polling | Accepted -> P2-3 |
| L5 two writer copies | Accepted with `tryClaim` size correction -> P2-2/P2-7 |
| L6 CRC passes | Accepted as benchmark-led optimization; distinct integrity properties remain -> P2-7 |
| L7 CRC duplicate allocation | Accepted -> P2-7 |
| L8 assembler growth | Accepted -> P2-5 |
| L9 MTU/chunk mismatch | Accepted -> P2-7 |
| L10 count-only barrier | Accepted -> P2-4 |
| L11 shared CRC lock domains | Accepted as a consequence of split transaction ownership -> P0-2 |
| L12 deadlines and all hot knobs | Partly accepted -> P2-11. Deadline consistency and default URI are valid; adding an environment key for every retry constant is rejected as configuration bloat. |
| L13 allocations/log/index work | Accepted/merged -> P2-6 through P2-9 |
| S1-S8 structure/visibility/names | Accepted and consolidated -> P3-1 through P3-8 |
| D1-D2/D5 contracts and glossary | Accepted -> P4-2/P4-3/P3-8 |
| D3 move design out of module Javadoc | Rejected. Module Javadoc remains canonical; detail moves only to owning package/entity Javadocs, not out of Javadocs -> P4-1. |
| D4 missing reader/writer package docs | Rejected as stale; both files exist. |
| Appendix StructuredTaskScope everywhere | Rejected as indiscriminate. Long-lived Aeron agents are not structured subtasks -> P2-10. |

### MIMO review

| Finding | Disposition |
|---|---|
| C1 half-initialized writer | Accepted -> P0-4 |
| C2 lease lock around offer | Accepted as liveness, not the claimed self-fencing mechanism -> P1-5 |
| C3 checkpoint fsync under lock | Accepted -> P2-1 |
| C4 shared PM close | Accepted -> P0-6 |
| C5 resolved state before callback | Accepted -> P0-7 |
| L1 housekeeper global clear | Accepted -> P1-2 |
| L2 coordinator ignores recovery | Merged into P0-4; fixing publication/fast-path rules removes the separate symptom. |
| L3 retention blocking/terminal latch | Accepted -> P1-3 |
| L4 transient watermark drop | Accepted -> P1-3 |
| L5 reconnect budget on quiet Archive | Not established. `PersistentSubscription.isLive()` and incident tracking can clear the incident without new data. Add a no-data regression test before changing policy. |
| L6 null-subscription dispose | Accepted -> P1-7 |
| L7 close resets `closing` | Rejected as a general bug. Current close implementations retain per-resource completion and intentionally permit retry; fix concrete P0-5 ownership instead. |
| L8 duplicate terminal after restart | Rejected with SPARK C7; cursor is post-frame. |
| L9-L15 hot-path issues | Accepted/merged -> P2-2, P2-3, P2-7, P2-9, P2-11 |
| S1-S11 | Accepted/merged -> P1-9 and P3-1 through P3-8 |
| R1 message-string retention classification | Accepted as lower-priority robustness: isolate it in an Aeron-version adapter, prefer error codes/state probes, and retain a pinned compatibility test. |
| R2 lease close registry removal | Accepted -> P1-4 |
| R3 driver failure vs admission | Accepted -> P1-8 |
| R4 interrupt drops prior Archive cause | Rejected; the retryer has no prior Archive exception to preserve on that branch. Preserve the interrupt itself, which it already does. |
| R5 reader-slot null window | Accepted as state/Javadoc mismatch -> P1-7 |
| R6 retention epoch file | Rejected as corruption: current restore fails closed on epoch mismatch. Deleting/rewriting it automatically would weaken evidence. |
| J1-J6/R7/R8 | Accepted/merged -> P3-3, P3-6 through P3-8, P4-1 through P4-3 |

### KIMI review

| Finding | Disposition |
|---|---|
| C1 publisher `failed` visibility | Rejected as stale; the field is currently `volatile`. |
| C2 cold cursor off by one | Rejected; cursor position is post-frame. |
| C3 native buffer leak on receiver throw | Rejected. `StorageBinaryDataReceiver` transfers ownership on every success/failure exit, and the merger releases on every validation/queue failure. Keep the ownership test. |
| C4 housekeeper clear | Accepted -> P1-2 |
| C5 close order | Accepted -> P0-5 |
| C6 deferred backup client disposal | Accepted -> P0-5 |
| C7 missing merger flush signal | Accepted -> P0-1 |
| C8 startup cast | Accepted -> P1-9 |
| C9 null Lucene context | Accepted -> P1-6 |
| C10 locking `GigaMap` | Accepted -> P1-6 |
| C11 exception base classes | Accepted -> P1-9 |
| C12 backup stop TOCTOU | Accepted -> P1-7 |
| C13 first-match reflection | Accepted -> P1-6 |
| P1 materializer allocations | Accepted -> P2-6 |
| P2 role/env reparse | Accepted. Capture `NodeRole` once in `ensureDataMessageAppliedListener`; include with P1-1/P3-3. |
| P3 derived nonce | Accepted -> P1-11 |
| P4 housekeeper platform threads | Corrected -> P2-10 (virtual for blocking maintenance, platform for Aeron polls). |
| P5 warning storm | Accepted -> P2-9 |
| P6 synchronized disk walk | Accepted -> P2-9 |
| P7 smaller allocations | Partly accepted -> P2-6/P2-7/P3-3. Lowercasing and cold record-accessor clones are cleanup, not hot-path priorities. |
| S1-S8, naming, docs, API assessment | Accepted/merged -> P3/P4. |
| “Verified quiet areas” | Omitted by policy: this review reports actionable problems, not praise. |

## Review limitations

This is source-level analysis. No Maven goal, JUnit suite, crash matrix, soak,
benchmark, JFR capture, live Aeron cluster, or production filesystem test was run,
as required by the review-only guardrail. Line numbers will drift; method and type
names are the durable references. The code graph had a partial parse around
`ClusterIndexValidation.java:415`, so index claims were checked directly against the
source rather than inferred from missing graph edges.
