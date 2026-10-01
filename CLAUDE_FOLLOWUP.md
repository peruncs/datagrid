# CLAUDE_FOLLOWUP — review of the OPUS_REVIEW.md work and full-codebase review

**Date:** 2026-09-30. **Tree:** `main` at `1fb370d` plus the uncommitted working tree (gather-offer
work, benchmark harness, `OPUS_REVIEW.md` rev 134).
**Mode:** review only (AGENTS.md). No build, test or benchmark was run and no source was changed;
every claim below is from reading code. File references are `M/` = `src/main/java/peruncs/cluster/`,
`T/` = `src/test/java/peruncs/cluster/`.

## 0. Coverage and limits (read this first)

Read line by line: `AeronReplicationPublisher`, `AeronReplicationWriteCoordinator`,
`AeronStorageBinaryReplicationTarget`, `EnvelopeFramer`, `AeronOfferRetryer`, `AeronWriterTailRecovery`,
`StorageBinaryDataMerger` (first 420 lines), `ApplyWorker`, `NativeBufferPool`, `NativeMemory`,
`TransactionAssembler` (first 560 lines), `AeronArchiveReader` (first 700 lines),
`StorageGraphCoordinator`, `StoreIndexReflection`, `ReplicationMark`, `AeronDistributionGate`,
`ReplicationPublisher` (head), `GuardingStorageManager` (lines 360–620 and an outline of the rest),
`NodeLifecycle` (lines 225–565), `AeronWriterTransport` (lines 100–175 and 420–610), the whole
uncommitted `src/main` diff and the publisher test diff. The remaining ~20k lines were surveyed with
greps only (synchronization, reflection, `Unsafe`, long parameter lists, FQNs, stale vocabulary,
thread creation, interfaces).

**Not reviewed in depth:** `BackupArchive`, `FilesystemVolumeBackupBackend`, `NodeConfig`,
`AeronSettings`, `AeronRuntime`, `AeronWatermarkChannel`, `AeronArchiveRetention`,
`ClusterIndexValidation`, `ClusterIndexMaintenance`, `AtomicFileWriter`, `AeronReplicationEnvelope`
decode side, the second halves of the merger, assembler, reader and guarding manager. Absence of a
finding there is not evidence of correctness. Backup zip extraction had only a grep pass
(`normalize`/`startsWith`/`NOFOLLOW_LINKS`/size checks are present).

**Not done:** AGENTS rule 27 (compare against the Aeron cookbook/examples and the Eclipse Store /
Serializer tests) was not performed beyond confirming Aeron's `ExclusivePublication.offer(
DirectBufferVector[])` copies synchronously (`$GITHUB_ROOT/aeron-io/.../ExclusivePublication.java:353`).
No P0 (data loss / corruption) was *confirmed*; B11 is a latent divergence hazard.

Severity follows OPUS: P0 correctness/data loss, P1 major performance/robustness/contract, P2
simplification or rule violation, P3 cosmetic.

## 1. Summary

| ID | Sev | Finding | Where |
|----|-----|---------|-------|
| B1 | P1 | Per-batch watchdog tasks are cancelled but stay queued for 3 min / 30 min | `StorageBinaryDataMerger.java:190`, `ApplyWorker.java:168,234,263` |
| B2 | P1 | Any Archive hiccup during writer recovery becomes a permanent `RESEED_REQUIRED` | `AeronWriterTailRecovery.java:182-188`, `AeronWriterTransport.java:496,588,604,548-562` |
| B3 | P1 | JVector "invalidation" writes foreign private fields through `Unsafe` without a lock or barrier | `StoreIndexReflection.java:84-123` |
| B4 | P1 | Vector warm-up still blacks out all graph reads for the rebuild time (24 s at 100k vectors) | `ApplyWorker.java:242-248` (known P1-4) |
| B5 | P2 | Writer prepare path holds the coordinator lock across fsync wait and local Store write; Javadoc says otherwise; failed prepare waits on the dead Archive twice | `AeronReplicationWriteCoordinator.java:178-194`, `AeronReplicationPublisher.java:402-406` |
| B6 | P2 | Materializer runs on a virtual thread (CPU-bound, 24 s); D-24 specified a platform thread | `StorageBinaryDataMerger.java:184` |
| B7 | P2 | Data Grid `ReplicationPublisher` SPI and its dead failover plumbing still exist | `storage/binary/ReplicationPublisher.java`, `node/aeron/AeronDistributionGate.java` |
| B8 | P2 | One writer state machine is spread over three classes (publisher 1098, coordinator 884, target 270 lines) | `storage/aeron/writer/*` |
| B9 | P2 | Writer checksums the payload three times; two passes are fence leftovers | `AeronReplicationWriteCoordinator.java:466`, `EnvelopeFramer.java:138-228` |
| B10 | P2 | Gather-offer change keeps two copy loops and an aliasing sentinel | `EnvelopeFramer.java:138-228`, `AeronReplicationEnvelope.java` (~192) |
| B11 | P2 | `PreparedTransaction.close()` defaults to ABORT even after the Store accepted the write | `AeronReplicationPublisher.java:1081-1096` |
| B12 | P2 | Recovery window ignores Aeron framing overhead | `AeronWriterTailRecovery.java:55-61` |
| B13 | P2 | `NodeLifecycle` has two ~110-line near-duplicate role start methods | `NodeLifecycle.java:264-373,404-558` |
| B14 | P2 | `GuardingStorageManager` is a 1226-line facade with two duplicated cause-chain walkers | `GuardingStorageManager.java:449-499` |
| B15 | P2 | Single-threaded `TransactionAssembler` takes two monitors per fragment (plus a third for staging) "for concurrent callers" | `TransactionAssembler.java:117-176` |
| B16 | P2 | Envelope codec takes 17/18 positional arguments on the hot path | `AeronReplicationEnvelope.encode*` |
| B17 | P2 | Test-only factories and public constructors live in production classes | `AeronReplicationPublisher.java:93-151`, `AeronStorageBinaryReplicationTarget.java:64-134` |
| B18 | P2 | Global static `IdentityHashMap` + monitor in `NativeMemory` | `NativeMemory.java:14-38` |
| B19 | P2 | `ReplicationMark` has public mutable fields written from another package | `ReplicationMark.java`, `AeronReplicationWriteCoordinator.java:221-224` |
| B20 | P2 | Documentation and formatting debt contradicts OPUS "done" claims | §4 |
| B21 | P3 | Retry pacing not configurable; every default timeout is 30 s | `AeronRetryPolicy.defaults()` |
| B22 | P3 | Benchmark smoke test in the default unit gate; benchmark depends on IT internals | `T/node/aeron/AeronFullPathBenchmarkTest.java` |
| B23 | P3 | Dead branch, double-synchronized read, pool accounting by logical bytes | §3 |

## 2. The uncommitted changes (OPUS P1-6 gather offers, benchmark harness)

### 2.1 What changed

`EnvelopeFramer` now offers full 128 KiB chunks as an Aeron `DirectBufferVector[]` (header vector plus
one vector per source segment) when `length >= DEFAULT_CHUNK_SIZE` and the offerer supports vectors;
`AeronOfferRetryer` gained a vector overload; `GatherScratch` reuses wrappers and vectors;
`AeronReplicationEnvelope` Javadoc was reworded; `AeronReaderTransport` overrides `currentSequence()`;
`StorageGraphCoordinator` and `StoreIndexReflection` Javadoc were corrected.

I could not find a functional defect in the gather loop itself: bounds, wrapper reuse, `finally`
clearing, vector-array caching and retry-after-back-pressure are consistent, and Aeron copies the
vectors before returning. The problems are design and test coverage.

### 2.2 Findings on the change

**B10 — duplicated loops, aliasing sentinel, heuristic threshold (P2).**
- `offerDataChunks` (staged, `EnvelopeFramer.java:138-186`) and `offerGatheredDataChunks`
  (`:188-228`) are the same 40-line walk over the source buffers; they differ only in "copy into
  `buffer` then offer" versus "record a vector then offer". Same for `offerDataChunk` vs
  `offerGatheredDataChunk` (`:305-336`) and for the two `AeronOfferRetryer.offer` overloads.
- OPUS rev 132 measured gather ≥ staging at every size (1.7% at 64 KiB, up to 9.6% at 1 MiB). There
  is no size at which staging wins, so the 128 KiB switch, `supportsVectors()` on `Offerer`,
  `MIN_GATHER_PAYLOAD_BYTES` and the whole staged branch exist only to cover an unmeasured case.
- The threshold is `DEFAULT_CHUNK_SIZE` (`:22`), but the live value is `PERUNCS_AERON_CHUNK_SIZE`.
  A node configured with 16 KiB chunks switches at 128 KiB of *transaction* size, which is not what
  the JMH run measured.
- `AeronReplicationEnvelope.encodeWithPayloadCrc` skips the payload copy when `payload == target`
  and `payloadOffset == targetOffset + HEADER_LENGTH`. The check pre-dates this change, but the gather
  path now depends on it too (`offerGatheredDataChunk` passes the header buffer as its own payload and
  the Javadoc was reworded to bless it). A reference comparison that silently turns off a copy is a
  trap for the next caller.

*Recommendation.* Delete the staged path and `supportsVectors`/`MIN_GATHER_PAYLOAD_BYTES`; always offer
`[header, payload-vectors…]`. For zero-payload frames (dictionary, markers) use a one-element vector.
Split the encoder into `encodeHeader(...)` (writes the 84-byte header, takes the payload CRC) and
`encode(...)` = `encodeHeader` + `putBytes`, so no caller depends on aliasing. The test offerers
implement the vector method once (the new test already does). Net effect: about 60 fewer lines, one
code path under test, one CRC pass shape (see B9).

**Test gaps for the gather path** (`T/storage/aeron/writer/AeronReplicationPublisherTest.java`, new
`gathersFullChunksWithoutChangingSourceBuffers`):
1. Only two sources, one chunk boundary inside a source. `ensureSourceCapacity` growth (more than 4
   sources), a source that ends exactly on a chunk boundary, and a chunk spanning three or more
   sources are untested.
2. No differential test that gathered and staged output are byte-identical for the same input
   (wire bytes, header CRC, chunk CRC, commit CRC). After B10 this becomes moot; until then it is the
   only proof the two paths agree.
3. No heap-backed / read-only source buffer case (`UnsafeBuffer.wrap` of a read-only heap buffer is
   used by `clearSources`).
4. The "data buffer length changed" `IllegalArgumentException` after a partial gather is untested;
   what the reader sees (some chunks, then an ABORT from `prepareWithRecovery`) is untested too.
5. The mocked offerer cannot catch an Aeron-side vector validation failure. The real-Archive path
   only gets gathered offers if an IT uses a payload ≥ 128 KiB; I did not find one in the
   `-Pintegration` list that is asserted to take the gather branch (add a counter or a package-private
   `gatheredChunks()` so the IT can assert the branch ran).

**Stale Javadoc introduced/left by the change.** `EnvelopeFramer.java:135-137` still says "The earlier
fence CRC remains a separate pass because it is the recovery evidence written before local Store
acceptance" — there is no fence (see B9, §4).

**Benchmark harness (B22, P3).** `AeronFullPathBenchmarkTest` starts a real multi-node Aeron setup
(default 1 s warm-up + 2 s window) in the default `mvn test` gate, and the benchmark reuses
IT-private fixtures (`AeronStoreIntegrationIT.store`, `latestSequence`, `ReaderNode.openForBenchmark`,
which wraps the production receiver with a test-only `StorageBinaryDataReceiver` copy). Move the
smoke test to `-Pbench` or an `*IT`, and move shared fixtures to a `T/fixture` package so the IT does
not grow benchmark hooks. The new `currentSequence()` override in `AeronReaderTransport` (`:302`)
serves that sampler (OPUS rev 127: avoid allocating a `ReplicationPosition` per sample); say so in its
Javadoc, since a production class now carries a benchmark-driven method.

## 3. New findings, by severity

### P1

**B1 — cancelled watchdog tasks accumulate (`StorageBinaryDataMerger.java:190-194`,
`ApplyWorker.java:168-171, 230-237, 263-269`).**
Each applied batch schedules two `ScheduledFutureTask`s on `Executors.newSingleThreadScheduledExecutor`
(materialization budget, default 60 s × 3 = 180 s; index-refresh budget ×10 = 1800 s) and then calls
`cancel(false)`. `ScheduledThreadPoolExecutor` only drops a cancelled task from its queue when
`setRemoveOnCancelPolicy(true)` is set; the policy is not set anywhere in `src/main`, and the
`newSingleThreadScheduledExecutor` wrapper cannot set it. So every batch leaves two dead tasks (each
capturing the worker and a long) in the delay queue for up to 3 min and 30 min.
*Failure scenario.* A reader replaying a large backlog or serving a busy writer at N batches/s holds
about 2,000 × N dead tasks (my estimate: hundreds of KB per batch/s, tens of MB at 100 batches/s),
plus O(log n) heap maintenance on every schedule. It is bounded by time, not by anything operators can
see. The comment at `ApplyWorker.java:143` ("steady state allocates nothing") is also wrong: two
tasks, two lambdas and the `graphUpdater` lambda are allocated per batch, and `Drain.ensureViews`
(`:377`) reallocates `views` whenever the transaction buffer count changes.
*Recommendation.* Replace per-batch scheduling with one watchdog loop: the worker writes a
`volatile long deadlineNanos` (and a phase code) at each phase boundary; a single periodic tick (for
example every second) compares `System.nanoTime()` to the deadline and latches the failure. No
per-batch objects, no cancel race, and `budgetLock`, `materializedAtNanos`, `indexRefreshedAtNanos`
and `indexRefreshWatchdog` disappear. If the scheduler stays, construct a `ScheduledThreadPoolExecutor`
directly and call `setRemoveOnCancelPolicy(true)`. Add a unit test that applies 10k trivial batches
and asserts `getQueue().size()` stays near zero.

**B2 — transient Archive failures during writer start are reported as reseed and are never retried
(`AeronWriterTailRecovery.java:182-188`, `AeronWriterTransport.java:496-497, 588-590, 604-605,
548-562`).**
`inspectWriterTail` wraps *every* `RuntimeException` from `getStartPosition/getStopPosition` in
`ReseedRequiredException`; `AeronWriterTailRecovery.Scan.read` does the same for any replay error
and for "Archive tail replay timed out before its stop position"; `ensureWriterLocked` wraps any
failure from `AeronArchiveReplicationPublisher.extend` the same way. The catch at `:548-562` stores
the typed failure and state `RESEED_REQUIRED`, and later calls rethrow the stored failure ("Never
retry from scratch", `:455-460`).
*Failure scenario.* A writer restarts while the Archive control channel is slow (GC pause, driver
still starting, 30 s response timeout). The first recovery attempt times out, the node reports
`RESEED_REQUIRED` for the rest of the process lifetime, and the README tells the operator to stop and
reseed. If the writer's Store/Archive are intact this is a false alarm that costs a restart at best
and an unnecessary reseed of every reader at worst.
*Recommendation.* Classify at the throw site: only conditions that prove divergence (rules 1–8 of the
A1.5 table, identity mismatch, mark ahead of Archive, missing recording) throw `ReseedRequiredException`.
I/O errors, `ArchiveException` with timeout/unavailable codes and replay timeouts throw
`ReplicationUnavailableException` (retryable), and `ensureWriterLocked` retries them with the existing
bounded-backoff helper (configurable attempts) before latching `FAILED`. Keep the latch only for
reseed-class failures. Add a test where `getStopPosition` throws once and the second attempt succeeds.

**B3 — `StoreIndexReflection` mutates third-party private state through `Unsafe`
(`StoreIndexReflection.java:84-123`).**
OPUS describes this as "the only production reflection". It is also the only place that breaks AGENTS
rule 31 and the thread-safety rules: it reads and writes `builder`, `index`, `deferredBuilderOps` and
`graphRebuilt` of `VectorIndex` with `XMemory.getObject/setObject/set_byte`, which are plain stores
with no `builderLock`, no volatile semantics, and no check that the field is not `final`/`volatile`
upstream. The upstream `invalidateGraph()` in PR #832 takes `builderLock.writeLock()`; this bridge does
not, so any upstream thread that touches the builder outside the PerunCS coordinator (background
persistence, `optimize`, deferred ops) races it.
*Recommendation.* The project already deploys from "the same dated 5.0.0-SNAPSHOT". Cherry-pick PR #832
into that snapshot build now and delete `StoreIndexReflection` and `StoreIndexReflectionTest`'s
reflection guard in the same change; the only cost is carrying the patch until the PR merges. If that
is unacceptable, the stop-gap should at least use `MethodHandles.privateLookupIn(...).unreflectVarHandle`
(volatile access, no `Unsafe`), fail startup (not first replicated batch) when the field layout does
not match, and hold the same lock upstream would (`builderLock`, reachable the same way).

**B4 — graph reads are blocked for the whole vector rebuild (`ApplyWorker.java:242-248`).**
Already measured by OPUS (69 ms at 1k vs 23.9 s at 100k vectors) and left open. It is listed because
nothing in the design bounds it: `PERUNCS_INDEX_VALIDATION_MAX_OBJECTS` bounds validation work, not
vector count, and the 1800 s refresh budget means a 30-minute reader blackout is considered healthy.
*Recommendation (in order).* (1) Take PR #832 (B3) and read its locking contract; if `invalidateGraph`
plus lazy rebuild is safe under `builderLock`, drop the warm-up from the exclusive section and let the
first search pay. (2) Until then expose the blackout: record warm-up duration in `ReplicationStatus`
(`lastApplyBlockedMillis`) and log at WARN above a threshold, so the 30-minute budget is observable.
(3) Cap the refresh budget at a value operators can reason about (for example 60 s) instead of
10 × materialization.

### P2

**B5 — coordinator lock discipline is not what the Javadoc says
(`AeronReplicationWriteCoordinator.java:178-194`, `AeronStorageBinaryReplicationTarget.java:21-24,
150-152`; `AeronReplicationPublisher.java:402-406`).**
`write()` runs `prepareWriteAtomically(() -> prepareWrite(data))`, which holds `writeLock` while it
validates indexes, offers every data chunk, waits for the Archive to record them
(`recordedPositionTimeoutNanos`, 30 s) and performs the local Store write. The class Javadoc and the
target Javadoc both say the slow Archive wait runs "with no coordinator lock held, so a slow Archive
never blocks health, maintenance, or dispose"; that holds only for the COMMIT offer. `dispose()`,
`cancelStoreCommit`, `retryPendingCommit` and `distributeTypeDictionary` use unbounded `lock()`.
Separately, when prepare fails, `prepareWithRecovery` offers an ABORT and calls
`commitPositionAwaiter` again against the same unresponsive Archive, so a dead Archive costs up to
30 s (prepare wait) + offer timeout + 30 s (ABORT wait) inside the application's exclusive
`GraphBoundary.write` section.
*Recommendation.* (a) Fix the Javadoc now. (b) Make the ABORT best-effort: offer it with a short
bounded budget and do not wait for recording; if the Archive is the problem, fail closed and let
A1.5 recovery append the marker (the spec already says an unrecorded ABORT latches). That halves the
worst case. (c) Use `tryLock` with the recorded-position deadline in `dispose`/`cancelStoreCommit`/
`retryPendingCommit`, as `withWritesPaused` already does. (d) Consider 5 s defaults for
`PERUNCS_AERON_RECORDED_POSITION_TIMEOUT_NANOS`/`OFFER_TIMEOUT_NANOS` on the writer; 30 s inside an
exclusive write section is a long time to stop all reads on the node.

**B6 — materializer on a virtual thread (`StorageBinaryDataMerger.java:184-186`).**
OPUS D-24 says "platform daemon `peruncs-apply`". The code uses
`Thread.ofVirtual().name("eclipse-datagrid-store-materializer")`. The work is CPU-bound (JVector
rebuild: 24 s at 100k vectors) and does blocking Store I/O. Virtual threads are not time-sliced, so
with the default scheduler parallelism equal to the core count, a long rebuild occupies a carrier
for its whole duration; on a 2-vCPU container the maintenance scheduler
(`NodeMaintenanceScheduler`: `newVirtualThreadPerTaskExecutor`), the retention agent
(`AeronArchiveRetention.java:130`), storage-limit checks and backups share the remaining carrier.
*Recommendation.* Use a platform daemon thread (`Thread.ofPlatform().daemon().name("peruncs-apply")`)
as specified; keep virtual threads for the I/O-bound maintenance tasks. Also rename the remaining
`eclipse-datagrid-*` / `datagrid-*` thread names and the `datagrid-<cluster>` alias
(`NodeConfig.java:418`) — OPUS D-00 says no Data Grid names are adopted.

**B7 — the Data Grid publisher SPI is still the writer's backbone (`storage/binary/ReplicationPublisher.java`,
`node/aeron/AeronDistributionGate.java`, `AeronWriterTransport.java:123-148,167-230`,
`NodeLifecycle.java:280,324,421,485`, `NodeCollaborators.java:361`).**
OPUS rev 5/D-11 say the Data Grid SPI and names are gone. `ReplicationPublisher` is that SPI:
`distributeData(Binary)` — which the only production implementation always throws from
(`AeronDistributionGate.distributeData`), `messageIndex(long)` (failover-era sequence tracking),
`ignoreDistribution(boolean)` (a mutable flag toggled around startup), `Caching` and `noOp()`
wrappers, and `Disposable`. Downstream of it: `AeronWriterTransport.nextSequence` (an `AtomicLong`),
`AeronArchiveReplicationPublisher.synchronizeNextSequence`, `AeronReplicationPublisher.
synchronizeNextSequence`, `AeronReaderTransport.java:131` calling
`facade.writerTransport().advanceSequence(...)` from a *reader's* transaction callback, and a pattern
`distributor instanceof ReplicationPublisher cluster` at `AeronWriterTransport.java:192,222` where
`distributor` is already declared `ReplicationPublisher` (always true when non-null). With fixed roles
and no failover (D-01), the Store mark, the recovery scan and the coordinator are the only sequence
authorities; four other copies of "next sequence" exist (`AeronWriterTransport.nextSequence`,
publisher `nextSequence`, coordinator `initialSequence`, mark) and must agree by construction.
*Recommendation.* Delete `ReplicationPublisher`, `Caching`, `noOp`, `AeronDistributionGate`,
`messageIndex`, `advanceSequence`, `synchronizeNextSequence` and `AeronWriterTransport.nextSequence`.
Replace with two small things: a `TypeDictionaryOutbox` (`AtomicReference<String>` with
`offer/consume`) used by `DistributingTypeDictionaryExporter` and the target, and a
`StartupGate` (`BooleanSupplier`) for "do not replicate bootstrap commits" — or better, start the
Store with the non-replicating target and swap in the replicating one after bootstrap, removing the
mutable flag. Confirm no remaining caller with the code graph before deleting; the grep shows none
outside the files above.

**B8 — one writer state machine in three places (`storage/aeron/writer/`).**
`AeronReplicationPublisher` keeps `closed, closeRequested, closeInProgress, preparing,
terminalOperation, failed, fencingTokenClaimed`, `reservedSequence`, `pendingTransaction`,
`coordinatorOwner`; `PreparedTransaction` adds `terminal, commitPending, abortAttempted,
abortActionInvoked, commitActionInvoked`; the coordinator re-expresses the same lifecycle as
`PreparedWrite.State {RESERVED, PREPARED, COMMITTING, REPLICATION_SUSPENDED, REJECTING}` plus
`markReservation`, `activeWrite`, `maintenance`; the target adds `distributionEnabled`. 43
`synchronized` sites in the publisher alone, callbacks (`onAbort`, `onCommit`, `terminalRecorded`)
invoked outside locks in a documented order that two classes must keep in sync. This is where every
recent correctness fix (C1, D-09, recorded-ABORT) had to touch all three files.
*Recommendation.* One `WriterSession` that owns the publication, sequence and a sealed state
(`Idle | Reserved(seq) | Prepared(tx) | Committing(tx) | Suspended(tx) | Failed(cause) | Closed`)
advanced by one `LockedExecutor`/`ReentrantLock`; `PreparedTransaction` becomes a top-level record
holding data, not flags; callbacks become return values (`terminalRecorded` is only ever "advance the
boundary to this position"). Delete `claimCoordinator/releaseCoordinator`, the four
`prepareTransaction` overloads, `publishTransaction`, `TransactionMetadata` and `reserveSequence`
as a public step (see B9, B17). Do this behind the existing publisher/coordinator tests; they pin the
behavior.

**B9 — three CRC passes per transaction; two are fence vestiges
(`AeronReplicationWriteCoordinator.java:466`, `AeronReplicationPublisher.java:470-472, 576-584`,
`EnvelopeFramer.java:138-261`).**
`transactionMetadata()` runs `computeDataCrc` over the whole payload before reservation;
`offerDataChunks` computes the transaction CRC *and* a per-chunk CRC; then
`prepareReserved` compares the second against the first ("transaction data changed after durable
fence"). Since A1 there is no durable fence; the Store mark is the evidence. All of this runs inside
the exclusive write section for payloads up to 64 MiB.
*Recommendation.* Delete the pre-pass, `TransactionMetadata` (keep `dataLength`/chunk count, which are
trivial arithmetic) and the equality check. Optionally define the COMMIT checksum as a CRC over the
sequence of chunk CRCs (a protocol change is allowed by AGENTS) so the payload is read once, by the
chunk CRC that the receiver must validate anyway; the recovery scan and reader assembler change in
step. Net: the payload is read once instead of three times.

**B11 — closing a prepared transaction after local acceptance can emit an ABORT
(`AeronReplicationPublisher.java:1081-1096`, `AeronStorageBinaryReplicationTarget.java:159-162`).**
`write()` uses `try (prepared) { … commitAcceptedStore(prepared) }`. `PreparedTransaction.close()`
calls `owner.abort(this)` unless the token is terminal, `commitPending`, or the publisher is failed.
If `beginCommit` throws before it fails the publisher (for example "prepared transaction does not own
Aeron write admission", `AeronReplicationWriteCoordinator.java:388-393`, or a non-`PREPARED` state),
the try-with-resources close offers an ABORT for a sequence whose bytes the Store already committed.
Readers then skip a transaction the writer persisted and the divergence is permanent. Today that needs
a coordinator invariant violation, so it is latent, but the default of "close means abort" is exactly
wrong after the local write.
*Recommendation.* Add `markLocallyAccepted()` on the token, called immediately after
`delegate.write(data)` returns; `close()` on a locally accepted token never aborts and calls
`failClosed()` instead. Test: force `beginCommit` to throw after a successful local write and assert
no ABORT frame is offered and the publisher is failed.

**B12 — recovery window ignores Aeron framing overhead (`AeronWriterTailRecovery.java:55-61`).**
`window = 2 × (maxTx + chunkCount × 84) + 4 × 84`. The Archive position advances by Aeron frame
headers (32 B per MTU fragment: ≈1.5 MiB per 64 MiB at MTU 1408, versus a slack of
2 × 512 × 84 B ≈ 86 KiB at 128 KiB chunks), by 32-byte alignment, and by padding frames at term
boundaries (up to one message per term). Two legitimate tail transactions each near
`maxTransactionBytes` exceed the window and the writer reports "Archive tail exceeds writer recovery
window" → reseed (and, per B2, permanently). Fails closed, so it is availability, not safety.
*Recommendation.* Compute the window in Archive positions: per transaction
`maxTx + chunks × 84 + ceil((maxTx + chunks × 84) / maxPayload) × 32 + termLength/8`, doubled, plus
the markers. Add a recovery test with two ~`maxTransactionBytes` transactions in the tail against a
real Archive (the current matrix uses small payloads).

**B13 — duplicated role startup (`NodeLifecycle.java:264-373` vs `404-558`).**
Both methods perform: restore policy, "no local Store image" reseed check, `ignoreDistribution(true)`,
Store start, `requireStoredMark`, `initializeRoot`, `validateStorageRoots`, `ignoreDistribution(false)`,
`queueWriterDictionary`, manager creation, the same `GcWorkaround` maintenance registration (interval
30 vs 60 minutes), the usage gauge schedule, `maintenance.start()`. OPUS A5 withdrew role-specific
assembly because it "would duplicate shared lazy-resource and close semantics"; the duplication is
already here, in the start path, and `startDevNode` is a third variant. `GcWorkaround` is also not a
workaround for anything documented — name it or delete it.
*Recommendation.* A `NodeStartup` that is a list of named steps
(`restore → verifyImage → openStore → verifyMark → initRoot → validateIndexes → scheduleMaintenance`)
with role-specific steps supplied by `NodeRole` (a sealed `RolePlan` record: `mayCreateRoot`,
`needsWriterLock`, `scheduledTasks`). `NodeLifecycle` keeps close ordering only. This also removes
the `this.assembly.nodeRole == …`/`writer && this.assembly.hasReplicationMark()` branching.

**B14 — `GuardingStorageManager` (1226 lines) and its cause-chain walkers (`:449-499`).**
`isCleanRejection` and `isPendingCommit` both walk the cause chain with a depth cap of 16 *and* a
Floyd cycle check; the cap alone already terminates any cycle, so the Floyd pointers are dead weight
and the two walkers disagree subtly about which `ReplicationException` subtypes are tolerated.
Four inner classes (`GuardedDatabase`, `BinaryPersistenceManagerAdapter`, `ClusterStorerAdapter`,
`GatedPersistenceTarget`, roughly 500 lines together) are nested in the facade.
*Recommendation.* One `CauseChain.classify(Throwable)` returning a record
`(rejected, abortRecorded, pending, unrelated)`; both predicates become one-line reads of it and are
unit-testable without a manager. Move the three inner classes to their own package-private files in
`node.store`; the gate table in `GateClassificationTest` then maps one class per Store interface.

**B15 — defensive monitors in a single-threaded callback (`TransactionAssembler.java:117-176`).**
`onFragment` takes `synchronized (delivery)` then `synchronized (this)` per fragment, and a third
`barrierLock` guards staging, with comments that justify it as "a hard boundary for direct/concurrent callers".
Aeron delivers fragments on the one polling thread (`AeronArchiveReader.java:469`: a platform thread;
the comment at `TransactionAssembler.java:139` still says "virtual-thread poller"). The only
cross-thread access is `dispose()`, `failure()` and the volatile snapshots.
*Recommendation.* Confine the state machine to the polling thread, expose the volatile
`resolvedBoundary`/`failure`/`lastApplied`, and make `dispose()` request disposal via a volatile flag
the poller honors (the reader already does this for stop). Three monitors become zero on the hot path
and the lock-order comments go away. (Rule 7, rule 28.)

**B16 — envelope encoder arity (`AeronReplicationEnvelope.encode` 17 params, `encodeWithPayloadCrc`
18, `set` 15, `validate` 14, `AeronReaderWatermark.decodeFrame` 12).**
OPUS rev 111 says the remaining long signatures are "package-private setup wiring". These are public
methods on the per-frame hot path. Primitive arguments avoid allocation, but the same goal is met
without 18 positionals by a writer-owned reusable mutable header encoder (the decode side already has
the reusable `EnvelopeView`): `encoder.wrap(buffer, offset).kind(...).sequence(...)…encode()`.
`AeronReplicationEnvelope` is `public` in an unexported package; make it package-private with a narrow
bridge, as OPUS did for `EntityHeaders`.

**B17 — test seams in production classes.**
`AeronReplicationPublisher.forTests` (×2), the integration-test `onPublication` overload,
`AeronStorageBinaryReplicationTarget.create` (×2) and three public constructors (one overload takes a
`Supplier` for lazy coordinator creation with a double-checked `synchronized` at `:94-103` — use
`LazyConstant`, rule 1), `TargetCallbacks` with three nullable components and *two* index-check
mechanisms (`commitScan` and `commitTouchesIndexes` + `writerIndexValidation`).
*Recommendation.* Keep one package-private constructor; build test instances in a `T/…/fixture`
factory in the same package. Pick one index-check mechanism (the combined `commitScan`), make it
required, and delete the other two plus the nullable components.

**B18 — global static registry in `NativeMemory` (`:14-38`).**
`ALLOCATIONS` is a JVM-wide `IdentityHashMap` guarded by `synchronized`, written for every
`allocateDirect` (publisher framing storage today) and read on every `releaseDirect`. It contradicts
OPUS SEC5 ("runtime state is instance-owned"), serializes unrelated nodes in one JVM (tests, embedded
multi-node), and leaks the arena if an owner forgets to release. `allocateScoped` already returns an
`Allocation` handle that the pool stores.
*Recommendation.* Remove `allocateDirect/releaseDirect`; the publisher keeps an `Allocation` field
and closes it. `NativeMemory` becomes a stateless allocator. (Also consider a `record Allocation` +
`AutoCloseable` so owners use try-with-resources, rule 10.)

**B19 — `ReplicationMark` encapsulation (`ReplicationMark.java`, coordinator `:221-224`).**
Seven public mutable fields, written directly by the coordinator in another package and read by
three more classes. Serializer needs a no-arg constructor and field access (the `opens` directive
already grants it); it does not need public fields.
*Recommendation.* Package-private/private fields, methods `reserve(recordingId, token, sequence,
startPosition)` (writer) and read accessors; keep `opens`. The invariants in A1.3 ("sequence only
advances through reserve") then live in one place.

### P3

**B21 — retry configuration (rule 23).** `AeronRetryPolicy.defaults()` is not reachable from
`NodeConfig`: idle spins/yields/park, jitter base/cap and probe delays cannot be tuned, while eight
timeouts default to 30 s. Add the five most useful keys (offer jitter base/cap, idle max park,
archive probe delay, catalog probe max delay) to `NodeConfig.Operations` and the README table
(`settingsMarkdown()` already enforces parity with a test).

**B23 — small items.**
- `AeronWriterTailRecovery.Scan.result()` `:256-258`: `markedCommit != null` together with
  `next.seen` is unreachable because `:246-248` already threw; delete the branch.
- `AeronReplicationPublisher.PreparedTransaction.onAbort` (`:1039-1053`) reads `abortPosition` in a
  second `synchronized` block after deciding `invoke` in the first; fold into one block.
- `NativeBufferPool` buckets are powers of two but resident-byte accounting in the queue counts
  `remaining()`; a 65 MiB transaction occupies 128 MiB of native memory. Account by capacity or
  document the 2× factor next to `PERUNCS_DATA_MERGER_LIMIT`.
- `AeronReplicationPublisher.java:466` allocates `new UnsafeBuffer(dictionary)` per transaction that
  carries a dictionary; wrap a reusable instance.
- `CursorSnapshot(sequence, position)` duplicates a subset of `ReplicationPosition`; keep one.
- `FaultInjection.invoke("AFTER_PREPARE", …)` uses string point names scattered through production
  code; an `enum CrashPoint` makes typos compile errors and lets the matrix enumerate points.

## 4. OPUS_REVIEW.md claims that the code does not support

| OPUS claim | Reality |
|-----------|---------|
| rev 76/111/J3: "no fully-qualified type references remain in production" | `M/node/aeron/AeronWriterTransport.java:191` uses `java.util.function.BooleanSupplier`. |
| rev 5/D-02/D-11: Data Grid names and SPI removed | B7: `ReplicationPublisher` SPI and its failover plumbing; threads `eclipse-datagrid-store-materializer`, `eclipse-datagrid-store-watchdog`, `eclipse-datagrid-aeron-watermarks`, `datagrid-aeron-archive-reader`, `datagrid-housekeeper-N`, `datagrid-retention-agent`; alias `datagrid-<cluster>`. |
| D-24: apply thread is a platform daemon `peruncs-apply` | B6: virtual thread, different name. |
| SEC5: "runtime state is instance-owned" | B18: JVM-global `NativeMemory.ALLOCATIONS`. |
| rev 111/J1-b: only "temporary reflection" remains | B3: it is `Unsafe` writes into upstream private fields (rule 31), not just reflection. |
| `AeronReplicationWriteCoordinator`/target Javadoc: Archive wait never holds a coordinator lock | B5. |
| ApplyWorker: "steady state allocates nothing" | B1: per-batch tasks/lambdas, `ensureViews` reallocation. |
| rev 19/A1: legacy checkpoint/cursor/fence vocabulary removed | Still in comments and one live check: `AeronReplicationPublisher.java:72,274,316,379,471,490,538,570,755,892`; `AeronStorageBinaryReplicationTarget.java:221,229` ("PREPARING fence", "recording REJECTED"); `EnvelopeFramer.java:136`; `AeronReplicationWriteCoordinator.java:45,245`; `storage/package-info.java:7` ("archive-first"). `AeronReplicationPublisher.java:470-472` still *enforces* a fence-era equality. |
| rev 111: "remaining long constructors are package-private setup wiring" | B16: 17/18-parameter public codec methods; `AeronArchiveReader.Configuration` has 16 components, `NodeConfig.Timeouts`/`AeronConfig` 12. |
| A5 withdrawn as duplicate machinery | B13: the duplication already exists in `NodeLifecycle`. |

Formatting debt not mentioned by OPUS: **577 Javadoc lines in 57 production files are indented by
8 spaces (`        /// …`) in front of 4-space members** — a leftover of a bulk edit (D-28 forbade a
repo-wide formatter, but these are broken, not unformatted). The brace layout at
`AeronStorageBinaryReplicationTarget.java:244-255` is also wrong. Fix mechanically with a one-off
script limited to lines matching `^        /// ` that precede a member at 4 spaces.

## 5. AGENTS.md rule-by-rule status (gaps only)

| Rule | Gap |
|------|-----|
| 1 modern Java | Virtual threads used where they hurt (B6); `LazyConstant` not used for the target's lazy coordinator (B17); poller/assembler comments assume a virtual-thread poller that does not exist. |
| 3 immutable/records | `ReplicationMark` mutable public fields (B19); `PreparedTransaction`/`Transaction`/`Scan` flag soup (B8). |
| 5/6/7 DRY, layers, overengineering | B7 (SPI), B8 (three-class state machine), B10 (two copy loops), B13, B15, B17. |
| 9 single-use/static-only types | `AeronWriterRecoveryBoundary` (9 lines), `CursorSnapshot` vs `ReplicationPosition`, `AeronDistributionGate`, `MergerLifecycle` (one implementor, kept "so tests can wedge a worker" — consider a test subclass instead). |
| 10 AutoCloseable/exceptions | `NativeMemory` handles (B18); recovery wraps any `RuntimeException` as reseed (B2); generic `IllegalStateException` for Archive timeouts (`AeronArchiveReplicationPublisher.java` `awaitRecorded`) instead of `ReplicationUnavailableException`. |
| 11/12/13 visibility, packages, god objects | Publisher, coordinator, guarding manager, lifecycle, `NodeConfig` (724 lines), merger (850), assembler (964), reader (950). `AeronReplicationEnvelope` public in unexported package. |
| 14/31 reflection/unsafe | B3. `jdk.internal.misc` export is upstream (Serializer `XMemory`), unchanged. |
| 16 FQN | One occurrence (§4). |
| 17/18 long signatures/builders | B16; `AeronArchiveReplicationPublisher.create/extend/createRemote/extendRemote` take 8–9 positionals each (four near-identical factories) — one `Request` record. |
| 19 static-only interfaces | None found. |
| 20 security | No gap found in what I read; backup zip extraction only grep-checked. |
| 21 performance | B1, B9, B4; `AeronWriterTailRecovery.Scan.read` polls with `parkNanos(100 µs)` (fine for a one-off). |
| 22 threading | B3 (foreign state without lock), B11, B15 (misleading), B18. |
| 23 configurable retries | B21; B2 (no retry where retry is correct). |
| 24 exception design | B2, B14, §3 generic exceptions. |
| 25 Javadoc correctness | §4 stale text; `AeronStorageBinaryReplicationTarget.java:21-24`, coordinator class doc, `EnvelopeFramer.java:135-137`. |
| 26 tests | §2.2 gather gaps; add tests listed in §6. |
| 27 Aeron/Store best practice | Not reviewed (§0). |
| 28 prefer Agrona/Eclipse locks over `synchronized` | 130 `synchronized` lines; publisher 43, assembler 15, watermark channel 15, reader 10. The assembler's are removable (B15); the publisher's disappear with B8. |
| 29 topology/no auth | Compliant in what I read. |
| 30 heap allocation in packing | `new UnsafeBuffer(dictionary)` per dictionary tx; per-transaction `EnvelopeFramer` allocates two `CRC32C`, a `ChecksumContext`, an `UnsafeBuffer` and an `AtomicBoolean` (`EnvelopeFramer.java:94-98,113`); `PreparedTransaction`; `ApplyWorker` per-batch objects (B1). Make `EnvelopeFramer` a reusable per-publisher object reset per transaction. |
| 32 Lucene/JVector | Tests exist (`ReaderLiveIndexFreshnessTest`, `AeronStoreIntegrationIT`, soak census, `ClusterStoreIndexesTest`, `WriterIndexValidationTest`); no test covers B4's blackout bound or a vector index above ~10k entries in the default gates. |
| 33/34 cluster correctness/performance | B2, B5, B9, B11, B12 and B1/B4/B6. |

## 6. Tests to add (ordered by value)

1. B1: apply 10k trivial batches through `ApplyWorker`; assert the watchdog queue stays small.
2. B2: writer start with an Archive whose `getStopPosition`/replay throws once → writer recovers on
   retry and reports `LIVE`, not `RESEED_REQUIRED`; a genuine rule-3 violation still reports reseed.
3. B11: local write succeeds, `beginCommit` throws → no ABORT is offered, publisher failed.
4. B12: real-Archive tail with two ~`maxTransactionBytes` transactions recovers.
5. B5: with an Archive stub that never records, `write()` fails within one bounded timeout (not two)
   and `dispose()` does not block behind the writer.
6. §2.2 gather matrix: >4 sources, boundary-aligned sources, read-only heap source, length-changed
   error, and a differential staged-vs-gathered wire comparison (until B10 deletes the staged path).
7. B3: a startup self-check that fails fast when the pinned Store version's `VectorIndex` field
   layout differs (today it fails on the first replicated batch).

## 7. Suggested order of work

1. B1, B2, B11, §4 comment and formatting fixes — small, local, remove the sharpest risks and the
   misleading documentation.
2. B3 + B4: take PR #832 into the pinned snapshot; delete the Unsafe bridge; re-measure warm-up.
3. B7, B9, B10, B17, B18, B19 — deletions that shrink the writer path before B8's rewrite, so the
   state-machine merge starts from the smaller surface.
4. B8, B5, B13, B14, B15, B16 — structural refactors, each behind the existing unit suite.
5. B6, B21, B22, B12 — configuration, threading and harness cleanup.

Rerun the D-25 comparison (OPUS step 4) after B1/B9/B10 land: they change the writer and reader hot
paths and the rev 134 sampling fix already invalidated the stored artifacts.
