# CLAUDE_FOLLOWUP — revision 2: combined review of OPUS_REVIEW.md work and the whole codebase

**Tree:** `main` at `ec76dce` (clean; `OPUS_REVIEW.md` rev 135). **Mode:** review only (AGENTS.md) — nothing was built,
run or changed; every statement comes from reading source, local Aeron sources
(`$GITHUB_ROOT/aeron-io`) or the external reviews pasted into the request.
**Paths:** `M/` = `src/main/java/peruncs/cluster/`, `T/` = `src/test/java/peruncs/cluster/`.
**Severity:** P1 = correctness / availability / operability contract; P2 = robustness, performance or AGENTS-rule
violation; P3 = cleanup. **Each finding is written so it can be implemented without further questions:** evidence,
impact, ordered change steps (file + member), behaviour that must be preserved, and the tests that prove it.

How to use: implement P1 items first, each as its own change with its tests; do not start a P3 refactor while a P1/P2
item touching the same files is open. "Confidence" is stated where a finding is a strong reading of the code but
needs a reproduction test before the fix is merged.

---

## Implementation status (updated after the implementation pass)

Unit gate: 820 tests green (`mvn test`); integration profile green for the Aeron, Archive, Store, backup,
crash-matrix (`ReplicationMarkCrashMatrixIT`) and retention suites that were re-run after the last change.
**Implemented and tested** unless a note says otherwise.

| ID | Status | What changed / why not |
|----|--------|------------------------|
| F-01 | Done | lock stage waits for transport + Store close; `NodeLifecycleTest.failedStoreShutdownKeepsTheWriterLockUntilTheRetrySucceeds` |
| F-02 | Done | retention close throws instead of reporting success, never interrupts the agent; owner keeps runtime open; `liveRetention()` uses `instanceof` |
| F-03 | Done | `classifyArchiveFailure`; transient failures are `ReplicationUnavailableException`, retried up to `PERUNCS_WRITER_RECOVERY_ATTEMPTS` consecutive times (default 3) and then latched `FAILED`; an unclean close of the failed candidate latches at once; reader reconnect budget → `FAILED` (not reseed) |
| F-04 | Done | `AeronWriterTailRecovery.Framing` bounds with MTU/term padding |
| F-05 | Done | provisional-negative caching with outermost-resolution commit (the external "treat back-edge as relevant" fix was rejected) |
| F-06 | Done | unknown measurement fails closed (`markUnknown`) and surfaces to the maintenance scheduler |
| F-07 | Done, **different fix than first written** | the reader now publishes (and the Store-mark view reports) the mark's prepare-start position. The first attempt (keep one max-transaction span before the watermark) broke retention with small segments and was replaced; `AeronStoreIntegrationIT` covers it |
| F-08 | Done | writer status/health uses `AeronHealth` for every role |
| F-09 | Done | the warm-up now runs after the exclusive write section (upstream guarantees the lazy rebuild runs once under the index monitor), so reads are no longer blocked by it; blocked-time WARN and `PERUNCS_INDEX_REFRESH_TIMEOUT_MILLIS` stay |
| F-10 | Done | backup backend supplied lazily; existing Store never touches the volume |
| F-11 | Done | bounded lock acquisition everywhere, shorter ABORT recording wait (`PERUNCS_AERON_ABORT_RECORDED_POSITION_TIMEOUT_NANOS`), Javadoc corrected |
| F-12 | Done | `markLocallyAccepted()`; accepted token never aborts |
| F-13 | Done | `setRemoveOnCancelPolicy(true)` watchdog |
| F-14 | Done | platform daemon threads for materializer, maintenance workers, storage-check and backup executors; all `datagrid` names → `peruncs` |
| F-15 | Done | `TypeDictionaryOutbox` replaces the Data Grid SPI; `ReplicationPublisher`, `AeronDistributionGate`, sequence plumbing deleted |
| F-16 | Done | CRC pre-pass, `TransactionMetadata` and the equality check removed; transaction CRC kept |
| F-17 | Done | the staged framing path, `supportsVectors` and the threshold are deleted; every data chunk is offered as vectors, and non-gathering test offerers use the interface's flattening default; differential tests cover it |
| F-18 | Done | `Outcome.TRANSIENT`, errors moved out of `internal`/`node.backup`, architecture test |
| F-19 | Done | handle-based `NativeMemory.Allocation`; global registry deleted |
| F-20 | Done | node-local export workspace, staging copy onto the volume, entry cap 2^20 and configurable, early failure at backup time |
| F-21 | Done | fixed `.storage-previous` name + `recoverInterruptedReplacement` at startup |
| F-22 | Done | `HeaderEncoder`; `EnvelopeView.set` inlined; watermark codec takes the watermark (`encodeInto(target)`) and `decodeFrame` is split; `AeronHealth` takes three probe records; Archive publisher `create/extend` take a `PublisherSetup` record and the unused remote variants are deleted. `AeronArchiveReader.Configuration`, `AeronReplicationConfiguration` and `NodeConfig` records already have builders (rule 18) |
| F-23 | Mostly | new settings (retry pacing, retention operation timeout, publication lock timeout, abort wait, index refresh, backup workspace/entries); `Thread.sleep` loops replaced by jittered parks; offerer branch tests. Remaining literal timeouts (merger dispose, watermark retry, gauge refresh) stay constants |
| F-24 | Done | `VarHandle` + upstream `builderLock` + startup layout check; module path needs `--add-opens` (README) |
| F-25 | Done | vocabulary, `///` indentation (601 lines fixed; the 388 remaining 8-space `///` lines are nested enum members and legitimate), `ponytail`, package-info verbs and `@since` |
| F-26 | Done | `PreparedTransaction` is its own file; the publisher's six boolean flags became a `Lifecycle` enum (OPEN, CLOSING, CLOSE_INTERRUPTED, CLOSED) and an `Operation` enum (IDLE, PREPARING, TERMINAL), while `failed` stays orthogonal; crash, recovery and IT suites pass |
| F-27 | Partly | shared `validateStoreRoots` / `scheduleStoreMaintenance`, task renamed; the two start methods still differ in role-specific steps |
| F-28 | Done | one bounded cause-chain walker; five inner classes extracted into package-private files |
| F-29 | Done | contract documented, stale comments fixed; monitors kept (dispose runs on another thread) |
| F-30 | Mostly | one target constructor + `LazyConstant`, single `commitScan`, publisher test factories moved to fixtures |
| F-31 | Done | private fields, accessors, `reserve(...)` |
| F-32 | Done | benchmark smoke test is now `AeronFullPathBenchmarkIT` in the integration profile |
| F-33 | Done | all items applied: single-implementation `BackupNodeControl` deleted; the remaining interfaces (`StorageBackupManager`, `StorageBackupTaskExecutor`, `StorageTaskExecutor`) carry a Javadoc naming their test doubles, as the item allows; one shared `UuidCodec` used by the backup format and the position codec; the per-batch Lucene retire stays unconditional and the code now documents why (index files cannot be told apart from plain data by type) with `ReaderLiveIndexFreshnessTest` as the guard; typed `FaultInjection.Point`; two `ClusterStorageManagers` factories |
| F-34 | Done | tests added for every listed gap: the named-module consumer now also runs on the module path, `ClusterIndexMaintenance`, the materializer, writer-role reader client, and the others listed; `AeronReaderTransport` is exercised through `AeronReplicationMonitoringTest` and the integration suites |
| N-A | Done | the final offer and the endpoint closes now run outside the channel monitor; new `concurrentPublishAndCloseNeitherHangsNorCorrupts` test |
| N-B | Done (no change needed) | audited all 49 public types outside `api`/`errors`: each has a caller in another package, so none can be narrowed without moving packages |
| N-E | Done | `NodeCollaborators` (501 → 326 lines) delegates to `BackupCollaborators` and `ReplicationCollaborators`; the wrapper interfaces were reviewed as in F-33 |
| N-G | Partly | `NativeMemory` handles done; `ApplyWorker` keeps its `budgetLock` (it makes the watchdog/phase-boundary race deterministic) |
| N-H | Mostly | per-task threshold, lock-spin jitter; `distributeTypeDictionary` on readers disappeared with F-15 |
| N-I | Done | README regenerated and corrected |
| N-J | Partly | see F-34 |

**Runtime verification:** crash matrix (17 ITs), 60 s soak with JFR + `jcmd Thread.print` + `jstat`: no deadlock, zero `JavaMonitorEnter`/`VirtualThreadPinned` events, GC total 0.29 s with a 30 ms worst pause; CPU is dominated by the soak's JVector queries (upstream).

**Corrections to this document made while implementing:** `StorageTaskExecutor.close` interrupts only the caller's wait on the Store
(not Store channel work), so the review item "use `cancel(false)`" was tried, broke `closeDuringRunCancelsTheCheck`, and was reverted.
`R3` (`Optional` "persisted" in `ClusterIndexValidation`) and the `EntityHeaders` `VarHandle` objection were rejected after reading the code.
A `git rm --cached` slip during F-15 staged one deletion; it was undone immediately with `git reset`, no other git state was touched.

---

## 0. Reconciliation with the external reviews

Five external reviews were supplied. Each point was re-checked against the source. Disposition:

| External point | Result | What changed in this revision |
|---|---|---|
| Cancelled watchdog tasks stay queued, but cancellation clears the callable, so tasks do not retain the worker/lambdas | **Accepted** (`FutureTask.cancel` nulls its callable) | F-13 now claims only "task objects stay queued until their deadline"; fix is `setRemoveOnCancelPolicy(true)`, no watchdog rewrite |
| B2 latch is `RESEED_REQUIRED` only on wrapped sites; unwrapped `ArchiveException` latches `FAILED`; both sticky; retry must account for recovery side effects | **Accepted** | F-03 rewritten with a classification table and an idempotency analysis; cites `AeronWriterTransport.java:465-470, 563-569` |
| `StoreIndexReflection`: upstream `builder`/`graphRebuilt` are volatile, a `builderLock` exists, a concrete race is unproven | **Accepted** | F-08 now says "bypasses upstream's volatile writes and lock; race unproven"; PR #832 claims marked "per the PR" |
| B5: target Javadoc is partly right (the accepted-Store COMMIT does not await recording); the false text is the coordinator class Javadoc | **Accepted** | F-11 corrected |
| B7: `Caching` preserves an authoritative restart snapshot; plain `AtomicReference` loses it; `StorageNodeManager.java:184` was missed | **Accepted** | F-15 replacement keeps the snapshot flag and removes `messageIndex` by a different route |
| B9: only the metadata pre-pass is a safe removal; chunk and transaction CRCs enforce different protocol checks; "fence" wording was wrong | **Accepted** | F-16 narrowed to the pre-pass |
| B10: benchmark does not justify deleting staging for every size/shape | **Resolved** | F-17 deleted staging after differential tests (`EnvelopeFramerTest`) covered the shapes |
| B12: formula incomplete (per-message fragmentation, dictionary messages, term padding); availability defect is P1 | **Accepted** | F-04 gives a message-shape based bound and is P1 |
| B15: removing every assembler monitor is unjustified (`dispose()` runs on another thread) | **Accepted** | F-29 reduced to "document the contract; measure before removing" |
| B16: codec is used across three packages, package-private needs a bridge; only `encode*` are public | **Accepted** | F-22 keeps visibility, adds a reusable encoder |
| B18: `StorageBinaryDataReceiver` defaults also use `allocateDirect/releaseDirect` | **Accepted** | F-19 includes the receiver defaults |
| B20: do not blanket-replace indentation; nested members legitimately use 8 spaces | **Accepted** | F-25 gives a safe, rule-based fixer |
| B21: pacing is programmable via `AeronReplicationConfiguration.Builder.retryPolicy`, not via `NodeConfig`; `AeronRetryPolicy` does exist (`storage/aeron/config/AeronRetryPolicy.java`, one reviewer's grep missed it) | **Accepted + disputed** | F-23 reworded; timeout count stated exactly (six `PERUNCS_AERON_*_TIMEOUT_NANOS` at 30 s plus the non-configurable 30 s/60 s literals listed there) |
| B23: pool accounts by capacity, the queue by `remaining()` | **Accepted** | wording fixed in F-33 |
| Rev-1 §5 rule-9 quote "so tests can wedge a worker" does not exist; `MergerLifecycle` has two implementors (test `ApplyWorkerBudgetTest.RecordingLifecycle`) | **Accepted** — the quote was a paraphrase of OPUS prose and is removed | rule table rewritten |
| B14 inner-class count was inconsistent (four/three) | **Accepted** — there are five (`GuardedDatabase`, `BinaryPersistenceManagerAdapter`, `ClusterPersistenceStorerAdapter`, `ClusterStorerAdapter`, `GatedPersistenceTarget`) | F-28 |
| §2.2 said no IT uses a ≥ 128 KiB payload | **Accepted** — `AeronStoreIntegrationIT` writes 256 KiB payloads; the real gap is that nothing *asserts* the gather branch ran | F-17 test list |
| `:892` "COMMITTING_UNCERTAIN" is current vocabulary | **Disputed**: no such state exists in `src/main` (grep: comments only) — kept as stale | F-25 |
| Failed close releases the writer process lock (`NodeLifecycle.java:872`) | **Verified** in `NodeLifecycle.java:872-877` and `CloseSequencer` | F-01 (P1) |
| Retention `close()` returns normally with a live agent | **Verified** `AeronArchiveRetention.java:470-482`, `AeronRetentionOwner.java:156-159` | F-02 (P1) |
| Cyclic type relevance caches a false negative | **Verified** `ClusterIndexValidation.java:146` | F-05 (P1). The external "return true on a back-edge" fix is **rejected** (see F-05) |
| Disk-measurement failures reopen write admission | **Verified** `StorageUsageGauge.java:667-686`, `StorageLimitGate.java:573-577` | F-06 (P1) |
| Existing Store start needs a writable backup volume | **Verified** `NodeLifecycle.java:423-426`, `BackupRestorePolicy.java:87-93`, `FilesystemVolumeBackupBackend.java:153` | F-10 |
| Store-directory replacement has a crash gap | **Verified** `AtomicFileWriter.java:329-332` | F-21 |
| `NodeException.outcome()` hints are wrong; `errors` imports inner layers | **Verified** `errors/NodeException.java:7-10,739-744`, `errors/BackupBusyException.java:3` | F-18 |
| `AeronRuntime.append` buries an `Error` | **Verified** `AeronRuntime.java:74-82` vs `CloseSequencer.java:123` | F-33 |

New findings from this pass that no earlier review reported: **F-07** (retention can delete bytes a restarting reader still needs),
**F-09b/F-09** (writer status ignores `AeronHealth`, hiding Archive capacity, driver and maintenance failures — the README capacity
procedure cannot be followed), **F-20** (backup export lives on the shared volume; 65,536-entry restore cap), plus several P3 items.

## 1. Coverage statement (what "entire codebase" meant here)

Read in full (every line): all of `M/node/**` (aeron, backup, replication, store, plus `NodeLifecycle`, `NodeCollaborators`,
`CloseSequencer`, `NodeMaintenanceScheduler`, `StorageNodeManager`, `ClusterNodeHandle`, `WriterProcessLock`), all of
`M/api/**` and `M/errors/**`, all of `M/storage/aeron/**` (config, mark, position, reader, wire, writer), all of
`M/storage/binary/**`, `M/storage/index/**`, `M/storage/io/AtomicFileWriter`, `StorageGraphCoordinator`, `Crc32C`,
`ReplicationRetry`, `ReplicationPosition`, `module-info.java`, every `package-info.java` (heads), `README.md`, and the whole
`OPUS_REVIEW.md`. Not read line-by-line: `GuardingStorageManager` lines 360-620 were read in the first pass; the 157 test
files and `bench/`/`src/bench` were sampled (structure, the gather test, the benchmark smoke test, the IT diffs) and
mechanically cross-referenced (main classes with zero test references are listed in F-34). Nothing was run, so every
behavioural claim is "by reading"; items marked **needs reproduction** must get a failing test before the fix.

Rule 27 (compare with Aeron cookbook / Eclipse Store & Serializer tests): checked only against the local Aeron sources —
`ExclusivePublication.offer(DirectBufferVector[])` copies synchronously; `AeronArchive.segmentFileBasePosition` /
`purgeSegments` semantics (used by F-07). The cookbook and the Store/Serializer test suites were **not** reviewed.

## 2. Backlog

| ID | Sev | Finding | Primary files |
|----|-----|---------|---------------|
| F-01 | P1 | Failed close still releases the writer process lock | `NodeLifecycle.java:872` |
| F-02 | P1 | Retention `close()` returns success while its agent is alive; runtime is closed beneath it | `AeronArchiveRetention.java:458-488`, `AeronRetentionOwner.java:156` |
| F-03 | P1 | Transient Archive failures latch `RESEED_REQUIRED`/`FAILED` permanently (writer recovery, reader reconnect) | `AeronWriterTailRecovery.java`, `AeronWriterTransport.java`, `AeronArchiveReader.java:395-475` |
| F-04 | P1 | Writer recovery window ignores Aeron framing | `AeronWriterTailRecovery.java:45-66` |
| F-05 | P1 | Cyclic types can cache "index-free" for types that reach an index | `ClusterIndexValidation.java:136-180` |
| F-06 | P1 | Failed disk measurement understates usage and can reopen write admission | `StorageUsageGauge.java`, `StorageLimitGate.java` |
| F-07 | P1 (needs reproduction) | Reader watermarks and backup positions carry the COMMIT *end* position; a restart resumes at the transaction's *start* — retention can purge it | `TransactionAssembler.java`, `AeronReaderTransport.java:128-137`, `AeronArchiveRetention.java:244-260` |
| F-08 | P1 | Writer `NodeStatus` ignores `AeronHealth`: no Archive capacity, driver, watermark or maintenance state | `StorageNodeManager.java:152-210` |
| F-09 | P1 (known) | Vector warm-up blocks every graph read (24 s at 100k vectors; 30-minute budget) | `ApplyWorker.java:242-248` |
| F-10 | P2 | Starting an existing Store requires the backup volume | `NodeLifecycle.java:423`, `BackupRestorePolicy.java:87` |
| F-11 | P2 | Coordinator lock held across prepare/Archive wait; failed prepare waits twice; stale Javadoc | `AeronReplicationWriteCoordinator.java`, `AeronReplicationPublisher.java:401-406` |
| F-12 | P2 | Closing a prepared token after local acceptance can emit ABORT | `AeronReplicationPublisher.java:1081-1096` |
| F-13 | P2 | Cancelled watchdog tasks stay queued until their deadline | `StorageBinaryDataMerger.java:190`, `ApplyWorker.java` |
| F-14 | P2 | Materializer is a virtual thread (spec said platform); Data Grid thread names | `StorageBinaryDataMerger.java:184` |
| F-15 | P2 | Data Grid `ReplicationPublisher` SPI and dead sequence plumbing | `storage/binary/ReplicationPublisher.java`, `AeronDistributionGate.java` |
| F-16 | P2 | Metadata CRC pre-pass and equality check are fence leftovers | `AeronReplicationWriteCoordinator.java:466`, `AeronReplicationPublisher.java:470,576` |
| F-17 | P2 | Gather path: aliasing trap, hard-coded threshold, untested matrix | `AeronReplicationEnvelope.java:193`, `EnvelopeFramer.java:22,138-228` |
| F-18 | P2 | `NodeException.outcome()` lies; `errors` depends on inner packages | `errors/NodeException.java` |
| F-19 | P2 | JVM-global native allocation registry | `NativeMemory.java:14`, `StorageBinaryDataReceiver.java:21` |
| F-20 | P2 | Backup export staged on the shared volume; 65,536-entry restore cap | `FilesystemVolumeBackupBackend.java:751-764`, `BackupArchiveLimits.java` |
| F-21 | P2 | Store-directory replacement has no crash recovery | `AtomicFileWriter.java:310-360` |
| F-22 | P2 | 17/18-parameter codec methods on the frame path | `AeronReplicationEnvelope.java:97,146` |
| F-23 | P2 | Retry pacing and ~10 fixed timeouts are not configurable | `AeronSettings.java:182`, see list |
| F-24 | P2 | `StoreIndexReflection` mutates upstream private state through `Unsafe` | `StoreIndexReflection.java` |
| F-25 | P3 | Stale vocabulary, mis-indented Javadoc, FQN, `datagrid` names | see list |
| F-26–F-34 | P3 | Writer decomposition, startup duplication, cause-chain walkers, assembler contract, test seams, `ReplicationMark`, benchmark harness, misc, test gaps | below |

---

## 3. P1 findings

### F-01 — Failed close releases the writer process lock
**Evidence.** `NodeLifecycle.closeNode` (`:776-878`) builds one `CloseSequencer`. Every stage after the first has a
`ready` predicate; the final stage `"writer process lock"` (`:872-877`) is ready whenever `this.writerProcessLock != null`.
`CloseSequencer.close()` runs every ready stage even after an earlier stage failed. If the application drain times out
(`appDrained` stays false), `"embedded storage"` is skipped (`afterAppDrain`), the transport and Store stay open, yet the
lock stage still runs and releases the `writer.lock`. The same happens when the Store stage throws
(`Store did not complete shutdown`) or the transport stage throws.
**Impact.** A second writer process can acquire the lock and open the same Store/Aeron directories while the first node's
Store, Archive and driver are still live — exactly what `writer.lock` exists to prevent.
**Change.**
1. In `NodeLifecycle` add `private volatile boolean transportClosed;` (instance field, so a *retry* knows what the previous
   attempt finished).
2. Transport stage action (`:800-802`): `collaborators.replicationTransport.get().close(); this.transportClosed = true;`.
   Treat a never-initialized transport as closed: define
   `private boolean transportDone(NodeCollaborators c){ return !c.replicationTransport.isInitialized() || this.transportClosed; }`.
3. Define `private static boolean storeDone(NodeCollaborators c){ final var s = c.embeddedStorageManager; return s == null || !s.isRunning(); }`.
4. Change the lock stage readiness to
   `() -> this.writerProcessLock != null && appDrained.get() && transportDone(collaborators) && storeDone(collaborators)`.
   Do **not** weaken `appDrained`: an application section still running means the Store is still in use.
5. A startup failure (`start()` catch → `close()`) must still release the lock when nothing started: with
   `embeddedStorageManager == null` and an uninitialized transport both predicates are true, `appDrained` is set by the
   always-ready first stage, so the lock is released. Keep that behaviour.
**Preserve.** Idempotent close; retry re-runs only unfinished stages; lock released exactly once; `closed=true` only when the
whole sequence succeeded (`closeNode` finally block).
**Tests (`T/node/NodeLifecycleTest` + `WriterProcessLockTest`).** (a) WRITER lifecycle with a held `graphBoundary().write`
section and a 100 ms drain timeout: `close()` throws `GraphDrainTimeoutException`; `WriterProcessLock.acquire(path)` throws
"another writer process holds"; release the section; second `close()` succeeds; `acquire` now succeeds. (b) Fake Store whose
`shutdown()` returns `false`: first close throws, lock still held; second close with `shutdown()==true` releases it.
(c) Transport whose `close()` throws once: lock held after the failed attempt.

### F-02 — Retention `close()` reports success while the agent thread still runs
**Evidence.** `AeronArchiveRetention.close()` (`:458-488`): after `agent.shutdown()` + 30 s wait, then `shutdownNow()` + 5 s
wait, a still-live agent logs a warning and **returns**; an interrupted wait calls `shutdownNow()` and **returns**. The caller
`AeronRetentionOwner.closeRetentionStage` (`:156-159`) then does `this.retention = null`, so `AeronTransport`'s `"retention"`
stage is considered complete and the `"runtime"` stage (ready when `!hasRetention()`) closes Aeron/Archive beneath a purge
that may still be executing Archive RPCs. The class comment itself says Archive RPCs are not interrupt-safe, yet
`shutdownNow()` interrupts them.
**Impact.** Use-after-close of the Aeron client during a segment purge/recording restart; the writer then fails closed
(`purgeSegmentsWhileWritesPaused` → `failClosed`) or the Archive is left with a stopped recording.
**Change.**
1. Add constructor parameter `long closeTimeoutMillis` (default `30_000`, plumbed from a new `NodeConfig.Operations`
   field `retentionCloseTimeout`, see F-23). Delete the `5_000` second wait.
2. Replace the body of `close()` after `this.closed = true; if (cleanupComplete) return;` with:
   `agent.shutdown(); boolean terminated; try { terminated = agent.awaitTermination(closeTimeoutMillis, MILLISECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new ReplicationUnavailableException("interrupted while stopping Aeron retention agent", e); }`
   `if (!terminated) throw new ReplicationUnavailableException("Aeron retention agent did not stop within %d ms; an Archive operation may still be running".formatted(closeTimeoutMillis));`
   then `quorum.close(); pendingWatermarks.clear(); cleanupComplete = true;`. **No `shutdownNow()`.**
3. `AeronRetentionOwner.closeRetentionStage` needs no change once `close()` throws: the exception propagates before
   `this.retention = null`, `hasRetention()` stays true, the `"runtime"` stage stays not-ready, and the next
   `AeronTransport.close()` retries (the sequencer is rebuilt per call but readiness is live).
4. `closeQuietly()` keeps catching `RuntimeException` and leaves the reference in place (already correct).
**Preserve.** `closed=true` immediately rejects new commands; queued commands still drain; a completed close is idempotent.
**Tests (new `T/node/aeron/AeronRetentionOwnerCloseTest`, owner-level because `AeronRetentionOwner`/`AeronRuntimeOwner`/
`AeronReaderTransport` have no direct test today).** `segmentPurger` blocked on a latch; `close()` with a 50 ms timeout throws;
assert `retentionOwner.hasRetention()` and `runtimeOwner.isStarted()` are still true after `AeronTransport.close()` fails;
release the latch; second `close()` succeeds and `runtimeOwner.isStarted()` is false. Plus an interrupted-wait case.

### F-03 — Transient Archive failures become permanent reseed/failed latches
**Evidence (writer).** `AeronWriterTailRecovery.Scan.read` wraps *any* `RuntimeException` and the replay timeout in
`ReseedRequiredException` (`:182-188`); `AeronWriterTransport.inspectWriterTail` wraps `getStartPosition/getStopPosition`
failures (`:588-590`) and any tail failure (`:604-605`); `ensureWriterLocked` wraps any `extend(...)` failure (`:496-497`).
The catch at `:563-569` stores `writerRecoveryState` (`RESEED_REQUIRED` for the typed sites, otherwise `FAILED` via
`writerRecoveryFailure`, `:94-102`) and `:465-470` rethrows the stored failure forever ("never retry from scratch").
**Evidence (reader).** `AeronArchiveReader.failIfReconnectBudgetExpired` (`:395-405`) throws `ReseedRequiredException` when the
Archive control channel stays down for the reconnect budget; `AeronHealth.stateUnchecked` (`:408-413`) maps that to
`RESEED_REQUIRED` with the comment "retrying the same mark would fail again" — false for a network outage; the mark and
recording are intact. (`classifySubscriptionFailure` for `INVALID_START_POSITION`/`RECORDING_NOT_FOUND` is correct and stays.)
**Impact.** A 30-second Archive hiccup during writer start permanently fails the node until the JVM restarts; operators following
the README ("RESEED_REQUIRED: stop and reseed") reseed every reader for nothing.
**Change (writer).**
1. Add `static RuntimeException classifyArchiveFailure(String context, RuntimeException failure)` in `AeronWriterTransport`:
   `ReseedRequiredException` and `NodeException` subtypes pass through unchanged; `ArchiveException` with
   `errorCode()==ArchiveException.UNKNOWN_RECORDING` → `reseedRequired(context + ": recording is missing", failure)`;
   any other `ArchiveException`, `AeronException`, `TimeoutException`-caused or replay-timeout failure →
   `new ReplicationUnavailableException(context, failure)`; `IllegalStateException("Aeron recording is still active …")` from
   `extendPublication` → `ReplicationUnavailableException`; `IllegalArgumentException` from `extendPublication` (unknown
   recording, stream mismatch, framing mismatch) → reseed; everything else → pass through.
2. Replace the three `reseedRequired(..., failure)` wrapping sites with `classifyArchiveFailure(...)`.
3. In `AeronWriterTailRecovery.Scan.read`: delete the blanket `catch (RuntimeException)` that wraps in reseed; replay timeout →
   `new ReplicationUnavailableException("Archive tail replay timed out before its stop position")`; keep reseed only for the
   protocol/identity violations thrown by `Scan.accept`/`Transaction`/`result()` (they are `ReseedRequiredException` already).
4. In the `ensureWriterLocked` catch (`:563-569`): if `classified instanceof ReplicationUnavailableException`, call
   `closeFailedWriter(candidate, classified)`, **do not** set `writerRecoveryState`/`writerRecoveryFailure`, and rethrow. Latch
   (current behaviour) only for `ReseedRequiredException`, `Error`, and when `closeFailedWriter` itself failed (then latch
   `FAILED` because the recording may still be active).
5. Optional bounded in-process retry: add `NodeConfig.Operations.writerRecoveryAttempts` (default 3) and loop inside
   `ensureWriterLocked` with `ReplicationRetry.fullJitterDelayNanos`; otherwise the next caller (position provider, health
   probe, node restart) retries naturally.
**Why restarting recovery from scratch is safe (idempotency, required by the review).** (a) The tail scan is read-only and
derives every decision from the Archive tail plus the Store mark. (b) `extendPublication` already stops the recording and
closes the publication on its own failure. (c) `appendRecoveryMarker` offers a COMMIT/ABORT and awaits recording; if the
failure happens after Aeron accepted the marker, `closeFailedWriter` → `AeronArchiveReplicationPublisher.close()` stops the
recording and `awaitStopped` flushes it, so the next scan sees the marker as terminal and appends nothing; if the marker never
reached the log, the next scan appends it. The invariant to test: *no sequence ever has two terminal markers*.
**Change (reader).** In `failIfReconnectBudgetExpired` throw `new ReplicationUnavailableException(<same message>, reconnectCause)`;
leave `ReseedRequiredException` only in `classifySubscriptionFailure`. Fix the `AeronHealth.java:408-413` comment. Document in
the README that after the reconnect budget the reader reports `FAILED` and a restart resumes from the same mark.
**Tests.** `AeronWriterTailRecovery`: archive stub whose `getStopPosition` throws `ArchiveException(TIMEOUT)` once → first
`ensureWriter` throws `ReplicationUnavailableException`, state stays null, second call succeeds and the node reports `LIVE`;
`UNKNOWN_RECORDING` → still `RESEED_REQUIRED`; replay timeout → unavailable; partial-marker case: fail `appendRecoveryMarker`
after offer, retry, assert exactly one COMMIT in the recording. Reader: control-channel outage past the budget →
`FAILED`, not `RESEED_REQUIRED`; invalid start position → `RESEED_REQUIRED`.

### F-04 — Recovery window ignores Aeron framing (false reseed)
**Evidence.** `AeronWriterTailRecovery.validateRecoveryBounds` (`:54-62`): `window = 2·(maxTx + chunkCount·84) + 4·84`.
Archive positions advance by *framed* lengths: every Aeron frame has a 32-byte `DataHeader` and is aligned to 32 bytes
(`DataHeaderFlyweight.HEADER_LENGTH = 32`); messages longer than `mtu−32` are split into fragments each carrying that header;
when a message does not fit the rest of a term Aeron appends a padding frame. At defaults (128 KiB chunks, MTU 1408, 64 MiB
transactions) fragmentation alone adds ≈ 2.3 % ≈ 1.5 MiB per transaction versus a slack of `2·512·84 ≈ 86 KiB`.
Dictionary chunks are separate messages and also consume positions; `dictionary + data ≤ maxTransactionBytes`.
**Impact.** Two legitimate tail transactions near the limit make a healthy writer report "Archive tail exceeds writer recovery
window" → reseed (and, before F-03, permanently). Fails closed, so it is availability, not safety.
**Change.** Replace the formula by `static long recoveryWindowBytes(int maxTx, int chunkSize, int mtu, int termLength)`:
```
maxPayload   = mtu - 32                                   // Aeron data header
chunkMsgs    = ceil(maxTx / chunkSize) + 1                // dictionary and data series each end with a partial chunk
payloadBytes = maxTx + 84 * chunkMsgs + 84                // envelope headers + terminal marker
frames       = ceil(payloadBytes / maxPayload) + chunkMsgs + 2
framedTx     = payloadBytes + frames * (32 + 31)          // header + worst-case alignment per frame
maxFramedMsg = (chunkSize + 84) + ceil((chunkSize + 84) / maxPayload) * 63
rollovers    = ceil(2 * framedTx / termLength) + 1
window       = 2 * framedTx + 4 * 128 + rollovers * maxFramedMsg      // two transactions, markers, term padding
```
Use `Math.*Exact`; keep the `ArithmeticException → reseed("window overflows")` behaviour. Thread `mtu` and `termLength`
through `Request` (they are in `settings().replication()`). The comparison stays `stopPosition > startPosition + window`.
**Tests.** Pure unit test of the formula for (MTU 1408 / 9000, term 64 KiB / 16 MiB, chunk 4 KiB / 128 KiB, maxTx 64 MiB);
real-Archive IT: recording whose tail holds a marked transaction without COMMIT plus a next transaction, both ≈ `maxTx` at
MTU 1408 and term rollovers inside both → recovery succeeds and `stopPosition − prepareStartPosition ≤ window` is asserted
(this empirically validates the bound; if it fails, the bound is too small).

### F-05 — Cyclic types can cache a false "index-free"
**Evidence.** `TypeRelevance.isRelevant` (`ClusterIndexValidation.java:136-180`) returns `false` when it meets a type already in
`resolving` (`:146`) and then caches the (possibly wrong) negative result of every intermediate type (`:178`). For `A{B b; GigaMap m}`
and `B{A a}`: querying `A` first evaluates member `b:B`, which hits the back-edge `A` (→ false), so `B` is cached **irrelevant**
before `A`'s second member proves `A` relevant. `offer()` (`:758`) then prunes every `B`, and the writer commit pre-filter
(`CommitPrefilterScratch.checkType`) treats `B` entities as index-free — an external Lucene/vector index reachable only through
`B` is not validated.
**Rejected external fix.** "Return `true` on a back-edge" makes every self-recursive plain type (`Node{Node next; int v}`)
relevant, so every `Node` instance is traversed and the 65,536-object bound fails healthy graphs. Do not do that.
**Change (least fixed point, order independent).**
1. In `TypeRelevance.load/observe` (the only places `definitions` changes) rebuild `Set<Class<?>> relevantTypes`:
   a. For each definition type `C`: `alwaysRelevant(C)` = `isIndexMetadata(C)` OR any member type `M` is a collection-like
      (`Iterable/Map/Optional/AtomicReference/Reference`), OR an array whose component is relevant-by-kind, OR `candidates(M)`
      is empty (unknown layout), OR `C` has no members and is not a leaf. `candidates(M)` = definitions whose type is
      assignable to `M` (existing `assignable` logic).
   b. Seed `S = { C | alwaysRelevant(C) }`.
   c. Repeat `for C not in S: if any candidate of any member of C is in S → add C` until no change (worklist; the graph is
      finite and small).
2. `isRelevant(type)`: keep the early leaf/index/array/collection rules; otherwise `candidates = assignable(type)`; relevant iff
   `candidates.isEmpty()` or any candidate ∈ `S`. Delete `resolving`, the per-call caching of negatives and the `relevant` map.
3. `observe(definition)` still clears caches and recomputes `S` (cheap; called only when a new type id appears).
**Tests.** Parameterised both query orders for mutual recursion with the index behind the cycle → both types relevant;
self-recursive plain `Node` → irrelevant and not traversed (assert `scanWork` unchanged on a 100k-node list); `Customer{List<Order>}`
unchanged (relevant); commit pre-filter test and root-traversal test each find an external `LuceneContext` behind `B`.

### F-06 — Failed disk measurement reopens write admission
**Evidence.** `StorageUsageGauge.measureDirectory` (`:667-686`) swallows *every* `RuntimeException` from `file.size()` and from
child-directory iteration and still returns the partial total as a successful measurement (`measureNow` stores it in
`cachedBytes`). `StorageLimitGate.createScheduledWork` (`:573-577`) logs "writes are disabled until a measurement succeeds" on a
negative reading but changes no state; `updateUsage(<0)` returns silently (`:520`). A permission/I/O failure on a subtree makes
usage look smaller and a previously reached limit is released by the hysteresis band.
**Change.**
1. Gauge: add `private static boolean disappeared(Throwable t)` — true when any cause in the chain is
   `java.nio.file.NoSuchFileException` or `java.io.FileNotFoundException`. In `measureDirectory` catch `RuntimeException failure`:
   if `disappeared(failure)` skip the entry (current behaviour) else set a local `incomplete[0]=true` and log WARN once.
   `measureNow()` returns `-1L` (and stores `cachedBytes=-1L`) when `incomplete`.
2. Gate: add `public void markUnknown()` = `state.updateAndGet(s -> s & ~MEASUREMENT_KNOWN)` (keeps `LIMIT_REACHED`; `limitReached()`
   already treats "unknown" as reached → fail closed). Call it from `createScheduledWork` when `usedBytes < 0` **and** from
   `updateUsage` when `usedBytes < 0`. Reset the "warning logged" latch when a good reading returns.
3. Surface it: in the same scheduled task, after `markUnknown()` throw `IllegalStateException("storage usage is unknown")` so
   `NodeMaintenanceScheduler` counts it toward `maintenanceFailureThreshold` and `status()` reports the node degraded.
   (`StorageUsageGauge` task `"StorageUsageGauge"` should likewise stop swallowing: it only measures.)
**Tests.** Fake `ADirectory` whose `file.size()` throws `IORuntimeException(new AccessDeniedException(..))` after the gate was open
→ `limitReached()` true; same after the limit was reached and usage "falls" → stays true; `NoSuchFileException` cause → skipped,
measurement still known; scheduler records a failure after the threshold.

### F-07 — Retention may delete the bytes a restarting reader needs  *(needs reproduction)*
**Evidence.**
- Reader restart authority is the Store mark: `AeronReaderTransport.createClient` starts replay at `startingMark.prepareStartPosition`
  (`:94-99,115-116`), i.e. the *start* of the last applied transaction's prepare (A1.3).
- The positions the reader publishes are different: `TransactionAssembler.accept(…, header.position())` → `commit(envelope, position)`
  → `Delivery.prepareCommit(…, position)` → `PendingDelivery.position` → `CursorSnapshot(resolvedTail.sequence(), resolvedTail.position())`
  (`flushDeliveries`, `:916-917`). `header.position()` is the Aeron position *after* the COMMIT frame. The transport forwards that to
  `publishReaderWatermark(...)` (`AeronReaderTransport.java:128-137`) with the comment "Publish its prepare position" — it is not.
  The same snapshot feeds `ClientAdapter.position()` (`:295-300`, placed in the `prepareStartPosition` slot of `ReplicationPosition`),
  hence `BackupMetadata.recordingPosition` and the backup retention boundary.
- Retention uses `AeronArchive.segmentFileBasePosition(start, min(quorumWatermark.position, requested.position), term, segment)`
  (`AeronArchiveRetention.java:244-260`): the *base of the segment containing the watermark position* becomes the new start and all
  earlier segments are purged (verified in `AeronArchive.java:195-203, 2237`).
**Failure scenario.** A transaction whose frames span a segment boundary (probability ≈ `txBytes / segmentBytes`; ≈ 50 % for a
64 MiB transaction with 128 MiB segments) is the quorum minimum. `watermark.position` = end of its COMMIT (in segment k+1), purge
removes segment k that contains `prepareStartPosition`. The reader restarts, asks for replay from `prepareStartPosition` and the
`PersistentSubscription` fails with `INVALID_START_POSITION` → `RESEED_REQUIRED` ("no longer covers this reader's Store mark").
The same applies to a Store restored from a backup whose boundary was recorded as the commit end.
**Reproduce first (`T/node/aeron/AeronRetentionResumeIT`).** Writer + one retention reader; `PERUNCS_AERON_TERM_LENGTH=65536`,
`…ARCHIVE_SEGMENT_FILE_LENGTH=65536`, chunk size 4096, payloads ≈ 150 KiB so every transaction straddles segments; apply N
transactions on the reader, run `maintainRetention()` until a purge happens, stop the reader node, restart it from its Store. Expected
today: `RESEED_REQUIRED`. After the fix: the reader reaches `LIVE`.
**Change.**
1. Define the invariant in Javadoc of `ReplicationPosition` and `AeronReaderWatermark`: *`position` of a reader watermark and of a
   backup boundary is the Archive position from which a Store carrying that boundary resumes replay* (= the Store mark's
   `prepareStartPosition`). The writer's `latest()` boundary stays "end of last terminal frame" and is only ever used as an *upper
   cap* (`min(quorum, requested)`), so it stays correct; say so in its Javadoc and rename the accessor in `AeronWriterRecoveryBoundary`
   to `terminalPosition`.
2. `AeronReaderTransport.createClient`: keep a `volatile CursorSnapshot restartBoundary` initialised to
   `(startingMark.sequence, startingMark.prepareStartPosition)`. In the `transactionResolved` callback (runs on the poller thread
   after `receiver.awaitApplied()` returned, so the materializer has finished and happens-before holds) compute
   `final CursorSnapshot restart = new CursorSnapshot(startingMark.sequence, startingMark.prepareStartPosition);` — only when
   `startingMark.sequence >= 0`; publish the watermark with `restart` (not with the assembler's snapshot) and store it in
   `restartBoundary`. An ABORT-only barrier does not change the mark, so the watermark simply repeats (already tolerated:
   `position == previous.position` for equal sequence is accepted by `monotonicProgress`/`recordReaderWatermarkOnAgent`).
3. `ClientAdapter.position()` returns `restartBoundary` (sequence + position) instead of `delegate().cursorSnapshot()`; leave
   `stopResult()` on the assembler snapshot (it is only used for stop diagnostics).
4. Keep the writer-side checks: `requireWithinDurableBoundary(sequence, position)` accepts `position ≤ terminal.position`, which holds.
5. Update README ("Archive retention") to state that retention keeps the segment containing every reader's resume position.
**Tests.** The IT above; unit: watermark published after a straddling transaction carries `mark.prepareStartPosition`; backup created
after the straddling transaction has `recordingPosition == mark.prepareStartPosition`; retention never deletes a segment containing
the minimum reader's `prepareStartPosition`.

### F-08 — Writer `NodeStatus` bypasses `AeronHealth` (capacity, driver and maintenance failures invisible)
**Evidence.** `StorageNodeManager.replicationReady/Healthy` (`:152-160`) and `replicationState` (`:201-207`) use
`dataDistributor.failure()` for writers and only readers use `healthCheck`. `replicationStatus` (`:179-195`) hard-codes
`archiveUsableBytes`, `writerDurablePosition` and `appliedSequence` to `-1` for writers. `AeronHealth` already implements a writer
branch (`writerRole`, `writerReady`, `writerState`, capacity `DEGRADED`, driver/watermark failure) and
`AeronArchiveCapacity` measures the *writer's* Archive directory — so the one node that has an Archive reports "unknown" while
readers (which have none) are the ones asked. `StorageNodeHealthCheck.available()` (maintenance health) is also skipped for writers.
**Impact.** A writer whose MediaDriver died, whose Archive volume is below `PERUNCS_AERON_MIN_ARCHIVE_FREE_BYTES` (writes rejected),
whose retention/limit maintenance fails repeatedly, or whose watermark channel failed still reports `ready=true`, `healthy=true`,
`LIVE`. README "Capacity procedure" step 1 ("Alert when `archiveUsableSpaceBytes` approaches …") cannot be followed on the writer.
**Change.**
1. `StorageNodeManager.replicationReady/Healthy`: `return this.healthCheck.isReady()/isHealthy()` for every role (the standalone
   case has `replicationEnabled=false` and `NoOp` health that delegates to storage readiness). Keep `validGraph()` and the executor check.
2. `replicationState()`: `return this.healthCheck.replicationState()` for every role; the writer branch of
   `AeronHealth.stateUnchecked` already maps recovery state, driver failure, capacity and `REPLICATION_SUSPENDED`.
3. `replicationStatus()`: remove the `writer ? -1L : …` ternaries for `archiveUsableBytes`, `writerDurablePosition`,
   `writerDurableSequence` (use health values for all roles); `appliedSequence` stays `-1` for the writer; `currentSequence` for the
   writer = `positionProvider.latest().sequence()` (this also removes the `messageIndex` dependency, see F-15).
4. Delete the stale comment "A writer reports its own failure state instead of consulting the reader health check".
**Preserve.** `ReplicationPendingException` → `REPLICATION_SUSPENDED` (already produced by `AeronWriterTransport.writerState`).
**Tests (`StorageNodeManagerRolesTest` + a real-Aeron writer IT).** writer with `minimumFreeBytes` larger than free space → `DEGRADED`,
`status().replication().archiveUsableBytes() >= 0`, writes rejected; `AeronTransport.stopDriverForTest` → writer `FAILED`, `ready=false`;
maintenance task failing past the threshold → writer `ready=false`.

### F-09 — Vector warm-up blocks every graph read
Known (OPUS P1-4/D-20): `ApplyWorker.java:242-248` runs `warmupVectorSearchGraphs()` inside the exclusive coordinator write
section; measured 69 ms (1k) vs 23.9 s (100k) by OPUS (not re-measured here). The only bound is `indexRefreshBudgetMs`
= 10 × `materializationBudgetMs` = 1800 s, i.e. a 30-minute read blackout is "healthy".
**Change (in order).** (1) After F-24 lands, read PR #832's locking contract (per the PR: `invalidateGraph` takes the builder lock)
and, only if upstream guarantees a safe concurrent first search, move `warmupVectorSearchGraphs()` outside the write section.
(2) Until then: expose the blockage — record `lastApplyBlockedMillis` (time spent holding the write side per batch) in
`ReplicationStatus` and log WARN above 5 s; cap the refresh budget with a new `NodeConfig.Timeouts.indexRefresh` (default 60 s)
instead of `10 × materializationBudget`. (3) Add a `>10k`-vector reader test with a time bound.

---

## 4. P2 findings

### F-10 — Starting an existing Store requires a writable backup volume
**Evidence.** `NodeLifecycle.startStorageNode` (`:423`) calls `assembly.getStorageBackupBackend()` before restore.
`FilesystemVolumeBackupBackend`'s constructor runs `probeAtomicPublication()` (`:153,163-189`: creates the directory, a temp file,
renames it, forces the directory) and `BackupRestorePolicy.restoreLatestBackupIfRequired` (`:87-93`) lists the volume even when
local storage exists, only to discard the result (`:121-123`). A read-only or unavailable cold-backup volume therefore blocks a
writer or reader that already has its Store.
**Change.**
1. `BackupRestorePolicy.restoreLatestBackupIfRequired(Path, Supplier<StorageBackupBackend>)`: compute `storageExists` first; if true
   log INFO ("Existing local storage found; retaining it and its Store replication mark") and `return false` **before** calling the
   supplier or `configuredIdentity()`.
2. `NodeLifecycle.startStorageNode`: remove `final var backend = this.assembly.getStorageBackupBackend();` and pass
   `this.assembly::getStorageBackupBackend`. `startBackupNode` keeps the eager backend (it needs the volume).
3. Reader without a Store keeps today's behaviour (supplier called, error message names the volume).
**Tests.** `NodeCollaborators` with a backup-backend factory that throws: writer/reader start with an existing Store succeeds and the
factory is never invoked; start without a Store fails with the existing reseed/volume message. Remove the dead "No backup … keeping local
storage" WARN test expectation if one exists.

### F-11 — Coordinator lock discipline, double Archive wait, stale Javadoc
**Evidence.** `AeronReplicationWriteCoordinator.prepareWriteAtomically` (`:178-194`) holds `writeLock` across index validation,
`coordinator.prepare` (every chunk offer plus the recorded-position wait, `AeronReplicationPublisher.java:476`) **and** the local Store
write (`AeronStorageBinaryReplicationTarget.java:150-152,217`). Class Javadoc `:31-36,50-58` claims the slow waits hold no coordinator
lock; that is true only for the COMMIT path — `commitAcceptedStore` offers COMMIT without awaiting recording, so the target Javadoc
`:21-24` is correct about *commit*. `dispose()` (`:855`), `cancelStoreCommit` (`:234`), `retryPendingCommit` (`:331`) and
`distributeTypeDictionary` (`:157`) use unbounded `lock()`; `withWritesPaused` (`:577-598`) is bounded. When prepare fails,
`prepareWithRecovery` (`:401-406`) offers ABORT and `commitPositionAwaiter` waits another full `recordedPositionTimeout` against the
same unresponsive Archive, so a dead Archive costs ≥ 30 s + offer timeout + 30 s inside the application's exclusive section.
**Change.**
1. Rewrite the coordinator class Javadoc and the field comment to state the real phases: *admission + prepare (offers, Archive wait) +
   local Store write run under `writeLock`; COMMIT offer and recording run without it*.
2. In `prepareWithRecovery`, bound the ABORT: use a new `NodeConfig.Timeouts.abortRecord` (default 5 s) for the
   `commitPositionAwaiter` call after an ABORT offer (pass a deadline-limited awaiter); if the ABORT is offered but not recorded in
   time, fail closed (`failPendingTransaction`) and let A1.5 recovery append/resolve it — the existing spec already says an unrecorded
   ABORT latches. Do **not** convert a timed-out accepted-write cleanup into an ABORT (see F-12).
3. Replace the unbounded `lock()` calls in `dispose`, `cancelStoreCommit`, `retryPendingCommit` with
   `tryLock(ReplicationRetry.remainingNanos(deadline(recordedPositionTimeout)))`; on timeout throw
   `ReplicationUnavailableException("timed out waiting for Aeron write admission")`. `distributeTypeDictionary` is called under the
   already-held lock from `prepareWrite`; leave it.
4. Do not shorten `recordedPositionTimeout` defaults silently; expose the new setting (F-23) and document the 30 s default inside an
   exclusive write section.
**Preserve.** Accepted-write recovery evidence: a timeout must never produce an ABORT after local acceptance.
**Tests.** Archive stub that never records: `write()` fails within ≈ `recordedPosition + abortRecord`, not `2×recordedPosition`;
`dispose()` during a stuck prepare returns/throws within the bound; sequence/ABORT state after the failure matches A1.5.

### F-12 — Closing a prepared token after local acceptance can emit ABORT
**Evidence.** `AeronStorageBinaryReplicationTarget.write` (`:159-162`): `try (prepared) { … commitAcceptedStore(prepared); }`.
`PreparedTransaction.close()` (`AeronReplicationPublisher.java:1081-1096`) calls `owner.abort(this)` unless the token is terminal,
`commitPending`, or the publisher is closed/failed. `commitAcceptedStore` → `beginCommit` can throw (`:388-393` "does not own admission",
`:391-393` wrong state) before any of those flags is set and without failing the publisher; `close()` then offers ABORT for a sequence
the Store already committed → readers skip a transaction the writer persisted (permanent divergence). Latent: needs a coordinator
invariant violation.
**Change.**
1. Add `void markLocallyAccepted()` to `PreparedTransaction` (volatile boolean). Call it from `prepareWrite` immediately after
   `this.delegate.write(data)` returns (before `FaultInjection AFTER_LOCAL_WRITE_BEFORE_COMMIT`).
2. `PreparedTransaction.close()`: `if (locallyAccepted && !terminal && !commitPending) { owner.failClosedAfterAcceptedWrite(this); return; }` where that method sets `terminal=true`, `pendingTransaction=null`, `failed=true`, closes the framer — i.e. the same effect as `abandonWithoutAbort()`. Never call `owner.abort` for an accepted token.
3. `AeronReplicationWriteCoordinator.beginCommit` failure paths should also call `this.publisher.failClosed()` (an accepted write whose commit state cannot be established is an uncertain outcome).
**Tests.** Force `beginCommit` to throw after a successful local write (fault hook `AFTER_LOCAL_WRITE_BEFORE_COMMIT`): assert no ABORT frame in the
offerer, publisher `isFailed()`, graph invalidated by the facade, and restart recovery appends COMMIT.

### F-13 — Cancelled watchdog tasks stay queued until their deadline
`StorageBinaryDataMerger.java:190-194` uses `Executors.newSingleThreadScheduledExecutor`; `ApplyWorker` schedules two tasks per batch
(`:168-171`, `:234-237`) and cancels them (`:232,263,267`). Without `setRemoveOnCancelPolicy(true)` (absent from `src/main`) each
cancelled task object (callable already cleared, so ≈ tens of bytes) stays in the delay queue for 180 s (materialization) or 1800 s
(index refresh). Linear in batch rate; not a captured-object leak.
**Change.** Replace the factory with
`final ScheduledThreadPoolExecutor wd = new ScheduledThreadPoolExecutor(1, r -> Thread.ofPlatform().daemon().name("peruncs-apply-watchdog").unstarted(r)); wd.setRemoveOnCancelPolicy(true); wd.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);`
(field type stays `ScheduledExecutorService`). Fix the comment at `ApplyWorker.java:143-146` ("allocates nothing" — two tasks and lambdas
are allocated per batch; `Drain.ensureViews` reallocates when the transaction buffer count changes).
**Test.** `ApplyWorkerBudgetTest`: apply 10,000 trivial batches, assert `((ScheduledThreadPoolExecutor) watchdog).getQueue().size()` stays ≤ 2.

### F-14 — Materializer thread kind and Data Grid thread names
`StorageBinaryDataMerger.java:184-186` creates the materializer as a virtual thread named `eclipse-datagrid-store-materializer`; OPUS D-24
(`OPUS_REVIEW.md:1127`) specifies a platform daemon `peruncs-apply`. The work is CPU-bound (index validation, JVector rebuild) plus blocking
Store I/O; virtual threads are not time-sliced, so a long rebuild can occupy a carrier that maintenance, retention and backup virtual
threads share (starvation scenario plausible, not demonstrated).
**Change.** `Executors.newSingleThreadExecutor(r -> Thread.ofPlatform().daemon().name("peruncs-apply").unstarted(r))`. Rename:
`eclipse-datagrid-store-watchdog`→`peruncs-apply-watchdog` (F-13), `eclipse-datagrid-aeron-watermarks`→`peruncs-watermarks`
(`AeronWatermarkChannel.java:77`), `datagrid-aeron-archive-reader`→`peruncs-archive-reader` (`AeronArchiveReader.java:469`),
`datagrid-housekeeper-N`→`peruncs-housekeeper-N` (`NodeMaintenanceScheduler.java:47`), `datagrid-retention-agent`→`peruncs-retention-agent`
(`AeronArchiveRetention.java:130`), `EclipseStore-StorageChecks/StorageBackup` → `peruncs-storage-checks/-backup`, channel alias
`datagrid-%s` (`NodeConfig.java:418`) → `peruncs-%s` (changes the recording alias: existing recordings need reseed — acceptable per AGENTS
"no backward compatibility"), error text "unknown DataGrid envelope" (`AeronReplicationEnvelope.java:343`). Optional: keep virtual threads
for the I/O-bound backup executor only if its zip compression is moved off it (compression is CPU-bound too).
**Acceptance.** Soak with `-Djdk.virtualThreadScheduler.parallelism=1` while a 100k-vector rebuild runs: maintenance health stays live.

### F-15 — Data Grid `ReplicationPublisher` SPI and dead plumbing
**Evidence (corrected inventory).** `storage/binary/ReplicationPublisher` (225 lines: `distributeData`, `messageIndex`, `ignoreDistribution`,
`failure`, `queueTypeDictionaryForNextTransaction`, `consumeTypeDictionary`, `noOp()`, `Caching`) and its only production backing
`AeronDistributionGate` (whose `distributeData` always throws, `:99-107`). Users: `NodeCollaborators.java:361`, `NodeLifecycle.java:280,324,421,485,568`,
`AeronWriterTransport.java:123-148,167-230` (including the always-true `distributor instanceof ReplicationPublisher cluster`, `:192,:222`),
`DistributedStorage`, `DistributingTypeDictionaryExporter`, `AeronStorageBinaryReplicationTarget.TargetCallbacks`, `ClusterReplicationTransport`,
**and `StorageNodeManager.java:152-205`** (`messageIndex()` is the writer's reported current sequence). Write-only state:
`AeronWriterTransport.nextSequence` (`:52,138,153,490` — never read), `AeronArchiveReplicationPublisher.synchronizeNextSequence` /
`AeronReplicationPublisher.synchronizeNextSequence` (always a no-op because the publisher already advanced `nextSequence`),
`AeronReaderTransport.java:131` (`writerTransport().advanceSequence(...)` from a reader's callback). Also dead in production:
`AeronArchiveReplicationPublisher.createRemote/extendRemote` (no main caller; external-Archive mode was removed by D-26).
**Semantics that must survive.** `Caching` keeps one staged dictionary with a flag `snapshot`: a restart snapshot
(`queueTypeDictionaryForNextTransaction`) replaces any staged incremental and later incrementals are ignored until it is consumed;
`distributeTypeDictionary(null)` clears only an incremental; staged text is cleared only after the consumer succeeded
(`consumeTypeDictionary` atomically returns and clears, the coordinator keeps the bytes for retry).
**Change.**
1. New `public final class TypeDictionaryOutbox` (package `storage.binary`, public because three packages use it) holding
   `AtomicReference<Pending>` where `record Pending(String dictionary, boolean snapshot)`; methods `stageIncremental(String)` (null clears
   only a non-snapshot), `stageSnapshot(String)`, `String consume()`; implementation = the three `Caching` methods verbatim.
2. `DistributingTypeDictionaryExporter.create(delegate, TypeDictionaryOutbox)` → `outbox.stageIncremental(assembled)`;
   `TargetCallbacks.dictionarySource` becomes `TypeDictionaryOutbox`; `AeronWriterTransport` commit-scan lambda calls
   `outbox.stageIncremental(...)`; `NodeLifecycle.queueWriterDictionary` calls `outbox.stageSnapshot(...)`.
3. Replace `ignoreDistribution(boolean)` by a `BooleanSupplier` backed by a lifecycle-owned `AtomicBoolean bootstrapping` passed into
   `persistenceTargetFactory(outbox, writerStorage, distributionEnabled)` (this is `TargetCallbacks.distributionEnabled` already).
4. Replace `distributor.failure()` uses in `StorageNodeManager` by `ClusterReplicationTransport.writerFailure()` (new default `null`;
   Aeron returns `writerTransport.pendingCommitFailure()`), or drop them after F-08 (health already covers it).
5. Replace `messageIndex()`: writer `currentSequence` = `positionProvider.latest().sequence()` (F-08 step 3). Note this also fixes the
   writer reporting `-1` after a restart until its first write.
6. Delete `ReplicationPublisher`, `AeronDistributionGate`, `AeronWriterTransport.nextSequence/advanceSequence/distributor()`, both
   `synchronizeNextSequence`, `AeronReaderTransport.java:131`, `ClusterReplicationTransport.distributor()` and `NoOp.distributor()`,
   `createRemote/extendRemote` (and the private pass-through statics `getStopPosition`, `getRecordingPosition`, `pollForErrorResponse`,
   `tryStopRecordingByIdentity`, `purgeSegments`, unused `extendRecording` in `AeronArchiveReplicationPublisher`).
**Tests.** Keep/port the existing `Caching` tests to `TypeDictionaryOutbox` (snapshot wins, incremental ignored, null clears only incremental,
consume clears); writer status after restart shows the recovered sequence.

### F-16 — Metadata CRC pre-pass and its equality check
`AeronReplicationWriteCoordinator.prepareLocked` (`:466`) calls `publisher.transactionMetadata` → `EnvelopeFramer.computeDataCrc` (a full pass),
`offerDataChunks` then computes the transaction CRC and a per-chunk CRC, and `prepareReserved` (`AeronReplicationPublisher.java:470-472`)
throws "transaction data changed after durable fence" if the two transaction CRCs differ. The fence this guarded no longer exists
(vocabulary: `:72,274,316,379,490,538,570`). The buffers are Serializer-owned and untouched inside one admission, so the check cannot fire.
**Change (only this pass).** Delete `TransactionMetadata`, `transactionMetadata(...)`, `EnvelopeFramer.computeDataCrc`, the `metadata`/
`verifyExpectedCrc`/`expectedCrc32c` parameters of `prepareTransaction/prepareWithRecovery/prepareReserved`, `PreparedWrite.metadata` and the
`prepared.dataCrc32c() != metadata.crc32c()` comparison (`Coordinator:518-521`). Compute `dataLength = totalRemaining(buffers, count)` and
`chunkCount = EnvelopeFramer.chunkCount(dataLength, chunkSize)` where they are used (`ensureWriteAdmitted`, `prepareTransaction` length check).
**Keep** the transaction CRC (COMMIT witness read by the reader assembler and by `AeronWriterTailRecovery`) and the per-chunk CRC (wire frame).
A protocol change that derives the commit checksum from chunk CRCs is a separate, optional step needing reader+recovery changes.
**Tests.** Existing publisher tests adapted; assert `offerDataChunks` returns `CRC32C` of the concatenated sources for 1, 3 and 8 sources.

### F-17 — Gather path: aliasing trap, hard-coded threshold, test matrix
**Evidence.** `AeronReplicationEnvelope.encodeWithPayloadCrc` skips the payload copy when `payload == target && payloadOffset == targetOffset + HEADER_LENGTH`
(`:191-195`); the staged framer path relies on it and `offerGatheredDataChunk` (`EnvelopeFramer.java:320-336`) passes the header buffer as its own
payload. `MIN_GATHER_PAYLOAD_BYTES = DEFAULT_CHUNK_SIZE` (`:22,140`) ignores the configured `PERUNCS_AERON_CHUNK_SIZE`. Two near-identical loops
(`:138-186`, `:188-228`). `AeronGatherOfferBenchmark` (`-Pbench`) already asserts staged vs gathered byte/CRC equivalence, but only outside the default gate and not through the production `EnvelopeFramer` methods; OPUS' JMH (rev 132) measured 4 source buffers only and 1.7 % gain at 64 KiB — it does not cover other shapes.
**Change.**
1. Add `AeronReplicationEnvelope.encodeHeader(MutableDirectBuffer target, int offset, …, int chunkLength, int payloadCrc32c, ChecksumContext ctx)`
   (header only); `encodeWithPayloadCrc` = `encodeHeader` + unconditional `putBytes`; **delete** the `payload != target` condition. The staged
   loop (payload already written at `HEADER_LENGTH`) and the gather path both call `encodeHeader`.
2. Gather threshold: `length >= this.chunkSize` (one full chunk) using the live configuration instead of the constant.
3. Do **not** delete the staged path yet. Delete it only after the matrix in the tests below passes against a real publication, then also remove
   `supportsVectors`/`MIN_GATHER_PAYLOAD_BYTES`.
**Tests (`AeronReplicationPublisherTest` + IT).** >4 sources (forces `GatherScratch.ensureSourceCapacity` growth); chunk boundary exactly on a source
boundary; a chunk spanning ≥3 sources; read-only heap source; `length changed` `IllegalArgumentException` mid-gather and what the offerer saw;
**differential**: same input via staged and gathered path produces byte-identical frames, header CRCs, chunk CRCs and COMMIT CRC; real-Archive IT (`AeronUdpReplicationIT:73` already writes a 200,000-byte transaction through a real `ExclusivePublication` and the production offerer reports `supportsVectors()==true`, so the gather branch executes today; what is missing is an *assertion* — add one) with
an assertion hook (`EnvelopeFramer` package-private counter `gatheredChunks()`) proving the branch ran; JMH covers 1, 2, 8, 32
sources at 64 KiB–1 MiB before any deletion.

### F-18 — `NodeException.outcome()` and the `errors` layering
**Evidence.** `errors/NodeException.java:739-744` returns `FAILED` for everything except three types, so `StorageLimitReachedException`,
`ReplicationUnavailableException`, `GraphDrainTimeoutException`, `BackupBusyException` all say "no generic retry" although the README maps
`DEGRADED` to "may serve". `NodeException` `permits` `errors.internal.ReplicationPositionUnavailableException` and
`node.backup.IncompleteArchiveException` (`:7-10`), and `BackupBusyException` imports `api.ClusterNode` (Javadoc only): `errors` depends on `api`,
`node.backup` and an unexported package while all of them depend on `errors`. `errors/package-info.java` mentions a "writer fencing" type that does not exist.
**Change.**
1. Add `Outcome.TRANSIENT` ("retry later with backoff or after freeing a resource; no restart needed"). Implement `outcome()` with an exhaustive
   pattern `switch (this)` over the sealed leaves (compile-time exhaustive):
   `WriteRejectedException→RETRYABLE; ReplicationPendingException→PENDING; ReseedRequiredException→RESEED_REQUIRED;`
   `BackupBusy, StorageLimitReached, ReplicationUnavailable, GraphDrainTimeout, ReplicationPositionUnavailable→TRANSIENT;`
   `GraphInvalidated, CorruptReplicationData, ReaderWriteRejected, WrongRole, IncompleteArchive→FAILED`.
2. Move `ReplicationPositionUnavailableException` and `IncompleteArchiveException` into `peruncs.cluster.errors` (public final, public constructors,
   Javadoc "thrown by the node internals; applications normally see it only as a cause"), update imports, delete `errors/internal`.
   (Making `IncompleteArchiveException` a non-`NodeException` would change the `catch (NodeException)` control flow in
   `FilesystemVolumeBackupBackend` — do not.)
3. Replace the `ClusterNode` import in `BackupBusyException` by plain text; remove the "writer fencing" sentence.
4. Add an architecture test (`T/ArchitectureTest`, plain `Files.walk` over `src/main` imports) failing on `errors → api|node.*|storage.*` imports.
**Tests.** Table-driven `NodeExceptionOutcomeTest` instantiating every leaf and asserting its outcome; README table of outcomes.

### F-19 — JVM-global native allocation registry (all owners)
**Evidence.** `NativeMemory.ALLOCATIONS` (`:14-38`) is a static `IdentityHashMap` guarded by a monitor. Owners of `allocateDirect/releaseDirect`:
`AeronReplicationPublisher.java:163,961` (framing storage) **and** the default methods of `StorageBinaryDataReceiver.java:21,28` (used by test receivers and
by any receiver that does not override them; `StorageBinaryDataMerger` overrides them with its pool). `NativeBufferPool` already uses the handle API
(`allocateScoped`/`Allocation`).
**Change.**
1. Make `NativeMemory.Allocation` public with `public ByteBuffer buffer()` and an idempotent `close()` (guard with `AtomicBoolean`; a second
   `Arena.close()` throws `IllegalStateException`). Add `public static Allocation allocate(int capacity)`.
2. `AeronReplicationPublisher`: hold `private final NativeMemory.Allocation framingAllocation`; `framingStorage = framingAllocation.buffer()`;
   `releaseFramingStorage()` closes it (keep the `framingStorageFreed` CAS or rely on idempotent close).
3. `StorageBinaryDataReceiver.allocateNativeBuffer/releaseNativeBuffer`: remove the default bodies (make them abstract); give test receivers a helper
   (`T/fixture/DirectBufferReceiver` using `ByteBuffer.allocateDirect`) — production code never needs a default.
4. Delete `ALLOCATIONS`, `allocateDirect`, `releaseDirect`.
**Tests.** Two publishers closed concurrently in one JVM do not contend (no static monitor); native-memory test (`NativeMemoryTrackingTest`) unchanged.

### F-20 — Backup export staged on the shared volume; entry cap
**Evidence.** `FilesystemVolumeBackupBackend.createExportWorkspace` (`:756-764`) creates the workspace under `backupVolumePath`; `issueFullBackup`
copies the *uncompressed* Store there, `compressStorage` writes the zip there, then renames within the volume. On a network volume this triples I/O
and needs free space for Store + zip. `BackupArchiveLimits.MAX_ENTRY_BUDGET = 1 << 16` caps entries; Eclipse Store's default data-file maximum is 8 MiB
(verify in the pinned snapshot), so ≈ 512 GiB Stores cannot be restored ("too many or duplicate entries") and the failure appears at restore time, not at
backup time.
**Change.**
1. Add `NodeConfig.BackupConfig.workspace` (`PERUNCS_BACKUP_WORKSPACE_PATH`, default `<PERUNCS_STORAGE_PATH>/backup-workspace`, node-local). Export + zip
   there. Publish by copying the finished zip to `<volume>/.publish-<uuid>.tmp` (`FileChannel.transferTo`, `force(true)`), then the existing atomic rename
   (rename is atomic only inside one filesystem — do not rename across filesystems). Keep orphan-workspace reaping for the local workspace.
2. Make the entry budget configurable: `PERUNCS_BACKUP_MAX_ENTRIES` (default 1,048,576) and fail the **backup** early: after `issueFullBackup`, count regular
   files under the export directory and throw `NodeException("Store has N files; restore limit is M")` if above the limit.
3. Stream entry validation instead of materializing `List<ZipEntry>` when the limit is raised above 100k (iterate `zip.entries()` twice).
**Tests.** Backup with the workspace on a different `FileSystem` mock; backup of a Store with `limit+1` files fails at creation; restore of an archive at exactly the limit succeeds.

### F-21 — Store-directory replacement has no crash recovery
**Evidence.** `AtomicFileWriter.replaceStorage` (`:310-360`) renames the live directory to `.storage-previous-<random>` (`:329`), forces the parent, then renames
the staged directory into place (`:332`). Exception rollback exists, process death between the two renames leaves no `storage` directory and an anonymous
sibling; startup then treats the node as having no Store (`isMissingOrEmpty`) and the previous image is stranded. Used only by `restoreUserUploadedStorage`
(backup-reader upload path).
**Change.** (1) Use the deterministic name `.storage-previous`. (2) Add `AtomicFileWriter.recoverInterruptedReplacement(Path parent, Path destination)`: if
`destination` is missing and `.storage-previous` exists → atomic move back + WARN; if both exist → the replacement completed, delete `.storage-previous`. (3) Call it at
the top of `NodeLifecycle.startBackupNode` before `isMissingOrEmpty(storageRootPath)` and at the start of `replaceStorage`. (4) New `FaultInjection` point
`AFTER_PREVIOUS_STORAGE_MOVED`.
**Tests.** Forked crash test (reuse the crashmatrix harness): kill at the new point, restart → the old Store is restored; kill at the existing
`AFTER_STORAGE_RENAME_BEFORE_DIRECTORY_SYNC` point → restart completes and removes `.storage-previous`.

### F-22 — 17/18-parameter codec methods on the frame path
`AeronReplicationEnvelope.encode` (17), `encodeWithPayloadCrc` (18), `EnvelopeView.set` (15, package-private, one caller), `validate` (14, private),
`AeronReaderWatermark.decodeFrame` (12, private), `encodeInto` (8, public). Primitives avoid allocation; a reusable encoder does too.
OPUS rev 111 (`OPUS_REVIEW.md:1176`) deliberately kept primitive codec signatures to avoid frame-path allocation; this is a rule-17 critique, not a contradiction, and the encoder keeps zero allocation.
**Change.** Add `public static final class HeaderEncoder` in `AeronReplicationEnvelope` (the type stays `public` — it is used by `storage.aeron.writer`,
`reader`, `node.aeron`): mutable fields, fluent setters (`clusterId`, `epoch`, `fencingToken`, `wireNonce`, `sequence`, `kind`, `payloadLength`, `chunkIndex`,
`chunkCount`, `chunkOffset`, `commitCrc32c`, `payloadCrc32c`, `chunkLength`), `int encodeHeader(MutableDirectBuffer, int)` and `int encode(MutableDirectBuffer, int, DirectBuffer payload, int payloadOffset)`;
one instance per `EnvelopeFramer`/publisher (single-threaded). Make `validate` private to the encoder. Keep the old static methods only as thin wrappers
for tests, then delete. Same pattern for `AeronReaderWatermark.encodeInto`. Replace `EnvelopeView.set(15 args)` by direct private field assignment inside the
outer class. Combine with F-17 step 1.

### F-23 — Retry pacing and fixed timeouts are not configurable
`AeronSettings.fromConfig` (`:182-193`) builds `AeronReplicationConfiguration` without `.retryPolicy(...)`, so `AeronRetryPolicy.defaults()` (nine literals:
idle 1/10/1 ns/1 ms, jitter 1 µs–1 ms, probes 10 ms/1 ms/100 ms) is always used; programmatic users can pass one via the builder but node operators cannot.
Other fixed values: six `PERUNCS_AERON_*_TIMEOUT_NANOS` default to 30 s (configurable); **not** configurable: retention close 30 s + 5 s, retention command 60 s
(`AeronArchiveRetention.DEFAULT_OPERATION_TIMEOUT_MILLIS`), watermark retry 1 s, backup publication lock 30 s + 10 ms spin
(`FilesystemVolumeBackupBackend:47-48`), reaper age 24 h, merger dispose 30 s + 5 s, merger caching 10 s default in code, `STALE_DRIVER_RETRY_DELAY` 100 ms
(`AeronRuntime:38`), `MAX_CACHED_BYTES` 1 GiB (`NodeConfig.Limits.applyQueueMaxBytes` has no `Setting`), `StorageUsageGauge.REFRESH_INTERVAL` 5 s, pending-commit retry
interval derived from the offer timeout (`AeronWriterTransport:268-270`), `StorageGraphCoordinator` default drain 5 s duplicated from `NodeConfig`.
**Change.** Add to `NodeConfig`: `AeronConfig.retry` (record of the nine policy values), `Operations.retentionCloseTimeout`, `retentionCommandTimeout`,
`publicationLockTimeout`, `writerRecoveryAttempts`, `Timeouts.abortRecord` (F-11), `Timeouts.indexRefresh` (F-09), `Limits.applyQueueMaxBytes` setting; new `Setting`
entries with `PERUNCS_` keys; thread them into `AeronSettings.fromConfig`, `AeronArchiveRetention`, the backup backend and the merger; delete the duplicated defaults;
README table regenerates from `settingsMarkdown()` (existing parity test must pass). Accept `PT30S`/`30s` durations in addition to nanos for the new keys.
Pace `AeronRuntime.launchDriver` and `StorageBackupManager.awaitNextPoll` with `ReplicationRetry`/`IdleStrategy` instead of `Thread.sleep`. Add `AeronOfferRetryerTest`
cases for `NOT_CONNECTED`, `ADMIN_ACTION`, `CLOSED`, `MAX_POSITION_EXCEEDED`.

### F-24 — `StoreIndexReflection` mutates upstream private state through `Unsafe`
**Evidence.** `StoreIndexReflection.invalidateVectorGraph` (`:84-123`) reads/writes `builder`, `index`, `deferredBuilderOps`, `graphRebuilt` of `VectorIndex` with
`XMemory.getObject/setObject/set_byte` (AGENTS rules 14 and 31). Upstream guards them with `builderLock` and declares `builder`/`graphRebuilt` volatile
(`VectorIndex.java:925,1005,1040,1079` in the pinned checkout); the bridge bypasses both and clears a queue upstream appends to. A concrete race is **unproven** under
the application's graph-boundary contract; the layout dependency fails at the first replicated batch, not at startup.
**Change.** (1) Preferred: cherry-pick Eclipse Store PR #832 (`VectorIndex.invalidateGraph()`, per the PR it takes the builder lock) into the pinned
`5.0.0-SNAPSHOT` build (deployments already use one dated snapshot), delete `StoreIndexReflection` and its guard test, call the new API from
`ClusterIndexMaintenance.resetVectorSearchGraph`. (2) If the patch cannot be carried: use `MethodHandles.privateLookupIn(VectorIndex.Default.class, lookup())`
+ `VarHandle` (volatile access, no `Unsafe`), take the same `builderLock`, and fail **startup** (first writer/reader validation) when the field layout does not
match. Requires `opens`/`--add-opens` for the Store module — document it.
**Tests.** Startup self-check test (layout mismatch fails fast); `ReaderLiveIndexFreshnessTest` unchanged.

---

## 5. P3 findings

### F-25 — Stale vocabulary, formatting, FQN, names (documentation debt)
1. **Stale checkpoint/fence text** (replace by "Store mark" wording; delete sentences about a durable fence): `AeronReplicationPublisher.java:72,274,316,379,471,490,538,570,755,892`,
   `AeronStorageBinaryReplicationTarget.java:221-233` ("PREPARING fence", "recording REJECTED"), `EnvelopeFramer.java:135-137`, `AeronReplicationWriteCoordinator.java:45,245`,
   `TransactionAssembler.java:139-143,334-336` (poller is a *platform* thread), `ClusterIndexMaintenance.beforeApply` stray text "Agents/src:",
   `errors/package-info.java:6`, `AeronHealth.java:408-413` (F-03).
2. **Mis-indented Javadoc.** 577 `///` lines in 57 files start at 8 spaces. Safe fixer (run once, then compile and `javadoc -Xdoclint`): for every maximal run of consecutive `///` lines, find the
   next non-comment, non-blank line `L`; if `indent(L) == indent(run) - 4` and `L` is a member/type declaration line (starts with a modifier, type, or annotation), re-indent the whole run to `indent(L)`.
   Leave runs whose following line has the *same* indent (legitimate nested members). Expected effect: 577 → 0 mismatches; reviewers' counts (577/57) must be reproduced before the change. Also fix the brace
   layout at `AeronStorageBinaryReplicationTarget.java:244-255`.
3. **FQN:** `AeronWriterTransport.java:191` `java.util.function.BooleanSupplier` → import (disappears with F-15 anyway).
4. **Personal marker:** `NodeConfig.java:261` "ponytail:" → plain comment. **Comment/code mismatch:** `AtomicFileWriter.deleteRegularFile:423-425` says attributes are unused but `original.fileKey()` is read at `:443`.
5. **Stale constants/messages:** magic numbers `DGAR/DGWM/DGBI` and the `"unknown DataGrid envelope"` text (rename messages only; changing magics breaks wire compatibility — allowed by AGENTS but needs a version bump `AeronReplicationEnvelope.VERSION`).
6. **package-info rewrite:** first sentence verb-first and without "This package…" (rules 15/25): `node/store, node/backup, node/aeron, storage/aeron/{config,reader,wire,writer}`; add `@since 1.0` to `storage/aeron/mark` and `errors/internal` (or delete the latter, F-18).
7. README/`module-info` consistency: README requires `--add-modules jdk.incubator.vector` but `module-info.java` has no `requires jdk.incubator.vector` (state why: CLI-only for JVector); README says `--add-exports …=org.eclipse.serializer.base` vs `opens …mark to org.eclipse.serializer.persistence.binary`; README lists `PERUNCS_STORAGE_LIMIT_CHECKER_INTERVAL_MINUTES` required but `NodeConfig` default is `null` (the node throws at `NodeLifecycle.java:536` — fine for prod, document for standalone dev).

### F-26 — Writer state machine decomposition (optional, only after F-11/F-12/F-16/F-17)
`AeronReplicationPublisher` (1098 lines, 42 synchronized code sites), `AeronReplicationWriteCoordinator` (884) and the target (270) track one lifecycle in three places.
External review: merging into one `WriterSession` is not proven necessary. Safe incremental path, each step independently shippable: (1) move `PreparedTransaction` to its own
package-private top-level class; (2) delete production-unused overloads (`publishTransaction`, `prepareTransaction(byte[],ByteBuffer[])`, `forTests` ×2, `onPublication` test overloads) after moving
their callers to a test fixture; (3) replace the boolean flags `preparing/terminalOperation/closeRequested/closeInProgress/closed/failed` by one `enum PublisherState` with a
transition table under the existing monitor; (4) only then evaluate merging coordinator and publisher using defect history (F-11/F-12 show where the two disagree).

### F-27 — `NodeLifecycle` startup duplication
`startBackupNode` (`:264-373`, ~110 lines) and `startStorageNode` (`:404-558`, ~155 lines) share restore, missing-image reseed, `ignoreDistribution` bracketing, Store start, `requireStoredMark`,
`initializeRoot`, `validateStorageRoots`, dictionary queueing, the `"GcWorkaround"` task (30 vs 60 minute default intervals) and `maintenance.start()`; `startDevNode` is a third variant.
**Change (no framework).** Extract private methods `openStoreAndVerify(storageRootPath, allowCreateRoot, requireMark)` and `scheduleCommonMaintenance(maintenance, config, defaultGcMinutes)`;
rename the task `"StoreGcAndCacheCheck"`; document or unify the 30/60 default (`PERUNCS_GC_INTERVAL_MINUTES` already overrides). Keep close ordering untouched.

### F-28 — Duplicated cause-chain walkers
`GuardingStorageManager.isCleanRejection` (`:449-475`) and `isPendingCommit` (`:481-499`) both cap depth at 16 and run a redundant Floyd cycle check (the cap already terminates cycles) and tolerate
different `ReplicationException` subtypes. Extract `private record CauseScan(boolean rejected, boolean recordedAbort, boolean pending, boolean unrelated)` and one `static CauseScan scan(Throwable)`;
the two predicates read it. Move the five nested classes to package-private files. Existing rejection tests (wrapped, cyclic, deep, unrelated) must pass unchanged.

### F-29 — `TransactionAssembler` monitors
`onFragment` takes `synchronized(delivery)` then `synchronized(this)` (`:154,162`) plus `barrierLock`. `dispose()`, `failure()`, `hasIncompleteTransaction()` and the progress getters are called from other
threads, so the monitors are not all removable. **Change:** document the contract in the class Javadoc (poller thread owns `Transaction`/`Delivery`; other threads only touch `failure`, `resolvedBoundary`, volatile
progress and call `dispose()`), add a test-only thread-affinity assertion, and run the existing JMH/soak before removing `synchronized(delivery)`; do not remove `synchronized(this)`.

### F-30 — Test seams inside production classes
`AeronReplicationPublisher.forTests/onPublication` overloads (`:93-151`), `AeronStorageBinaryReplicationTarget.create` ×2 and three public constructors (`:64-134`) with a double-checked `synchronized` lazy
coordinator (`:94-103` → `LazyConstant`), `TargetCallbacks` with three nullable components, and two index-check mechanisms (`commitScan` vs `commitTouchesIndexes`+`writerIndexValidation`, `:184-194`).
**Change.** One package-private constructor taking non-null collaborators (use no-op lambdas, not `null`); move factories to `T/storage/aeron/writer/WriterFixtures`; keep only `commitScan`; replace the DCL by
`LazyConstant<AeronReplicationWriteCoordinator>`.

### F-31 — `ReplicationMark` has seven public mutable fields
Written from another package (`AeronReplicationWriteCoordinator.java:221-224`). Make fields private; add `reserve(long recordingId, long fencingToken, long sequence, long prepareStartPosition)`,
`bootstrap(…)`, accessors; keep the public no-arg constructor Serializer needs; the module `opens … to org.eclipse.serializer.persistence.binary` already grants field access.

### F-32 — Benchmark harness coupling
`T/node/aeron/AeronFullPathBenchmarkTest` (1 s warm-up + 2 s window, real multi-node Aeron) runs in the default surefire gate; the benchmark depends on `AeronStoreIntegrationIT` internals
(`store`, `latestSequence`, `ReaderNode.openForBenchmark` with a test-only receiver wrapper). Move the test behind `-Pbench` or rename to `*IT`; extract the shared node/reader fixtures to
`T/fixture`. Document in `AeronReaderTransport.currentSequence()` that it is benchmark-originated but also used on the production status path (`StorageNodeManager:184`, `BackupNodeManager:90` via the `ReplicationApplier.currentSequence` default, which allocates a `ReplicationPosition`). `NativeBufferPool` checked mode changes retention; benchmarks must run with
`peruncs.pool.checked` unset.

### F-33 — Smaller items (each independent)
- `AeronRetentionOwner.liveRetention()` (`:118-120`) casts the anonymous "unsupported" stub to `AeronArchiveRetention`; use `instanceof`. Latent today because writers do not call `AeronHealth` (F-08 will make it
  live) — fix in the same change as F-08.
- `BackupRestorePolicy.configuredIdentity` catches `RuntimeException` around `positionProvider.latest()` (`:67`); catch only `ReplicationPositionUnavailableException`.
- `AeronRuntime.append` (`:74-82`) keeps the first failure and suppresses later `Error`s; also `AeronWatermarkChannel.append`, `AeronTransport.appendFailure`: call `CloseSequencer.append` (Error priority). Test: ordinary failure then `Error` → the `Error` is surfaced.
- `AeronWriterTailRecovery.Scan.result` (`:256-258`) unreachable branch (`markedCommit != null` with `next.seen` already threw at `:246-248`); `Scan.read` polls with `parkNanos(100 µs)` — use `retryPolicy.idleStrategy()`.
- `AeronReplicationPublisher.PreparedTransaction.onAbort` reads `abortPosition` in a second `synchronized` block (`:1039-1053`); fold into one.
- `ApplyQueue.cachedTransactionLengths` is an `ArrayDeque<Integer>` (boxing per transaction); use an `int[]` ring. `AeronReplicationPublisher.java:466` allocates `new UnsafeBuffer(dictionary)` per dictionary transaction; `EnvelopeFramer` allocates two `CRC32C`, a `ChecksumContext`, an `UnsafeBuffer` and an `AtomicBoolean` per transaction — make it a per-publisher object reset per transaction.
- Pool accounting: `NativeBufferPool` accounts retained memory by `capacity()`; `ApplyQueue` admission by `remaining()`; a 33 MiB transaction occupies a 64 MiB power-of-two bucket (the cap is 64 MiB, so 65 MiB cannot occur). Account `capacity()` in `ApplyQueue`/limit docs.
- `ClusterIndexMaintenance.refreshMaps` closes every Lucene index on every batch (`retireLuceneView`, deliberate to avoid torn reads); restrict to maps whose reachable ids intersect the batch once F-05's relevance set exists, or document the cost.
- `StorageTaskExecutor.close` uses `cancel(true)` + `shutdownNow()` (interrupts Store GC/checks; Store may swallow interrupts) while `StorageBackupTaskExecutor` forbids interruption: use `cancel(false)`, `shutdown()`, bounded `awaitTermination`, retry on timeout.
  `NodeMaintenanceScheduler.close` uses `shutdownNow()` for workers: drain first, `shutdownNow` only after the bounded wait; an interrupted retention purge makes the writer fail closed (acceptable only during shutdown — comment it).
- Two symlink policies: `AtomicFileWriter.isSystemPrivateAlias` (macOS `/private` aliases for `/tmp,/var,/etc`) vs `AeronRuntime.isSystemTemporaryAlias` (`/tmp,/var` by string equality) and `AeronSettings.temporaryPath` (`/tmp` literal). One `PathSafety` helper in `storage.io` with one documented policy and a unit test for macOS and Linux cases; apply the component check to the lock/restore paths too (`WriterProcessLock`, `BackupRestorePolicy.isMissingOrEmpty` use leaf-only checks).
- Duplicates: UUID byte codec in `BackupArchive` vs `AeronPositionCodec`; `AeronReplicationEnvelope.crc32c` delegates vs `Crc32C`; three `noOp` objects (`ReplicationPublisher`, `ReplicationApplier`, `ClusterReplicationTransport`); close-aggregation helpers (`StorageNodeManager.CloseFailures`, `CloseSequencer.append`, `AeronTransport.appendFailure`) → use `CloseSequencer.append` everywhere; `NodeCollaborators.isMissingOrEmpty` vs `BackupRestorePolicy.isMissingOrEmpty`.
- Interfaces with one production implementation and a nested `Default`: `StorageBackupManager`, `StorageBackupTaskExecutor`, `StorageTaskExecutor` (D2 left them; if kept for test doubles say so in the Javadoc, otherwise fold into final classes). `ClusterStorageManagers` has six overloads that only call two constructors; `DistributedStorage` is a static bag whose `Configurator` can fold into `NodeLifecycle`. `AeronWriterRecoveryBoundary`, `CursorSnapshot` (subset of `ReplicationPosition`) are single-use records.
- `StorageBinaryDataMaterializer.ITERATOR` is a static `BinaryEntityRawDataIterator` shared by every merger in the JVM; confirm it is stateless in the pinned Serializer, otherwise make it an instance field.
- `StorageBinaryDataMerger.receiveData` (copy path), `StorageBinaryDataImporter.copyOwned`, `StorageBinaryBuffers.importArray/releaseDirect` are unreachable in production (`canReceiveDataOwned()` is always `true` for the only receiver). Delete them together with `canReceiveDataOwned()` once the owned path is the only contract (tests move to the owned API).
- `FaultInjection.invoke("AFTER_PREPARE", …)` string point names → `enum CrashPoint`.
- `NodeConfig.Builder` rebuilds the 9-argument record in every setter and `NodeConfig.java` is 724 lines: split `Setting` schema/markdown from parsing, give each sub-record a `with…` helper; add `Limits.applyQueueMaxBytes` as a `Setting`.
- Retention observability (`NodeLifecycle.java:544-554`): log `DEFERRED_ACTIVE_REPLAY` at WARN and `NOTHING_TO_DELETE` at DEBUG; expose the last `MaintenanceResult` in `ReplicationStatus`; count rejected watermarks (`WatermarkCollector.noteRejection` only logs powers of two).
- Starter-backup timeout (`NodeLifecycle.awaitStarterBackup`): `CompletableFuture.cancel` does not stop the backup (documented in `ClusterNode.createBackup`); the real guarantee is "upload kept". Keep, but log that the backup may still complete.

### F-34 — Test gaps
Main classes with **no** test reference by name (indirect coverage only): `AeronReaderTransport`, `AeronRetentionOwner`, `AeronRuntimeOwner`, `AeronTransportShared`, `AeronPositionCodec`, `ApplicationSections`, `ClusterNodeHandle`,
`ClusterStorageFoundation`, `EnvelopeFramer`, `NativeMemory`, `ReadOnlyStorageManager`, `StorageBinaryDataMaterializer`, `BackupNodeControl`, `StorageNodeControl`, `GraphBoundary` (interface). Classes with exactly one reference include
`AeronWriterTransport`, `AeronWriterTailRecovery`, `AeronHealth`, `AeronWatermarkChannel`, `ApplyQueue`, `BackupRestorePolicy`, `AeronSettings`. No test references `ClusterIndexMaintenance`/`ClusterIndexValidation` bound decisions directly
(only prefilter/scratch tests). The module-path probe runs in the unnamed module and cannot catch a missing `requires transitive`.
**Add (each tied to a finding):** F-01 close/lock tests; F-02 owner-level close test; F-03 recovery classification + idempotency; F-04 formula + real-Archive worst case; F-05 cycle matrix; F-06 gauge/gate failures; F-07 straddling-segment resume IT;
F-08 writer status (capacity/driver/maintenance); F-10 lazy backend; F-11 stuck-Archive bounds; F-12 accepted-write close; F-13 watchdog queue; F-17 gather matrix + differential; F-18 outcome table + architecture test; F-20 entry-limit and workspace;
F-21 forked crash; a named-module consumer implementing `DocumentPopulator` (`ModulePathRuntimeProbeTest` must run on the module path); reader-side index maintenance (retire-before-swap, vector rebuild trigger) and materializer corrupt-framing tests;
`AeronReplicationEnvelope` fuzz for the new encoder; per-timeout override tests (`AeronSettingsTest:205-221` only covers all-30 s and reconnect).

---

## 6. AGENTS.md rule status (gaps only)

| Rule | Gap → findings |
|------|----------------|
| 1 modern Java | Virtual thread for CPU-bound materializer (F-14); DCL instead of `LazyConstant` (F-30); `VarHandle` in `EntityHeaders` is fine (supported API) |
| 3 immutable/records | `ReplicationMark` (F-31); flag soup in publisher (F-26) |
| 5/6/7 DRY, layers, over-engineering | F-15, F-26, F-27, F-28, F-30, F-33 (dead copy path, duplicate helpers) |
| 10 resources/exceptions | F-02, F-03, F-18, F-33 (`AeronRuntime.append`); generic `IllegalStateException` for Archive timeouts in `AeronArchiveReplicationPublisher.awaitRecorded` (use `ReplicationUnavailableException`) |
| 11/12/13 visibility, packages, god classes | `errors` layering (F-18); `GuardingStorageManager` 1226, publisher 1098, `FilesystemVolumeBackupBackend` 969, `TransactionAssembler` 964, `NodeLifecycle` 915, coordinator 884, merger 850, `BackupArchive` 813, `NodeConfig` 724 (F-26/27/28/33); public types in unexported packages are not API leaks but undocumented intent |
| 14/22 reflection, unsynchronized foreign-state writes | F-24 (rule 31 concerns off-heap access and does not apply: `StoreIndexReflection` mutates on-heap fields) |
| 16 FQN | F-25.3 |
| 17/18 arity/builders | F-22; `AeronArchiveReader.Configuration` (16), `NodeConfig.Timeouts/AeronConfig` (12), `AeronHealth` (13 suppliers), `AeronArchiveReplicationPublisher.create/extend` (8-9) |
| 20 security | no gap found in read code; leaf-only symlink checks (F-33); spoofable watermarks are by design (no auth) but rejections are only logged (F-33) |
| 21 performance | F-09, F-16, F-17, F-33 (boxing, per-transaction objects, Lucene close per batch) |
| 22 threading | F-01, F-02, F-12, F-24 |
| 23 retries/network robustness | F-03, F-23, F-11 |
| 24 exceptions | F-03, F-18, F-33 |
| 25 Javadoc | F-25 |
| 26 tests | F-34 |
| 27 Aeron/Store best practice | partly checked (see §1); cookbook and Store/Serializer tests not reviewed |
| 28 prefer Agrona/Eclipse locks | 130 `synchronized` lines (publisher 42, assembler 15, watermark channel 15, reader 10); removable subset only via F-26/F-29 |
| 29 topology | compliant |
| 30 heap allocation | F-33 (boxing, per-transaction framer objects), `new UUID` in `EnvelopeView.clusterId()` |
| 32 Lucene/JVector | covered by IT/soak; F-05 and F-09 add the missing guards |
| 33/34 cluster correctness/performance | F-03, F-04, F-05, F-07, F-08, F-11, F-12 / F-09, F-13, F-14 |

## 7. Order of work and dependencies

1. **Independent P1 fixes (parallelisable, each with its tests):** F-01, F-02, F-06, F-05, F-08(+F-33 `liveRetention` cast), F-04.
2. **F-03** (recovery classification) — after F-04 so false window rejections do not mask it; then **F-07** (reproduce first; it changes the watermark/position contract, so land it before F-20's backup-position tests).
3. **F-10, F-11, F-12, F-13, F-14, F-18, F-21** (small, local).
4. **Deletions shrinking the writer path:** F-16 → F-17 → F-15 → F-19 → F-22 (F-15 depends on F-08 step 3 for the writer sequence).
5. **F-23** (configuration surface) once F-02/F-03/F-11 have introduced their new settings, so README and `settingsMarkdown()` change once.
6. **F-24 + F-09** (upstream PR), **F-20** (backup workspace), then F-25–F-33 cleanups; F-26 only if defects keep clustering there.
7. Re-run the D-25 comparison (OPUS step 4) after F-13/F-16/F-17/F-22 — they touch hot paths — and all profiles (`-Pintegration`, `-Pcrashmatrix`, `-Psoak`) before declaring any P1 closed; this review ran none of them.

---

## 8. Appendix — item-by-item mapping of the two full-codebase external reviews (N1–N24, R1–R34)

Added after a systematic re-read of both reviews. "In" = already covered by the cited finding. "New" = added here with a
change. "Rejected" = checked and not accepted. Items marked *(ext.)* are reviewer claims I did not re-verify line by line;
re-check the cited line before editing.

### 8.1 Reviewer qualifications of rev 1, still binding
- **Background vector indexing:** validation rejects it, so the earlier "supported-configuration race" must not be treated as an established finding; F-24 claims only a bypass of upstream's lock/volatile semantics.
- **Preliminary payload CRC:** it provides recovery evidence before local acceptance. F-16 therefore removes only the *pre-pass and equality check* and keeps the transaction CRC; any later "CRC-of-chunk-CRCs" protocol change must first show a replacement for that evidence (reader assembler + `AeronWriterTailRecovery` both consume it).
- **Visibility reductions** (N16/R11): before narrowing any `public` type, grep for callers in other packages; cross-package users (e.g. `AeronReplicationEnvelope` in three packages) need a bridge or must stay public with a "module-internal" Javadoc.

### 8.2 Mapping
| Ext. item | Disposition |
|---|---|
| N1, N2, N13 (errors outcome/layering/sealed leak) | In F-18 |
| N3, N7(writer), R10 recovery classification | In F-03 |
| N4, N8, N5, N6, N9, N11, N12, N14, N15, N17, N18, N19, N20–N23 | In F-04, F-11, F-12, F-13, F-23, F-33, F-22/F-33, F-26–F-28, F-33, F-34, F-14/F-25, F-25 |
| N7 | In F-24 |
| N10 watermark channel monitors | **New N-A** |
| N16, R11 public surface / misplaced classes | **New N-B** |
| N24 duplicate sleep helpers | In F-23 (pacing) + **N-C** |
| R1 virtual threads on blocking/CPU work; unbounded per-task executor | **New N-D** |
| R1 `Pattern.compile` static; `EntityHeaders` VarHandle | `Pattern`: P3 trivial, **N-D**. `VarHandle`: **Rejected** — a supported API, not reflection/Unsafe; leave |
| R2 facade/owner ping-pong, `NodeCollaborators` locator, `ReadOnlyStorageManager`/`DistributedStorage` wrappers | **New N-E** |
| R2 SPI/sequence slots, framer duplicate loops, startup duplication | In F-15, F-17, F-27 |
| R3 `Optional` in `ClusterIndexValidation:41,651` ("persists Optional") | **Rejected** — those lines *traverse* an `Optional` payload during graph scan (`case Optional<?> optional ->`); nothing persists it. AGENTS rule 3 limits `Optional` as a *parameter/return*, not graph-type handling |
| R3 single-use fold list | In F-33 (partial) + **N-E** |
| R10 generic `IllegalStateException` for Archive failures | **New N-F** (was only a rule-table remark) |
| R10 `AeronHealth` demotes reseed to FAILED | **New N-F**, minor: probe catch (`:124-127,143-146`) is intentional (a probe must not throw) but logs; keep the catch, make `state()` return `RESEED_REQUIRED` when the caught failure *is* a `ReseedRequiredException` |
| R10 `FilesystemVolumeBackupBackend:471` fallback `allocateDirect(1 MiB)` never freed | **New N-F**: use `ByteBuffer.allocate(1<<16)` heap scratch (restore path, not hot) or `Arena`-scoped buffer closed in the try |
| R10 `BackupRestorePolicy`, `liveRetention`, starter-backup cancel, executor interrupts, scheduler `shutdownNow` | In F-33 |
| R14/R31 | F-24; `Arena.ofShared()` per pool miss (`NativeMemory:46`) → **N-G** |
| R21 coordinator lock, CRC passes | In F-11, F-16 |
| R21 `ApplyWorker` six `synchronized(budgetLock)` for two longs | **New N-G** |
| R21 pool double lock / poison under lock; publisher coarse monitor; per-tx allocations; Lucene close per batch | **N-G** (pool/publisher), F-33 (allocations, Lucene) |
| R20 TOCTOU leaf-only symlink checks | In F-33; spoofed-watermark metric → F-33 (count rejections) |
| R23 maintenance gaps: `latest()` throw kills maintenance; retention monotonic throw in scheduler; threshold 3 hides retention failures; backup lock retries only on stamp change, 10 ms spin; `distributeTypeDictionary` stored on readers | **New N-H** |
| R23 unconfigurable constants | In F-23; add the extra items listed in **N-H** |
| R15/R25 README/module-info/package-info | In F-25.6–7; derived-default footnote and `NodeAssembly:796` → **N-I** |
| R26/R27/R32 missing tests | In F-34; extra cases listed in **N-J** |

### N-A — `AeronWatermarkChannel` monitor contention *(ext.; verify)*
Reader's applied-listener thread calls `publish()` (`:148-163`) while the worker and `close()` share one monitor and `close()` performs the final offer and closes endpoints inside it (`:345-394`). **Change:** keep one `AtomicReference<ReaderWatermark> latest` slot written by `publish()` (lock-free, last-writer-wins is correct because watermarks are monotonic), have only the worker thread offer, and do the final arbitration offer and `close()` of publication/subscription *outside* any monitor after the worker has stopped (`worker.shutdown(); awaitTermination`). **Test:** concurrent `publish()` ×1000 while `close()` runs; close completes within its timeout and the last published value is offered or the failure is reported.

### N-B — Public surface and misplaced classes *(ext.; verify callers first)*
Candidates to make package-private (after grep proves no other package uses them): `NativeMemory` (F-19 makes it internal anyway), `Crc32C`, `ReplicationRetry`, `FaultInjection` (needs a test bridge: keep public only if tests in other packages use it), `AtomicFileWriter`, `DistributedStorage`, `ClusterStorageManagers`. Relocations: `AeronPositionProvider` (implements a `node.replication` interface from `node.aeron`) → keep, document; `StorageGraphCoordinator` implements `api.GraphBoundary` from package `storage` → move beside `GuardingStorageManager` in `node.store` if no other package references it. Procedure per type: `grep -rn "<Type>" src/main src/test | grep -v "<own package>"`; if empty, drop `public`; if only tests, move the test into the package; else add the Javadoc line "Module-internal; not API." Do this last (F-25 era), as it churns many files.

### N-C — One park-based wait helper
`StorageBackupManager.awaitNextPoll` and `sleepRetentionRetryDelay` are duplicates using `Thread.sleep`; `FilesystemVolumeBackupBackend:911-932` spins with fixed `parkNanos`. **Change:** one `static void parkOrInterrupt(long nanos)` in `ReplicationRetry` (`LockSupport.parkNanos`, on interrupt restore flag and throw `InterruptedIoException`/domain exception already used there); replace the three; the backup lock spin uses `ReplicationRetry.fullJitterDelayNanos`.

### N-D — Executors and threads
- `StorageTaskExecutor:78` and `StorageBackupTaskExecutor:158` use virtual single-thread executors for blocking Store GC/checks and zip I/O (CPU-bound compression). Change both to `Thread.ofPlatform().daemon().name(…)` (same pattern as F-14); behaviour and shutdown unchanged.
- `NodeMaintenanceScheduler:31` `newVirtualThreadPerTaskExecutor()` is unbounded, fire-and-forget. Change to a fixed platform pool sized to the number of registered tasks (tasks are a small fixed set), or run each task under a `StructuredTaskScope` with the task's own timeout; keep the failure-count semantics.
- `AeronRuntime:41,44` static `Pattern`s: wrap in `LazyConstant` only if class-load cost matters; otherwise leave (compiled once, immutable). Low value.

### N-E — Structural simplifications *(ext.; each only after the P1/P2 work)*
`AeronTransport` ↔ `AeronTransportShared`/`AeronRuntimeOwner`/`AeronRetentionOwner`: fold `AeronTransportShared` state into the facade and keep the two owners (they own distinct lifecycles, see F-02). `NodeCollaborators` (521 lines, 20 lazy holders, 15 `ensure*`): split into `ReplicationCollaborators`, `BackupCollaborators`, `StorageCollaborators` records built by `NodeLifecycle` per role. `ReadOnlyStorageManager`/`DistributedStorage` wrappers: replace the subclass-as-flag by a `boolean readOnly` in `GuardingStorageManager` only if its rejection tests still pass unchanged. `ClusterStorageManagers` overloads: delete (F-33). `BackupArchive` (813 lines, static): split into `IdentityCodec`, `ArchiveExtractor`, `ArchiveWriter`; share one `UuidCodec` with `AeronPositionCodec` (F-33).

### N-F — Exception and resource details
Replace generic `IllegalStateException` for Archive/coordinator/close failures with `ReplicationUnavailableException` (or `ReplicationPositionUnavailableException` for position reads) at: `AeronArchiveReplicationPublisher:346,356,360,383,426,532,536,542-548`, `AeronWriterTransport:457,461,622,645`, `AeronTransportShared:73,81` *(ext.; line numbers from review)*, restoring the interrupt flag where `InterruptedException` is caught. Callers that currently `catch (IllegalStateException)` must be found with grep first; update their handlers and the F-03 classifier. Items for `AeronHealth` and the `allocateDirect(1 MiB)` fallback are in the mapping table.

### N-G — Native/lock micro-issues
- `NativeMemory.allocateScoped` creates `Arena.ofShared()` per pool miss (`:46`). Shared arenas cost a handshake on close; use `Arena.ofConfined()` only if the buffer is always closed by the allocating thread (pool release may run on another thread → then keep `ofShared`). **Decision:** keep `ofShared`, and instead make the pool retain/reuse (already bounded by `PERUNCS_POOL_MAX_RETAINED_BYTES`); no change unless profiling shows close cost.
- `ApplyWorker` six `synchronized(budgetLock)` blocks guard two longs (`:164,230,264,289,299,318`). Replace with `AtomicLong` deadline/budget fields (or `volatile` + single writer, since only the materializer thread writes and the watchdog reads). Verify the writer/reader of each field first.
- `NativeBufferPool`: do the allocation outside the lock (reserve accounting inside, `Arena` creation outside, roll back on failure) and poison released buffers outside the lock *(ext.)*.
- `AeronReplicationPublisher` coarse monitor across `totalRemaining` + fault injection + reserve: covered by F-26 step 3 (state enum); do not introduce a second lock.

### N-H — Maintenance and retry robustness *(ext.; verify)*
- `BackupRestorePolicy`/retention scheduler: a thrown `latest()` or a monotonic-violation exception inside a scheduled maintenance run must be logged and counted, not abort the task chain (`AeronArchiveRetention:434-445`). Preserve the fail-closed behaviour of `purgeSegmentsWhileWritesPaused`.
- `NodeMaintenanceScheduler:70-77`: failure threshold 3 delays reporting retention failures by 3 intervals. Make the threshold per task (`NodeConfig.Operations.maintenanceFailureThreshold` map or a 1 for transport/retention tasks) and log WARN with the quorum state on every failure.
- `FilesystemVolumeBackupBackend:523-534,911-932`: the publication-lock loop retries only on stamp change; retry transient `IOException` with jittered backoff up to the existing 30 s budget.
- `distributeTypeDictionary` on a reader stores text that is never consumed (`AeronDistributionGate:80-82` via `DistributedStorage:89-92`): becomes moot with F-15's `TypeDictionaryOutbox` — make the reader's outbox a no-op implementation.
- Extra unconfigurable constants to include in F-23: `BackupArchiveLimits` 1 TiB / 64 KiB, `AeronWatermarkChannel` 5 s (`:108,135`), `StorageBackupTaskExecutor:147` 60 s, `ClusterReplicationTransport:55-57` 1 s, merger `171-181` (10 s / 64 MB / 1 GB / 30 s / 5 s), `MergerLifecycle:19` retries and `ApplyWorker:50` multiplier, reader `AeronReplicationConfiguration:61,63,71` (256 / 64 / 2 ms), `NodeLifecycle:445-451` `ofMinutes(1)`.

### N-I — Documentation
README: footnote that `PERUNCS_AERON_DIRECTORY` and `…ARCHIVE_DIRECTORY` print `unset` in the generated table although derived defaults exist (or make `settingsMarkdown()` print the derivation; `NodeConfigSettingsTest:140-147` checks containment only). `NodeAssembly:796` widened a private member to package scope for `NodeLifecycle`; expose it through a method on the assembly interface instead *(ext.)*. `package-info` first sentences must be verb-first (list in F-25.6) including `peruncs/package-info`, `cluster/package-info` (leading blank line), `api/package-info`, `NodeConfig:10`, `FaultInjection:6`.

### N-J — Extra tests requested by the reviews
10k-batch FIFO/backpressure test for `ApplyQueue`; starter-layout overlap / absolute-path / tmpfs rejection test; `AeronOfferRetryer` branches `NOT_CONNECTED`, `ADMIN_ACTION`, `CLOSED`; assembler corrupt-data test with `maxTx` at the production default and a tail of `2*maxTx` (all current assembler tests pin `maxTx=1024`); never-recording Archive → single timeout and non-blocking `dispose`; `>10k`-vector rebuild time + recall bound; observable blackout (now only the WARN in `ApplyWorker.noteBlockedTime`; the unread `lastApplyBlockedMillis` field was removed).

### Still outstanding
Rule 27 (Aeron cookbook; Eclipse Store/Serializer tests) is not done; both the reviewers and I only checked `ExclusivePublication.offer(DirectBufferVector[])` copy semantics.


## External review round 2: disposition

**Fixed:** writer-recovery classification (unexpected failures latch `FAILED`, never "reseed"; unstopped recording is
transient; explicit clean-close flag; consecutive-transient budget `PERUNCS_WRITER_RECOVERY_ATTEMPTS`, default 3; blanket
reseed catch in the tail scan removed); reader `position()` reports the restart boundary (the Store mark's sequence and prepare-start position) captured as one immutable pair at the end of each apply batch inside the graph write section, `-1/-1` only while the Store holds no mark, and it lags the applied cursor by the batching window (conservative for retention);
`StoreIndexReflection` no-ops when no graph was ever built; backup copy fallback uses a heap buffer; `NodeException.outcome()`
names every leaf; dead `ApplyWorker.lastApplyBlockedMillis` removed.

**Rejected:** `failClosed()` on `beginCommit` ownership/state violations: a foreign token is a caller error, and
`AeronReplicationWriteCoordinatorTest.foreignTokenCannotStealActiveWrite` pins that it must not fail the publisher.

**Deferred with reasons:** abstract `StorageBinaryDataReceiver` allocation (20 test implementers; production has one,
the merger, which overrides both methods; the default is GC-reclaimed and safe); removing `TargetCallbacks.committedSequence`
(the production wiring is a no-op but the coordinator test asserts "not live until COMMIT is offered" through it);
`WriterSession`/`NodeStartup` merges, assembler monitors, per-transaction `EnvelopeFramer` allocation (no measured cost;
behaviour-neutral refactors with high churn); `INDEX_REFRESH_BUDGET_MS` stays 600 s because warm-up now runs outside
the write section.

**Measured:** `StorageBinaryPoolOwnershipIT` (312 s) is fsync-bound, not CPU-bound: JFR/jcmd show ~3.4 s of JVM CPU in 173 s,
`FileForce` on both Stores per transaction (~12 ms each on macOS), no GC pressure, no lock contention. Default
transaction count is now 1,000 (`-Dpool.transactions=10000` restores the full run).

**Still open (accepted as legitimate, not yet implemented):**
- `ArchitectureTest`: full package-dependency matrix plus FQN and vocabulary guards (its own `new java.io.UncheckedIOException` FQN included);
- `EnvelopeFramerTest`: rename "staged" wording, add a `length changed` mid-gather case and 1/3/8-source shapes;
- `NodeConfig.RetryPacing`: expose `jitterBase` (and catalog-probe delays) instead of hard-wiring the defaults in `AeronSettings.retryPolicy`;
- a test for the writer-recovery retry budget through `ensureWriterLocked` (needs a real Archive; candidate for `AeronStoreIntegrationIT`);
- `peruncs/cluster/package-info.java`, `api/package-info.java`, `NodeConfig.java:10`, `FaultInjection.java:6`: verb-first first sentences;
- F-24 wording: the startup failure applies to reader roles only;
- `BackupArchive.listEntries()` still materialises a `List<ZipEntry>` for very large entry counts;
- verification: crash matrix and soak not re-run after the round-2 edits (unit gate 825 and the earlier 27 integration tests are green).


## External review round 3: disposition

**Fixed:**
- *Torn reader boundary (highest):* the watermark and `position()` no longer read the live, in-place-written `ReplicationMark`.
  `ReplicationCollaborators.updateGraph` runs each apply batch in the graph write section and calls
  `ClusterReplicationTransport.batchApplied()` at its end; `AeronReaderTransport` stores one immutable `CursorSnapshot`
  in a volatile (`AeronReaderBoundaryTest`). The backup log no longer prints the lagging position next to the live one.
- *One classifier:* `writerRecoveryFailure` deleted; every recovery failure goes through `classifyArchiveFailure`.
  `IllegalArgumentException` and unrelated `IllegalStateException` are now defects (`FAILED`); only the "recording is still
  active" state is transient; the one proven incompatibility (extend) wraps `IllegalArgumentException` into reseed at its site.
- *Testable budget:* `AeronWriterTransport.recoveryOutcome(...)` is a pure decision (retry / latch FAILED / latch RESEED) with
  boundary tests; the counter increments independent of the short-circuit; every latch and retry is logged once through a static logger.
- `StoreIndexReflection`: the lock-null no-op now also requires empty deferred work and no rebuild; Javadoc corrected
  (a loaded index creates `builderLock` lazily, so null genuinely means "never built"); `Default`-only layout check documented.
  A unit test for the null-lock branch is not possible without instantiating `VectorIndex.Default` (package-private constructor).
- `AERON_RETRY_JITTER_BASE_NANOS` added (validated against the cap, README row); `GuardingStorageManager.CauseScan` classifies
  the chain once; `NodeException.outcome()` redundant case removed and `ReplicationException` asserted; `ArchitectureTest`
  gained storage-below-node, api-facade and a production FQN guard (and its own FQN fixed); `EnvelopeFramerTest` gained 1/3/8-source
  shapes and the mid-gather length-changed failure, "staged" renamed "flattened"; README documents the recovery budget, the
  second-writer hint and the full pool sweep; Javadoc on the receiver defaults, `PreparedTransaction.close()` and the merger watchdog.

**Still open:**
- retry-budget test through `ensureWriterLocked` and an end-to-end two-transaction tail recovery against a real Archive (needs the Archive IT harness);
- `TargetCallbacks.committedSequence` removal (re-express the coordinator test through its own state first);
- abstract receiver allocation plus `DirectBufferReceiver` fixture; `AeronRetryPolicy` consuming `RetryPacing` and catalog-probe settings;
- `NodeLifecycle` role-start step extraction (F-27); list-materialising `BackupArchive` restore; periodic full 10k pool sweep in CI;
- crash matrix and soak re-run after these edits; new files are untracked (do not commit the index as staged).
