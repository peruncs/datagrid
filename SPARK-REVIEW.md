# SPARK-REVIEW — peruncs/datagrid static code review (AGENTS.md §§1–34)

Scope: `src/main/java` (137 files), `src/test` (173 files), `pom.xml`, `src/main/java/module-info.java`, `README.md`.
Graph: `Users-hristo-projects-github-peruncs-datagrid` @ `2026-09-25T02:27:30Z`, 6588 nodes / 37077 edges; one `parse_partial` (`storage/index/ClusterIndexValidation.java:429`) re-checked by source read.
Mode: review-only — no builds, no tests executed, no code modified.

All items below are gaps with file:line and a concrete replacement. No correctly-implemented areas are listed per AGENTS.md review policy.

---

## 1. Modern Java (virtual threads, pattern matching, ScopedValue, StructuredTaskScope, LazyConstant, ThreadLocal)

**1.1 No `StructuredTaskScope` anywhere; ad-hoc executors + `synchronized` instead.**
`rg StructuredTaskScope src/main/java` returns zero hits while the codebase fans out writer/query/audit/chaos workers (`storage/aeron/writer/AeronReplicationWriteCoordinator.java`, `node/aeron/AeronArchiveRetention.java:130`, `node/NodeMaintenanceScheduler.java:31`, `node/store/StorageTaskExecutor.java:64`).
Recommendation: introduce one `StructuredTaskScope.ShutdownOnFailure` (JDK 27 preview) per composed operation (soak worker set, backup pause-snapshot-resume, reader replay+live join). Replace `Executors.newVirtualThreadPerTaskExecutor()` in `NodeMaintenanceScheduler.java:31` and `Thread.ofVirtual().name(...).unstarted(task)` in `AeronArchiveRetention.java:130` with a scoped scope owned by the lifecycle so failure propagation and cancellation are structured instead of scattered `AtomicBoolean running` flags (`NodeMaintenanceScheduler.java:197`).

**1.2 `synchronized` on virtual-thread paths — pinning risk, violates own rule 27.**
~100 `synchronized` sites: `node/aeron/AeronWatermarkChannel.java:148,166,216,227,254,292,306,311,323,334,340,345,397,426`, `node/aeron/WriterFencingLease.java:175,194,457,465,499-500,515,544,549,591-592,673,692-693,727`, `node/aeron/AeronWriterTransport.java:151,196,239,330,567,846`, `node/NodeLifecycle.java:85,103,124,146,591,743`, `storage/aeron/reader/TransactionAssembler.java:236,263`, `storage/binary/ApplyWorker.java:156,220,244,282,292,311`.
`StorageTaskExecutor.java:64` and `StorageBinaryDataMerger.java:175` create `Executors.newSingleThreadExecutor(Thread.ofVirtual()...)` and then immediately `synchronized` on `budgetLock`/`dictionaryParseLock` (`ApplyWorker.java:156ff`, `StorageBinaryDataMerger.java:503`). On JDK 27 virtual threads, `synchronized` blocks that park (fsync, Archive await, `LockSupport.parkNanos`) pin the carrier.
Recommendation: replace every monitor that guards only state (not foreign objects) with `ReentrantLock` or `org.eclipse.serializer.concurrency.LockedExecutor` already used in `node/replication/DurableCursorFile.java:51` and `storage/binary/StorageBinaryDataMerger.java:186`. Keep `synchronized` only where Aeron demands the publication monitor (`AeronArchiveReplicationPublisher.java:110,203,231` — document why). Add a `SoakJfrReport`-gated pinning assertion (JFR `jdk.VirtualThreadPinned` event) to fail the soak when pinning appears.

**1.3 `ThreadLocalRandom` used while `ThreadLocal` is banned.**
`storage/ReplicationRetry.java:3,111` imports `java.util.concurrent.ThreadLocalRandom` and calls `ThreadLocalRandom.current().nextLong(bound)` for full-jitter offer spacing.
Recommendation: replace with `java.util.concurrent.ThreadLocalRandom` → `java.util.SplittableRandom` instance per `AeronRetryPolicy`/`ReplicationRetry`, or `RandomGeneratorFactory.of("L64X128MixRandom")`. This removes the implicit thread-local and makes jitter seedable for the deterministic soak (`soak.seed`).

**1.4 `LazyConstant` (third preview, JEP 531) is load-bearing without fallback.**
`node/store/GuardingStorageManager.java:44`, `node/backup/BackupArchive.java:38`, `node/NodeAssembly.java:251-260`, `storage/aeron/writer/EnvelopeFramer.java:26`.
Recommendation: isolate behind a tiny `Lazy<T>` seam (e.g. `peruncs.cluster.storage.LazyHolder`) with a `Supplier`-memoizing fallback so a `--enable-preview` removal or JEP rename touches one file, not four packages. Document in `module-info.java` that the module requires preview solely for this type.

## 2. OOP design / naming

**2.1 `NodeAssembly` is a misleading name for a lifecycle factory.**
`node/NodeAssembly.java:47-56` is `public interface NodeAssembly extends AutoCloseable` with a single static `create()` returning a mutable `Builder`, while `node/NodeLifecycle.java:46` is the real implementation (`final class NodeLifecycle implements NodeAssembly`). Callers read `NodeAssembly.create()` and expect assembly, not a running node.
Recommendation: rename `NodeAssembly` → `ClusterNodeFactory` (or fold into `api/ClusterNode.java:49` `open()`), rename inner `Builder` → `NodeConfiguration`, and make `NodeLifecycle` the package-private implementation. Update `module-info.java:3-10` narrative accordingly.

**2.2 Builder naming is inconsistent (`setX` vs `withX`).**
`node/NodeAssembly.java:69,84,93` uses `setRootSupplier/setEmbeddedStorageFoundation/setNodeSettingsSource`, while `api/NodeOptions.java:51,59` uses `withEmbeddedStorageFoundation/withNodeSettingsSource` and `storage/aeron/config/AeronReplicationConfiguration.java:189ff` uses bare `termLength()/mtuLength()/...`.
Recommendation: standardize on `withX` for immutable-copy methods (`NodeOptions`) and bare nouns for mutable builders (`AeronReplicationConfiguration.Builder`, `NodeAssembly.Builder`). Rename the three `set*` methods to bare nouns.

**2.3 `AeronTransportShared` leaks lifecycle booleans without a state machine.**
`node/aeron/AeronTransportShared.java:24-27,81-95` exposes `closed()/closing()/closing(boolean)/closed(boolean)` as four independent methods; `node/aeron/AeronTransport.java:340-367` synchronizes on `shared` and branches manually.
Recommendation: replace with `enum TransportState { OPEN, CLOSING, CLOSED }` + `compareAndSetState()` and `ensureOpen()`. Delete the four boolean accessors.

## 3. Records / immutability / functional style / Optional

**3.1 Large mutable coordinators should be records or split, not 800–1100-line classes.**
`storage/aeron/reader/TransactionAssembler.java` (1092 lines), `storage/aeron/reader/AeronArchiveReader.java` (1066), `storage/aeron/writer/AeronReplicationPublisher.java` (1004), `node/aeron/AeronWriterTransport.java` (852). Each mixes framing, durability gating, barrier batching, and Store import.
Recommendation: extract `DeliveryBarrier` (barrier staging/flush, `TransactionAssembler.java:124,637,652`), `DurabilityGate` (already an interface — move live-marker withholding `TransactionAssembler.java:228-260` into it), and `FragmentDecoder` (envelope decode + validation). Leave `TransactionAssembler` as a thin `record AssemblerState(...)` + coordinator under 300 lines.

**3.2 `Optional` appears only as a pattern-match subject, not as API — but validation walks `Optional` fields reflectively.**
`storage/index/ClusterIndexValidation.java:405` does `case Optional<?> optional ->` inside a reflective graph walk.
Recommendation: unwrap `Optional` at the domain boundary (reject `Optional` entity fields at registration in `ClusterStoreIndexes`) instead of supporting it in the validator. Document that persisted entities must not contain `Optional` (heap + serializer overhead).

## 4. Functional gaps / architectural design

**4.1 No reader→writer promotion path forces full restart + reseed for failover.**
`module-info.java:52-72` and `node/NodeRole.java` fix roles at configuration; `node/aeron/AeronReaderTransport.java:92` and `AeronWriterTransport.java` share no promotion seam. `README.md:238-250` admits “manual promotion and automated failover remain deployment responsibilities.”
Recommendation: add an explicit `StorageNodeControl.promoteToWriter(WriterPromotionPlan)` that reuses the existing fencing-lease steal path (`WriterFencingLease.java:199,263`) + checkpoint reconcile (`AeronWriterTransport.java:614-649`) instead of requiring operators to copy Store dirs by hand. Until then, ship a `promote.sh` runbook script that performs freeze-copy-replace-cursor-restart exactly as the soak reseed does (`README.md:372-378`).

**4.2 Watermark retention is fire-and-forget with no delivery acknowledgement.**
`node/aeron/AeronWatermarkChannel.java:148-227` publishes watermarks unconditionally; `README.md:193-196` states “an unreceived value is retained for retry until the writer subscribes (or discarded at close if none ever does).”
Recommendation: add a bounded watermark ACK (reuse the existing watermark stream with a `Kind.WATERMARK_ACK`, or expose `RetentionStatus.pendingWatermarks()` on `api/ReplicationStatus.java`) so operators can observe pre-purge quorum completeness instead of discovering a missing watermark only via preserved history.

**4.3 `NodeOptions.embeddedStorageFoundation` silently replaces the file provider.**
`api/NodeOptions.java:13-20,51-54` documents that “the node always derives the live file provider from the configured storage path,” discarding a caller-supplied provider without error.
Recommendation: fail fast if the supplied foundation carries a non-default file provider (`Objects.requireNonNull` + explicit `IllegalArgumentException`), rather than silently replacing it. Document the exact preserved vs replaced foundation parts in the `withEmbeddedStorageFoundation` Javadoc.

## 5. Simplification / DRY

**5.1 Five copies of the ScopedValue test-hook pattern.**
`storage/io/AtomicFileWriter.java:43-69`, `storage/aeron/checkpoint/AeronReplicationCheckpointStore.java:41-47`, `node/backup/FilesystemVolumeBackupBackend.java:38-48`, `storage/aeron/writer/CrashHook.java:20-61`, `storage/aeron/reader/TransactionAssembler.java:472-491`. Each declares its own `ScopedValue`, `runWithHook/callWithHook/inheritCurrent`.
Recommendation: extract one `peruncs.cluster.storage.TestHooks<T>` final class (`static <T> ScopedValue<T> newHook()`, `run/call/inherit`) and replace all five copies with `TestHooks`. Delete ~80 duplicated lines. `CrashHook` becomes `TestHooks<BiConsumer<String,Long>> HOLDER`.

**5.2 `AeronSettings.java` (826 lines) mixes parsing, validation, and secrets.**
One package-private `record AeronSettings(...)` plus six nested records (`Topology, ArchivePolicy, Timeouts, Channels, StorageIdentity, Directories`) plus ~600 lines of `fromEnvironment/value/parseLong/loopbackEndpoint/wireNonce/authenticatorSupplier` (`AeronSettings.java:239-260,361-411,462-487,658-798`).
Recommendation: split into `AeronTopologySettings` (identity/channels/dirs), `AeronArchivePolicySettings` (recording/retention/capacity), `AeronTimeoutSettings` (all nanos/millis), each with its own `fromProperties()` validator. Leave `AeronSettings` as a 4-field composition record with no parsing logic.

**5.3 `AtomicFileWriter.write` overload chain is confusing.**
`storage/io/AtomicFileWriter.java:89-111` (`write(path,encoder,phase)`), `:162` (`write(path,encoder)`), `:171` (`writeBytes`), plus private `write(path,encoder,before,during,afterTemp,afterRename)` with four nullable `String` hook names.
Recommendation: collapse to `write(path, encoder, Phase)` + `writeBytes(path, bytes)` only; derive hook names inside the private method from `Phase` instead of passing four strings. Delete the 6-arg overload from the visible surface (make it private with a `Phase` param).

## 6. Overengineering

**6.1 `NodeCollaborators.LazyHolder` indirection is unnecessary.**
Graph hotspot `node/NodeCollaborators.LazyHolder.get` has fan-in 335 (highest in the project). `NodeAssembly.java:209,256-260` wraps every collaborator in `LazyConstant` because “the wiring graph is circular.”
Recommendation: break the cycle by constructor injection (pass `Supplier<StorageConnection>` instead of the live connection) and delete the lazy-holder layer. Circular wiring is a design smell; lazy caching hides startup-order bugs that currently surface only as `NodeException("cluster foundation did not produce a storage manager")` (`NodeLifecycle.java:96-98`).

**6.2 Crash-matrix hook names leak into production signatures.**
`storage/io/AtomicFileWriter.java:77-106`, `storage/aeron/writer/CrashHook.java:29-71`, `AeronWriterTransport.java:47,789` thread `Phase`/`CHECKPOINT_SEQUENCE`/`CrashHook` through production code paths to name test milestones.
Recommendation: move all hook-name formatting behind the `TestHooks.isBound()` guard (as `AtomicFileWriter.java:90-95` already does for one path) and pass opaque `Object context` instead of phase strings, so production call sites never allocate hook strings (`"BEFORE_%s_TEMP_WRITE".formatted(...)` in `AtomicFileWriter.java:98-105`).

## 7. Better approach (how I would implement it)

**7.1 Replace the hand-rolled envelope framing args with a `FrameParameters` record.**
`storage/aeron/wire/AeronReplicationEnvelope.java:101-119,150-169,203-218` repeats an 18-parameter list (`target, targetOffset, clusterId, epoch, fencingToken, wireNonce, sequence, kind, payloadLength, chunkIndex, chunkCount, chunkOffset, commitCrc32c, payload, payloadOffset, chunkLength, checksumContext`) across `encode/encodeWithPayloadCrc/validate`.
Recommendation: introduce `record FrameHeader(UUID clusterId, long epoch, long fencingToken, long wireNonce, long sequence, Kind kind, int payloadLength, int chunkIndex, int chunkCount, int chunkOffset, int commitCrc32c)` and change signatures to `encode(MutableDirectBuffer target, int offset, FrameHeader header, DirectBuffer payload, int payloadOffset, int chunkLength, ChecksumContext ctx)`. This eliminates the 18-arg hot-path method (rule 16), prevents transposed `chunkIndex/chunkCount/chunkOffset` bugs, and can be stack-allocated (no GC pressure — scalar-replaceable).

**7.2 Replace `WriterFencingLease` file polling with Aeron cluster consensus or at least `WatchService`.**
`node/aeron/WriterFencingLease.java:175,311,499-592` polls a shared NFS file with `LockSupport.parkNanos(LOCK_RETRY_PARK_NANOS)` and dual `synchronized(mutexFor(path))` + `synchronized(stateLock)` nesting.
Recommendation: short-term, use `java.nio.file.WatchService` on the lease directory + `FileLock` with lease-time backoff instead of fixed park; long-term, replace NFS fencing with an Aeron-cluster election (the project already depends on `aeron-cluster` transitively via Archive). Document the NFSv4-only limitation (`AeronTransport.java:147`) as a deprecated path.

## 8. Single-use methods / weak entities

**8.1 `AeronTransportShared.claimStream/clearStreamClaim/claimedStream` triple is single-use ceremony.**
`node/aeron/AeronTransportShared.java:54-79` — `claimStream` is called once per transport, `clearStreamClaim` once on teardown, `claimedStream` only in an error message.
Recommendation: inline into `ensureOpen()` + constructor (`distributorStream` becomes `final String`), deleting 25 lines.

**8.2 `CrashHook` (1 record + 4 static methods) and `WriterLeaseGate` (1 interface + 2 anonymous impls) should fold into their owners.**
`storage/aeron/writer/CrashHook.java:19-71` is only called from `AeronReplicationWriteCoordinator`; `storage/aeron/writer/WriterLeaseGate.java:22-89` is only implemented by `AeronArchiveReplicationPublisher`.
Recommendation: nest `CrashHook` inside `AeronReplicationWriteCoordinator` as a private static final class, and `WriterLeaseGate` inside `AeronReplicationPublisher` as a nested interface. Delete two top-level files.

## 9. AutoCloseable / exception handling / swallowed exceptions

**9.1 `AeronWriterTransport` / `AeronReaderTransport` are not `AutoCloseable`.**
`node/aeron/AeronWriterTransport.java:45` (`final class AeronWriterTransport {`) and `node/aeron/AeronReaderTransport.java:41` expose `close()`-like teardown via owners (`AeronRuntimeOwner`, `AeronRetentionOwner`, `AeronTransport.java:340-367`) but cannot be used in try-with-resources. `AeronTransportShared.java:15` similarly holds `WatermarkCollector` + `AeronArchiveCapacity` without a close contract.
Recommendation: make all three `implements AutoCloseable` and register them in `NodeLifecycle`’s `CloseSequencer` (`node/CloseSequencer.java:143-154`) so a missed owner close is caught by the sequencer instead of leaking a MediaDriver/Archive.

**9.2 Protocol violations throw `IllegalStateException` instead of typed failures.**
`storage/aeron/reader/TransactionAssembler.java:244,360,372,388,391,412,438` throws `IllegalStateException/IllegalArgumentException` for sequence regression, gap, duplicate terminal mismatch, live-marker-without-header. These cross the replication boundary as untyped runtime failures instead of `CorruptReplicationDataException`/`ReseedRequiredException` (which `api/ClusterStorageManager` documents and operators map to HTTP codes per `README.md:290-296`).
Recommendation: replace all seven with `CorruptReplicationDataException` (torn/gap/mismatch) or `ReseedRequiredException` (regression past durable cursor), preserving the formatted message. Add a test that asserts the typed mapping.

**9.3 `getMessage().contains(...)` string-matching on Aeron failures is fragile.**
`node/aeron/AeronArchiveFailures.java:16-17,39-40,46`, `node/aeron/AeronRuntime.java:220,273` match `ArchiveException.getMessage()` against `CONTROL_RESPONSE_DISCONNECTED`, `REPLAY_IN_PROGRESS_DETACH`, mark-file regexes.
Recommendation: switch on `ArchiveException.errorCode()` / `ControlResponseCode` enum values instead of message substrings. Pin the Aeron version (`pom.xml:38` `1.53.1`) with a test that fails when upstream message text changes.

## 10. Visibility surface

**10.1 `public` on non-exported packages is used as a test escape hatch.**
`storage/aeron/writer/CrashHook.java:19`, `storage/aeron/writer/WriterLeaseGate.java:22`, `storage/aeron/writer/AeronReplicationWriteCoordinator.java:41`, `storage/aeron/writer/AeronStorageBinaryReplicationTarget.java:23`, `storage/aeron/writer/AeronArchiveReplicationPublisher.java:30`, `storage/io/AtomicFileWriter.java:31,89,162,171,181` are all `public` but the JPMS module exports only `peruncs.cluster.api` + `peruncs.cluster.errors` (`module-info.java:201-202`). Production encapsulation relies on JPMS while tests run on the class path (`pom.xml:253-255` `useModulePath=false`), so `public` is load-bearing for tests only.
Recommendation: demote all internal types to package-private and grant tests access via `src/test/java/module-info.test` `--add-opens` or JPMS `opens ... to` test harness instead of widening production visibility. Document the rule: “no `public` outside `api`/`errors`.”

**10.2 `AtomicFileWriter` exposes 12 public static helpers that should be internal.**
`storage/io/AtomicFileWriter.java:89,162,171,181,221,243,266,290,311,329,337,376,400,428,456,484` — `verify/delete/forceDirectory/ensureNoSymbolicLinks/isSystemPrivateAlias/isMacOs/installStorage/cleanup/moveFileAtomically/deleteRegularFile/openRegularFile/deleteDirectory`.
Recommendation: keep `write/writeBytes/delete` public (used by checkpoint/cursor/backup stores); demote the rest to package-private. `isMacOs/isSystemPrivateAlias` belong in a private `Platform` nested class.

## 11. Package structure

**11.1 Missing `package-info.java` for `peruncs`.**
`src/main/java/peruncs/package-info.java` does not exist (verified: `MISSING src/main/java/peruncs/package-info.java`; `src/main/java/peruncs/cluster/package-info.java` exists). Every other package has one (18 total).
Recommendation: add `peruncs/package-info.java` with the one-sentence product narrative (“Peruncs Cluster — single-writer in-memory data grid over Aeron and Eclipse Store”) so Javadoc `doclint:all` (`pom.xml:363-364`) covers the root.

**11.2 Cross-package import of `storage.binary` from `node.store` bypasses the storage facade.**
`node/store/DistributedStorage.java:11-12`, `node/store/GuardingStorageManager.java:15`, `node/store/RejectingPersistenceTarget.java:6` import `storage.binary.*` directly, while `module-info.java:14-18` describes `storage.binary` as transport-internal.
Recommendation: route all `node.store` → `storage.binary` access through `storage.binary.ReplicationPublisher/ReplicationApplier` interfaces only; move `DistributingTypeDictionaryExporter` and `StorageBinaryDataImporter` references behind `storage.StorageGraphCoordinator`. Forbid `node.*` imports of `storage.binary.*Impl` via `maven-dependency-plugin analyze-only` + `bannedDependencies` rule.

## 12. God objects / interfaces / records

**12.1 `AeronSettings` (826 lines), `NodeAssembly` (797), `NodeLifecycle` (770) are god types.**
Each spans parsing + validation + lifecycle + close sequencing. Hotspot data confirms: `NodeAssembly.Builder.build` fan-in 96, `NodeRole.resolve/of` fan-in 242/185.
Recommendation: apply the split in §5.2 + §7.1; cap each file at ~300 lines. Extract `NodeStarter` (role validation + Store/Aeron start order, `NodeLifecycle.java:85-146`), `NodeCloser` (close sequencer wiring, `NodeLifecycle.java:564-743`), and `SettingsParser` (env → records) as separate package-private types with focused unit tests.

**12.2 `TransactionAssembler` holds five monitors/locks.**
`storage/aeron/reader/TransactionAssembler.java:89,124,236,263,637,652` — `delivery` monitor, `this` monitor, `barrierLock`, plus `envelopeView` reuse and `chunkObserver` capture.
Recommendation: consolidate to one `ReentrantLock` + `Condition` for barrier flush, and make `envelopeView` a method-local (or `ThreadLocal`-free pooled view passed via `ChecksumContext`-style explicit context, mirroring `AeronReplicationEnvelope.ChecksumContext`).

## 13. Reflection

**13.1 `StoreIndexReflection` reflects on upstream Store internals by field name.**
`storage/index/StoreIndexReflection.java:36-186` resolves `builder/index/graphRebuilt/deferredBuilderOps` (JVector), `LuceneContext`, `indexGroups` via `getDeclaredFields()` + name+type matching, then reads/writes via `XMemory.objectFieldOffset` (`:210-238`). `storage/index/ClusterIndexMaintenance.java:19` and `ClusterIndexValidation.java:18-19,517` import `java.lang.reflect.Field/Modifier` directly.
Recommendation: replace name-based reflection with upstream-supported seams: request `LuceneIndex.close()`-style public handles for JVector graph reset (already used for Lucene per `:88-93` comment) and a `GigaMap.indexGroups()` accessor from Eclipse Store instead of `indexGroupsField`. Until upstream ships those, pin the exact Store snapshot (`pom.xml:36-37` `5.0.0-SNAPSHOT`) with a `StoreIndexReflectionTest` contract test that fails on any field rename (exists — `StoreIndexReflectionTest.java` — but must run in the default `mvn verify` gate, not only `-Pintegration`).

**13.2 Fully-qualified `java.util.ArrayList` hides a missing import.**
`storage/index/StoreIndexReflection.java:137` uses `new java.util.ArrayList<>()` despite no conflicting `ArrayList` in scope.
Recommendation: import `java.util.ArrayList` normally.

## 14–15. Javadoc / FQN

**14.1 `peruncs/package-info.java` missing (see §11.1); `AtomicFileWriter.write(path,encoder,before,during,afterTemp,afterRename)` has no Javadoc.**
The 6-arg private overload `storage/io/AtomicFileWriter.java:109-111` carries only inline comments while `doclint:all,failOnWarnings=true` (`pom.xml:363,364`) requires full docs on public types — the private overload escapes the gate but confuses maintainers.
Recommendation: document the private overload with `///` (hook-name contract + TOCTOU re-verification note `:119-120`) or fold it away per §5.3.

**15.1 No production-code FQN violations — keep it that way via enforcement.**
The `peruncs.cluster.*` hits are Javadoc links and `import` statements, not inline FQNs; `java.lang.*` hits are `static import System.Logger.Level.*` (acceptable). The single `new java.util.ArrayList` (§13.2) is the only inline FQN.
Recommendation: add a checkstyle/forbiddenapis rule banning `new java\.` and `peruncs\.cluster\.[a-z]+\.[A-Z]` inline references in `src/main`.

## 16. Methods with >5 arguments

**16.1 `AeronReplicationEnvelope.encode` (18 params) and `encodeWithPayloadCrc` (19 params).**
`storage/aeron/wire/AeronReplicationEnvelope.java:101-119,150-169`. The hot-path framing method takes every header field as a separate arg; transposition of `chunkIndex/chunkCount/chunkOffset/payloadOffset/chunkLength` is a single-typo data-corruption bug.
Recommendation: apply the `FrameHeader` record from §7.1. Same for `validate(...)` (`:203-218`, 15 params).

**16.2 `TransactionAssembler(...)` takes 10 params.**
`storage/aeron/reader/TransactionAssembler.java:144-155` (`configuration, clusterId, epoch, initialSequence, initialPosition, receiver, transactionResolved, deliveryListener, wireNonce, durabilityGate`).
Recommendation: introduce `record AssemblerInputs(AeronReplicationConfiguration configuration, UUID clusterId, long epoch, CursorSnapshot initial, StorageBinaryDataReceiver receiver, Consumer<CursorSnapshot> resolved, ReaderDeliveryListener listener, long wireNonce, CommitDurabilityGate gate)` and a `forTests()` factory (mirroring `AeronReplicationPublisher.forTests`, `:106`). Keep the 10-arg constructor private.

**16.3 `AeronReplicationConfiguration` record has 13 components (rule 17 overlap).**
`storage/aeron/config/AeronReplicationConfiguration.java:35-49` — 13 fields with a hand-written `Builder` (correct) but no `withX` copy methods.
Recommendation: keep the `Builder` (do not replace with a telescoping constructor), and add `withChunkSize/withMaxTransactionBytes/...` compact copy methods for test overrides so tests stop rebuilding via `builder().termLength(...).mtuLength(...)...build()` chains.

## 17. Builder pattern

**17.1 `AeronCheckpointCodec.SerializedNodeIdentity` and `AeronReaderWatermark` lack builders despite 6–8 fields.**
`storage/aeron/checkpoint/AeronCheckpointCodec.java:204` (`SerializedNodeIdentity(UUID,UUID,UUID)` — 3 fields, borderline) and `storage/aeron/checkpoint/AeronReaderWatermark.java:28` (7 fields constructed via `new AeronReaderWatermark(readerId, clusterId, storeGeneration, writerEpoch, recordingId, ...)` at `:74` with positional `long,long,long` triples).
Recommendation: add a `WatermarkBuilder` (or static `of(readerId, clusterId, storeGeneration, ...)` with named parameter objects `RecordingId`, `SequencePosition`) so `recordingId/sequence/position` (`long,long,long`) cannot be transposed. The `validateFields(...)` call at `:99` already exists — move it into the builder’s `build()`.

## 18. Interface with static-only methods

No violation: every `public interface` (`api/ClusterStorageManager, api/GraphBoundary, api/NodeSettingsSource, node/store/*, node/replication/*, storage/binary/*`) declares instance methods. `NodeAssembly.create()` (`node/NodeAssembly.java:51-53`) coexists with instance lifecycle methods, so the interface is justified. No action — do not convert `NodeAssembly` to a utility class.

## 19. Security gaps

**19.1 Fencing-lease theft window relies on wall-clock synchrony with no monotonic guard.**
`node/aeron/WriterFencingLease.java:70` documents “must run a synchronized wall clock (NTP, chrony)” and `README.md:172-175` rejects far-future heartbeats as skew, but `WriterFencingLease.java:175,263,499-592` compares `heartbeatMillis` across hosts with `System.currentTimeMillis()` and a configurable `maxStaleness`.
Recommendation: add a monotonic `System.nanoTime()`-based hold-down on the stealing path (require two consecutive stale reads spaced by `lockTimeout` before stealing), and expose `lastSkewMillis` in `NodeStatus` so operators can alert before a false steal. Document that the lease directory must have `nodev,nosuid` mounts (in addition to NFSv4).

**19.2 Backup ZIP extraction budget is enforced post-decompression in one path.**
`node/backup/BackupArchive.java:439,472,582,599-649,687-731` validates entry names/links/duplicates/sizes, but `extractEntry(zip, root, entry, extractedBytes, budget, transferBuffer)` (`:544`) streams through a shared `transferBuffer` while `extractedBytes` accumulates — a zip-bomb with many small entries passes per-entry checks and fails only at the aggregate budget.
Recommendation: enforce `BackupArchiveLimits` (`backup/BackupArchiveLimits.java:15`) pre-extraction via central-directory size summation (already read at `:667-669` `Failed to open backup archive`) and abort before writing any bytes when the declared total exceeds the budget. Add a dedicated zip-bomb test with a 10000×1 KiB-entry archive.

**19.3 Mutating `ClusterNode` operations carry no in-process auth — documented but unenforced.**
`README.md:290-296` states backups/storage-checks/pause-resume “carry no authentication of their own: the embedding application MUST authenticate.” `api/ClusterNode.java:106-133` (`startStorageChecks/createScheduledBackup/createManualBackup`) exposes them without a `Principal` or capability token.
Recommendation: add an explicit `Authorizer` functional parameter to `NodeOptions` (`Supplier<Principal>` or `Predicate<Operation>`) that defaults to `allowAll` in dev and `denyAll` in prod (`isProdMode`), forcing embedders to wire auth. Log every mutating call with caller identity at `INFO`.

## 20. Performance

**20.1 `AeronReplicationEnvelope.copyToOwned` allocates a heap `byte[]` per decoded envelope.**
`storage/aeron/wire/AeronReplicationEnvelope.java:267-268` (`final byte[] payload = new byte[view.payloadLengthOnWire]`) runs on the reader polling thread for every fragment; `TransactionAssembler.java:814` (`new UnsafeBuffer(replacementStorage)`) similarly allocates per replacement.
Recommendation: decode into a pooled `ExpandableArrayBuffer` / Agrona `UnsafeBuffer` reused via the existing `ChecksumContext`-style explicit context (`AeronReplicationEnvelope.java:61-67`). Pass the scratch buffer from `AeronArchiveReader` (which already polls in a loop, `:565`) instead of allocating per envelope. Assert with the existing `perTransactionHeapAllocationStaysWithinBudget` test — extend it to the decode path.

**20.2 `.formatted()` on hot/error paths allocates even when logging is disabled.**
`storage/aeron/reader/TransactionAssembler.java:316,356,360,372,388,391,412,438`, `storage/aeron/writer/AeronArchiveReplicationPublisher.java:355`, `storage/aeron/writer/AeronReplicationPublisher.java:442`.
Recommendation: guard with `LOGGER.isLoggable(Level.DEBUG)` or pass `Supplier<String>` to the logger; for thrown exceptions, keep `.formatted()` (exception paths are cold) but replace string-concat checks on the live-marker gate (`TransactionAssembler.java:239-260`) with precomputed constants.

**20.3 `Thread.sleep` in backup and driver-retry loops burns reaction time.**
`node/backup/StorageBackupManager.java:439,492` (`Thread.sleep(POLL_INTERVAL_MILLIS/RETENTION_RETRY_DELAY_MILLIS)`), `node/aeron/AeronRuntime.java:172` (`Thread.sleep(STALE_DRIVER_RETRY_DELAY_MILLIS)`).
Recommendation: replace with `AeronRetryPolicy.idleStrategy()` (`storage/aeron/config/AeronRetryPolicy.java:72-74`, already backed by `BackoffIdleStrategy`) so these loops honor the configured `idleMaxSpins/idleMaxYields/idleMinPark/idleMaxPark` instead of hard-coded sleeps.

## 21. Threading / races / deadlocks / TOCTOU / corruption

**21.1 Dual-lock nesting `mutexFor(path)` → `stateLock` risks lock-order inversion.**
`node/aeron/WriterFencingLease.java:194` (`synchronized(active.stateLock)` inside `synchronized(mutexFor(path))`), `:499-500,591-592` (same nesting), while `:457,465,544,549,673,727` lock `stateLock` alone. Any future path that locks `stateLock` → `mutexFor` deadlocks.
Recommendation: establish and document a global order (`pathMutex → stateLock`, never the reverse) in the class Javadoc, add an assertion helper `assertLockOrder()`, and replace `synchronized(mutexFor(path))` with a `StripeLockedExecutor` keyed by canonical path (per rule 27) so cross-host lease files stripe instead of sharing one monitor.

**21.2 `AeronWatermarkChannel` TOCTOU: `available()` reads unsynchronized.**
`node/aeron/AeronWatermarkChannel.java:50` (“Read unsynchronized by available(); every mutation holds the monitor”) with mutations at `:148,166,216,227,254,292-345,397`.
Recommendation: make the read field `volatile` (or `VarHandle` acquire/release) and document the benign-race contract (stale `available()` → retry, never corrupt). Add a JCStress-style test with publisher + `available()` poller.

**21.3 `AtomicFileWriter` symlink check-then-act window is acknowledged but still racy.**
`storage/io/AtomicFileWriter.java:117-120` (`rejectSymbolicLinks(absolute)` then `Files.createDirectories(parent)` with “Re-verify the parent immediately before creating the temp file”).
Recommendation: open the parent with `OPEN` + `NOFOLLOW_LINKS` and operate via `Path.toRealPath(NOFOLLOW_LINKS)` file descriptors instead of re-checking strings; on filesystems without `O_NOFOLLOW` dir-fds, retain the re-verification but add a final `Files.isSymbolicLink(tempFile)` check after creation before `forceDirectory`.

**21.4 `TransactionAssembler` releases the assembler monitor before Store import but holds `delivery` across decode.**
`storage/aeron/reader/TransactionAssembler.java:236` (`synchronized(this.delivery)`) wraps `decodeView` + durability-gate probe (`:236-260`), while `dispose()` from another thread drops staged entries (comment at `:120-124`).
Recommendation: narrow the `delivery` critical section to the detach copy only; move the `durabilityGate.isDurablyRecorded()` Archive RPC (`:248`) outside the monitor so a slow Archive control channel cannot stall `dispose()` or the next fragment.

## 22. Network robustness / configurable retries

**22.1 Reader reconnect budget is single-valued; no per-phase backoff.**
`storage/aeron/config/AeronReplicationConfiguration.java:44` (`readerStopTimeoutNanos`, default 30 s at `:86`) bounds both live-terminal withholding (`TransactionAssembler.java:316`) and Archive-tail reconnect. `storage/aeron/reader/AeronArchiveReader.java:565` parks a fixed `RECORDED_POSITION_REFRESH_MILLIS`.
Recommendation: split into `liveWithholdTimeoutNanos` + `reconnectTimeoutNanos` + `probeDelayNanos` (reuse `AeronRetryPolicy.archiveProbeDelayNanos/catalogProbeInitialDelay/MaxDelay`), and wire `AeronArchiveReader.java:565` to `retryPolicy` instead of the constant. Expose both in `ECLIPSE_DATAGRID_AERON_*` env keys.

**22.2 Crash matrix explicitly excludes UDP loss/duplication, ENOSPC, SIGSTOP.**
`README.md:537-541` states the matrix “does not claim to model arbitrary UDP loss or duplication, disk-full ENOSPC, SIGSTOP, or network authentication.”
Recommendation: add three focused fault-injection ITs (loopback packet-loss via `tc`/Aeron loss hook, `ENOSPC` via small tmpfs Archive dir asserting `StorageLimitReachedException`, `SIGSTOP`-pause of the reader asserting bounded reconnect → `RESEED_REQUIRED`) before claiming production readiness. The hooks exist (`CrashHook`, `AtomicFileWriter.TEST_HOOK`) — reuse them.

## 23. Exception design / propagation / reporting

**23.1 `NodeException` is used as both config and I/O failure, hiding retryability.**
`api/NodeSettingsSource.java:237,249,301,311`, `node/backup/BackupArchive.java:81-841`, `node/replication/DurableCursorFile.java:84,107` all throw `NodeException` for parse errors, corrupt ZIPs, and transient I/O alike.
Recommendation: split `NodeException` into `NodeConfigurationException` (fail-fast, never retry) vs `NodeIOException` (retryable) vs existing `ReplicationException` subtree. Map each to distinct operator actions in `api/NodeStatus.java` Javadoc (currently only `ReplicationState FAILED/DEGRADED/RESEED_REQUIRED` are actionable).

**23.2 `CloseSequencer.Stage` swallows per-stage failures into a single message.**
`node/CloseSequencer.java:75-77,92,125` rethrows `Error/RuntimeException` directly but wraps checked failures in `NodeException(failureMessage, failure)`, losing the stage name in the cause chain.
Recommendation: attach stage names as suppressed exceptions (`failure.addSuppressed(new NodeException("stage: "+name, stageFailure))`) so a failed teardown identifies which collaborator (Archive vs Store vs watermark channel) failed.

## 24. Javadoc correctness

**24.1 `AeronReplicationConfiguration` barrier Javadoc promises “identical” crash state — overstrong.**
`storage/aeron/config/AeronReplicationConfiguration.java:63-81` states “The persisted state after a crash is identical” regardless of `readerBarrierMaxTransactions`.
Recommendation: soften to “crash-recoverable to the same durable boundary via the uncertainty marker; mid-barrier transactions are re-resolved from the Archive, not skipped.” Reference the crash-matrix cell that proves it (`ProviderCrashMatrixIT` barrier case).

## 25. Tests / simulation coverage

**25.1 Crash-matrix and soak are profile-gated, not in the default gate.**
`pom.xml:267-271` default failsafe includes only `AeronArchiveReplicationIT` + `AeronUdpReplicationIT`; `ProviderCrashMatrixIT/ExternalArchiveCrashIT/WriterTakeoverCrashMatrixIT/BackupCrashMatrixIT/AeronCrashMatrixIT/AeronReaderCrashMatrixIT` require `-Pcrashmatrix` (`:555-609`), soak requires `-Psoak` (`:610-685`), Store integration requires `-Pintegration` (`:535-553`).
Recommendation: promote at least one cell per boundary (prepare/local-write/commit-offer/checkpoint-rename/cursor-torn) into the default `mvn verify` gate so a one-line durability regression fails without remembering profile flags. Keep the full matrix behind profiles for duration, not for coverage.

**25.2 `ModuleDescriptorConsistencyTest` parses `module-info.class` because tests run off the module path.**
`pom.xml:245-255` sets `useModulePath=false` for surefire/failsafe (“forked crash fixtures, dynamic ScopedValue test hooks, non-modular helpers assume unnamed module”), with consistency guarded by a bytecode-parsing test.
Recommendation: migrate forked fixtures to explicit `--add-modules peruncs.cluster --add-reads` launches and re-enable `useModulePath=true` for unit tests, deleting the bytecode-parsing workaround. The current setup silently allows split-package drift between `src/main` and `src/test`.

## 26. Aeron / Store / Serializer best practices

**26.1 Direct `synchronized(archive)` instead of Aeron’s recommended single-threaded Archive client discipline.**
`storage/aeron/writer/AeronArchiveReplicationPublisher.java:110,203,231`, `node/aeron/AeronRuntimeOwner.java:198,205,212,228,236` synchronize on the shared `AeronArchive` instance for `listRecording/listRecordings/offer` paths.
Recommendation: confine each `AeronArchive` to its owning thread (writer thread regimes already exist via `AeronReplicationWriteCoordinator.writeLock:60` + `ApplyQueue.queueLock`) and use Aeron’s `IdleStrategy`-paced `poll()` instead of cross-thread `synchronized(archive)`. Cross-check against `aeron-io/aeron` `ArchiveTool`/`ReplayMerge` examples referenced in `GITHUB_ROOT`.

**26.2 `AeronOfferRetryer.offer/offerGated` duplicates Aeron’s `Publication.offer` back-pressure contract.**
`storage/aeron/writer/AeronOfferRetryer.java:53,64,89,137` wraps `DirectBuffer offer` with `BooleanSupplier stillOwner` + `WriterLeaseGate` + `LockSupport.parkNanos`.
Recommendation: delegate to `io.aeron.logbuffer.BufferClaim` zero-copy offer (no staging copy in `EnvelopeFramer.java:69,194`) and use Aeron’s `ChannelUri` + `Publication.BACK_PRESSURED/ADMIN_ACTION` codes directly instead of re-deriving them in `AeronArchiveFailures`.

## 27. Agrona / Store thread utils vs `synchronized`

See §1.2 (primary) plus:
**27.1 `ApplyQueue.queueLock` is a `ReentrantLock` but `ApplyWorker.budgetLock` and `StorageBinaryDataMerger.dictionaryParseLock` remain `synchronized`.**
`storage/binary/ApplyQueue.java:33` vs `storage/binary/ApplyWorker.java:156,220,244,282,292,311` vs `storage/binary/StorageBinaryDataMerger.java:503`.
Recommendation: convert the latter two to the already-imported `LockedExecutor` (`StorageBinaryDataMerger.java:3`, `ApplyWorker.java:3`) so all Store-import paths share one documented lock domain. Delete `dictionaryParseLock` in favor of the existing `materialization LockedExecutor` (`:186`).

## 28. Cluster constraints (1-writer/N-reader, no auth, no transport encryption)

**28.1 Single-writer enforcement depends on operators provisioning NFSv4 + setting two flags.**
`README.md:238-250` + `node/aeron/AeronTransport.java:117-152` reject production startup without `ECLIPSE_DATAGRID_BACKUP_PATH` on `nfs4` + `ECLIPSE_DATAGRID_AERON_SHARED_LEASE_FILESYSTEM=true` + `ECLIPSE_DATAGRID_AERON_TRUSTED_NETWORK=true`. A dev topology that accidentally points at production channels starts without error (loopback rejection only fires in prod mode, `AeronSettings.java:462-466`).
Recommendation: add a `clusterId`-scoped “prod channel fingerprint” check that fails fast whenever a non-loopback channel is used without `TRUSTED_NETWORK=true`, regardless of `isProdMode`. Never allow a dev node to join a prod `clusterId`.

## 29–31. Memory / Unsafe / native buffers

**29.1 `PreparedWrite` retains `ByteBuffer[]` + `byte[] dictionary` on the writer hot path.**
`storage/aeron/writer/AeronReplicationWriteCoordinator.java:94` (`record PreparedWrite(long sequence, byte[] dictionary, ByteBuffer[] buffers, int bufferCount, ...)`).
Recommendation: replace `ByteBuffer[]` with Agrona `DirectBuffer[]` views over a single pooled `ExpandableDirectByteBuffer`, and pass `dictionary` as `DirectBuffer` (as `EnvelopeFramer.offerDictionaryChunks(DirectBuffer,int)` at `:73` already accepts). This avoids one heap `byte[]` + one array object per transaction on the single-writer hot path.

**30.1 `XMemory.objectFieldOffset/getObject/setObject` is Unsafe-adjacent with no fallback.**
`storage/index/StoreIndexReflection.java:210-238` uses `org.eclipse.serializer.memory.XMemory` offset access to bypass `setAccessible`/`--add-opens`. `pom.xml:255,264,628` already passes `--add-exports java.base/jdk.internal.misc=ALL-UNNAMED` + `jdk.incubator.vector`.
Recommendation: isolate all `XMemory` calls behind a `FieldAccessor` seam with a `MethodHandles.privateLookupIn`-based fallback, so a JDK 28 removal of `jdk.internal.misc` access fails with a clear “unsupported JDK” error instead of `NoSuchMethodError` deep in index refresh. Remove the `--add-exports jdk.internal.misc` flag if the fallback proves sufficient.

**31.1 `TransactionAssembler.EMPTY_BUFFER` uses `ByteBuffer.allocateDirect(0)` instead of Agrona.**
`storage/aeron/reader/TransactionAssembler.java:34` allocates a zero-length NIO direct buffer where every other hot-path buffer is Agrona `DirectBuffer/UnsafeBuffer` (`storage/aeron/wire/AeronReplicationEnvelope.java:3-4`, `storage/aeron/writer/EnvelopeFramer.java:4-5`).
Recommendation: replace with `Unpooled.EMPTY_BUFFER` / Agrona `EMPTY_BUFFER` constant to keep one buffer abstraction on the read path.

## 32. Embedded Lucene / JVector indexes

**32.1 `ClusterIndexValidation` partial parse gap at line 429.**
Graph `index_status` reports `parse_partial: storage/index/ClusterIndexValidation.java:429-429`. Source read shows the `Optional`-case + field-walk region (`:405-517`) surrounding the flagged line.
Recommendation: fix the syntax construct at `:429` (likely a preview `switch` pattern + `when` guard) so tree-sitter parses cleanly, then re-index. Until then, graph queries over index validation are incomplete — re-verify all §32 findings by `grep`, not `search_graph`.

**32.2 Vector graph reset mutates upstream `boolean` via raw byte write.**
`storage/index/StoreIndexReflection.java:227-238` (`readBoolean` via `XMemory.get_byte(...) != 0`, `writeBoolean` via `set_byte(..., (byte)(value?1:0))`) toggles `graphRebuilt` on JVector indexes.
Recommendation: replace with a `VarHandle` boolean accessor over the resolved `Field` so JVM memory semantics (volatile vs plain) are explicit. If the upstream field is non-volatile, document why a plain byte write is safe (reader rebuild is single-threaded per `ClusterIndexMaintenance`).

## 33–34. Cluster correctness / performance (liveliness, throughput, threading)

**33.1 Writer restart mints a greater fencing token but readers accept any greater token without an upper bound.**
`node/aeron/WriterFencingLease.java:734` (`record LeaseFile(long token, UUID nodeId, UUID holderId, long heartbeatMillis)`), `storage/aeron/reader/TransactionAssembler.java:356` (“stale writer fencing token %s below accepted %s”).
Recommendation: bind the accepted token window to `[floor, floor+maxRestarts]` (configurable, default e.g. 128) and fail closed with `ReseedRequiredException` when a token jumps beyond it — a runaway restart loop currently looks identical to legitimate fencing.

**33.2 `AeronArchiveRetention.purgeSegmentsWhileWritesPaused` pauses admission but has no deadline.**
`node/aeron/AeronWriterTransport.java:560-561` (`withWritesPaused(() -> currentWriter.purgeSegmentsWhileWritesPaused(boundary))`), `node/aeron/AeronArchiveRetention.java:124,225`.
Recommendation: wrap the pause in `offerTimeoutNanos` (or a dedicated `retentionPauseTimeoutNanos`) so a stalled `purgeSegments` cannot wedge the single writer indefinitely. Surface pause duration in `ReplicationMetrics` (`node/replication/ReplicationMetrics.java:28`).

**34.1 Reader barrier idle flush (2 ms) + fragments-per-poll (256) are tuned for replay, not live tail.**
`storage/aeron/config/AeronReplicationConfiguration.java:61,72-81` (`DEFAULT_READER_FRAGMENTS_PER_POLL=256`, `DEFAULT_READER_BARRIER_IDLE_FLUSH_NANOS=2ms`).
Recommendation: make both adaptive: `fragmentsPerPoll = min(256, backlogEstimate)` from `AeronArchiveReader` recording-position lag, and `idleFlushNanos = 200µs` at live tail vs `2ms` during backlog. Measure live-tail p99 in the soak (`soak.lagSlots=100`) and assert it in `AeronWriterReaderSoakIT` instead of only asserting backlog throughput.

---

## Appendix — evidence index (representative paths)

- Envelope hot path: `storage/aeron/wire/AeronReplicationEnvelope.java:101-218,262-306,592-666`
- Reader state machine: `storage/aeron/reader/TransactionAssembler.java:120-260,472-491,637-814`
- Writer coordination: `storage/aeron/writer/AeronReplicationWriteCoordinator.java:60-112,575-760`
- Archive publisher: `storage/aeron/writer/AeronArchiveReplicationPublisher.java:74-373,525,861`
- Transports: `node/aeron/AeronWriterTransport.java:47-330,550-649,789`, `node/aeron/AeronReaderTransport.java:41-415`, `node/aeron/AeronTransport.java:34-367`, `node/aeron/AeronTransportShared.java:15-146`
- Fencing: `node/aeron/WriterFencingLease.java:70-734`
- Settings: `node/aeron/AeronSettings.java:40-826`, `storage/aeron/config/AeronReplicationConfiguration.java:35-343`, `storage/aeron/config/AeronRetryPolicy.java:21-74`
- Lifecycle: `node/NodeLifecycle.java:46-770`, `node/NodeAssembly.java:47-797`, `node/CloseSequencer.java:75-154`
- Storage: `storage/binary/StorageBinaryDataMerger.java:59-765`, `storage/binary/ApplyWorker.java:29-333`, `storage/binary/ApplyQueue.java:14-33`
- Files/cursors: `storage/io/AtomicFileWriter.java:31-564`, `node/replication/DurableCursorFile.java:51-115`, `storage/aeron/checkpoint/AeronReplicationCheckpointStore.java:41-211`
- Indexes: `storage/index/StoreIndexReflection.java:26-239`, `storage/index/ClusterStoreIndexes.java:51-318`, `storage/index/ClusterIndexValidation.java:405-517`
- Backup: `node/backup/BackupArchive.java:38-841`, `node/backup/FilesystemVolumeBackupBackend.java:38-865`
- API: `api/ClusterNode.java:19-189`, `api/NodeOptions.java:22-60`, `api/NodeStatus.java:28`, `api/ReplicationStatus.java:27-50`
