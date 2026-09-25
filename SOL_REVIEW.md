# Aeron Datagrid Code Review

This is a static review of the current codebase, with emphasis on cluster correctness, liveness, throughput, memory efficiency, package and visibility boundaries, naming, and human-readable Javadocs. No build, test, or benchmark was run as part of this review.

## Priority summary

| Priority | Recommendation | Primary risk |
|---|---|---|
| P0 | Fix apply-queue byte accounting while a batch is in flight | Permanent false backpressure and replication liveness failure |
| P0 | Make writer admission, dictionary ownership, and commit ownership one atomic transition | Spurious concurrent-write failure and lost type dictionaries |
| P1 | Stop using `IllegalStateException` to determine the node role | Real storage failures can be misclassified and masked |
| P1 | Allocate transaction assembly storage from the declared payload length | Repeated direct-memory allocation and copying on large transactions |
| P1 | Remove per-transaction heap churn from Store import | Allocation rate scales with replicated transaction count |
| P1 | Stop rebuilding/scanning indexes at every apply barrier | Apply throughput degrades with total data size rather than changed data |
| P1 | Replace rename-and-directory-fsync checkpoint transitions with a fixed journal | Metadata I/O dominates small replicated writes |
| P1 | Constrain writer fencing to storage whose lock semantics are actually guaranteed | Split-brain risk on shared/network filesystems |
| P1 | Treat live Aeron channels as an explicit unauthenticated trust boundary | Frame/watermark injection can stop replication or affect retention |
| P2 | Remove the unsupported asynchronous-distribution API and collapse the write path | Dead configuration and duplicated state ownership |
| P2 | Delete the mostly inert `NodeConfiguration` injection graph | Large nullable service locator with no public configuration path |
| P2 | Redraw packages around runtime, write, apply, index, and filesystem ownership | Cross-package `public` types and blurred responsibilities |
| P2 | Rename misleading types and split only at state-ownership boundaries | Large classes obscure the actual concurrency model |
| P2 | Replace virtual polling workers with dedicated Aeron agent threads | Carrier scheduling overhead and unpredictable polling latency |
| P2 | Fix broad `Throwable` handling and shutdown error propagation | Original failures can be replaced or silently discarded |
| P2 | Delete dead utilities and normalize factory naming | Unnecessary concepts and non-idiomatic API surface |
| P2 | Rewrite module, package, and API Javadocs around operator-visible contracts | Contradictory security guidance and hidden lifetime rules |

## P0 — correctness and liveness

### 1. Fix apply-queue byte accounting while another batch is in flight

**Evidence.** `StorageBinaryDataMerger.Default.scheduleMaterialization` computes the admission total as `cachedBytes + inFlightBytes + incomingBytes`, then assigns that total back to `cachedBytes` ([`StorageBinaryDataMerger.java:418`](src/main/java/peruncs/cluster/storage/types/StorageBinaryDataMerger.java#L418), [`StorageBinaryDataMerger.java:449`](src/main/java/peruncs/cluster/storage/types/StorageBinaryDataMerger.java#L449), [`StorageBinaryDataMerger.java:461`](src/main/java/peruncs/cluster/storage/types/StorageBinaryDataMerger.java#L461)). When the worker drains the queued batch, it subtracts only the drained batch from `cachedBytes` and accounts it separately in `inFlightBytes`; completion later subtracts only `inFlightBytes` ([`StorageBinaryDataMerger.java:605`](src/main/java/peruncs/cluster/storage/types/StorageBinaryDataMerger.java#L605), [`StorageBinaryDataMerger.java:718`](src/main/java/peruncs/cluster/storage/types/StorageBinaryDataMerger.java#L718)).

If batch A is in flight when batch B arrives, `cachedBytes` becomes `A + B`, even though only B is cached. Draining B leaves a phantom A in `cachedBytes`. That residue never reaches zero and can permanently reject later work as cache-full.

**Recommendation.** Keep the counters disjoint:

```java
var newCachedBytes = Math.addExact(cachedBytes, incomingBytes);
var admittedBytes = Math.addExact(newCachedBytes, inFlightBytes);
if (admittedBytes > maxCachedBytes) {
    // reject
}
cachedBytes = newCachedBytes;
```

Use a small private value object only if it prevents the invariant from being repeated; otherwise keep the arithmetic inline. State the invariant next to the fields: `cachedBytes` is queued ownership, `inFlightBytes` is worker ownership, and their sum is the admission total.

**Required regression.** Block the materializer after it takes batch A, enqueue batch B, release both, await idle, and assert both counters are zero. Then enqueue a batch exactly at the configured capacity and assert it is accepted. The existing merger tests do not exercise admission while non-zero bytes are in flight.

### 2. Make writer admission, dictionary ownership, and commit ownership atomic

**Evidence.** `AeronStorageBinaryReplicationTarget.write` prepares under `executeWriteAtomically`, returns from that critical section, and only then calls commit ([`AeronStorageBinaryReplicationTarget.java:110`](src/main/java/peruncs/cluster/storage/aeron/AeronStorageBinaryReplicationTarget.java#L110)). The target consumes the staged type dictionary before recording it as coordinator state ([`AeronStorageBinaryReplicationTarget.java:144`](src/main/java/peruncs/cluster/storage/aeron/AeronStorageBinaryReplicationTarget.java#L144)). `executeWriteAtomically` releases its lock immediately after the callback returns ([`AeronReplicationWriteCoordinator.java:200`](src/main/java/peruncs/cluster/storage/aeron/AeronReplicationWriteCoordinator.java#L200)), while the commit guard is not installed until `beginCommit` ([`AeronReplicationWriteCoordinator.java:247`](src/main/java/peruncs/cluster/storage/aeron/AeronReplicationWriteCoordinator.java#L247), [`AeronReplicationWriteCoordinator.java:271`](src/main/java/peruncs/cluster/storage/aeron/AeronReplicationWriteCoordinator.java#L271)).

A second writer can enter that gap, consume and overwrite `pendingDictionary`, and then fail because the publisher still owns a prepared transaction. The first transaction's cleanup subsequently clears the coordinator dictionary. The second dictionary has already been removed from `StorageBinaryDataDistributor.Caching` ([`StorageBinaryDataDistributor.java:184`](src/main/java/peruncs/cluster/storage/types/StorageBinaryDataDistributor.java#L184)), so schema information is lost. Even when there is no dictionary, the gap makes concurrent `Store` calls fail nondeterministically rather than at a defined admission boundary.

**Recommendation.** Introduce one immutable `PreparedTransaction` that owns the payload metadata, optional dictionary, transaction ID, and publisher reservation. Install it as the sole in-flight transaction while still holding the admission lock. Commit must consume that exact object; cleanup must clear it by identity. Do not keep a separate mutable `pendingDictionary` field.

If the API deliberately supports only one caller at a time, reject the second caller before consuming any of its state and say so in the public contract. A serializing lock is simpler and safer than a prepare lock followed by a separately installed commit guard.

**Required regression.** Put a latch immediately after prepare and before commit ownership is installed. Start writer A, then writer B with a distinct dictionary. Verify that B either waits and succeeds or fails before its dictionary is consumed; after A completes, B's dictionary must still be published with B. The existing concurrency test starts its second writer after commit has already begun, so it misses this window.

## P1 — high-impact correctness and performance

### 3. Stop using `IllegalStateException` to discover the node role

**Evidence.** `ClusterNode.control()` calls `foundation.storageNodeManager()` and catches every `IllegalStateException` as a signal to return the backup control instead ([`ClusterNode.java:87`](src/main/java/peruncs/cluster/api/ClusterNode.java#L87)). `ClusterFoundation.storageNodeManager()` can throw for the role mismatch, but its delegated manager access can also throw a genuine storage lifecycle failure ([`ClusterFoundation.java:751`](src/main/java/peruncs/cluster/node/ClusterFoundation.java#L751)). The catch therefore changes the meaning of unrelated failures and can replace the useful cause with a backup-role error.

**Recommendation.** Select the control object once from the configured role while opening the node and store it as a final field. Alternatively expose a total `ClusterFoundation.nodeControl()` method that switches on the explicit role. Reserve exceptions for invalid calls, not control flow.

**Required regression.** Make storage-manager construction or access throw `IllegalStateException`; `ClusterNode.control()` must propagate that same failure and cause rather than attempting the backup path.

### 4. Allocate transaction assembly storage from the declared payload length

**Evidence.** `TransactionAssembler.Transaction` already knows and validates the complete `payloadLength`, but `ensureCapacity` starts with at most 64 KiB and repeatedly doubles a direct buffer, copying the accumulated bytes each time ([`TransactionAssembler.java:607`](src/main/java/peruncs/cluster/storage/aeron/reader/TransactionAssembler.java#L607), [`TransactionAssembler.java:667`](src/main/java/peruncs/cluster/storage/aeron/reader/TransactionAssembler.java#L667)). A 64 MiB transaction consequently performs a sequence of native allocations, copies nearly another payload's worth of bytes, and temporarily retains old and new direct buffers.

**Recommendation.** Allocate the validated transaction payload length on the first payload fragment. Keep the configured maximum as the hard allocation guard before allocating. If production measurements later justify pooling, pool by a small set of bounded size classes; do not add a pool speculatively.

**Required regression.** Assemble a maximum-size fragmented transaction and instrument direct-buffer allocations and copied bytes. The acceptance criterion should be one payload allocation and no growth copies.

### 5. Remove per-transaction heap churn from Store import

**Evidence.** `StorageBinaryDataImporter.importDirect` allocates a `ByteBuffer[]` and duplicates every buffer for every imported transaction ([`StorageBinaryDataImporter.java:138`](src/main/java/peruncs/cluster/storage/types/StorageBinaryDataImporter.java#L138)). The replication path is otherwise explicitly designed around off-heap ownership. The allocation test allows 256 KiB per transaction, which is too loose to prevent this regression ([`StorageBinaryDataMergerAllocationTest.java:21`](src/test/java/peruncs/cluster/storage/types/StorageBinaryDataMergerAllocationTest.java#L21)).

**Recommendation.** Reuse one batch-local scratch array sized to the largest transaction in the batch. If Store import is synchronously consuming the buffers, pass the owned buffers while saving and restoring their position/limit rather than duplicating them. First pin that synchronous-consumption assumption with a contract test. If duplicates are required for correctness, reuse wrapper objects through a bounded batch context rather than allocating them per transaction.

**Required regression.** Tighten the allocation test to a measured, low budget after the change and cover multi-buffer dictionaries and data. Report both heap bytes per transaction and retained direct memory.

### 6. Stop rebuilding or scanning all indexes at every apply barrier

**Evidence.** The apply worker holds graph write ownership through import, materialization, and index work ([`StorageBinaryDataMerger.java:646`](src/main/java/peruncs/cluster/storage/types/StorageBinaryDataMerger.java#L646)). It calls `refreshImportedIndexes` before materialization, then validation/rebuild afterward ([`StorageBinaryDataMerger.java:671`](src/main/java/peruncs/cluster/storage/types/StorageBinaryDataMerger.java#L671), [`StorageBinaryDataMerger.java:700`](src/main/java/peruncs/cluster/storage/types/StorageBinaryDataMerger.java#L700)). `ClusterIndexMaintenance.refreshImportedIndexes` traverses stored roots/maps and clears vector/Lucene views ([`ClusterIndexMaintenance.java:69`](src/main/java/peruncs/cluster/storage/types/ClusterIndexMaintenance.java#L69)); the post-import path scans again and forces vector graph readiness. The vector graph rebuild therefore scales with the entire dataset at each barrier, whose default size is only 64 transactions ([`AeronReplicationConfiguration.java:63`](src/main/java/peruncs/cluster/storage/aeron/AeronReplicationConfiguration.java#L63)).

This makes sustained apply cost approach `O(dataset size × barriers)` and holds the writer/reader graph exclusion while doing it. Replication lag will grow with stored data even when each transaction changes only a few indexed objects.

**Recommendation.** Feed the changed object IDs/types produced by Store Binary import into index maintenance. Update or invalidate only affected Lucene/JVector entries, and discover newly introduced index roots from the changed graph rather than rescanning every root. As an intermediate step, combine pre- and post-materialization discovery into one pass and retain the discovered index registry across barriers, invalidating it only when changed types can introduce/remove an index.

Do not merely raise the barrier size: that delays the nonlinear work but does not change its growth curve.

**Required regression.** Add a scaling benchmark with fixed transaction size and growing stored datasets, separately for Lucene and JVector. Enforce a bound on apply time per changed object and maximum reader backlog. The current full-path benchmark thresholds are opt-in and do not guard this dataset-size slope ([`AeronFullPathBenchmarkTest.java:21`](src/test/java/peruncs/cluster/benchmark/AeronFullPathBenchmarkTest.java#L21)).

### 7. Replace rename-and-directory-fsync checkpoint transitions with a fixed journal

**Evidence.** `AtomicFileWriter.write` creates a temporary file, writes and forces it, atomically renames it, and forces the parent directory ([`AtomicFileWriter.java:101`](src/main/java/peruncs/cluster/storage/types/AtomicFileWriter.java#L101)). The writer persists PREPARING/ENQUEUED state and then COMMITTED state for every transaction, with in-flight marker cleanup in between ([`AeronReplicationWriteCoordinator.java:393`](src/main/java/peruncs/cluster/storage/aeron/AeronReplicationWriteCoordinator.java#L393), [`AeronReplicationWriteCoordinator.java:458`](src/main/java/peruncs/cluster/storage/aeron/AeronReplicationWriteCoordinator.java#L458), [`AeronReplicationWriteCoordinator.java:740`](src/main/java/peruncs/cluster/storage/aeron/AeronReplicationWriteCoordinator.java#L740)). `AeronReplicationCheckpointStore` also allocates a new encoded byte array per write while its Javadoc describes checkpoint writes as infrequent ([`AeronReplicationCheckpointStore.java:36`](src/main/java/peruncs/cluster/storage/aeron/AeronReplicationCheckpointStore.java#L36)).

Small Store transactions therefore pay multiple file creations, renames, file forces, and directory forces. That cost is serialized into commit latency and throughput.

**Recommendation.** Replace the separate marker/checkpoint files with one pre-created, fixed-size, two-slot state journal. Each slot should contain a monotonically increasing sequence, state, transaction metadata, and checksum. Write and force the inactive slot, then select the highest valid sequence on recovery. Keep the channel open for the node lifetime and reuse an off-heap encoding buffer. This retains crash recovery without allocating, creating, renaming, and syncing a directory for every state transition.

**Required regression.** Crash after each slot write/force boundary and verify deterministic recovery. Benchmark Store-to-Archive commit with 1–4 KiB transactions and require a throughput/latency floor with durability enabled, not only an Aeron publication benchmark.

### 8. Constrain writer fencing to filesystems with proven lock semantics

**Evidence.** `WriterFencingLease` promises cross-machine writer exclusion on an arbitrary shared volume and bases ownership on a Java file lock, an atomic file replacement, wall-clock expiry, and periodic renewal ([`WriterFencingLease.java:29`](src/main/java/peruncs/cluster/node/replication/WriterFencingLease.java#L29), [`WriterFencingLease.java:181`](src/main/java/peruncs/cluster/node/replication/WriterFencingLease.java#L181)). File-lock and rename coherence varies across NFS/SMB/cluster filesystems, and expiry depends on clock coordination. The checkpoint capability checks do not prove that two hosts observe one linearizable lock domain. A process using the same node ID can also take over an unexpired lease, relying on later token checks to stop the previous holder.

**Recommendation.** Narrow the contract instead of implying universal shared-volume safety. Name and document the supported filesystem/mount semantics, validate them with a two-host startup probe, and fail closed when they cannot be established. If the intended deployment cannot provide such semantics, the simpler operational model is a statically assigned writer with no automatic stale takeover. Only introduce an external linearizable lease service if automatic failover is a hard requirement; do not simulate consensus with timestamps and file replacement.

**Required regression.** Run two real processes on the actual production filesystem from different hosts, exercise acquisition, renewal, pause beyond lease duration, same-ID restart, and partition/reconnect. Assert that at most one process can pass the final token check before every publication.

### 9. Treat live Aeron channels as an explicit unauthenticated trust boundary

**Evidence.** The module documentation says cluster-node authentication/authorization is unnecessary and describes the environment as both “air-gaped” and VPN-protected ([`module-info.java:19`](src/main/java/module-info.java#L19)). A VPN is not an air gap. Archive authentication does not authenticate application frames or reader watermarks. Cluster IDs, nonces, CRCs, and configured reader IDs detect accidents/corruption; they do not prove who sent a frame. A host able to publish to these channels can therefore inject invalid data to trip fail-closed behavior or spoof reader progress used by retention.

**Recommendation.** Make the supported security model explicit and enforceable: bind live channels to an isolated, authenticated network overlay; reject wildcard/public endpoints in production configuration; and disable automatic retention when watermark publishers are not inside that boundary. If hostile peers are in scope, authenticated frames are required—but that is a separate security design, not a documentation patch.

**Required regression.** Add an integration test with an unconfigured publisher sending a validly encoded frame and watermark. The documented deployment mode must either prevent delivery at the network boundary or leave retention and durable reader state unchanged.

## P2 — simplification, structure, naming, and documentation

### 10. Remove asynchronous distribution instead of implementing it

**Evidence.** `NodeOptions` exposes `asynchronousDistribution` ([`NodeOptions.java:14`](src/main/java/peruncs/cluster/api/NodeOptions.java#L14)), and the value is threaded through the foundation and distributor factories. The only production Aeron transport rejects `true` at startup ([`AeronClusterReplicationTransportProvider.java:301`](src/main/java/peruncs/cluster/node/aeron/AeronClusterReplicationTransportProvider.java#L301)). The distributor name also implies it publishes Store data, while the Aeron implementation deliberately routes actual transaction publication through a separate persistence target and uses the distributor mainly for dictionary staging.

**Recommendation.** Delete `asynchronousDistribution` from `NodeOptions`, its builder, `NodeConfiguration`, and the transport/distributor signatures. Do not add an asynchronous mode. Collapse dictionary staging into the immutable prepared transaction recommended in item 2, then remove `StorageBinaryDataDistributor` or reduce it to a package-private dictionary encoder with no lifecycle/state. One transaction owner should stage, publish, commit, and clean up all transaction state.

### 11. Delete the mostly inert `NodeConfiguration` injection graph

**Evidence.** `NodeConfiguration` is a 21-field record ([`NodeConfiguration.java:46`](src/main/java/peruncs/cluster/node/NodeConfiguration.java#L46)). `ClusterFoundation.Builder` mirrors those fields, but only four have public setters; the remaining collaborator fields are never assigned through the public builder. `ClusterNode.open` is the only production construction path. The node then wraps every nullable collaborator in `configured == null ? factory : configured` lazy selection.

**Recommendation.** Delete the unreachable collaborator fields and their conditional factories. Build the runtime directly from the root supplier, Store foundation, and replication properties. After removing asynchronous distribution, `NodeConfiguration` likely has no reason to exist as a separate public-shaped record. Use direct `LazyConstant.of(this::createX)` dependencies inside a package-private runtime and expose explicit test seams only where a test genuinely substitutes behavior.

This also makes ownership and close order visible: a component created by the runtime is closed by the runtime, without a hidden “possibly injected” branch for every service.

### 12. Redraw package boundaries around ownership

**Evidence.** `storage.types` contains replication contracts, apply-queue code, Store import/materialization, filesystem durability/security, and index discovery/validation. Its package Javadoc explicitly defends the mixture as a way to share package-private helpers ([`storage/types/package-info.java:20`](src/main/java/peruncs/cluster/storage/types/package-info.java#L20)). The Aeron implementation is split between `node.aeron`, `node.replication`, `storage.aeron`, and `storage.aeron.reader`. Types such as the Aeron publisher, reader, coordinator, checkpoint store, `StorageFileOperations`, and `CloseSequencer` are `public` largely so those packages can call each other. Only `peruncs.cluster.api` is exported by the module, so this is an internal source-boundary problem rather than a reason to preserve the current layout.

**Recommendation.** Reorganize by state ownership:

- `cluster.runtime`: node assembly, role selection, lifecycle, and close sequencing.
- `cluster.replication.write`: prepared transaction, writer checkpoint, fencing, publication.
- `cluster.replication.apply`: reader cursor, assembly, apply queue, import/materialization.
- `cluster.index`: Lucene/JVector discovery, validation, and maintenance.
- `cluster.fs`: the few reusable durable-file and path-security primitives.
- `cluster.replication.aeron`: the single transport implementation behind one package-private or narrow facade.

Co-locate the Aeron coordinator, publisher, reader, codec, and checkpoint state that share invariants, then make them package-private. Leave one small `AeronReplicationTransport` facade across the runtime boundary. Do not create a generic `internal` package; it would preserve the same ownership ambiguity under a different name.

Rewrite `node.replication/package-info.java`, which currently describes Aeron even though the package name claims a transport-neutral layer ([`node/replication/package-info.java:1`](src/main/java/peruncs/cluster/node/replication/package-info.java#L1)).

### 13. Rename types to describe responsibility, then split only where state ownership changes

Several names hide what the type actually controls:

- `StorageBinaryDataMerger` does not merge values; it queues replicated transactions, applies Store Binary imports, materializes objects, and refreshes indexes. Rename the concrete owner to `ReplicationApplyQueue`. Because the graph shows one production implementation, remove the interface-plus-`Default` pair unless a real alternate implementation is planned.
- `StorageBinaryDataClient` is the replication reader contract. Rename it `ReplicationReader`.
- `ClusterFoundation` is a lifecycle/service-locator implementation. Hide it behind `ClusterNode` and name the concrete internal object `ClusterNodeRuntime`.
- `AeronClusterReplicationTransportProvider.Transport` should become the top-level `AeronReplicationTransport`; the provider should be a tiny factory or disappear.
- `applyTimeoutMs` is not a timeout that interrupts a hung apply. The implementation explicitly treats it as a watchdog after work returns, while the worker and buffers remain pinned if work never returns. Rename it `applyFailureBudgetMs` (or equivalent) and state that exceeding it terminates replication; do not imply preemption.
- `WriterFencingLease.isCurrentUncached` performs a fresh ownership validation. Rename it `validateOwnershipNow` or `isOwnerNow`.

Avoid splitting large files merely to reduce line count. Split `AeronClusterReplicationTransportProvider`, `ClusterFoundation`, `StorageBinaryDataMerger`, and `AeronReplicationPublisher` where a new object can exclusively own a state machine or resource lifetime. A helper that still mutates its parent's fields is only file shuffling.

### 14. Use dedicated platform agent threads for Aeron polling loops

**Evidence.** `AeronArchiveReader` starts its controlled-poll loop on a virtual thread ([`AeronArchiveReader.java:473`](src/main/java/peruncs/cluster/storage/aeron/AeronArchiveReader.java#L473)). The loop repeatedly polls and invokes an Agrona `IdleStrategy`; the watermark channel uses a similar virtual worker. Polling/backoff loops are CPU-oriented agents, not blocking request-per-task work. They can occupy carrier threads while spinning and add scheduler variability to latency-sensitive polling.

**Recommendation.** Run Aeron pollers with a named dedicated platform thread or Agrona `AgentRunner`, with the configured idle strategy and a deterministic close/join path. Keep virtual threads for operations that genuinely block independently, such as backup upload or retention filesystem work.

**Required benchmark.** Compare platform versus virtual pollers under idle, low-rate, and saturated traffic. Record CPU, carrier utilization, p50/p99 publication-to-apply latency, fragments/sec, and shutdown time. Choose the simpler platform agent unless measurements contradict it.

### 15. Fix broad `Throwable` handling and shutdown propagation

**Evidence.** `ClusterStorageManager.shutdown` catches `Throwable` from its callback and delegate but rethrows only `Error` or `RuntimeException`; an unexpected checked throwable can be silently discarded ([`ClusterStorageManager.java:365`](src/main/java/peruncs/cluster/node/ClusterStorageManager.java#L365)). `ClusterFoundation.start` catches `Throwable` and casts every non-`Error` to `RuntimeException`, which can replace the original failure with `ClassCastException` ([`ClusterFoundation.java:800`](src/main/java/peruncs/cluster/node/ClusterFoundation.java#L800)).

**Recommendation.** Catch `RuntimeException | Error` where those are the actual contracts. Where cleanup must aggregate arbitrary failures, use one close sequencer: attempt every close, keep the first failure, add later failures as suppressed, and wrap a checked first failure in `NodeLibraryException`. Update lifecycle state only at the point the corresponding transition has really completed.

**Required regression.** Use a sneaky checked throwable from both startup and shutdown collaborators. Verify that cleanup continues, the original throwable remains the cause, and later cleanup failures are suppressed rather than lost.

### 16. Delete dead utilities and normalize factories

**Evidence.** `UserUploadValidator` has no production caller, while backup upload validation is implemented independently in `BackupArchive` ([`UserUploadValidator.java`](src/main/java/peruncs/cluster/storage/types/UserUploadValidator.java), [`BackupArchive.java:51`](src/main/java/peruncs/cluster/node/backup/BackupArchive.java#L51)). `AeronCheckpointCodec` is an interface containing only constants, static methods, and a nested reader ([`AeronCheckpointCodec.java:12`](src/main/java/peruncs/cluster/storage/aeron/AeronCheckpointCodec.java#L12)). Factory methods named `New`, `NoOp`, `Caching`, and `Default` use type-style capitalization for methods and make searches ambiguous.

**Recommendation.** Delete `UserUploadValidator`; do not add a second abstraction to rescue unused code. Convert `AeronCheckpointCodec` to a final utility class with a private constructor, or replace its simple primitive operations with a consistently ordered `ByteBuffer` if measurement shows no regression. Rename static factories to lower camel case—`create`, `noOp`, `caching`, `defaults`—and remove interface factories when there is only one production implementation and no meaningful polymorphism.

### 17. Rewrite Javadocs around human-visible contracts

The documentation currently mixes useful contracts with protocol narration, implementation history, and contradictory claims. Apply these changes together with the code restructuring:

1. Reduce `module-info.java` to a short statement of purpose, exported API, single-writer rule, failure model, and security boundary. Move the full protocol/recovery/retention narrative to `README.md` or `docs/replication-protocol.md`.
2. Replace “authentication is not required” and “air-gaped VPN” with the precise statement from item 9. Correct spelling and do not use “air gap” for a routed network.
3. In every public type, make the first sentence answer “what does this let a caller do?” Follow with ownership, threading, blocking, failure, and lifetime rules. Avoid internal names such as checkpoint frames unless a caller must understand them.
4. Move historical explanations and bug archaeology out of class Javadocs. For example, the long rationale in `ClusterIndexMaintenance` belongs in an ADR or focused regression test; its class Javadoc should explain when index maintenance runs, what it scans, and which lock it requires.
5. Document the missing lifetime rule on `ClusterStore.read`: a returned value must be detached or immutable and must not retain live Store-managed objects after the callback. The implementation only rejects returning the exact root and cannot detect a nested mutable escape ([`ClusterStore.java:23`](src/main/java/peruncs/cluster/api/ClusterStore.java#L23), [`ClusterStorageManager.java:486`](src/main/java/peruncs/cluster/node/ClusterStorageManager.java#L486)). Prefer a name such as `query` once the contract is explicit.
6. Delete claims that no longer match execution, notably “checkpoint writes are infrequent.”
7. After package changes, give each package a two-to-four sentence `package-info.java`: purpose, owned state/resources, and allowed inbound dependency. Do not use package Javadocs to justify mixed responsibilities.

## Recommended execution order

1. Add the two P0 regression tests and fix their state invariants before doing structural moves.
2. Make `PreparedTransaction` the sole writer-side owner of dictionary, payload reservation, checkpoint transition, and cleanup.
3. Fix assembler/import allocation behavior and add enforceable allocation counters.
4. Replace the checkpoint files and measure the durable small-write path.
5. Redesign index maintenance around changed objects and add dataset-scaling gates.
6. Constrain fencing/security deployment contracts and test them on the real shared filesystem/network boundary.
7. Remove dead configuration and collaborators, then move packages and narrow visibility. Moving first would create noisy mechanical churn around code whose ownership still needs to change.
8. Finish with the naming and Javadoc rewrite so the documentation describes the simplified model rather than the current transitional one.

## Review limitations

- This was intentionally a static review; builds, tests, simulations, benchmarks, and JFR recordings were not run.
- The code graph was used for package, dependency, implementation-count, and visibility analysis, then findings were verified against source. Its snapshot was older than some working-tree file metadata, so source reads are authoritative for the line-level findings above.
- The graph reports one partial parse location in `ClusterIndexValidation.java` at line 415 and two partial test-file locations outside the cited evidence. Those gaps do not support any conclusion in this report.
