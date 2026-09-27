# Consolidated review findings

Reviewed the six `*review.md` files modified on 2026-09-26: `SPARK-REVIEW.md`, `final-review.md`, `KIMI-REVIEW.md`, `MIMO-REVIEW.md`, `ds-review.md`, and `glm-review.md`. Each candidate below was checked against the code before implementation; duplicate and already-fixed reports were consolidated. All surviving mitigations below have been applied. A follow-up code sweep added the hardening and regression checks below; the full unit, integration, crash-matrix, and soak profiles pass.

## P0 — Stop writer startup on a missing authoritative Store

When the Store directory is missing, `BackupRestorePolicy` restores the newest compatible backup and its cursor at `src/main/java/peruncs/cluster/node/backup/BackupRestorePolicy.java:149-152`. The writer-only protection against replacing its authoritative image is below that branch (`:155-165`), so it does not run for a missing Store. Writer startup then reads its checkpoint and extends the existing Archive from `checkpoint.transactionSequence() + 1` in `src/main/java/peruncs/cluster/node/aeron/AeronWriterTransport.java:365-381`; that path validates the Archive boundary, not that the restored Store image matches the checkpoint. A reader-seed backup can therefore be older than the writer checkpoint, producing a writer whose object graph and next replicated sequence describe different histories.

**Mitigation:** If a configured writer has a committed checkpoint but its local Store is absent or empty, fail startup with `ReseedRequiredException`. Do not seed an authoritative writer from a reader backup. Permit recovery only through an explicit writer-image restore that proves the Store image and checkpoint/cursor are the same boundary. Add a regression scenario for a missing writer Store plus a newer checkpoint and older backup.

## P1 — Hold authoritative writer ownership through Archive retention

Retention admission calls `WriterFencingLease.isCurrent()` through `ensureMaintenanceAdmitted()` (`src/main/java/peruncs/cluster/storage/aeron/writer/AeronReplicationWriteCoordinator.java:423-446`). `isCurrent()` explicitly reuses a cached ownership result for up to one third of the staleness interval (`src/main/java/peruncs/cluster/node/aeron/WriterFencingLease.java:432-471`). The purge then runs under the local write lock, but without holding the interprocess lease lock (`AeronReplicationWriteCoordinator.java:460-490`, `src/main/java/peruncs/cluster/node/aeron/AeronWriterTransport.java:548-561`). A writer deposed after the cached check can therefore reach Archive stop/purge/extend without an authoritative ownership proof; a stale retention operation can delete replay history needed by readers or the new writer.

**Mitigation:** Make Archive maintenance use an uncached ownership proof serialized against lease takeover, and keep that proof across the Archive mutation sequence. If ownership cannot be held for the whole operation, revalidate at each destructive boundary and abort before purging. Add a takeover-during-retention test that proves a deposed writer cannot purge or extend the recording.

## P1 — Do not turn reader shutdown interruption into permanent replication failure

`AeronReaderLifecycle.stopAndClose()` interrupts the poller at `src/main/java/peruncs/cluster/storage/aeron/reader/AeronReaderLifecycle.java:110-135`. The poller then unconditionally flushes staged deliveries in `AeronArchiveReader.run()` (`src/main/java/peruncs/cluster/storage/aeron/reader/AeronArchiveReader.java:601-604`). That flush waits for the merger at `src/main/java/peruncs/cluster/storage/aeron/reader/TransactionAssembler.java:1002-1016`; `StorageBinaryDataMerger.awaitApplied()` treats interruption as terminal, releases queued buffers, and throws `ReplicationUnavailableException` (`src/main/java/peruncs/cluster/storage/binary/StorageBinaryDataMerger.java:725-730`). Thus normal disposal can poison the reader while it is trying to establish its final durable cursor boundary.

**Mitigation:** Stop polling cooperatively and wake the poller without interrupting the final durability wait. If shutdown must interrupt, distinguish that shutdown signal from an unexpected materialization interruption and preserve the prior cursor unless the final flush completes. Add a regression test where disposal arrives with a staged commit awaiting materialization.

## P1 — Force restored Store files before publishing their cursor

ZIP extraction writes staged files through `Files.newOutputStream()` and closes them without forcing file contents (`src/main/java/peruncs/cluster/node/backup/BackupArchive.java:741-755`). `AtomicFileWriter.installStorage()` atomically moves the staged directory and forces only the destination parent (`src/main/java/peruncs/cluster/storage/io/AtomicFileWriter.java:349-364`). `BackupRestorePolicy.restoreBackupAndCursor()` then persists the matching cursor (`src/main/java/peruncs/cluster/node/backup/BackupRestorePolicy.java:211-219`). After a power loss, the directory entry and cursor can survive while some extracted Store bytes do not, leaving a durable cursor that claims a boundary the Store cannot reproduce.

**Mitigation:** Force every extracted regular file before installation, force the relevant staged directories, then atomically install and force the parent before writing the cursor. Keep the cursor as the final durable step. Add a crash-ordering test or filesystem fault simulation around file force, directory install, and cursor publication.

## P1 — Do not classify arbitrary `Number` and enum classes as safe index-free leaves

`ClusterIndexValidation.isLeafValue()` prunes every `Number` subclass and every enum before inspecting reachable fields (`src/main/java/peruncs/cluster/storage/index/ClusterIndexValidation.java:549-565`). Custom `Number` implementations and enums can carry references to index metadata; a reachable external or unsupported index behind one of these values is then invisible to the fail-closed scan. This conflicts with the validator’s role as the guard against indexes that would diverge across nodes.

**Mitigation:** Whitelist only known immutable scalar wrapper classes and inspect custom `Number` subclasses. Do not prune arbitrary enums unless their instance state is proven irrelevant; otherwise traverse their fields using the existing bounded scanner or reject them when inspection is incomplete. Add tests with a custom `Number` and an enum holding a forbidden index reference.

## P2 — Keep the previous local image until an uploaded archive is staged and validated

`NodeLifecycle` validates a user upload and deletes the current Store before calling restore (`src/main/java/peruncs/cluster/node/NodeLifecycle.java:198-215`). The backend validates the path again, but then `restoreArchive()` opens the archive again for extraction (`src/main/java/peruncs/cluster/node/backup/FilesystemVolumeBackupBackend.java:424-455`). Validation and extraction therefore do not operate on one pinned archive identity. If the shared-volume file changes between those opens, the second read can fail after the local image has already been deleted, or install a different valid archive than the one initially checked.

**Mitigation:** Extract and validate into a private staging directory while the existing Store remains intact. Pin the upload identity for validation and extraction (for example, use one open archive handle or an immutable staged copy), verify its digest, then replace the old Store only after the staged image is complete. Delete the upload after successful installation.

## P2 — Restrict orphan cleanup to workspace directories, not their lease files

`reapOrphanWorkspaces()` selects every path whose name starts with `.backup-export-` (`src/main/java/peruncs/cluster/node/backup/FilesystemVolumeBackupBackend.java:781-789`). The workspace lease file has the same prefix. If a live export lasts past the 24-hour orphan age, its lease file can be treated as a workspace; cleanup locks a different `.lease.lease` path and calls `deleteDirectory()` on the active lease file. `deleteDirectory()` walks and deletes regular-file roots too (`src/main/java/peruncs/cluster/storage/io/AtomicFileWriter.java:484-516`). A later pass can then recreate the real lease and delete the active export workspace, causing backup publication to fail.

**Mitigation:** Filter candidates to real directories with `NOFOLLOW_LINKS` before deriving a lease path. Keep the existing lock check for those directories. Add a test with an old workspace and an old-looking lease file while the workspace lock is held.

## P2 — Make scheduler close prove its workers have stopped before Store shutdown

`NodeMaintenanceScheduler.close()` interrupts its worker pool, waits for a bounded interval, logs a timeout, and marks itself closed regardless (`src/main/java/peruncs/cluster/node/NodeMaintenanceScheduler.java:159-194`). `NodeLifecycle` then proceeds to close replication and the Store (`src/main/java/peruncs/cluster/node/NodeLifecycle.java:644-648,709-730`). A maintenance task that ignores interruption can continue using the Store after it has been shut down; the scheduler’s `closed` check only prevents a task that has not started yet.

**Mitigation:** Track active maintenance work and make the close stage fail or remain incomplete if workers are still running. Ensure the close sequencer does not reach Store shutdown until this stage proves quiescence; keep the node admission-closed and allow a later close retry. Add a test with a maintenance task blocked beyond the close budget.

## P2 — Synchronize index registration with graph validation

Public index registration methods serialize their own check-and-act operations through `ClusterStoreIndexes.REGISTRATION` (`src/main/java/peruncs/cluster/storage/index/ClusterStoreIndexes.java:34-36,70-75,118-133`). The validator scans map index groups without that lock (`src/main/java/peruncs/cluster/storage/index/ClusterIndexValidation.java:255-299`), and the writer invokes the scan on every distributed write (`src/main/java/peruncs/cluster/node/aeron/AeronWriterTransport.java:219-229`). `GraphBoundary` documents that registration follows the graph-boundary lock order, but these public static registration methods do not enforce that precondition. A caller that registers outside a graph write section can race commit-time validation and expose a partial or changing index topology to the scan.

**Mitigation:** Either reject registration after node startup or place registration and validation in the same synchronization domain. Prefer the startup-only rule if runtime registration is not a product requirement; document and enforce it at the registration boundary.

## P2 — Bound actual bytes and memory while inspecting backup archives

`contentDigestOfArchive()` checks the sum of declared ZIP sizes, but when any entry has an unknown size the sum becomes `-1` and the digest loop reads the storage payload without counting actual inflated bytes (`src/main/java/peruncs/cluster/node/backup/BackupArchive.java:432-470,697-709`). The default entry limit is also capped at `1 << 24` (about 16 million entries) (`src/main/java/peruncs/cluster/node/backup/BackupArchiveLimits.java:14-24`); `listEntries()` materializes entry objects, names, and a set up to that limit (`BackupArchive.java:681-695`). Digesting an existing same-name publication occurs while the cross-process publication lock is held (`FilesystemVolumeBackupBackend.java:531-555,581-643`), so a hostile or malformed archive can consume excessive memory/CPU and hold up other publishers.

**Mitigation:** Count actual decompressed bytes in every digest loop and stop at `maxExtractedBytes`; fail closed on unknown or contradictory sizes. Set a practical default entry ceiling and avoid retaining duplicate full entry/name collections where possible. Keep expensive validation outside the publication lock, then recheck the pinned archive identity before resolving the publication.

## P2 — Close the lease state before releasing the interprocess lock

`WriterFencingLease.close()` warns if the heartbeat executor misses its wait budget, acquires and releases the interprocess lock, and only then sets `closed` (`src/main/java/peruncs/cluster/node/aeron/WriterFencingLease.java:691-730`). A renewal that was delayed behind the close lock can acquire it in the gap and observe `closed == false` in `refreshHeartbeatLocked()` (`:541-553`), writing a fresh heartbeat after `close()` returns. This can delay another node’s failover until the refreshed heartbeat ages out.

**Mitigation:** Publish the closed state under `stateLock` before releasing/excluding renewal work, and ensure any renewal already past admission either finishes before the release proof or observes closed before writing. Add a deterministic test that parks renewal at the file-lock boundary while close runs.

## P2 — Represent unknown replication sequences as unknown in the public status API

`ReplicationStatus` documents that no `-1` sentinel leaks, but its `currentSequence` and `latestSequence` components are primitive `long`s (`src/main/java/peruncs/cluster/api/ReplicationStatus.java:8-35`). `ClusterNode.replication()` passes raw metrics through without `present()` conversion for those fields (`src/main/java/peruncs/cluster/api/ClusterNode.java:160-174`); `StorageNodeManager.latestSequence()` explicitly returns `-1` when unavailable (`src/main/java/peruncs/cluster/node/StorageNodeManager.java:188-198`). `lagTransactions()` can therefore report zero for an unknown boundary, which is indistinguishable from no lag.

**Mitigation:** Use `OptionalLong` for both sequence boundaries, as already done for other optional metrics, and derive lag as optional or explicitly unknown if either boundary is absent. Keep the public Javadoc and constructor validation aligned with the chosen representation.

## P2 — Bound graph drain without closing the Store beneath an active section

`StorageGraphCoordinator.drain()` closes admission and then waits on an unbounded write-lock acquisition (`src/main/java/peruncs/cluster/storage/StorageGraphCoordinator.java:271-285`). A caller that blocks indefinitely inside a graph section can therefore hang node close forever. Simply adding a timeout is unsafe because `CloseSequencer` continues later stages even after a stage failure.

**Mitigation:** Give graph drain a bounded, interruption-aware wait. If it times out, preserve closed admission and make the close sequence defer the embedded Store stage until a later retry proves the graph is drained. Surface the timeout with the stage name.

## P3 — Remove authentication documentation left behind after the feature was removed

The project has no node authentication and requires a trusted network. Removing the stale claims from `AeronRuntime` and `AeronSettings` exposed remaining claims that Archive control authentication exists in `README.md:137`, `module-info.java:33`, and `AeronReplicationEnvelope.java:23`. Those statements could lead operators to expose unauthenticated control or replay channels.

**Mitigation:** Remove the stale claims and state consistently across runtime, module, envelope, and operator documentation that none of the node's channels provides authentication; require network isolation.

## Claims not carried forward

- The blanket recommendation to replace `synchronized` for virtual-thread pinning is stale for the configured JDK 27 target: JEP 491 shipped in JDK 24 and removed monitor-related carrier pinning for blocking synchronized code. See the [Oracle JDK 24 migration guide](https://docs.oracle.com/en/java/javase/24/migrate/significant-changes-jdk-24.html).
- `NodeStatus.replication == null` is explicitly documented for nodes without replication; it is not an API defect.
- Making graph write callbacks automatically invalidate on every exception contradicts `GraphBoundary`’s documented dirty-state contract (`src/main/java/peruncs/cluster/api/GraphBoundary.java:44-55`).
- Authentication APIs, runtime role promotion, and replacing the fixed-role lease design conflict with the repository’s explicit cluster constraints.
- Duplicated findings already fixed in the current checkout were not repeated as open issues.

## Follow-up sweep

- Backup extraction now uses overflow-safe remaining-budget accounting and rejects content whose actual length differs from its declared ZIP size. The under-sized-entry regression test passes.
- Index registration stays excluded for the whole reader import/materialize/refresh batch, closing the gap between the previous before/after guards.
- Uploaded storage remains available until the restored cursor and starter backup are durable, allowing bootstrap retry from the same source image.
- Failed Store replacement before its directory-sync durability point restores the previous image; a fault-injection regression test covers failure after the new directory rename.
- Public `ReplicationStatus` construction rejects negative present sequence and byte metrics, matching its documented unknown-value representation.
- The README now describes VPN, firewall, and NetworkPolicy rules as network isolation; it no longer implies that they provide Data Grid node identity or that the transport encrypts traffic.
- The compatible-backup restore test now publishes a populated Store image and checks the restored root, rather than relying on an empty archive fixture. An async backup failure test now waits for the failure signal instead of racing the worker's initial idle state.

## Verification

All commands ran on the configured JDK with local Aeron sockets enabled:

- `mvn -o verify` — 740 unit tests (1 skipped), 12 default integration tests; passed.
- `mvn -o -Pintegration verify` — 740 unit tests (1 skipped), 10 integration tests; passed.
- `mvn -o -Pcrashmatrix verify` — 740 unit tests (1 skipped), 69 integration tests, including 35 provider crash scenarios; passed.
- `mvn -o -Psoak verify` — 740 unit tests (1 skipped), soak passed with 395 transactions, 31,556 queries, zero torn reads, and successful reader convergence.
