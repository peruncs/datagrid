# DS-REVIEW — Independent static review of `peruncs/datagrid`

- **Revision reviewed:** working tree at `cd39892` (`Review round 6: interrupt-safe relock restore,
  read-side admission latch, backup flag in task body, cached guarded Database, dispose retry
  coverage, shutdown-false retry test`) plus the uncommitted import/visibility edits
  (`module-info.java`, `NodeAssembly`, `NodeLifecycle`, tests).
- **Method:** static read-only analysis. No builds, tests, benchmarks, or code changes were made.
  Findings were derived by direct source reading plus five parallel subsystem scans; every P1
  and most P2 findings were re-verified line-by-line against the current working tree. Where a
  consequence is inferred rather than proven, the finding says so.
- **Scope:** all of `src/main/java` (137 files, ~31k LOC) with depth-first review of the reader,
  writer/lease, backup, lifecycle/API, and merger/index subsystems; `src/test/java` was audited
  for coverage shape only.
- **Severity:** P1 = data loss/corruption or node-wide liveness break; P2 = robustness, security,
  deadlock/hang, or hot-path performance; P3 = design, API, documentation, dead code.

---

## Executive summary

| Severity | Count | Headline risks |
|---|---|---|
| P1 | 3 | Clean reader disposal can permanently fail the shared Store merger; restored backup images are not fsynced before a durable cursor is installed; writer restart still has no Store-vs-checkpoint validation |
| P2 | 25 | Upload TOCTOU, zip-bomb digest under publication lock, live-lease reaping, lease close race and cached admission, unbounded graph drain, index-registration race, fail-open `Number`/`Enum` pruning, per-commit full-graph scan, several reader-stop/withhold budget defects, Windows metadata writes, no CI |
| P3 | 11 groups | Backup archive edge cases, dead code, style/API drift, god classes, doc-vs-code mismatches, cold storage gauge, ABORT token asymmetry, unresolved cursor convention, reader-replacement marker ordering |

**Top 5 to fix first:** DS-01 (reader dispose vs shared merger), DS-02 (restore fsync),
DS-03 (writer recovery validation), DS-06 (zip-bomb digest under lock), DS-08 (lease close
race) / DS-11 (drain hang).

Note on prior rounds: the round-6 fixes for the coordinator relock interruption
(`AeronReplicationWriteCoordinator.java:734-757` now re-acquires uninterruptibly while
re-asserting the interrupt) and for the backup cancel race (`StorageBackupTaskExecutor.java:139-158`
now sets `backupRunning` inside the task body) are present and correct, and are **not** repeated
here. The remaining reviewer-visible state of older findings is called out explicitly where still open.

---

## P1 findings

### DS-01 — Clean reader disposal can permanently fail the shared Store merger

- **Evidence:**
  - `src/main/java/peruncs/cluster/storage/aeron/reader/AeronReaderLifecycle.java:119` — `thread.interrupt();`
    (the only stop signal for the polling thread).
  - `src/main/java/peruncs/cluster/storage/aeron/reader/AeronArchiveReader.java:603` —
    `this.assembler.flushDeliveries();` runs **after** the loop exits on `active == false`, with
    the interrupt flag still set.
  - `src/main/java/peruncs/cluster/storage/aeron/reader/TransactionAssembler.java:1016` —
    `if (hasData) receiver.awaitApplied();`
  - `src/main/java/peruncs/cluster/storage/binary/StorageBinaryDataMerger.java:720-725` —
    `catch (final InterruptedException e) { Thread.currentThread().interrupt();
    this.noteFailure(...); ... throw new ReplicationUnavailableException(...); }`, and
    `recordLifecycleFailure` (`:791-793`) latches the **first terminal** failure.
- **Scenario:** a reader is disposed (node close, or `AeronReaderTransport` replacing the reader
  at `:146`) while a delivery barrier with data is staged and the merger still has queued or
  in-flight bytes. The final `flushDeliveries()` calls `awaitApplied()`, whose blocking wait
  (`Future.get`) throws `InterruptedException` immediately because the polling thread is
  interrupted. The merger latches a terminal failure and releases queued buffers. Any later
  reader on the same node then fails every import, and the store/cursor never advances again —
  a clean shutdown/replacement turns into a node-wide permanent failure.
- **Recommendation:** make the stop signal cooperative for the flush: after the loop, clear the
  interrupt status before `flushDeliveries()` (`boolean interrupted = Thread.interrupted();` ...
  restore after), and/or make `StorageBinaryDataMerger.awaitApplied` treat interruption during a
  final drain as retryable rather than terminal. The interrupt must never be able to reach a
  durability boundary. Add a regression test: stage a barrier, block the Store import, call
  `dispose()`, and assert the merger remains usable.
- **Confidence:** verified from source (control flow). The failing branch requires queued data at
  dispose time, which is routine under backlog.

### DS-02 — Restored backup files are not fsynced before the durable cursor is installed

- **Evidence:**
  - `src/main/java/peruncs/cluster/node/backup/BackupArchive.java:747-755` — extraction writes
    with `Files.newOutputStream(target, CREATE_NEW, WRITE, NOFOLLOW_LINKS)` and never forces the
    file (`channel.force(true)` is absent).
  - `src/main/java/peruncs/cluster/storage/io/AtomicFileWriter.java:356-364` — `installStorage`
    does `Files.move(source, destination, ATOMIC_MOVE)` then only `forceDirectory(destination.getParent())`;
    directory fsync does not flush file contents.
  - `src/main/java/peruncs/cluster/node/backup/BackupRestorePolicy.java:218-219` — after
    `backend.restoreBackup(...)`, the cursor is written via `cursorManager.get().set(backup)`,
    which **is** fsynced (`AtomicFileWriter.write` forces file + parent).
- **Scenario:** restore completes into page cache, the cursor becomes durable, then power loss /
  panic. Restart trusts the durable cursor over a Store image whose files were never written
  back; Store recovery sees truncated/zero-length files at best, and the node resumes from a
  boundary the local image does not represent. This is exactly the corruption class the
  backup/cursor protocol is designed to prevent.
- **Recommendation:** force every extracted file before install: open the output with
  `FileChannel` and `force(true)` after writing (or walk the staged tree and force each file),
  force the staged `storage` directory, and only then install + persist the cursor. Add a
  crash-harness case that kills the process between install and cursor force, and one between
  extraction and install (already covered by the crash matrix for the latter).
- **Confidence:** verified from source; the power-loss persistence semantics are standard POSIX.

### DS-03 — Writer restart still has no Store-vs-checkpoint validation [previously reported, still open]

- **Evidence:**
  - `src/main/java/peruncs/cluster/node/backup/BackupRestorePolicy.java:149-152` — the
    `!storageExists` branch restores the selected backup **before** the writer guard at
    `:155-165` (`ownAuthoritativeStore`). A writer that lost its Store directory is therefore
    reseeded from an older backup.
  - `src/main/java/peruncs/cluster/node/NodeLifecycle.java:337-341` and `:491-513` — a writer may
    create a fresh root (`mayCreateRoot()` is `role == WRITER`), so a writer with a live durable
    checkpoint but an empty/missing Store silently starts with a new root.
  - `src/main/java/peruncs/cluster/node/aeron/AeronWriterTransport.java:365-373` — recovery reads
    the writer checkpoint and extends the recording; nothing compares the opened Store image to
    the checkpoint boundary, and the writer's replication client is not a replay client.
- **Scenario:** the writer's checkpoint (separate `ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH`) and
  Archive recording survive, but the Store directory is deleted/corrupted or was replaced by a
  backup seed at sequence N. Recovery resumes publication at checkpoint+1 over an image missing
  all transactions N+1..N+K. No failure is raised; readers that have those transactions diverge
  from the writer's local Store.
- **Recommendation:** make backup/restore policy a no-op for the writer role, and add an explicit
  authoritative-writer recovery gate: if a durable writer checkpoint exists and the Store
  directory is missing/empty, fail with `ReseedRequiredException`; if a compatible backup cursor
  is older than the checkpoint, reject it; optionally persist a small per-commit boundary marker
  in the Store root to prove the image. Add the restart test: backup at N, acknowledged writes
  through N+K, wipe Store, restart, assert reseed rather than silent resume.
- **Confidence:** verified from source; the concrete residual loss path was confirmed in the
  earlier review round and the corresponding code has not changed materially.

---

## P2 findings

### DS-04 — User-upload validation and extraction open the archive twice (TOCTOU)

- **Evidence:** `FilesystemVolumeBackupBackend.java:423-428` documents re-validation “on the same
  open archive”, but calls `BackupArchive.validateUpload(path)` (which opens its own `ZipFile`)
  and then `restoreArchive(path)`, which opens the path again inside `extractArchive`
  (`BackupArchive.java:66`, `:538`). `NodeLifecycle.java` deletes local storage before restore.
- **Scenario:** on the shared backup volume, another process replaces
  `user-uploaded-storage.zip` between the two opens; the extracted image was never validated
  (manifest unicity, non-empty payload, budgets). The new file is still rooted and bounded by
  extraction, so this is an integrity/trust bypass, not traversal.
- **Recommendation:** validate and extract from one pinned handle (open `ZipFile` once and pass it
  into `extractArchive`), or capture file key + size + mtime at validation and re-verify
  immediately before extraction. Prefer staging extraction before deleting local storage.
- **Confidence:** verified from source.

### DS-05 — `contentDigestOfArchive` inflates without a true-byte budget while holding the volume lock

- **Evidence:** `BackupArchive.java:436-470` checks `declaredTotalBytes(entries)` (attacker-controlled
  central-directory sizes; `-1` when any is unknown, `:703`) and then reads each entry fully into
  a digest with no byte budget. It is called from `isIdenticalPublication`
  (`FilesystemVolumeBackupBackend.java:638`) while the volume publication file lock is held.
- **Scenario:** a crafted upload declaring small sizes inflates to gigabytes; the digest loop
  burns CPU/IO while every publish/delete in every process sharing the volume blocks behind the
  publication lock. `validateUpload` has a dry-run budget for exactly this; the digest path does not.
- **Recommendation:** route the digest loop through `transferBounded` with a true-byte budget and
  count unknown sizes against it, mirroring the extraction budget.
- **Confidence:** verified from source.

### DS-06 — Orphan-workspace reaper can delete a live lease and then a live workspace

- **Evidence:** `FilesystemVolumeBackupBackend.java:787` filters only
  `name.startsWith(EXPORT_WORKSPACE_PREFIX)`; lease files are `<workspace>.lease`
  (`:839-840`), so they match. `deleteOrphanWorkspace` (`:806-836`) then opens
  `<lease>.lease`, locks it, calls `AtomicFileWriter.deleteDirectory(workspace)` — which deletes
  regular files too (`AtomicFileWriter.java:502-513`) — and afterwards deletes the lease path.
- **Scenario:** when listing reaches `.backup-export-X.lease` first, the real lease file is
  unlinked. The live publisher's `FileLock` is held on the now-unlinked inode, so when listing
  reaches `.backup-export-X`, the reaper recreates the lease file, `tryLock()` succeeds, and the
  live workspace is deleted under the running export. Requires an export older than the reaping
  cutoff — unusual but possible for very large stores.
- **Recommendation:** filter with `Files.isDirectory(path, NOFOLLOW_LINKS)` and skip
  `WORKSPACE_LEASE_SUFFIX`; pin the lease file identity (file key) before deleting a workspace.
- **Confidence:** verified control flow; the live-workspace outcome needs the stated ordering/cutoff.

### DS-07 — Restore staging directories are never reaped

- **Evidence:** `FilesystemVolumeBackupBackend.java:441` creates `.backup-restore-*` under the
  storage parent; the only reaper scans `backupVolumePath` for `.backup-export-*` (`:787`).
- **Scenario:** a JVM kill/OOM during extraction leaves a full Store copy behind; crash loops fill
  the storage filesystem and prevent the Store from starting.
- **Recommendation:** reap stale `.backup-restore-*` directories at startup with the same
  age/lock guard as export workspaces, or place a lease in them and clean on first storage use.
- **Confidence:** verified from source.

### DS-08 — `WriterFencingLease.close()` can write a heartbeat after close returns

- **Evidence:** `WriterFencingLease.java:691-731` — the interprocess lock is released by
  try-with-resources at `:707-718`, and `closed = true` is only set afterwards at `:726-729`.
  `refreshHeartbeatLocked` (`:541-555`) checks `closed` under `stateLock` but not under the
  interprocess lock.
- **Scenario:** renew() passed its `closed` check, then close() proves release and drops the file
  lock, then renew() acquires it and writes a fresh heartbeat after `close()` returned. A
  successor then refuses to steal for up to the staleness bound (default 30 s), delaying failover.
  The class Javadoc (`:682-690`) claims the opposite ordering.
- **Recommendation:** set `closed = true` under `stateLock` while still holding the interprocess
  lock (inside the try-with-resources) and remove from `ACTIVE` after; keep the final check as
  defense. Add an interleaving test with a deliberately slowed renew.
- **Confidence:** verified from source.

### DS-09 — Lease validity is a cached rate-limited check but is documented as immediate

- **Evidence:** `AeronWriterTransport.java:504-512` says “Write admission deliberately bypasses the
  lease freshness cache”, yet returns `lease.isCurrent()`, which serves a cached result for up to
  `maxStaleness/3` (`WriterFencingLease.java:394`, `:455-461`). The cached check gates write
  admission, the post-ack commit check, maintenance admission, and `writerReady()`.
- **Scenario:** after a takeover, a deposed writer can pass admission for up to 10 s (30 s
  default staleness) and can reach `purgeWithWritesPaused` (`AeronWriterTransport.java:548-562`),
  stopping/purging the shared recording, before any ownership proof. The per-offer
  `executeUnderOwnership` path is the only authoritative fence.
- **Recommendation:** add an uncached ownership read for admission/maintenance/purge (re-read under
  the interprocess lock), or amend the Javadoc to state that only per-offer ownership proves
  ownership and mark the cached window as accepted risk in the fencing model.
- **Confidence:** verified from source; the purge consequence follows from the missing uncached check.

### DS-10 — Durability wait is far shorter than the index-refresh budget it must cover

- **Evidence:** `StorageBinaryDataMerger.java:228` —
  `materializationBudgetMs = applyTimeoutMs * (APPLY_TIMEOUT_RETRIES + 1)`; `awaitMaterialization`
  (`:762-774`) waits `APPLY_TIMEOUT_RETRIES × applyTimeoutMs`; `ApplyWorker.java:47` sets
  `INDEX_REFRESH_BUDGET_MULTIPLIER = 10`. With defaults, the caller gives up at 3×60 s while the
  worker's own refresh budget is 10×180 s.
- **Scenario:** a healthy but slow post-materialization index refresh (whole-store root scan +
  JVector rebuild) exceeding 180 s is declared a terminal merger failure by `awaitApplied` even
  though the worker is progressing and its watchdog has not fired. The reader fails and the
  cursor stops advancing.
- **Recommendation:** size the durability wait from the worker's total phase budget
  (`materializationBudgetMs + indexRefreshBudgetMs`), or make it progress-aware.
- **Confidence:** verified from source.

### DS-11 — `StorageGraphCoordinator.write()` latches invalidity on a clean admission rejection

- **Evidence:** `StorageGraphCoordinator.java:144-160`:
  ```java
  this.lock.writeLock().lock();
  try {
      this.ensureAdmission();   // throws IllegalStateException after drain()
      this.ensureValid();
      update.run();
  } catch (final RuntimeException | Error failure) {
      this.invalidate(failure); // clean rejection is latched as graph damage
      throw failure;
  }
  ```
  `writeExclusive` (`:215-246`) correctly does not invalidate.
- **Scenario:** after `drain()` sets `admissionClosed`, any in-flight replication/integration
  section that had not yet acquired the lock is rejected and the failure is recorded as
  `GraphInvalidatedException("Store graph may be partially updated")` although no callback ran.
  The latch then survives a failed close that is retried, misreporting a healthy graph.
- **Recommendation:** call `ensureAdmission()`/`ensureValid()` before entering the
  invalidating catch (or invalidate only around `update.run()`).
- **Confidence:** verified from source.

### DS-12 — `drain()` waits on the write lock without timeout or interruption

- **Evidence:** `StorageGraphCoordinator.java:278-286` — `this.lock.writeLock().lock(); ... unlock();`.
- **Scenario:** an application section that blocks indefinitely (external I/O, user deadlock)
  parks the close sequencer's “graph drain” stage forever; `ClusterNode.close()` hangs with no
  failure. `ReentrantReadWriteLock.lock()` ignores interrupts and has no deadline.
- **Recommendation:** use a bounded `tryLock(timeout)` drain and fail the close stage explicitly.
- **Confidence:** verified from source.

### DS-13 — Runtime index registration races merger index maintenance

- **Evidence:** `ClusterStoreIndexes.java:55` guards registration only with its own
  `REGISTRATION` `LockedExecutor` (`:84`, `:149`); the merger's scans iterate the same upstream
  `GigaIndices.indexGroups` structure (`ClusterIndexValidation.java:207-232`) without that lock.
  Upstream `GigaIndices` stores groups in a non-thread-safe `BulkList`
  (`eclipse-store .../gigamap/types/GigaIndices.java:175`).
- **Scenario:** application code calls `registerLucene`/`addVector` while a replication batch runs
  `beforeApply`/`afterApply`; the iteration can observe a torn list, an
  `ArrayIndexOutOfBoundsException`, or a null element (which fails validation as an unsupported
  group) — a terminal merger failure.
- **Recommendation:** route registration through the per-Store coordinator write side (expose it
  via `graphBoundary()`), or enforce/document registration strictly before the node starts and
  reject runtime registration.
- **Confidence:** registration bypass verified; the exact upstream failure mode is inferred from
  `BulkList` semantics.

### DS-14 — `isLeafValue` prunes arbitrary `Number`/`Enum` subclasses, making validation fail-open

- **Evidence:** `ClusterIndexValidation.java:549-565` returns `true` for
  `Number.class.isAssignableFrom(type)` and `Enum.class.isAssignableFrom(type)`; pruning happens
  before enqueueing (`:463`).
- **Scenario:** a user `Number` subclass (or an enum with fields) reachable from a root can carry
  a `GigaMap`, external Lucene context, or vector configuration; the scan never inspects it, so a
  writer that smuggled an unsupported index past registration passes validation and diverges
  readers. The class Javadoc promises “anything unprovable stays relevant”.
- **Recommendation:** restrict leaves to known-final JDK numeric wrappers (`Integer`, `Long`,
  `BigDecimal`, …), and analyze enums/custom `Number` subclasses like any other class (walk
  fields), or fail closed on unknown subclasses.
- **Confidence:** verified from source.

### DS-15 — Every distributed write performs a full root-graph scan with a fresh scratch [previously reported, still open]

- **Evidence:** `AeronWriterTransport.java:219-229` wires a validation runnable that calls
  `ClusterStoreIndexes.validateStorageRoots(connection)`; it runs from
  `AeronStorageBinaryReplicationTarget.prepareWrite` before every distributed write.
  `ClusterIndexValidation.java:325-327` allocates `new ValidationScratch()` per call, and
  `validateGraph` (`:275-299`) re-walks every index-relevant object with no topology caching.
  The `isLeafValue` change (DS-14 fix direction) will make more classes relevant.
- **Scenario:** commit latency and CPU grow with unrelated root data; a million-element
  collection of scalars is re-iterated on every commit while counting as one object in the bound.
- **Recommendation:** cache per-`GigaMap` validation results / generation and skip unchanged
  topology; reuse a writer-confined scratch; charge collection element visits to the work budget;
  add the constant-transaction/growing-collection benchmark.
- **Confidence:** verified from source.

### DS-16 — Reconnect bookkeeping accumulates one suppressed exception per failed attempt

- **Evidence:** `AeronArchiveReader.java:775-781` — every failed `PersistentSubscription.create`
  is either the first cause or `addSuppressed`.
- **Scenario:** with a 1 ms idle pace over the default stop budget, tens of thousands of Aeron
  exceptions (each with a stack trace) are retained, then thrown/logged with the reseed failure —
  memory spike and unusable diagnostics.
- **Recommendation:** keep the first cause plus an attempt counter; log subsequent failures at a
  sampled rate; cap retained suppressed causes.
- **Confidence:** verified from source.

### DS-17 — `withholdSinceNanos` survives reconnect/dispose, so a later withhold fails instantly

- **Evidence:** `TransactionAssembler.java:54`, reset only at `:260` (the recorded-resolution
  branch); `dispose()` (`:683-686`) does not clear it, and replay-sourced markers
  (`liveSource == false`) skip the gate entirely.
- **Scenario:** a live terminal marker is withheld at t0, the reader reconnects, the marker is
  resolved from replay without resetting the field; the next live withhold sees age ≥ the stop
  timeout and fails closed immediately instead of granting a fresh budget.
- **Recommendation:** clear the budget whenever the withheld sequence resolves by any path, in
  `dispose()`, and on reconnect; or key it to the withheld sequence.
- **Confidence:** verified from source.

### DS-18 — Stop-at-latest can declare TIMED_OUT although the final flush made progress

- **Evidence:** `AeronArchiveReader.java:643` calls `extendStopDeadline()` **before** the blocking
  `assembler.flushDeliveries()` at `:647-648`; `runPollingLoop` re-checks `timedOut` after the
  poll returns (`AeronReaderLifecycle.java:60-63`).
- **Scenario:** the flush blocks in `awaitApplied` past the remaining deadline; it advances
  `lastAppliedSequence`, but the deadline was already extended using pre-flush progress, so the
  loop reports TIMED_OUT and latches a failure for a reader that just made durable progress.
- **Recommendation:** extend the deadline after `flushDeliveries()` (or have the flush update the
  progress counters before the timeout check).
- **Confidence:** verified from source.

### DS-19 — Recorded-position refresher polls the Archive control channel at ~1 kHz and swallows failures

- **Evidence:** `AeronArchiveReader.java:342` `RECORDED_POSITION_REFRESH_MILLIS = 1L`;
  `:556-570` runs a dedicated platform thread calling `recordedPosition.getAsLong()` in a loop
  and silently ignores every `RuntimeException` (`:561-563`).
- **Scenario:** continuously (including during replay and after failure) each iteration issues a
  synchronous Archive control RPC under the shared Archive monitor; a stalled control channel
  blocks each call up to the control timeout and retries back-to-back. Failures never surface, so
  the eventual failure is reported as “Archive recording did not durably cover …”, pointing at the
  writer instead of the control query.
- **Recommendation:** run the refresher only while a live terminal marker is actually withheld,
  back the cadence off to tens of ms, stop it in `fail()`, and attach the last refresh failure as
  a suppressed cause to the stall exception.
- **Confidence:** verified from source.

### DS-20 — Dev mode silently ignores the configured replication role/transport and storage path

- **Evidence:** `NodeSettingsSource.java:233-256` returns `false` for `isProdMode()` unless one of
  three production-only paths is set; `NodeLifecycle.java:156-157` then takes `startDevNode()`,
  which starts an always-writable `guarding` manager and never configures replication, enforces
  the reader role, or applies the reseed gates (`:539-551`). It also never calls
  `prepareEmbeddedStorage`, so `ECLIPSE_DATAGRID_STORAGE_PATH` is ignored although `NodeAssembly`
  and `NodeOptions` document it as authoritative.
- **Scenario:** an operator configures `TRANSPORT=aeron`, `ROLE=reader` and identity but forgets
  `PROD_MODE=true`; the node opens as a writable, unreplicated dev node that ignores the role.
  `status()`/`storageNodeManager()` throw `WrongRoleException` (which also breaks the README
  example that calls `node.status()`).
- **Recommendation:** make dev mode incompatible with an explicitly configured transport/role
  (fail startup or demand `PROD_MODE`), and route the dev start through
  `prepareEmbeddedStorage(storageParentPath().resolve("storage"))`.
- **Confidence:** verified from source.

### DS-21 — External `ReplicationStatus` leaks the `-1` unknown sentinel

- **Evidence:** `ClusterNode.java:170-174` passes `metrics.currentSequence()`/`latestSequence()`
  raw, while only some fields go through `present(...)`; `ReplicationMetrics.java:47-50`
  deliberately returns `-1` for unknown; `ReplicationStatus.java:9-13` claims “No `-1` placeholder
  ever leaks into this record”, and `lagTransactions()` (`:66-67`) turns `-1 - x` into `0`.
- **Scenario:** a reader whose position provider is temporarily unavailable reports a lag of 0
  instead of unknown/failed — monitoring silently declares it healthy and caught up.
- **Recommendation:** make `currentSequence`/`latestSequence` `OptionalLong` (or apply `present()`)
  and adapt `lagTransactions()`; keep the internal `-1` representation behind that mapping.
- **Confidence:** verified from source.

### DS-22 — Backup executor close is a 5 s hard failure for a non-interruptible export

- **Evidence:** `StorageBackupTaskExecutor.java:97` `CLOSE_TIMEOUT_MILLIS = 5_000L`; `:204`
  `task.cancel(false)` (deliberately non-interrupting, `:184-192`); `:207-208` throws when
  `awaitTermination` expires. `NodeLifecycle` gates the Store stage on
  `isRunningBackup()` so corruption is avoided, but close fails.
- **Scenario:** routine full-Store exports take minutes; every node shutdown with a fresh backup
  reports a failure and `backupClosed` stays false, forcing a retry of the whole close.
- **Recommendation:** use a configurable budget sized for exports (or an interrupt policy that
  cannot leave Store-owned buffers), and expose a distinct “export draining, Store close
  deferred” state.
- **Confidence:** verified from source.

### DS-23 — Metadata writes are POSIX-only and fail on Windows

- **Evidence:** `AtomicFileWriter.java:44` defines `OWNER_ONLY` via
  `PosixFilePermissions.asFileAttribute(...)` and `:127-130` always passes it to
  `Files.createTempFile`, converting `UnsupportedOperationException` into `IOException`. The same
  file acknowledges Windows elsewhere (`WINDOWS` at `:39`, `forceDirectory` at `:270`), and
  `FilesystemVolumeBackupBackend.privateDirectoryAttributes` (`:753-760`) probes
  `supportedFileAttributeViews()` instead.
- **Scenario:** on Windows every cursor, checkpoint, and backup-identity write fails, even though
  the rest of the storage layer is portability-aware.
- **Recommendation:** probe `supportedFileAttributeViews()` (as the backup backend does) or create
  with defaults and tighten permissions only when the POSIX view exists.
- **Confidence:** verified from source; the Windows JDK rejection of POSIX initial attributes is
  documented JDK behavior (not executed here).

### DS-24 — Default archive-entry budget allows ~16 M materialized entries

- **Evidence:** `BackupArchiveLimits.java:17-21` — `DEFAULT_MAX_EXTRACTED_BYTES = 1 TiB`,
  `MAX_ENTRY_BUDGET = 1 << 24`; `BackupArchive.listEntries` (`:681-695`) materializes every
  `ZipEntry` plus a `String` name into lists/sets before any budget check.
- **Scenario:** a hostile upload with millions of tiny entries (well under 1 TiB) exhausts heap on
  the startup thread and later inodes during extraction; the Javadoc “cannot exhaust inodes” claim
  is not meaningful at 16 M.
- **Recommendation:** lower the default entry budget to tens of thousands, validate declared entry
  counts as a cheap pre-check, and/or stream-validate without materializing the full list.
- **Confidence:** verified from source.

### DS-25 — One 128 KiB direct staging buffer is allocated and freed per transaction

- **Evidence:** `EnvelopeFramer.java:68` `ByteBuffer.allocateDirect(chunkSize + HEADER_LENGTH)`;
  a framer is created per transaction (`AeronReplicationPublisher.java:294`) and freed via
  `BufferUtil.free` on close (`:229`).
- **Scenario:** sustained commit rates cause native-allocator/Cleaner churn on the hot path even
  though the publisher allows only one pending transaction at a time, so a single reusable buffer
  would suffice.
- **Recommendation:** pool one staging buffer on the publisher, reset per transaction, and release
  only on publisher close.
- **Confidence:** verified from source.

### DS-26 — Maintenance-in-progress is surfaced as `IllegalStateException`, not a retryable failure

- **Evidence:** `AeronReplicationWriteCoordinator.java:494-495`, `:514-515`, `:529-530` throw
  `IllegalStateException("Aeron Archive maintenance is in progress; write admission is closed")`,
  while sibling admission failures use `ReplicationUnavailableException`.
- **Scenario:** a Store write arriving during routine retention maintenance is classified as a
  fatal/persistence error by callers instead of the retryable “transport busy” category.
- **Recommendation:** use `ReplicationUnavailableException` or a dedicated maintenance subtype so
  callers can retry.
- **Confidence:** verified from source.

### DS-27 — Maintenance tasks can outlive close into Store shutdown

- **Evidence:** `NodeMaintenanceScheduler.java:182-185` only logs when the scheduler/worker
  termination budget expires and then sets `closed`; `NodeLifecycle.java:645-648` closes the
  scheduler and later (`:721-737`) shuts the Store with a guard only for the backup executor
  (`:725-726`), not for a still-running `GcWorkaround` (`issueFullCacheCheck`/`issueFullGarbageCollection`).
- **Scenario:** a GC workaround that ignores interruption for longer than 5 s can still call into
  the Store while `embeddedStorageManager.shutdown()` runs. Store behavior under concurrent
  `issue*` during shutdown was not verified.
- **Recommendation:** make the scheduler close report incomplete termination (or expose
  `isStopped()`), and gate the embedded-storage stage the way the backup executor is gated.
- **Confidence:** suspected — timeout/warning race verified; Store tolerance not verified.

### DS-28 — No CI pipeline in the repository

- **Evidence:** `.github/workflows` is absent; commit `41dd6f0` (`deleted`) removed
  `.github/workflows/verify.yml` (41 lines). The `pom.xml` still declares enforcer convergence,
  dependency analysis, javadoc doclint, and crash/soak profiles that no automation now runs.
- **Scenario:** regressions in atomic files, crash matrices, and JPMS descriptor checks can merge
  undetected; the deleted workflow was the only record of the intended gates.
- **Recommendation:** restore the workflow (or an equivalent CI definition) covering
  `mvn verify` plus the `integration`, `crashmatrix`, and `soak` profiles on a schedule.
- **Confidence:** verified from source and git history.

---

## P3 findings

### DS-29 — Backup archive edge cases
- `containsStoragePayload` (`BackupArchive.java:642-643`) accepts a regular **file** named
  `storage` as a complete payload, while restore requires a directory; conflicting publications
  are refused and restore later fails. Require `entry.isDirectory()`.
- Duplicate detection is exact-string only (`listEntries`, `:686`) while extraction normalizes
  paths (`:729`): `manifest` and `./manifest` both pass uniqueness and collide after
  normalization (verified JDK normalization behavior), aborting restore after local storage was
  deleted. Canonicalize entry names before uniqueness/uniquness checks.
- `BackupRestorePolicy.java:131` selects the newest compatible backup and throws if its manifest
  is unreadable, instead of trying the next compatible candidate even though older complete
  backups exist. Catch incomplete-manifest failures and fall back.

### DS-30 — Dead code and stale API surface
- `AeronArchiveReplicationPublisher.clearInFlightFence` (`:876`) and its single implementation
  (`AeronWriterTransport.java:578`) have no production caller; the fail-closed restart path
  (`AeronWriterTransport.java:651`) makes every unproven local write a reseed. Either wire it into
  a provable pre-enqueue rejection path or delete it and document the reseed requirement.
- `AeronReplicationPublisher.releaseReservedSequence` (`:489`), `abandonReservedSequence` (`:502`),
  `failedPrepare` field/getter (`:46`, `:403`), and the `forTests` factories (`:103`, `:121`) have
  no production callers.
- `AeronSettings` still carries Archive-auth leftovers after auth removal (commit `0d70c4f`):
  `MAX_SECRET_FILE_BYTES` (`:50`), `ARCHIVE_PROTOCOL_ID` (`:51`), `READER_ARCHIVE_ACTIONS`
  (`:57`), `WRITER_ARCHIVE_ACTIONS` (`:75`), unused `booleanSetting` (`:509`), and floating Javadoc
  blocks (`:787-826`) describing `authenticatorSupplier`/`credentialsSupplier` methods that no
  longer exist. Module-info and README still reference Archive-control authentication.
- `CloseSequencer.add(String, boolean, Runnable)` (`:55-59`) is unused and freezes readiness at
  build time; remove it or evaluate `ready` at run time.
- `StorageBinaryDataImporter.importOwned` (`:27`) and `ObjectMaterializer.materialize()` (`:82`)
  have no production callers; `importDirect`'s boolean result is ignored by its only caller
  (`ApplyWorker.java:176`), making the API ambiguous.
- `AeronReplicationEnvelope.Envelope` Javadoc omits `wireNonce` (`:594`), and the public
  `EnvelopeView` accessors are largely undocumented.

### DS-31 — Style and API-convention drift
- Inline fully-qualified JDK names: `WriterFencingLease.java:382-383`
  (`java.util.concurrent.atomic.AtomicReference`), `AeronReplicationWriteCoordinator.java:64-65`
  (`...AtomicBoolean`), `TransactionAssembler.java:489` (`java.util.concurrent.Callable`) and
  `:545` (`java.nio.charset.CharacterCodingException`).
- The uncommitted `module-info.java:1` replaces the explicit `peruncs.cluster.api` imports with
  `import peruncs.cluster.api.*;`, hiding the documented export surface for the Javadoc links.
- The uncommitted `AeronRuntime.launchDriver(Context, Path, Supplier)` replaces the previous
  `Optional<Path>` input with a nullable parameter, contrary to the project convention “Optional
  only for method input params”.
- `NodeAssembly` close Javadoc (`:110-112`) says callers may close only one of the two entry
  points, while README and tests treat both as idempotent.

### DS-32 — God classes and test-only hooks on production types
`TransactionAssembler` (1092 lines), `AeronArchiveReader` (1066), `GuardingStorageManager` (1033),
`AeronReplicationPublisher` (1004), `FilesystemVolumeBackupBackend` (900),
`AeronArchiveReplicationPublisher` (880), `AeronWriterTransport` (852), `WriterFencingLease` (841),
`AeronReplicationWriteCoordinator` (838), `NodeAssembly` (797), `NodeLifecycle` (770).
`AeronArchiveReplicationPublisher.publishTransaction` (`:559-562`) and
`purgeSegmentsWhileWritesPaused` (`:613`) are documented as test fixtures but live on the
production type. Recommend extracting sequence/reservation ownership, framer lifecycle,
checkpoint I/O, and lease truth away from the protocol owners, and moving fixtures behind
package-private test seams.

### DS-33 — Cold storage gauge leaves the limit gate open
`StorageUsageGauge.java:62-80` serves `cachedBytes` (initially 0) until the first asynchronous
measurement; `StorageLimitGate.java:62-68` keeps `limitReached` false for a 0 reading. A node
started on an over-limit directory accepts writes until the first refresh. Treat “not yet
measured” as unknown and fail the gate closed, or perform one synchronous measurement at startup.

### DS-34 — ABORT does not validate the buffered transaction's fencing token (asymmetric with COMMIT)
`TransactionAssembler.java:426-430` silently discards the buffered transaction on ABORT and
adopts the new token, while COMMIT (`:522-523`) and `Transaction.add` (`:742`) reject mixed
tokens. Depending on the takeover frame order, a recovered writer's re-sent ABORT succeeds or a
re-sent data chunk permanently fails the reader after restart. Define one rule (ABORT rejects
mismatch, or higher tokens may supersede buffered chunks) and test both orderings.
**Confidence:** asymmetry verified; production ordering suspected.

### DS-35 — Unresolved writer position is exposed as a “resolved” cursor
`AeronPositionProvider.java:78-83` returns `ReplicationCursor.of(..., boundary.sequence(), new byte[0])`
when `recordingId < 0 || position < 0`; consumers decode unconditionally and throw
`IllegalArgumentException` (`AeronArchiveRetention.java:526-537`). Throw
`ReplicationPositionUnavailableException` or make the unresolved state explicit.

### DS-36 — Reader replacement checks the uncertainty marker before disposing the previous reader
`AeronReaderTransport.java:108` validates the on-disk uncertainty marker before
`readers.replace(...)` at `:146` disposes the current reader. A healthy installed reader inside
its marker-open window makes replacement fail with RESEED_REQUIRED. Move the check inside the
replacement (after disposal) or bind the marker to the reader instance.
**Confidence:** ordering verified; production path limited but the API documents replacement.

### DS-37 — Repeated stop-at-latest resets the overall stop cap
`AeronArchiveReader.java:866-871` resets `stopRequestedNanos` on every call, and the overall cap
is derived from it at `:889-892`; repeated requests (e.g. retried backup flows) push the cap
forward without bound. Anchor the cap to the first request or reject a second request while a
stop is in progress.

---

## Coverage appendix

**AGENTS checklist items with no material finding in this pass:** no `ThreadLocal` (only
`ThreadLocalRandom`), no `System.exit`/stdout/stack traces in main, no empty catch blocks, no
TODO/FIXME, no `sun.misc`/`jdk.internal`/`Unsafe` use, no FQN outside the four sites in DS-31,
`Optional<>` used only once (a pattern case) with `OptionalLong` for API absence. Every main
package has a `package-info.java` (18/18), and `module-info.java` exports only `api` and `errors`.
Cluster trust constraints (single writer, no auth, isolated network) are stated consistently in
the module docs; the deletion of Archive-control auth left only stale declarations (DS-30).

**Tests:** 173 test files, 799 `@Test` methods, plus integration, crash-matrix (7 ITs), soak, and
four benchmark classes; Lucene/JVector are exercised by 15 test files.
Coverage gaps implied by these findings: reader disposal with a staged barrier (DS-01); a
power-loss/crash point after restore install but before cursor force (DS-02); writer restart over
a wiped Store with a live checkpoint (DS-03); upload swap between validate and extract (DS-04);
digest budget with lying sizes (DS-05); reaper ordering with a `.lease` file (DS-06); lease
close/renew interleavings (DS-08); index registration concurrent with materialization (DS-13);
per-commit validation scaling benchmark (DS-15); a fake Store manager returning `false` then
`true` from `shutdown()` (verified absent in tests); Windows attribute availability (DS-23).

**Method limits:** no execution was performed, so timing-dependent scenarios (DS-01, DS-08,
DS-13, DS-27) are reasoned from control flow, not reproduced. Reflection-based
`StoreIndexReflection` writes into upstream `volatile` fields (`ClusterIndexMaintenance`) were
flagged by one scan as bypassing volatility; I could not verify a concrete failure within a
joined coordinator section and therefore list it only here as an unverified suspicion with the
recommendation to use a volatile-aware write if those fields must keep their upstream contract.

## Recommended remediation order

1. DS-01, DS-02, DS-03 (data loss / permanent node failure).
2. DS-05, DS-06, DS-08, DS-09 (security/trust and fencing).
3. DS-10, DS-11, DS-12, DS-13, DS-15 (availability and concurrency under load).
4. DS-04, DS-07, DS-17, DS-18, DS-19, DS-22, DS-23, DS-24, DS-28 (robustness/portability/ops).
5. DS-14, DS-16, DS-20, DS-21, DS-25, DS-26, DS-27, then the P3 cleanup groups.
