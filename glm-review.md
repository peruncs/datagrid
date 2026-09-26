# KIMI-REVIEW.md — Full code review of `peruncs.cluster`

Static analysis only. No builds, junit tests, or benchmarks were run; no
code was modified. Scope: working tree (uncommitted delta) plus the current
main-tree state, reviewed against the AGENTS.md rule list. Line references
are to the current working tree.

Section A consolidates the correctness findings from the 11-issue review
round that are **still open in the tree**. Section B contains new findings
from this full sweep, keyed to the AGENTS.md rules they violate.

---

## A. Open correctness findings (from the 11-issue round)

### A1 [P1] Interruption during coordinator relock still breaks the lock-ownership invariant
`AeronReplicationWriteCoordinator.notifyStateOutsideAdmission`
(AeronReplicationWriteCoordinator.java:732-748)

The timeout path is fixed (`failClosed()` + blocking relock, original failure
preserved as suppressed). The **interrupt path is not**: when
`lockInterruptibly()` throws `InterruptedException`, the method throws
`ReplicationUnavailableException` *without the admission hold*. The callers'
cleanup then runs unlocked: `markCommittingUncertain`'s `finally` calls
`clearActiveWrite()` → `writeDone.signalAll()` without the lock →
`IllegalMonitorStateException` **thrown from the finally**, which discards —
does not even suppress — the unavailable that carried the original checkpoint
failure. The outer admission unlock then IMSEs the same way. This is exactly
the failure-masking the original finding required to prevent.

**Fix:** re-acquire with an uninterruptible park loop, restoring the interrupt
flag only after ownership is back:

```java
private void relockAfterFailedJournalWrite() {
    boolean interrupted = false;
    while (true) {
        try {
            this.writeLock.lockInterruptibly();
            break;
        } catch (final InterruptedException e) {
            interrupted = true;
        }
    }
    if (interrupted) Thread.currentThread().interrupt();
}
```

Call it in the `!relocked` branch after `failClosed()`, then throw a single
`unavailable` (built once, with the original failure suppressed — also
removes the current duplication of the two identical throw blocks).
**Test:** interrupt the relock path and assert (1) the caller's finally
performs no unlocked mutation, (2) the original failure survives as
suppressed, (3) no IMSE is thrown.

### A2 [P1] `backupRunning` latches `true` before `submit` — permanent close deadlock
`StorageBackupTaskExecutor.Default` (StorageBackupTaskExecutor.java:139)

`this.backupRunning = true` is set *before* `backupExecutor.submit(...)`. If
`submit` throws (`RejectedExecutionException` after executor shutdown), or
the task is queued but never starts, the body's `finally` never runs:
`isRunningBackup()` returns `true` forever, the Store-close stage throws
"backup still running" on every retry, and the node can never close.

**Fix:** move the flag set to the first statement inside the task's `try`,
and wrap the `submit` call:

```java
this.backupTask = null;
try {
    this.backupTask = this.backupExecutor.submit(() -> {
        this.backupRunning = true;   // flipped only when execution truly begins
        try { ... } finally { this.backupRunning = false; }
    });
} catch (final RuntimeException submissionFailure) {
    this.backupRunning = false;
    throw submissionFailure;
}
```

Keep `backupTask != null && !backupTask.isDone()` as a secondary check so a
*queued* (not yet executing) backup still defers Store teardown.
**Test:** export held beyond the close timeout → close fails, retry after the
export exits succeeds; executor-rejected submit → close still possible.

### A3 [P2] Boundary reads: admission checked only before the lock, drain is a no-op latch
`GuardingStorageManager.newApplicationBoundary` (read wrappers) and
`StorageGraphCoordinator.drain()` (StorageGraphCoordinator.java:267-270)

Writes now re-check admission inside the exclusive section (`persist()`,
boundary `write`). Reads do not: the wrapper calls `ensureOpen()` *outside*
the lock, and `drain()` is a bare write-lock acquire/release. A thread that
passed `ensureOpen` before close began can acquire the read lock *after* the
drain stage released it and traverse while the embedded Store shuts down
underneath — the exact interleaving finding #7 described.

**Fix:** make the drain observable. `drain()` sets a `volatile boolean
drained`; every coordinator entry (`read(Runnable)`, `read(Supplier)`,
`write(...)`, `writeExclusive(...)`) fails with `IllegalStateException` once
it is set. The merger — the only internal lock user — is disposed in an
earlier close stage, so no node-internal path is affected, and the check is a
single volatile read on the read side. **Test:** pause a reader thread
between the outer admission check and lock acquisition; close the node;
assert the late read rejects instead of traversing a dead Store.

### A4 [P2] Per-commit index validation still traverses unrelated collections without a work budget
`ClusterIndexValidation.enqueueReachable`
(ClusterIndexValidation.java:388-405 and callers)

Unchanged since the finding: writer-side validation iterates every element of
every reachable `Iterable`/`Map`/array on *every distributed write*, and the
work budget counts dequeued objects, not visits. The #9 fix
(non-final concrete types are never pruned, ClusterIndexValidation.java:508)
widened the conservative set, so a single small transaction against a root
with a million-element holder collection now costs *more* than before.

**Fix:** account element visits in the budget (count them against
`maxValidatedIndexObjects`), and cache per-root topology classification so
unchanged holders are not re-walked per commit. **Benchmark:** constant
transaction size, growing unrelated collection; throughput must not decay
proportionally (AeronReplayBenchmark is the template).

### A5 [P2] Writer recovery: two failure paths journal different terminal states
`AeronStorageBinaryReplicationTarget.prepareWrite`
(AeronStorageBinaryReplicationTarget.java:162-184)

`RuntimeException` from `delegate.write` now records `COMMITTING_UNCERTAIN`
and preserves the fence — correct. The `Error` catch (:162-167) abandons the
token *without* marking, leaving the journal at `PREPARING`. Restart fails
closed either way, but the same hazard class (enqueue succeeded, outcome
unknown) produces two different recovery states, and restart diagnostics must
understand both.

**Fix:** either mark `COMMITTING_UNCERTAIN` on the Error path too (marking
itself may fail on OOM — wrap the attempt and suppress), or document in the
restart-recovery code that both `PREPARING`-with-fence and
`COMMITTING_UNCERTAIN` mean "uncertain local write". **Test (still missing
from the original finding):** a fake Store target that *enqueues* and then
throws (Store's enqueue-then-interrupt semantics), asserting UNCERTAIN — the
current tests use delegates that throw before doing any work, which the
original finding explicitly called insufficient.

### A6 [P2] `mayCreateRoot` name collision carries opposite meanings
`BackupRestorePolicy` constructor parameter (BackupRestorePolicy.java:59,
wired at NodeAssembly.java:633-642) vs `NodeAssembly.mayCreateRoot()`
(NodeAssembly.java:653).

On `BackupRestorePolicy` the flag means "this node *is* the authoritative
writer"; on `NodeAssembly` the same name means "may manufacture a fresh
root". A reader node can satisfy the second while violating the first. This
is a live trap for the next maintainer wiring a new startup path.

**Fix:** rename the policy field/parameter to `authoritativeWriter`
(`ownsAuthoritativeImage`). The requested end-to-end restart scenario
(backup at sequence N, acknowledged local writes through N+K survive
restart) is also still only proxied by a marker-file unit test — add an IT
with a real Store+cursor image ahead of the newest backup.

### A7 [P3] Missing tests the previous round explicitly required

| Original finding | Required test | Status in tree |
|---|---|---|
| #2 | enqueue-then-complete fault injection | absent |
| #3 | blocked async import across disposal timeout | absent |
| #5 | export held beyond close timeout | absent |
| #6 | interruption during reacquisition + original-failure preservation | absent (and the path is broken, see A1) |
| #7 | interleaving between outer check and lock acquisition | absent |
| #8 | delegate false-then-true Store shutdown | absent |
| #11 | scaling benchmark | absent |

---

## B. New findings from the full sweep

### B1 [rule 21] `TransactionAssembler` mixes three monitors on one object
TransactionAssembler.java:236 (`synchronized (this.delivery)`), :263
(`synchronized (this)`), :637/:652 (`synchronized` instance methods), :689
(`synchronized (this)`), :932/:953 (`synchronized (barrierLock)`).

Three distinct monitor roles (delivery object, the assembler itself, and a
dedicated `barrierLock`) coexist on one class. Package-private `synchronized`
*methods* (`deliveryBarrierFull()`, `hasIncompleteTransaction()`) expose the
assembler's own monitor to every caller in the package, so any future
`synchronized (assembler)` elsewhere silently shares the ingestion monitor.
Rule 21 hazards (missed signals, accidental lock sharing) grow directly from
this.

**Fix:** one private final lock object per concern — `deliveryLock` around
the delivery barrier and a separate `ingestLock` if truly needed — and no
`synchronized` methods; static private helpers taking the lock object
explicitly. Prefer Agrona/Eclipse Serializer `LockedExecutor` for the
bounded-acquisition spots (rule 27).

### B2 [rule 12] God files: five central classes exceed 850 lines

| File | Lines |
|---|---|
| `storage/aeron/reader/TransactionAssembler.java` | 1092 |
| `storage/aeron/reader/AeronArchiveReader.java` | 1066 |
| `node/store/GuardingStorageManager.java` | 1033 |
| `storage/aeron/writer/AeronReplicationPublisher.java` | 1004 |
| `node/backup/FilesystemVolumeBackupBackend.java` | 900 |

`GuardingStorageManager` alone carries six roles in one file: the facade
delegation, `GuardedDatabase`, `BinaryPersistenceManagerAdapter`,
`ClusterStorerAdapter`/`ClusterPersistenceStorerAdapter`,
`GatedPersistenceTarget`, and the boundary wrapper. The inner classes are
already cohesive units — extract them to package-private top-level types
(`GuardedDatabase`, `GuardedPersistenceManager`, `GuardedStorer`,
`GuardedPersistenceTarget`) so each is independently constructible and
unit-testable against a proxy delegate. Same split applies to
`TransactionAssembler` (barrier assembly vs. dictionary handling vs. buffer
management) and `AeronArchiveReader` (replay control vs. reconnection vs.
watermark plumbing).

### B3 [rule 16] 14-positional-argument constructor; codebase already has the better pattern
`AeronArchiveRetention` constructor — 14 positional parameters
(AeronArchiveRetention.java:84-99): two `UUID`s, four lambdas, a `long`, a
`Path`, and a `BooleanSupplier` in sequence. The test file shows the
resulting unreadable call sites (`..., CLUSTER, GENERATION, 1, () -> 1MB,
() -> 8MB, () -> true, state, 60_000L)`). `AeronPositionProvider` follows
with 9.

`StorageBinaryDataMerger.Configuration` and `StorageNodeManager.Configuration`
already prove the house pattern: a record input. These are startup-only
wirings, so GC pressure (the rule's caveat) does not apply.

**Fix:** collapse each to `record Configuration(...)` and a single-arg
constructor; it also gives the tests a named-object constructor.

### B4 [rule 2] `NodeClose` is no longer a close callback — it grew admission
`NodeClose` (NodeClose.java) now carries `close()` **and** `checkOpen()`, and
`GuardingStorageManager.ensureOpen()` consults `nodeClose.checkOpen()`
(GuardingStorageManager.java). The name says "close", the responsibility is
"node lifecycle handle". The second abstract method also silently ended its
`@FunctionalInterface` career.

**Fix:** either rename to `NodeLifecycleHandle` (still internal), or better:
split — keep `NodeClose` as the functional teardown callback and pass the
admission check as a separate internal `Runnable checkOpen` to the
`ClusterStorageManagers.guarding(...)` factory. Two single-method inputs beat
one dual-role interface for the same test surface.

### B5 [rule 23] Blind cast regression in the startup failure path
`NodeLifecycle.start` failure rethrow (NodeLifecycle.java:170):
`throw (RuntimeException) failure;` — the defensive
`instanceof Error / instanceof RuntimeException / wrap-in-NodeException`
chain was removed in the round-5 commit. The cast is safe only while the
enclosing catch stays `RuntimeException | Error`; any future widening of the
catch turns every startup failure into a `ClassCastException` at the point
where the original cause matters most. This is the same fragile pattern that
was already hardened in `closeNode`.

**Fix:** restore the three-branch rethrow
(`Error` as-is, `RuntimeException` as-is, otherwise `new NodeException("Cluster node startup failed", failure)`),
mirroring `closeNode`'s waiter handling.

### B6 [rules 14/24] Malformed javadoc indentation across three files; one first sentence deleted by the uncommitted diff

- `AeronCheckpointCodec.FrameReader.readByte` (AeronCheckpointCodec.java:138-141):
  the uncommitted edit deleted the first sentence ("Reads one byte and
  advances past it."), leaving an orphaned `///` + `@return` block. Restore
  it — the first sentence is the rule's most important one.
- `StorageGraphCoordinator`: persistent 8-space javadoc indents
  (`        /// Runs application graph access...`) at lines 83, 102, 119,
  144, 168, 176, 210, 225 — the markdown-javadoc rendering is broken for
  most methods of the concurrency-critical class.
- `NodeSettingsSource.java:73-80`: duplicated `/// Reports whether this node
  restores backups.` block over `isBackupNode()` (one copy orphaned), plus
  `@since 1.0` mid-description.
- `StoreIndexReflection.java`: double blank line after
  `record VectorGraphFields` and stray indent on its javadoc.

**Fix:** normalize all `///` blocks to column 4/8 consistently and re-run a
javadoc lint pass; these are mechanical but they are the module's
documentation surface.

### B7 [rules 20/29/34] Hot-path allocation and lookup nits on the writer's application path

1. `GuardingStorageManager.database()` (GuardingStorageManager.java:139)
   allocates a **new `GuardedDatabase` per call**, and every
   `GuardedDatabase` method re-resolves `delegate.database()` on each
   invocation. Cache the instance in the constructor (the pattern already
   used for `graphBoundary` and the `persistenceManager` adapter) and store
   the delegate `Database` in a field.
2. `persist(...)` (GuardingStorageManager.java) allocates one capturing
   lambda per `store(instance)` / `storeAll(...)` call — the hottest
   application write path. Non-capturing method refs (`this.delegate::storeRoot`)
   are free; the capturing ones can be avoided by inlining the
   validate/lock/recheck/try-catch shape into the two or three hottest
   methods. Low priority (young-gen allocation), but rule 29 says prove it
   or avoid it — a comment with that argument would also do.
3. Boundary `write(Runnable)` still chains two capturing lambdas
   (boundary wrapper → `writeExclusive(Supplier)` → coordinator). Add the
   missing `writeExclusive(Runnable)` primitive on the coordinator that
   locks directly (the coordinator already has the runnable overload for
   `write`).
4. The global graph write lock serializes all application writes on the
   single writer node — accepted as a documented tradeoff, but there is no
   benchmark pinning it. Add a writer-throughput benchmark (boundary write +
   explicit commit, N threads) so the "initial serialization tradeoff to
   measure" actually gets measured.

### B8 [rule 28] Aeron authenticator wiring needs an explicit constraint statement
`ECLIPSE_DATAGRID_AERON_AUTH_ENABLED` (NodeSettingsSource `Env.EnvKeys`,
:AERON_AUTH_ENABLED) plus `AeronSettings.authenticatorSupplier()` wired into
both the media driver and archive contexts (AeronRuntime). AGENTS.md forbids
"node authentication features" for this cluster. If this is strictly
Aeron-driver-level `ChallengeResponse` (transport access control on an
untrusted network) and not node identity, the module javadoc and
`module-info` should say so in one sentence, because today the constraint and
the setting appear to contradict. If it is unused in any supported
deployment, remove it (dead attack surface).

### B9 [rule 11] `storage` package mixes graph coordination with wire-protocol plumbing
`peruncs.cluster.storage` holds `StorageGraphCoordinator` (application
locking domain), `ReplicationCursor`/`ReplicationRetry` (replication
domain), `Crc32C` and `io/` (binary plumbing). The application boundary type
is implemented by a class sitting next to CRC utilities. Minor, but the
"graph coordination" concept deserves its own package (`cluster.graph`) so
the lock domain is not importable alongside wire internals.

### B10 [cosmetic, uncommitted diff]
- `AeronRuntime.launchDriver`: the new 3-argument overload has exactly one
  caller; fold it away and pass `null` explicitly (rule 8). Its
  `@param archiveMarkFile` doc still says "`null` when the node runs without
  an embedded Archive" — correct after the rework, but the 4-arg overload's
  doc says the same thing twice; keep one wording.
- `AeronWriterRecoveryBoundary.java`: 8-space indent on
  `/// Fails closed unless a stopped Archive ends exactly at this durable
  boundary.` carried over from the deleted file; the class javadoc still
  reads "writer terminal boundary" while the type is now the recovery
  boundary — align the narrative.

---

## Priority order

1. **A1** (relock interrupt path) and **A2** (backup flag) — new
   liveness/masking defects introduced by the fix round; both are small,
   surgical changes.
2. **A3** (read-side drain latch) — last open half of the shutdown-race
   family.
3. **B5** (startup blind cast) — one-line restoration of an already-proven
   pattern.
4. **A4** (traversal budget) — performance finding now amplified by the #9
   conservative widening; needs the benchmark.
5. **B1/B2/B3** (monitors, god files, 14-arg constructor) — structural debt
   concentrated on the replication hot path; schedule as refactors with
   test-extraction (B2) rather than one-off edits.
6. **A5–A7, B4, B6–B10** — smaller correctness, documentation, and polish
   items in any order.
