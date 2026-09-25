# ES.md — Eclipse Store-Compatible Application API for PerunCS Cluster

Design change: promote the guarded Store facade to a public, exported
`StorageManager`-compatible application API, matching the compatibility level
Eclipse DataGrid offers through its `ClusterStorageManager`, while keeping
PerunCS cluster constraints, fail-closed semantics, and all construction
internal.

Status: reviewed design. Only this plan is being corrected; no Java code
changed and no builds or tests run.

The library is general-purpose, like Eclipse DataGrid. Plain Store callers,
explicit-storer users, application-owned units of work, and framework-managed
consumers are all first-class clients. TourBiz is one integration example,
not a dependency or the definition of the public API.

---

## 1. Goal, compatibility level, and acceptance

### 1.1 Compatibility level

This design delivers **`StorageManager` / `Persister` compatibility**:

```
PersistenceStoring -> Persister -> (StorageConnection) -> StorageManager
                                                     ^
                        peruncs.cluster.api.ClusterStorageManager<T>
```

PerunCS `ClusterStorageManager<T>` extends `org.eclipse.store.storage.types.StorageManager`,
which extends `StorageConnection` (extends `UsageMarkable`, `Persister`),
so the `PersistenceStoring` and `Persister` operations (`store`, `storeAll`,
`storeRoot`, `createStorer`, ...) are inherited by every consumer holding a
`StorageManager`-typed reference. `persistenceManager()` hands out the adapted
`PersistenceManager<Binary>` chain (existing `BinaryPersistenceManagerAdapter`,
gated storers, gated target).

**Explicitly not claimed:** `EmbeddedStorageManager` subtype compatibility.
Eclipse DataGrid does not establish it either — its `ClusterStorageManager`
wraps a delegate `StorageManager` (created from an `EmbeddedStorageFoundation`).
Consumers declare common `StorageManager` (or `Persister`) types.

### 1.2 Acceptance criterion

General acceptance: applications retain their Store-based persistence code
and change bootstrap, common manager declarations, root handling, graph
coordination, and lifecycle. No PerunCS-specific persistence runtime is
required. As one concrete acceptance example, TourBiz changes only:

1. bootstrap (build `NodeOptions`, open `ClusterNode`),
2. common `StorageManager` declarations in place of `EmbeddedStorageManager`,
3. root handling (obtain/traverse the root inside the public graph boundary),
4. lifecycle (shutdown through either entry point),
5. application coordination wrapping comparable to Eclipse DataGrid's
   application locking.

TourBiz's persistence runtime (`EclipseStoreWriteRuntime`), durable scopes
(`DurableWriteScope`), explicit multi-phase commits, GigaMaps, and domain
commands remain **unchanged**.

### 1.3 Out of scope / deferred

- No `EmbeddedStorageManager` subtype, no `EmbeddedStorage` static-factory
  mimicry (a `ClusterStorage.open` adds a forwarding layer without adding
  configuration parity — rejected).
- No `ClusterStorageManagerProvider` SPI; deferred until a framework
  integration module (Micronaut/Spring/Helidon) is actually needed.
- No change to cluster constraints: 1-writer/N-reader, no node authentication,
  no transport encryption, no application imports, no raw `PersistenceTarget`
  writes that bypass gates.

---

## 2. Verified current state and gaps

| # | Gap | Evidence |
|---|-----|----------|
| G1 | The `StorageManager`-compatible facade exists but is not exported; the application gets `ClusterStore`, a non-Store type | `module-info.java:186` exports only `peruncs.cluster.api` and `errors`; `ClusterNode.java:73-75` returns `ClusterStore`; `ClusterStore.java` re-delegates 1:1 to the internal manager |
| G2 | Reader `root()` and `viewRoots()` throw `UnsupportedOperationException` — read operations, incompatible with drop-in use | `ReadOnlyStorageManager.java:54-68` |
| G3 | `writeRoot()` persists implicitly (`delegate.storeRoot()` after the callback) and its javadoc claims an "atomic mutation" boundary; explicit multi-phase storer commits (TourBiz `EclipseStoreWriteRuntime.commit`) cannot use it | `GuardingStorageManager.java:316-337`, especially `:334`; `EclipseStoreWriteRuntime.java:55-83` |
| G4 | `ClusterStorageManager`'s static factories construct package-private `GuardingStorageManager`/`ReadOnlyStorageManager` and `graphCoordinator()` exposes the non-exported `StorageGraphCoordinator` — the interface as written cannot move to `peruncs.cluster.api` | `ClusterStorageManager.java:34-88,138`; impls at `GuardingStorageManager.java:32`, `ReadOnlyStorageManager.java:30` |
| G5 | `NodeAssembly.Builder.rootSupplier` is `Supplier<Object>` (`NodeAssembly.java:52`); a generic `<T> startStorageManager()` would let the caller invent an unrelated `T` | `NodeAssembly.java:52,113`; unchecked cast at `ClusterNode.java:57-58` |
| G6 | Failure behavior is inconsistent: `writeRoot()` latches graph invalidation through `coordinator.write(...)` catch, but direct `store()`, `storeAll()`, `storeRoot()`, and `Storer.commit()` only `validateState()` and delegate — no latch | `GuardingStorageManager.java:233-254` and `:633-636` vs `StorageGraphCoordinator.java:124-162` |
| G7 | Lifecycle: the manager is documented as a borrow; `ShutdownCallback` is a no-op because `NodeLifecycle.close` owns teardown (`NodeLifecycle.java:243-245`), so a DI container calling `manager.shutdown()` (or default `close()`, `StorageController.java:175`) shuts the raw Store down **outside** the close sequencer, racing replication and backup stages | `NodeAssembly.java:102-107`; `ClusterStoreLifecycle.java:40-68`; `NodeLifecycle.java:530-647` |

Existing capabilities that are kept (not gaps): typed `Lazy<T> root()` on
writers (`GuardingStorageManager.java:306-314`), the full
`PersistenceManager`/`PersistenceStorer`/`Storer` adapter chain
(`GuardingStorageManager.java:369-705`), the write/limit/raw-target gates,
reader write rejection, and `GraphInvalidatedException` already in the
exported `peruncs.cluster.errors` package.

---

## 3. Design decisions

### D1 — Public application contract; construction stays internal

- New public interface `peruncs.cluster.api.ClusterStorageManager<T> extends StorageManager`
  containing **only** the application contract: typed root, covariant start,
  and the public graph boundary accessor. No static factories, no nested
  `ShutdownCallback`/`StorageSizeValidation` (construction policy, stays
  internal), no `graphCoordinator()` (exposes a non-exported type).
- New public interface `peruncs.cluster.api.GraphBoundary` exposing application
  coordination over the same underlying lock used by replication. Its write
  admission is role-aware; it is not the raw replication coordinator.
- Implementations (`GuardingStorageManager`, `ReadOnlyStorageManager`) stay
  package-private in `peruncs.cluster.node.store`, constructed through a new
  public factory (`ClusterStorageManagers`) in the non-exported `node.store`
  package. Its methods must be public because `NodeLifecycle` is in `node`.
  `StorageGraphCoordinator` stays internal; the manager supplies a role-aware
  boundary backed by it. Replication must still write the graph on readers,
  while application mutation must reject a reader before invoking its callback.

### D2 — Ordinary root access on every role, guarded by the public boundary

New roots are wrapped as `Lazy<T>` by `NodeLifecycle.initializeRoot`.
Existing roots are currently accepted without validating that shape, and
`setRoot` accepts arbitrary objects. Establish and enforce the Lazy-root
contract rather than assuming it already holds: validate existing root
representation on startup; reject incompatible data clearly without deleting
or silently migrating it. The public inherited `setRoot` accepts only a
non-null Lazy wrapper on a writer under the write boundary, preserving
Store's return semantics; reject other shapes before mutation. A supplier
cannot prove the runtime type of existing data under Java erasure.

The documented representation for supported cluster roots is:

- `root()` returns the live `Lazy<T>` on **writers and readers alike**; the
  reader overrides that throw (`ReadOnlyStorageManager.java:54-68`) are
  deleted. `viewRoots()` likewise delegates.
- **Contract** (application locking shared with replication, Eclipse
  DataGrid-style): on reader/backup-reader nodes, every graph traversal —
  `root().get()` and everything reachable from it — must run inside
  `graphBoundary().read(...)`. The replication merger already takes the write
  side of the same coordinator (`ObjectGraphUpdateHandler` →
  `StorageBinaryDataMerger` configured with `NodeAssembly.graphCoordinator`,
  `NodeAssembly.java:481,599,621`), so a traversal inside the read section can
  never observe a half-applied replication batch.
- Writers also join the boundary for reads racing application mutations.
  Raw Java references cannot enforce that discipline; isolation applies only
  to participating accesses. Retained references must only be dereferenced
  inside a later section; detached results can be used outside it.
- The read lock is a fair `ReentrantReadWriteLock` read lock, reentrant per
  thread, so nested `read(...)` sections inside one request join cheaply.

### D3 — Coordinated mutation boundary with no implicit persistence

- `graphBoundary().write(...)` is the mutation boundary: exclusive section,
  **no implicit `storeRoot()` or other automatic persistence**. The application
  persists explicitly (storer commits, `store(...)`), exactly as
  `EclipseStoreWriteRuntime.commit` does today.
- The old `writeRoot(Function)` (implicit `storeRoot()` at
  `GuardingStorageManager.java:334`) and `readRoot(Function)` are **deleted**
  together with `ClusterStore`. The "atomic mutation" javadoc claim is
  removed: locking provides isolation; storing the root does not automatically
  persist every modified existing descendant — persistence is whatever the
  application explicitly commits.

### D4 — Failure admission and dirty-state responsibility

- Every persistence entry checks current lifecycle and graph validity, plus
  applicable role/size gates. Cached storers and persistence adapters use
  those checks too. Latching only boundary reads while allowing subsequent
  direct stores is insufficient.
- A pre-delegation rejection does not itself prove corruption. If the caller
  already mutated the heap, its dirty-failure handling must still invalidate.
- Delegate persistence failures are conservatively potentially uncertain;
  latch invalidity before releasing exclusive ownership and rethrow the
  original failure. This is not proof that every exception wrote bytes.
- Keep invalidate-on-failure for internal replication materialization.
  Application `write` is a lock/admission boundary: an ordinary callback
  validation failure need not poison a healthy graph. A caller must report
  a potentially dirty failure through `GraphBoundary.invalidate(cause)` before
  leaving the section. Plain Store callers validate before mutation and
  conservatively invalidate on mutation-block failures; an existing unit of
  work connects its dirty/fatal callback. No TourBiz runtime is required.
- Audit storer registration failures and metadata/registry mutations by their
  actual effects; not all are durable writes, and not all are harmless reads.
  Preserve existing gates and cleanup (`clear`) after failure.
- Invalidation is permanent for the node instance, affects health/admission,
  and blocks later direct writes as well as sections. Arbitrary Java
  references cannot be revoked. Preserve the existing acknowledged-write,
  fencing, and archive protocol.

### D5 — Single shutdown owner; both entry points coordinated and idempotent

- `NodeLifecycle.close()` (the `CloseSequencer` graph, `NodeLifecycle.java:530-647`)
  remains the **only** teardown owner.
- The manager's `shutdown()` (and default `close()`) becomes a trigger that
  runs the full node close via an installed internal `NodeClose` callback —
  so DI containers and `try`-with-resources that own a `StorageManager`
  tear the complete node down in the correct order.
- The sequencer's final stage no longer routes through the facade (avoiding
  recursion); it shuts the raw embedded manager down directly.
- `NodeLifecycle.close()` becomes idempotent **and** concurrency-safe:
  re-entry from the owner thread returns; a concurrent second thread waits
  for the running close and then returns (or rethrows its recorded failure);
  a retry after a failed stage re-runs only the stages still owing work
  (existing per-stage completion semantics).
- Stop admission and drain active work before teardown; do not join workers
  while holding a graph lock. Reject synchronous close from an application
  graph section and arrange fatal teardown after it unwinds. Never restart
  raw Store through the facade after the node closes.
- `ClusterStoreLifecycle` and `ShutdownCallback` are deleted.

### D6 — Bootstrap typing: one justified cast

- Keep `ClusterNode.open(NodeOptions<T>)` as the only public bootstrap.
- `NodeOptions<T>` is the typed boundary; retain the single internal unchecked
  cast at `ClusterNode.open` (today `ClusterNode.java:57-58`) with a written
  justification comment. Do **not** make `startStorageManager()` generic
  (`NodeAssembly.java:52` stores `Supplier<Object>`; a caller-chosen `<T>`
  would be unsound).
- `NodeOptions` gains only the configuration hooks actual consumers need
  (any embedding application): an optional `EmbeddedStorageFoundation`
  (Store tuning, custom type handlers, supported backup configuration)
  and an optional `NodeSettingsSource` (programmatic configuration without
  environment variables). These are general embedding hooks. If moving
  `NodeSettingsSource` to `api`, remove its internal `NodeRole` return type
  from the public contract and keep role resolution in node code; audit all
  other signatures, imports, and nested implementation dependencies.

### D7 — Delete `ClusterStore`

`ClusterStore` is a 1:1 re-delegation of the manager
(`ClusterStore.java:45-137`: `withRootRead→readRoot`, `withRootWrite→writeRoot`,
`store/storeAll/storeRoot` pass-through). After D3 its two closure methods
disappear anyway; `ClusterNode.storageManager()` returns the real facade.
One less layer, and the returned object is the drop-in type.

### D8 — SPI deferred

No provider SPI in this change. Revisit when a framework integration module
is requested (mirror Eclipse DataGrid's
`ClusterStorageManagerProvider` + `@Replaces(StorageManager.class)` factory
pattern then).

---

## 4. Precise code changes

All paths relative to `src/main/java` unless noted. Javadoc style: `///`
as used throughout the codebase. Package visibility is the default;
`public` only where marked.

### 4.1 NEW `peruncs/cluster/api/GraphBoundary.java`

```java
public interface GraphBoundary {
    void read(Runnable action);
    <R> R read(Supplier<R> action);
    void write(Runnable action);
    <R> R write(Supplier<R> action);
    void invalidate(Throwable cause);
}
```

Add imports and complete Markdown Javadocs covering these obligations:

- A read covers root acquisition, lazy loading, and complete graph traversal,
  including streams/iterators. Returning a live reference does not authorize
  later uncoordinated dereference.
- A write checks open/healthy/writer admission before callback execution and
  after acquiring exclusivity. Preserve authoritative fencing at commit.
  No automatic persistence or heap rollback occurs.
- Nested reads and writes, and reads inside writes, are supported on the same
  thread. Reject a read-to-write upgrade immediately instead of deadlocking.
  Locks are thread-owned and are not inherited by spawned virtual threads.
- Lock order is graph boundary, domain/keyed locks, registration, commit.
  Never take the global write lock while holding a domain lock.
- A callback may perform several explicit commits; they remain separate
  durable phases. Ordinary clean exceptions do not imply invalidation.
  `invalidate` reports potentially dirty state, latches the first cause,
  and cannot be reset. See D4.
- Choose a read or write section before live context binding. Never wrap all
  requests/jobs in a read section and then invoke writes inside it. Keep
  slow I/O and streaming outside locks when detached results permit it.

These are generic execution-boundary rules, not a required HTTP integration.
The global writer lock is an initial serialization tradeoff to measure.

### 4.2 `peruncs/cluster/storage/StorageGraphCoordinator.java`

Keep the existing coordinator and lock shared with replication. Do not
implement the public application boundary directly: internal reader
replication needs exclusive writes that the application facade must reject.
Keep the existing invalidation-on-exception behavior for replication.
Provide an internal application locking path with explicit invalidation as
specified in D4 and 4.1. Detect read-to-write upgrades before blocking.

Make invalidation callable across internal packages, preserving first-cause
latching and null validation. The manager's public `GraphBoundary` delegates
to this operation. Also make graph-validity checks available internally for
all exposed persistence entry points, including previously obtained adapters.

### 4.3 NEW `peruncs/cluster/api/ClusterStorageManager.java` — the application contract

```java
package peruncs.cluster.api;

import org.eclipse.serializer.reference.Lazy;
import org.eclipse.store.storage.types.StorageManager;

/// Guarded Store facade for application access on a cluster node.
///
/// A `StorageManager`-compatible application facade with explicit cluster
/// restrictions. Common persistence interfaces and adapted storers are
/// available; role, root, coordination, and lifecycle rules still apply:
///
/// - reader and backup-reader nodes reject every application write with
///   [peruncs.cluster.errors.ReaderWriteRejectedException], so a reader can
///   never persist an unreplicated local divergence;
/// - a writer at its configured storage limit rejects writes with
///   [peruncs.cluster.errors.StorageLimitReachedException] until space is
///   restored;
/// - application imports are reserved for node-owned replication and
///   bootstrap paths and throw `UnsupportedOperationException`;
/// - a persistence failure with an uncertain durable outcome latches the
///   graph: later coordinated sections fail with
///   [peruncs.cluster.errors.GraphInvalidatedException] until the node
///   reloads or reseeds.
///
/// # Root and coordination
///
/// The root is always a [Lazy] reference. On readers, obtain and traverse
/// it inside [GraphBoundary#read]; wrap a writer's mutation *and* its
/// explicit persistence in [GraphBoundary#write]. The boundary persists
/// nothing by itself.
///
/// # Lifecycle
///
/// [shutdown] performs the complete, ordered node teardown (replication,
/// backups, Store) and is idempotent; closing via the owning
/// [ClusterNode] is equivalent.
///
/// @param <T> root type
public interface ClusterStorageManager<T> extends StorageManager {
    /// Returns the live root reference of this node's graph.
    ///
    /// The root is registered as a `Lazy<T>`; materialize it with
    /// `root().get()`. On reader nodes the whole traversal must run inside
    /// [GraphBoundary#read] — see [#graphBoundary()].
    ///
    /// @return live lazy root reference
    @Override
    Lazy<T> root();

    /// Starts the manager; the manager returned by [ClusterNode#open] is
    /// already started, so this returns immediately.
    ///
    /// @return this manager
    @Override
    ClusterStorageManager<T> start();

    /// Returns the graph coordination boundary shared with replication.
    ///
    /// @return boundary for coordinated reads and mutation sections
    GraphBoundary graphBoundary();
}
```

Notes:

- No static factories, no `ShutdownCallback`, no `StorageSizeValidation`, no
  `readRoot`/`writeRoot`, no `graphCoordinator()`. The inherited
  `PersistenceStoring`/`Persister`/storer/persistence-manager surface is the
  compatible API.
- `Lazy<T> root()` overriding `StorageManager`'s `<R> R root()` is the same
  covariant override the implementation already compiles with today
  (`GuardingStorageManager.java:306-314`), mirroring Eclipse DataGrid's
  `ClusterStorageManager.root()`.

### 4.4 DELETE `peruncs/cluster/node/store/ClusterStorageManager.java`; NEW internal factory and types

Delete the old interface. Its surviving contents move as follows:

| Old member (file:line) | Destination |
|---|---|
| `static create(...)` / `static ReadOnly(...)` (34-88) | public factory in non-exported `node.store` (below) |
| `interface ShutdownCallback` (144-165) | deleted — replaced by `NodeClose` (D5) |
| `interface StorageSizeValidation` (167-180) | public internal interface in non-exported `node.store` |
| `readRoot`/`writeRoot` (107, 130) | deleted (D3) |
| `graphCoordinator()` (138) | deleted; public contract uses `graphBoundary()` |

These construction types need Java `public` visibility for callers in
`node`, but remain outside the exported API. Prefer substituting JDK
`BooleanSupplier` for the two simple callbacks during implementation if that
removes these types without reducing clarity; do not export `node.store`.

**NEW** `peruncs/cluster/node/store/StorageSizeValidation.java`:

```java
package peruncs.cluster.node.store;

/// Reports whether the configured storage limit has been reached.
/// Construction policy: wired by the node assembly, never application API.
public interface StorageSizeValidation {
    /// Reports whether another Store write must be rejected.
    ///
    /// @return `true` when the limit is reached
    boolean isStorageLimitReached();

    /// Returns a validation that never rejects, for read-only managers.
    ///
    /// @return validation that never reports the limit as reached
    static StorageSizeValidation notReached() {
        return () -> false;
    }
}
```

**NEW** `peruncs/cluster/node/store/NodeClose.java`:

```java
package peruncs.cluster.node.store;

/// Internal callback running the complete, ordered node teardown.
///
/// Implemented by the owning node lifecycle; invoked when an application
/// shuts the manager down through the public manager's shutdown method.
@FunctionalInterface
public interface NodeClose {
    /// Runs the node close; `true` when this call performed the teardown,
    /// `false` after observing another caller's successful close; concurrent
    /// callers wait for their close attempt and propagate its failure.
    boolean close();
}
```

**NEW** `peruncs/cluster/node/store/ClusterStorageManagers.java`:

```java
package peruncs.cluster.node.store;

import org.eclipse.store.storage.types.StorageManager;
import peruncs.cluster.api.ClusterStorageManager;
import peruncs.cluster.storage.StorageGraphCoordinator;

import static org.eclipse.serializer.util.X.notNull;

/// Internal construction of guarded cluster storage managers.
///
/// Construction policy is not application API: the node lifecycle is the
/// only caller and supplies the storage-limit validation, the node-close
/// owner, and the coordinator shared with replication.
public final class ClusterStorageManagers {
    private ClusterStorageManagers() {
    }

    /// Creates a write-gated manager for writer nodes.
    public static <T> ClusterStorageManager<T> guarding(
            final StorageManager delegate,
            final StorageSizeValidation storageSizeValidation,
            final NodeClose nodeClose,
            final StorageGraphCoordinator graphCoordinator) {
        return new GuardingStorageManager<>(
                notNull(delegate), notNull(storageSizeValidation),
                notNull(nodeClose), notNull(graphCoordinator));
    }

    /// Creates a read-only manager for reader and backup-reader nodes.
    public static <T> ClusterStorageManager<T> readOnly(
            final StorageManager delegate,
            final NodeClose nodeClose,
            final StorageGraphCoordinator graphCoordinator) {
        return new ReadOnlyStorageManager<>(
                notNull(delegate), notNull(nodeClose), notNull(graphCoordinator));
    }
}
```

### 4.5 `peruncs/cluster/node/store/GuardingStorageManager.java` — rework

#### 4.5.1 Type and constructor

- Change declaration to
  `class GuardingStorageManager<T> implements peruncs.cluster.api.ClusterStorageManager<T>`
  (import it; drop the now-unneeded `peruncs.cluster.node.store` self-type).
- Replace the `ClusterStoreLifecycle lifecycle` field and constructor
  parameter `ShutdownCallback shutdownCallback` with
  `private final NodeClose nodeClose;`.
- Keep `storageSizeValidation`, `delegate`, `graphCoordinator`, and the
  shared `LazyConstant<PersistenceManager<Binary>>` adapter exactly as-is
  (`GuardingStorageManager.java:33-55`).

#### 4.5.2 Lifecycle methods (D5)

`shutdown()` triggers the owning node's full close and returns whether this
caller completed teardown. Concurrent callers observe their close attempt's
completion/failure; they do not silently return while teardown is running.

`start()` is an idempotent check for an already open node. It must not call
raw `delegate.start()` to resurrect Store after node close or invalidation.
Reject closing, closed, and invalid states. Initial startup remains owned by
the node lifecycle. Nested `persistenceManager().close()` remains a borrowed
view operation, not a route to close the raw Store.

#### 4.5.3 Boundary accessor (D2/D3)

Replace `graphCoordinator()` with `graphBoundary()`, returning a cached,
role-aware application boundary backed by the same concrete coordinator.
Keep the coordinator private for replication wiring. Application writes reject
reader roles before invoking the callback; internal replication writes do not
use that application admission gate. Do not allocate a new wrapper per call.

#### 4.5.4 Delete `readRoot`/`writeRoot` (D3)

Delete `GuardingStorageManager.java:316-356` entirely. The half-materialized
read protection and the exclusive mutation section live in the boundary
(4.1/4.2); the "must not return the live root" escape check dies with the
closure API — the boundary contract replaces it, and the application's copy-out
discipline is application-level as it already is today.

#### 4.5.5 Root access (D2)

Keep the delegation in `root()` (`:306-314`) for all roles after adding
current lifecycle/graph-validity checks and the D2 root-shape validation (the reader override that
throws is deleted in 4.6). Update its javadoc to the public contract:

```java
/// Returns the live lazy root reference of this node's graph.
///
/// On readers the whole traversal must run inside
/// [GraphBoundary#read]; writer graph access also participates in the
/// documented read/write coordination contract.
@Override
@SuppressWarnings("unchecked") // fixing the inherited type variable to this Store's root type is sound for typed managers
public Lazy<T> root() {
    return this.delegate.root();
}
```

#### 4.5.6 Uniform failure model (D4)

Add one private helper and wrap every persistence delegation listed below.
The helper does not capture a lambda:

```java
/* Conservatively treat a delegated persistence failure as potentially
 * uncertain — bytes may already be written locally or offered for
 * replication. Latch the graph so every later coordinated section fails
 * closed until the node reloads or reseeds. Gate rejections thrown by
 * validateState() never reach this method. */
private void reportPersistenceFailure(final Throwable failure) {
    this.graphCoordinator.invalidate(failure);
}
```

The following is only the exception-handling shape. It is not a complete
implementation: first check lifecycle/graph validity, and run the whole call
under the shared exclusive graph section (joining an existing write section
reentrantly). Reject persistence from a read-only section rather than upgrade.
Recheck admission after lock acquisition. Standalone calls protect only the
call, not prior field mutations. Ensure registration/traversal and commit
share the application's write section.

Keep pre-delegation gates outside the delegate-failure catch:

```java
@Override
public long store(final Object instance) {
    this.validateState();
    try {
        return this.delegate.store(instance);
    } catch (final RuntimeException | Error failure) {
        this.reportPersistenceFailure(failure);
        throw failure;
    }
}
```

Initial write-path audit list (current line numbers). Include custom storer
factories and fluent adapter returns; verify all reachable paths. Metadata
methods require effect-specific handling, not an assumption that every
exception is an uncertain durable commit:

| Method | Line |
|---|---|
| `store(Object)` | 233 |
| `storeAll(Object...)` | 239 |
| `storeAll(Iterable<?>)` | 245 |
| `storeRoot()` | 251 |
| `ClusterStorerAdapter.commit()` | 633 |
| `BinaryPersistenceManagerAdapter.store(Object)` | 445 |
| `BinaryPersistenceManagerAdapter.storeAll(Object...)` | 451 |
| `BinaryPersistenceManagerAdapter.storeAll(Iterable<?>)` | 457 |
| `BinaryPersistenceManagerAdapter.updateMetadata(...)` | 498 |
| `BinaryPersistenceManagerAdapter.updateCurrentObjectId(long)` | 533 |
| `GatedPersistenceTarget.write(Binary)` — wrap only `this.delegate.write(data)`; the `writeGate.run()` stays outside the `try` | 730-733 |

`ClusterPersistenceStorerAdapter` inherits the gated `commit()` from
`ClusterStorerAdapter` — no extra change.

The following are not automatically classified as uncertain durable writes;
they still require lifecycle/graph validity and the relevant coordination,
role, cleanup, and effect-specific checks: `setRoot` (in-memory only, `:216-219`),
`ensureObjectId*`/`createRegisterer`/registerer adapters (registry-only,
reader-safe by the existing design), all `issue*` maintenance, exports,
statistics, `collect`/`lookup*`/`get`/`getObject` reads,
`createLoader`, and the import paths (they reject before delegation via
`rejectApplicationImport()`).

#### 4.5.7 Adapter classes

`BinaryPersistenceManagerAdapter`, `ClusterPersistenceRegistererAdapter`,
`ClusterPersistenceStorerAdapter`, `ClusterStorerAdapter`,
`GatedPersistenceTarget` retain their delegation structure but must all obey
the new live admission and failure contract. Do not promise byte-identical
adapters. In particular, old storers/targets cannot continue writing after
invalidation, and registration failures need safe cleanup/dirty handling.

### 4.6 `peruncs/cluster/node/store/ReadOnlyStorageManager.java`

1. Constructor signature: replace the `ShutdownCallback` parameter with
   `NodeClose`:

   ```java
   ReadOnlyStorageManager(final StorageManager delegate,
                          final NodeClose nodeClose,
                          final StorageGraphCoordinator graphCoordinator) {
       super(delegate, StorageSizeValidation.notReached(), nodeClose, graphCoordinator);
   }
   ```

2. **Delete the `root()` and `viewRoots()` overrides** (`:54-68`) — D2.
   Both now inherit the guarding implementation, returning the live lazy
   root / delegating to the delegate's roots view. Update the class javadoc:
   reads (including `root()`, `viewRoots()`, `persistenceManager()` reads)
   work on readers; every *durable mutation* is rejected; graph traversal
   belongs inside `graphBoundary().read(...)`.

3. `validateState()` (`:38-41`), `rejectApplicationImport()` (`:44-47`), and
   `gateTarget(...)` (`:50-52`) retain reader rejection. Shared lifecycle and
   graph-validity checks must still run; an override must not bypass them.

### 4.7 DELETE `peruncs/cluster/node/store/ClusterStoreLifecycle.java`

Its staged-callback-then-Store logic is replaced by the `NodeClose` trigger
and the sequencer's raw-Store stage (4.8). Delete the file.

### 4.8 `peruncs/cluster/node/NodeLifecycle.java` — shutdown ownership

#### 4.8.1 Manager creation sites pass the real close

Replace `ClusterStorageManager.ShutdownCallback.noOp()` with `this::closeNode`
at all three sites:

- backup reader (`:246-247`),
- storage node writer/reader (`:375-383`),
- dev node (`:511-515`).

New call shape (writer branch shown):

```java
this.assembly.clusterStorageManager = writer
        ? ClusterStorageManagers.guarding(
                embeddedStorageManager,
                this.assembly.getStorageSizeValidation(),
                this::closeNode,
                this.assembly.graphCoordinator)
        : ClusterStorageManagers.readOnly(
                embeddedStorageManager,
                this::closeNode,
                this.assembly.graphCoordinator);
```

(If the size validation is currently built inline at the site, keep the
existing wiring expression — only the callback argument changes.)

#### 4.8.2 Coordinated close attempts

Keep one lifecycle owner. Implement the following invariants using existing
coordination facilities where possible, rather than prescribing a new
monitor-based wait/notify implementation:

- Stop new application admission before teardown and drain admitted work.
  Do not hold a graph lock while joining workers that may need that lock.
- Concurrent callers observe the result of the close attempt they joined.
  A subsequent retry must not overwrite that attempt's failure before its
  waiters can observe it. Retry outstanding stages; preserve completed work.
- Required deferred stages (for example a still-running backup) prevent
  successful close and clearing resource references. A skipped predicate
  alone is not evidence that a resource was closed.
- Reject synchronous close from an application graph section before beginning
  teardown, rather than waiting on the caller itself. A dirty-failure callback
  invalidates immediately and arranges teardown after the section unwinds.
  Account for node-owned workers that cannot join their own termination.
- Preserve partial-startup cleanup and original failures. Both node and
  manager entry points follow the same rules.

Do not claim that simply adding `wait()` around today's close body proves
these properties. Cover close/failure/retry and work-draining interleavings
with simulation tests.

#### 4.8.3 Sequencer final stage: raw Store shutdown only

Replace the two final stages (`:620-626`):

```java
/* 5. Close the Store last. A backup that outlived its executor budget must
 * block this stage instead of losing the race to a shutdown Store. The raw
 * embedded manager is shut down here — never through the facade, whose
 * shutdown() now triggers this whole close. */
.add(CloseSequencer.stage("embedded storage",
        () -> collaborators.embeddedStorageManager != null,
        () -> {
            if (backupTaskExecutor != null && backupTaskExecutor.isRunningBackup()) {
                throw new IllegalStateException("Store close deferred: backup still running");
            }
            collaborators.embeddedStorageManager.shutdown();
        }));
```

The facade owns no independent teardown sequence: nothing in the sequencer calls it, so no
recursion through `shutdown()` → `closeNode()` exists. The old comment at
`:243-245` ("shutdown callback is a no-op by design") is replaced everywhere
by the new wiring comment:

```java
/* The manager's shutdown() triggers this lifecycle's complete close, so a
 * DI container or try-with-resources owning the StorageManager tears the
 * whole node down in the sequencer's order. */
```

#### 4.8.4 Shared admission

Preserve `ensureOpen()`'s rejection of closed, closing, and failed-close states;
connect equivalent current lifecycle admission to the manager and its retained
adapters. Also reject invalidated graph state. A closed facade must not reopen
its raw Store independently of the node.

### 4.9 `peruncs/cluster/node/NodeAssembly.java`

- `ClusterStorageManager<?> startStorageManager()` (`:113`) stays
  **public within the non-exported interface**, returning a wildcard (D6) — only `ClusterNode.open` performs the
  typed handoff. Update its javadoc (`:102-107`): the returned manager's
  `shutdown()` now triggers the assembly's complete close; the assembly still
  owns the lifecycle and `close()` remains valid for the embedder.
- No other signature changes. `NodeCollaborators` keeps its
  `StorageGraphCoordinator graphCoordinator` field (`:191`) and the merger
  wiring (`:481,599,621`) untouched.

### 4.10 `peruncs/cluster/api/ClusterNode.java`

1. Replace `store()` (`:73-75`) with:

   ```java
   /// Returns the guarded, Store-compatible storage manager owned by this node.
   ///
   /// The manager is a drop-in [org.eclipse.store.storage.types.StorageManager]:
   /// common Store interfaces remain available under the documented role,
   /// root, coordination, and lifecycle restrictions.
   /// Reads and mutations on the graph join [peruncs.cluster.api.GraphBoundary];
   /// its `shutdown()` performs the complete node teardown, as does closing
   /// this node.
   ///
   /// @return the guarded Store facade owned by this node's lifecycle
   public ClusterStorageManager<T> storageManager() {
       return this.storage;
   }
   ```

   Change the field to `private final ClusterStorageManager<T> storage;`
   (rename from `store`), and `new ClusterStore<>(storage)` at `:59` becomes
   just `storage`:

   ```java
   return new ClusterNode<>(assembly, storage, assembly.nodeRole());
   ```

2. Keep `open(...)` (`:51-68`) with the existing cast, adding the justified
   suppression comment (D6):

   ```java
   /* NodeOptions<T> is the typed boundary: the root supplier's type fixes
    * the intended manager root type; existing disk data must also satisfy
    * the documented schema/root contract. The supplier alone cannot prove
    * a deserialized payload's type. The assembly stores Supplier<Object> because
    * the assembly is role-generic, so this single cast is the one place
    * where the application's type meets the assembly's erasure. */
   @SuppressWarnings("unchecked")
   ```

3. Update the class javadoc: the node owns the lifecycle and hands out the
   `StorageManager`-compatible facade; no `ClusterStore` remains.

### 4.11 DELETE `peruncs/cluster/api/ClusterStore.java`

Deleted (D7). All its documentation value moves into the public
`ClusterStorageManager`/`GraphBoundary` javadocs (4.1, 4.3): the reader
rejection types, the fencing and availability failures, and the
uncertain-commit guidance now documented on the interface itself.

### 4.12 `peruncs/cluster/api/NodeOptions.java` — configuration hooks

Extend the record with general embedding configuration hooks (D6), keeping `of(...)`
source-compatible for existing callers:

```java
package peruncs.cluster.api;

import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;

import java.util.Objects;
import java.util.function.Supplier;

/// Immutable application-owned inputs for opening one cluster node.
///
/// Transport, role, paths, security, retention, and operational limits are
/// read from the node settings source — environment-backed by default —
/// and validated before startup.
///
/// @param rootSupplier               creates a root for an empty Store
/// @param embeddedStorageFoundation  optional Store foundation with custom
///                                   tuning, type handlers, or backup setup;
///                                   the node always derives the live file
///                                   provider from the configured storage path
/// @param nodeSettingsSource          optional programmatic settings source,
///                                   replacing environment variables
/// @param <T>                         root type
public record NodeOptions<T>(
        Supplier<? extends T> rootSupplier,
        EmbeddedStorageFoundation<?> embeddedStorageFoundation,
        NodeSettingsSource nodeSettingsSource) {

    /// Validates the immutable node options.
    public NodeOptions {
        Objects.requireNonNull(rootSupplier, "rootSupplier");
    }

    /// Creates options using the default Store foundation and environment settings.
    ///
    /// @param <T>          root type
    /// @param rootSupplier creates a root for an empty Store
    /// @return immutable node options
    public static <T> NodeOptions<T> of(final Supplier<? extends T> rootSupplier) {
        return new NodeOptions<>(rootSupplier, null, null);
    }

    /// Returns these options with a custom Store foundation.
    ///
    /// @param foundation Store foundation for node startup
    /// @return new options with the foundation set
    public NodeOptions<T> withEmbeddedStorageFoundation(final EmbeddedStorageFoundation<?> foundation) {
        return new NodeOptions<>(this.rootSupplier, foundation, this.nodeSettingsSource);
    }

    /// Returns these options with a programmatic settings source.
    ///
    /// @param settings programmatic node settings
    /// @return new options with the settings source set
    public NodeOptions<T> withNodeSettingsSource(final NodeSettingsSource settings) {
        return new NodeOptions<>(this.rootSupplier, this.embeddedStorageFoundation, settings);
    }
}
```

`ClusterNode.open` forwards both optional hooks into
`NodeAssembly.create()` (builder setters already exist:
`NodeAssembly.java:79-90`):

```java
final NodeAssembly.Builder builder = NodeAssembly.create()
        .setRootSupplier(options.rootSupplier()::get);
if (options.embeddedStorageFoundation() != null) {
    builder.setEmbeddedStorageFoundation(options.embeddedStorageFoundation());
}
if (options.nodeSettingsSource() != null) {
    builder.setNodeSettingsSource(options.nodeSettingsSource());
}
```

A reader's application facade is read-only; its underlying Store remains
writable for replication/bootstrap. Do not install globally read-only Store
controllers merely because the node is a reader. Validate foundation tuning
against node-owned replication requirements. A supplied mutable foundation
belongs to one node startup; document overridden settings and forbid sharing
it concurrently across nodes.

### 4.13 MOVE `peruncs/cluster/node/NodeSettingsSource.java` → `peruncs/cluster/api/NodeSettingsSource.java`

Applications must implement it (its javadoc already promises that:
"embedding applications may implement this interface directly"), so it moves
to the exported package:

1. Change `package peruncs.cluster.node;` → `package peruncs.cluster.api;`.
2. Keep useful environment behavior, but audit the move: `nodeRole()` returns
   the internal `NodeRole`, so move that resolution into node code rather
   than exposing an inaccessible type. Update package-dependent nested `Env`
   implementation references and Javadocs too.
3. Update imports in every internal consumer — grep
   `peruncs.cluster.node.NodeSettingsSource` across
   `cluster.node`, `cluster.node.aeron`, `cluster.node.backup` (also
   `Env`/`EnvKeys` references) and switch to the new package.
4. `NodeAssembly.Builder.setNodeSettingsSource` keeps its type; only the
   import changes.

### 4.14 `module-info.java` and package docs

- No implementation package export is needed (`peruncs.cluster.api` is
  exported). Verify transitive readability of public Store/Serializer types
  with an external JPMS consumer; classpath compilation is insufficient.
- Update the module javadoc (`:12-19`): the exported `cluster.api` package
  now contains the `StorageManager`-compatible application contract
  (`ClusterStorageManager`, `GraphBoundary`) in addition to `ClusterNode`.
- `peruncs/cluster/api/package-info.java`: document the drop-in contract,
  the boundary discipline, and the deviation list (imports rejected,
  uncertain-commit fail-closed) as the first thing an Eclipse Store user
  reads.
- `peruncs/cluster/node/store/package-info.java`: construction policy only.
- `README.md`: replace the `node.store().withRootRead(...)` example with the
  new surface:

  ```java
  // Schematic example: DataRoot must supply a real no-argument factory and
  // be the changed collection. Own node lifetime with try-with-resources.
  ClusterNode<DataRoot> node = ClusterNode.open(NodeOptions.of(DataRoot::new));
  ClusterStorageManager<DataRoot> storage = node.storageManager();

  int seen = storage.graphBoundary().read(() -> storage.root().get().size());

  long id = storage.graphBoundary().write(() -> {
      var root = storage.root().get();
      try {
          root.add(entry);
          return storage.store(root); // persist the changed collection itself
      } catch (RuntimeException | Error failure) {
          storage.graphBoundary().invalidate(failure);
          throw failure;
      }
  });
  ```

### 4.15 `peruncs/cluster/errors` — no changes

`GraphInvalidatedException`, `ReaderWriteRejectedException`,
`StorageLimitReachedException`, `WriterFencedException`,
`ReplicationUnavailableException`, `NodeException` already exist and are
exported. The public interface javadocs reference them.

---

## 5. General integration and TourBiz acceptance example

| Consumer area | Integration |
|---|---|
| Plain Store application | Select cluster bootstrap; pass the guarded manager as `StorageManager` or `Persister`; use ordinary stores under the shared boundary |
| Explicit storer application | Register changed objects and commit explicitly inside write sections; no implicit final commit and no PerunCS persistence runtime |
| Root handling | Cluster roots are Lazy wrappers. Handle standalone plain-root versus cluster Lazy-root differences in application wiring; common `StorageManager.root()` cannot simply be followed by `.get()` without establishing its type |
| Existing unit of work | Wrap its execution in the boundary and connect its dirty/fatal callback to invalidation; clean validation errors may propagate without poisoning |
| Framework/DI consumer | Supply public bootstrap configuration and dispose the manager through its full-node shutdown; framework modules/SPI stay deferred |
| Background and diagnostics | Join the same boundary for jobs, live queries, browser tools, and index traversal |

Do not wrap every request in `read` and then call `write`: that is a lock
upgrade and can deadlock. Choose the boundary before binding and using live
context. Obtaining a root under a short lock and using it outside the lock is
not safe. Keep graph-backed streams/iterators inside their section.

TourBiz is one example, not the API's defining consumer. Its existing
`EclipseStoreWriteRuntime` receives the manager as `Persister` unchanged.
Wrap `runInWriteScope` in `boundary.write`; compose its existing fatal callback
to invalidate before leaving the section and perform teardown after unwind.
Its `DurableWriteScope`, explicit phases/checkpoints, GigaMap-first registration,
and domain commands stay unchanged. Update common manager declarations,
bootstrap/root handling, background/request coordination, and lifecycle.

Use TourBiz's existing component-building root factory: `TourBizDataRoot`
is a record with required components, so `TourBizDataRoot::new` is not a
no-argument supplier. Bind its existing writer-authority assertion appropriately
at bootstrap; a status snapshot does not replace authoritative commit fencing.
These application-specific details do not enter PerunCS's core contracts.

For plain applications without dirty tracking, validate before mutation and
conservatively invalidate a failing mutation/persistence block before releasing
the write section. Document and test this without any TourBiz dependency.

## 6. Test coverage plan

Migrate (`src/test/java`):

- `peruncs/cluster/api/ClusterStoreWriteBoundaryTest.java` → rewrite as
  `ClusterStorageManagerBoundaryTest`: mutation via
  `graphBoundary().write` + **explicit** `createStorer`/`store`/`commit`;
  assert with an instrumented delegate that `storeRoot()` is never invoked
  implicitly; reader rejection still flows from the same calls.
- `peruncs/cluster/node/store/StorageWriteGatingTest.java`,
  `ClusterStorageManagerShutdownTest.java`,
  `RejectingPersistenceTargetTest.java`, `StorageLimitGateTest.java`:
  mechanical updates to `ClusterStorageManagers.guarding/readOnly`,
  `StorageSizeValidation`, `NodeClose` (replace `ShutdownCallback.noOp()`
  with `() -> false` or a recording stub).

New coverage (all required by the acceptance criterion):

1. **Reader traversal during replication**: a `StorageBinaryDataMerger`
   write section (coordinator write side) applying a batch concurrently with
   `graphBoundary().read` sections asserting readers never observe
   half-applied state; reader `root()`/`viewRoots()` return rather than
   throw; an active read completes before materialization can take exclusivity;
   a read arriving during materialization waits until it finishes. Application
   write callbacks on readers are rejected before execution.
2. **Explicit multi-phase commits without an extra commit**: inside one
   `write` section run two `DurableWriteScope`-style phases, each
   `createStorer()` → store → `commit()`; a counting delegate proves
   exactly two commits and no implicit `storeRoot()`.
3. **GigaMap persistence**: `gigaMap.store(storer)` + `storer.commit()`
   inside `write` against a real `EmbeddedStorageManager` on a temp
   directory; reload and read back the persisted entities.
4. **Failure invalidation, uniformly**: for each latched path of 4.5.6 —
   `store`, `storeAll(x2)`, `storeRoot`, storer `commit`,
   `persistenceManager().store*`, raw `target().write` — a delegate failure
   propagates and every later `boundary.read` fails with
   `GraphInvalidatedException`; gate rejections (reader role, storage limit)
   before mutation do **not** latch; rejection after caller mutation is
   invalidated by that caller's dirty-failure handler. Cached storers/targets
   and direct calls must reject after invalidation, not only `boundary.read`.
5. **Shutdown through both entry points**: `manager.shutdown()` runs the
   full sequencer (assert stage order: replication transport closed before
   the embedded Store); idempotent second call returns `false`; a concurrent
   `ClusterNode.close()` during an in-flight `shutdown()` waits and
   completes without `IllegalStateException`; a failed stage retries through
   either entry point completing only the remaining stages;
   try-with-resources on the manager closes the node.
6. **Bootstrap hooks**: `NodeOptions.withEmbeddedStorageFoundation` /
   `withNodeSettingsSource` reach the assembly (foundation honored except
   the live file provider, settings honored instead of env). Validate existing
   root shape and setRoot replacement/reload; no silent migration or wiping.
7. **Generic consumers and JPMS**: plain Store calls, Persister injection,
   explicit storers, eager/lazy/custom factories, and application-managed dirty
   failures compile and work without TourBiz dependencies. Add an external
   module consumer to catch inaccessible signatures and missing readability.
8. **Concurrency and failures**: reject lock upgrades promptly; cover clean
   validation after successful phases versus dirty failures, close under a
   graph section, concurrent close/failure/retry, deferred backups, admission
   draining, and attempts to restart a closed manager. Use simulations.
9. **Persistence and indexes**: reload changed existing collections and
   GigaMaps; retain Lucene/JVector consistency tests for replicated updates.
   Preserve acknowledged-write and fencing recovery coverage.

---

## 7. Work order

1. `GraphBoundary` plus internal coordinator application path and role-aware
   adapter (4.1, 4.2); preserve replication invalidation semantics.
2. `StorageSizeValidation`, `NodeClose`, `ClusterStorageManagers`,
   rework `GuardingStorageManager`/`ReadOnlyStorageManager`, delete
   `ClusterStoreLifecycle` and the old internal interface (4.4-4.7).
3. `NodeLifecycle` shutdown ownership (4.8); `NodeAssembly` javadoc (4.9).
4. `ClusterNode.storageManager()`, delete `ClusterStore`,
   `NodeOptions` hooks, move `NodeSettingsSource` (4.10-4.13).
5. Failure model wiring (4.5.6) with its tests (6.4).
6. Migrated and new tests (6).
7. Docs: `module-info`, package-infos, `README.md` (4.14).

These steps have compilation and behavior dependencies. Land the public
surface, factories, adapter admission/failure behavior, lifecycle, and callers
as a coherent change; do not temporarily expose an ungated manager. Remove
ClusterStore atomically with its callers. Verify ordinary Store consumers and
an external JPMS module, with TourBiz as an additional acceptance example.

---

## 8. Compatibility summary vs. Eclipse DataGrid

| Capability | Eclipse DataGrid | PerunCS after this design |
|---|---|---|
| `StorageManager` subtype for applications | `ClusterStorageManager<T> extends StorageManager` | same, exported (`peruncs.cluster.api`) |
| Typed root | `Lazy<T> root()` | `Lazy<T> root()`, validated shape and documented replacement restrictions |
| Full `PersistenceManager`/storer chain | adapter classes in `Default` | existing adapters, gated + failure-latched |
| DI drop-in | `@Replaces(StorageManager.class)` factory | supported: `shutdown()` performs full node teardown; SPI deferred |
| Application locking shared with replication | `ClusterLockScope`-based application locking | role-aware `GraphBoundary` over the same internal coordinator |
| Size limiting | `StorageSizeValidation` | unchanged internal gate |
| Imports through app facade | gated | rejected outright (cluster constraint, documented) |
| `EmbeddedStorageManager` subtype | not provided | not provided |

The Eclipse DataGrid checkout is an API reference, not a binary-compatibility
promise: it pins Store 4.0.1 while this PerunCS checkout uses 5.0.0-SNAPSHOT.
Verify this plan against the pinned Store/Serializer artifacts.
