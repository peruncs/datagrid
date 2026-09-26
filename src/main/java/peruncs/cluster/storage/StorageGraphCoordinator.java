package peruncs.cluster.storage;

import peruncs.cluster.errors.GraphInvalidatedException;
import peruncs.cluster.storage.binary.ObjectGraphUpdateHandler;
import peruncs.cluster.storage.binary.StorageBinaryDataMerger;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import static org.eclipse.serializer.util.X.notNull;

/// Coordinates object-graph access for one Store instance.
///
/// Replication materialization takes the write side while ordinary
/// application graph access takes the read side: reads proceed concurrently
/// with each other, and a materialization excludes every read until it has
/// finished. The lock is fair, so a steady stream of application reads
/// cannot starve a waiting materialization.
///
/// One coordinator serves exactly one Store. Coordinators for different
/// Stores are fully independent and never serialize each other.
///
/// # Joining the read side
///
/// Application code that touches the graph directly must wrap its access in
/// [#read(Runnable)] or [#read(Supplier)]; the replication merger already
/// routes every materialization through [#write(Runnable)] via its
/// [ObjectGraphUpdateHandler]. A node-owned read path — a query endpoint, a
/// snapshot, a validation scan — joins the same way:
///
/// ```java
/// coordinator.read(() ->
/// {
///     // graph access that must never observe a half-applied replication batch
/// });
/// ```
///
/// The merger's own scans join too, on different sides: view retirement,
/// materialization, validation, and index refresh run on the write side as
/// one section, so joined reads never observe a half-refreshed batch; only
/// the type-dictionary conflict scan runs through [#read(Runnable)] when the
/// merger was built with this coordinator (see
/// [StorageBinaryDataMerger#graphCoordinator()]).
/// The type-dictionary *mutation* deliberately does not use the read side —
/// it runs through the update handler on the write side, because the read
/// side would not exclude a concurrent materialization from the handlers
/// being registered.
///
/// # Paths that do not join, and why
///
/// Queue bookkeeping (cached byte counts, flush flags) and transport cursor
/// reads never touch the object graph, so joining would only add contention;
/// they stay guarded by their own lock. Merger instances built without a
/// coordinator run their scans directly for the same reason there is nothing
/// to join — legacy wiring without a shared lock — while their mutations
/// still flow through the update handler.
///
/// # Fail-closed on a partial update
///
/// A failing [#write(Runnable)] section may have partially applied its
/// mutations: the durable replication cursor stays at the previous boundary,
/// which protects restart recovery, but it cannot undo the in-memory side. The
/// write side therefore latches the graph as invalid *before* releasing the
/// write lock, and every later joined read or write fails with
/// [GraphInvalidatedException] until the node reloads or reseeds its Store
/// image. The latch is conservative by design: the coordinator cannot prove
/// whether the throwing update already mutated state, so ANY failing write
/// section invalidates the graph — callers that must not invalidate must not
/// throw from it (pre-validate outside the section).
///
/// The two write flavors differ ONLY in invalidation ownership:
/// [#write(Runnable)] auto-invalidates a throwing section (replication,
/// persistence entries with uncertain durability), while
/// [#writeExclusive(Supplier)] requires the caller to report a dirty failure
/// explicitly through [#invalidate(Throwable)] — clean application validation
/// failures must not poison a healthy graph. Both flavors reject a
/// read-to-write upgrade immediately: a thread holding only a read hold would
/// deadlock against the fair write lock otherwise.
public final class StorageGraphCoordinator {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    /* Latched while a failed write section still holds the write lock: after
     * release, every coordinated access must fail closed instead of serving a
     * potentially half-applied graph. */
    private final AtomicReference<GraphInvalidatedException> invalidity = new AtomicReference<>();

    /// Creates a fair coordinator for one Store object graph.
    public StorageGraphCoordinator() {
    }

        /// Runs application graph access under the shared read side.
    ///
    /// Concurrent reads overlap; a materialization holding the write side
    /// delays the read until it has finished. Once a write section failed,
    /// reads fail closed: the graph may be partially updated.
    ///
    /// @param action graph access to run
    /// @throws GraphInvalidatedException when a previous write section failed
    public void read(final Runnable action) {
        notNull(action);
        this.lock.readLock().lock();
        try {
            this.ensureValid();
            action.run();
        } finally {
            this.lock.readLock().unlock();
        }
    }

        /// Runs application graph access under the shared read side.
    ///
    /// @param <T>    result type
    /// @param action graph access to run
    /// @return action result
    /// @throws GraphInvalidatedException when a previous write section failed
    public <T> T read(final Supplier<T> action) {
        notNull(action);
        this.lock.readLock().lock();
        try {
            this.ensureValid();
            return action.get();
        } finally {
            this.lock.readLock().unlock();
        }
    }

        /// Runs a graph mutation under the exclusive write side.
    ///
    /// The write excludes every read and every other write until the update
    /// has finished. A throwing update leaves the graph potentially
    /// half-applied, so the coordinator invalidates it before releasing the
    /// lock; later coordinated reads and writes fail closed.
    ///
    /// @param update graph mutation to run
    /// @throws GraphInvalidatedException when a previous write section failed
    public void write(final Runnable update) {
        notNull(update);
        this.rejectReadToWriteUpgrade();
        this.lock.writeLock().lock();
        try {
            this.ensureValid();
            update.run();
        } catch (final RuntimeException | Error failure) {
            /* Latch before the finally releases the write lock: no coordinated
             * read may ever observe the graph this failed section left. */
            this.invalidate(failure);
            throw failure;
        } finally {
            this.lock.writeLock().unlock();
        }
    }

        /// Runs a graph mutation under the exclusive write side and returns its result.
    ///
    /// Same exclusivity and invalidation semantics as [#write(Runnable)].
    ///
    /// @param <T>    result type
    /// @param update graph mutation to run
    /// @return update result
    /// @throws GraphInvalidatedException when a previous write section failed
    public <T> T write(final Supplier<T> update) {
        notNull(update);
        this.rejectReadToWriteUpgrade();
        this.lock.writeLock().lock();
        try {
            this.ensureValid();
            return update.get();
        } catch (final RuntimeException | Error failure) {
            /* Latch before the finally releases the write lock: no coordinated
             * read may ever observe the graph this failed section left. */
            this.invalidate(failure);
            throw failure;
        } finally {
            this.lock.writeLock().unlock();
        }
    }

        /// Returns the latched graph invalidity, or `null` while every write
    /// section so far completed.
    ///
    /// @return invalidity failure, or `null`
    public GraphInvalidatedException graphFailure() {
        return this.invalidity.get();
    }

        /// Runs an application mutation under the exclusive write side.
    ///
    /// Unlike the replication write sections, this path does NOT infer
    /// invalidation from a thrown callback: application code validates before
    /// mutation and must report a potentially dirty failure through
    /// [#invalidate(Throwable)] itself before leaving the section
    /// ([GraphInvalidatedException] documentation explains the rule). The
    /// lock is checked before running and the validity latch still applies.
    ///
    /// Read-to-write upgrades are rejected outright: a thread holding only
    /// this coordinator's read lock would deadlock against the fair write
    /// lock, so the upgrade throws immediately instead of blocking. Nested
    /// writes and reads-inside-writes are reentrant and supported.
    ///
    /// @param <T>    result type
    /// @param update application mutation to run
    /// @return update result
    /// @throws GraphInvalidatedException when the graph was previously invalidated
    /// @throws IllegalStateException     when the calling thread already holds the read lock
    public <T> T writeExclusive(final Supplier<T> update) {
        notNull(update);
        this.rejectReadToWriteUpgrade();
        this.lock.writeLock().lock();
        try {
            this.ensureValid();
            return update.get();
        } finally {
            this.lock.writeLock().unlock();
        }
    }

        /// Runs an application mutation under the exclusive write side.
    ///
    /// Same semantics as [#writeExclusive(Supplier)].
    ///
    /// @param update application mutation to run
    /// @throws GraphInvalidatedException when the graph was previously invalidated
    /// @throws IllegalStateException     when the calling thread already holds the read lock
    public void writeExclusive(final Runnable update) {
        notNull(update);
        this.rejectReadToWriteUpgrade();
        this.lock.writeLock().lock();
        try {
            this.ensureValid();
            update.run();
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    /// Explicitly invalidates the graph: the first cause wins and cannot be reset.
    ///
    /// Called by the application boundary when an application unit of work
    /// reports a potentially dirty state before leaving its exclusive section.
    /// Replication's write sections call this automatically on failure.
    ///
    /// @param cause first observed potentially-dirty failure
    public void invalidate(final Throwable cause) {
        this.invalidity.compareAndSet(null, new GraphInvalidatedException(
                "Store graph may be partially updated after a failed write section; reload or reseed is required",
                Objects.requireNonNull(cause, "cause")));
    }

    /// Reports whether the calling thread currently holds either side.
    ///
    /// Used by the lifecycle: a graph section must never synchronously run
    /// node teardown, which joins workers that themselves take this lock.
    ///
    /// @return `true` when this thread holds the read or write side
    public boolean isHeldByCurrentThread() {
        return this.lock.isWriteLockedByCurrentThread() || this.lock.getReadHoldCount() > 0;
    }

    /// Waits until no graph section is active, without touching validity.
    ///
    /// The close sequencer runs this as its drain stage after application
    /// admission stopped: every reader or writer in flight completes before
    /// teardown proceeds. New admissions arriving from other threads are
    /// rejected by the closed check in their own entry points; writers must
    /// not hold this lock themselves while draining.
    public void drain() {
        this.lock.writeLock().lock();
        this.lock.writeLock().unlock();
    }

    /* ReentrantReadWriteLock never upgrades a read hold to a write hold, so
     * any store/commit round entered while holding a read would block on its
     * own lock forever. Fail immediately instead: replication never holds the
     * read side when it takes the write side. */
    private void rejectReadToWriteUpgrade() {
        if (!this.lock.isWriteLockedByCurrentThread() && this.lock.getReadHoldCount() > 0) {
            throw new IllegalStateException(
                    "read-to-write lock upgrade is not supported; start a write section before holding a read");
        }
    }

    private void ensureValid() {
        final GraphInvalidatedException failure = this.invalidity.get();
        if (failure != null) throw failure;
    }
}
