package peruncs.datagrid.cluster.storage.types;

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
public final class StorageGraphCoordinator {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    /// Creates a fair coordinator for one Store object graph.
    public StorageGraphCoordinator() {
    }

        /// Runs application graph access under the shared read side.
    ///
    /// Concurrent reads overlap; a materialization holding the write side
    /// delays the read until it has finished.
    ///
    /// @param action graph access to run
    public void read(final Runnable action) {
        notNull(action);
        this.lock.readLock().lock();
        try {
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
    public <T> T read(final Supplier<T> action) {
        notNull(action);
        this.lock.readLock().lock();
        try {
            return action.get();
        } finally {
            this.lock.readLock().unlock();
        }
    }

        /// Runs a graph mutation under the exclusive write side.
    ///
    /// The write excludes every read and every other write until the update
    /// has finished.
    ///
    /// @param update graph mutation to run
    public void write(final Runnable update) {
        notNull(update);
        this.lock.writeLock().lock();
        try {
            update.run();
        } finally {
            this.lock.writeLock().unlock();
        }
    }

        /// Runs a graph mutation under the exclusive write side.
    ///
    /// @param <T>    result type
    /// @param update graph mutation to run
    /// @return update result
    public <T> T write(final Supplier<T> update) {
        notNull(update);
        this.lock.writeLock().lock();
        try {
            return update.get();
        } finally {
            this.lock.writeLock().unlock();
        }
    }
}
