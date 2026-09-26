package peruncs.cluster.api;

import peruncs.cluster.errors.GraphInvalidatedException;
import peruncs.cluster.errors.ReaderWriteRejectedException;

import java.util.function.Supplier;

/// The application coordination boundary over a node's Store graph.
///
/// One boundary is shared with the replication machinery: replication
/// materialization takes the exclusive write side, so any application read or
/// mutation inside this boundary can never observe a half-applied replicated
/// batch. Obtain it from [ClusterStorageManager#graphBoundary()].
///
/// # Reading
///
/// A read section covers root acquisition, lazy resolution (`root().get()`),
/// and the complete graph traversal, including streams and iterators over live
/// nodes. Do not retain references obtained inside a section and dereference
/// them later outside one: copy or detach whatever travels out. Do not wrap a
/// whole request in a read and then write inside it — that is a read-to-write
/// upgrade, which the fair write lock would deadlock; the boundary instead
/// rejects the upgrade immediately. Nested reads are reentrant.
///
/// # Writing
///
/// A write section is exclusive against reads and other writes. It checks the
/// node's admission state before running the callback, takes no automatic
/// persistence action, and performs no heap rollback: store deliberately
/// (`store(...)`, `storeRoot()`, explicit storer commits) exactly as plain
/// Store code would. A section may perform several explicit commits; they stay
/// separate durable phases. Application writes on a reader or backup-reader
/// node are rejected before the callback runs.
///
/// Lock order: graph boundary first, then domain/keyed locks, registration,
/// last commit; never take the global write lock while holding a domain lock.
/// The boundary's lock guards the calling thread only — sections are not
/// inherited by virtual threads the callback spawns. Nested writes and reads
/// inside a write are reentrant; reads never upgrade.
///
/// Choose the section before binding live context: never read the graph
/// outside its read section, and keep slow I/O and streaming outside both.
///
/// # Failure and dirty state
///
/// A clean callback failure (validation, missing precondition) propagates
/// without poisoning the graph — persistence entry points still propagate the
/// FIRST store failure. When a failure may already have mutated the graph
/// or written undetermined bytes, the application reports it through
/// [#invalidate(Throwable)] before leaving the enclosing write section.
/// Plain applications without dirty tracking should validate before mutation
/// and conservatively invalidate a failing mutation/persistence block before
/// the write section ends. Invalidation latches the first cause, cannot be
/// reset, fails every later coordinated read and both boundary and direct
/// writes, and flips the node's health until the node reloads or reseeds.
public interface GraphBoundary {
    /// Runs a read section over the node graph.
    ///
    /// @param action read-only action over the graph
    /// @throws GraphInvalidatedException when the graph was previously invalidated
    void read(Runnable action);

    /// Runs a read section over the node graph and returns its result.
    ///
    /// The result must contain only detached values, never live graph objects.
    ///
    /// @param <R>    result type
    /// @param action read-only action over the graph
    /// @return the detached result
    /// @throws GraphInvalidatedException when the graph was previously invalidated
    <R> R read(Supplier<R> action);

    /// Runs an exclusive write section.
    ///
    /// Replication materialization and concurrent reads wait outside the
    /// section; reads already in flight finish first (fair ordering). The
    /// callback owns persistence explicitly; nothing is stored automatically.
    ///
    /// @param action mutation section
    /// @throws GraphInvalidatedException       when the graph was previously invalidated
    /// @throws ReaderWriteRejectedException on a reader or backup-reader node
    /// @throws IllegalStateException          on a read-to-write upgrade attempt
    void write(Runnable action);

    /// Runs an exclusive write section and returns its result.
    ///
    /// The result must contain only detached values.
    ///
    /// @param <R>    result type
    /// @param action mutation section
    /// @return the detached result
    /// @throws GraphInvalidatedException       when the graph was previously invalidated
    /// @throws ReaderWriteRejectedException on a reader or backup-reader node
    /// @throws IllegalStateException          on a read-to-write upgrade attempt
    <R> R write(Supplier<R> action);

    /// Reports a potentially dirty failure observed inside a write section.
    ///
    /// Call this before leaving the section when a callback's failure may
    /// already have mutated the graph or committed unknown bytes. The first
    /// reported cause wins; later calls drop theirs. Invalidation cannot be
    /// reset without reloading or reseeding the Store image.
    ///
    /// @param cause first observed potentially-dirty failure
    void invalidate(Throwable cause);
}
