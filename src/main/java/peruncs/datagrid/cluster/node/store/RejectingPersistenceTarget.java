package peruncs.datagrid.cluster.node.store;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import peruncs.datagrid.cluster.errors.ReaderWriteRejectedException;
import peruncs.datagrid.cluster.storage.binary.StorageBinaryDataImporter;

import static org.eclipse.serializer.util.X.notNull;

/// A persistence target that rejects every application write.
///
/// Readers and backup-readers must reproduce the writer's history through
/// the replication import path, which bypasses this target and uses
/// {@link StorageBinaryDataImporter} directly. Installing this target in the
/// reader's Store foundation turns any locally originated write into a
/// {@link ReaderWriteRejectedException} instead of an unreplicated divergence.
///
/// The target deliberately reports itself writable: a `false` answer would let
/// the Store skip the write silently instead of calling {@link #write(Binary)}, and
/// the divergence would never surface. Every write entry point throws, while
/// target lifecycle calls delegate so storage startup and shutdown are
/// unaffected.
///
/// The write-controller validators are deliberately not overridden: Store's
/// default write-controller predicates derive `validateIsWritable()` and
/// `validateIsStoringEnabled()` from the always-true predicates below, so the
/// defaults already let the write reach {@link #write(Binary)} and be rejected with
/// the read-only domain failure. Only that failure type carries the reader
/// semantics; a bare `IllegalStateException` from a validator would be less
/// precise.
public final class RejectingPersistenceTarget implements PersistenceTarget<Binary> {
    private final PersistenceTarget<Binary> delegate;

        /// Creates a rejecting target around a delegate.
    ///
    /// @param delegate lifecycle delegate, never `null`
    /// @return rejecting target
    public static RejectingPersistenceTarget create(final PersistenceTarget<Binary> delegate) {
        return new RejectingPersistenceTarget(notNull(delegate));
    }

    private RejectingPersistenceTarget(final PersistenceTarget<Binary> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void write(final Binary data) {
        throw new ReaderWriteRejectedException(
                "node role is read-only; application writes are rejected because they would diverge from the writer");
    }

    /// Always returns `true` so the Store routes every write into {@link #write(Binary)},
    /// where it is rejected loudly instead of skipped silently. Store's default
    /// `isStoringEnabled()` delegates here, and both default validators are
    /// no-ops for a writable, store-enabled target.
    ///
    /// @return always `true`
    @Override
    public boolean isWritable() {
        return true;
    }

    @Override
    public void prepareTarget() {
        this.delegate.prepareTarget();
    }

    @Override
    public void closeTarget() {
        this.delegate.closeTarget();
    }
}
