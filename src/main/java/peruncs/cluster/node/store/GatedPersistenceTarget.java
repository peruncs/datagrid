package peruncs.cluster.node.store;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.*;
import org.eclipse.store.storage.types.*;
import peruncs.cluster.errors.*;
import java.util.function.*;

/// Validates the write gate before every raw-target write.
///
/// The persistence manager's raw target otherwise bypasses the storer
/// `commit` gate, so a fluent binary write would escape storage-limit
/// enforcement on writers and application-write rejection on readers.
/// The owning manager is kept because a delegate I/O failure must latch its
/// graph invalidity.
final class GatedPersistenceTarget implements PersistenceTarget<Binary> {
    private final GuardingStorageManager<?> owner;
    private final PersistenceTarget<Binary> delegate;
    private final Runnable writeGate;

    GatedPersistenceTarget(final GuardingStorageManager<?> owner, final PersistenceTarget<Binary> delegate,
                           final Runnable writeGate) {
        this.owner = owner;
        this.delegate = delegate;
        this.writeGate = writeGate;
    }

    @Override
    public boolean isWritable() {
        return this.delegate.isWritable();
    }

    @Override
    public void write(final Binary data) {
        /* The admission gate stays OUTSIDE the delegate failure catch:
         * a gate rejection is a policy refusal, never graph damage. The
         * delegate write itself runs the exclusive section, so a raw
         * fluent write cannot race an application boundary write. */
        this.writeGate.run();
        this.owner.persist(() -> this.delegate.write(data));
    }

    @Override
    public void prepareTarget() {
        this.delegate.prepareTarget();
    }

    /// A borrowed view never owns the live target: closing it must be a
    /// no-op so an application cannot shut the shared Store down through
    /// the persistence-manager adapter.
    @Override
    public void closeTarget() {
    }
}
