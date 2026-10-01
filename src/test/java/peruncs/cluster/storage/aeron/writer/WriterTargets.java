package peruncs.cluster.storage.aeron.writer;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import peruncs.cluster.storage.binary.TypeDictionaryOutbox;
import peruncs.cluster.storage.index.ClusterStoreIndexes;

import java.util.function.BooleanSupplier;
import java.util.function.ToIntFunction;

/// Builds replication targets for tests whose Store access is not under test.
final class WriterTargets {
    /// Treats every commit as carrying the mark, touching indexes and being a bootstrap write.
    static final ToIntFunction<Binary> VALIDATE_EVERYTHING = binary ->
            ClusterStoreIndexes.COMMIT_HAS_REPLICATION_MARK | ClusterStoreIndexes.COMMIT_TOUCHES_INDEXES |
            ClusterStoreIndexes.COMMIT_BOOTSTRAP;

    private WriterTargets() {
    }

    /// Creates a target replicating every write without dictionary staging.
    static AeronStorageBinaryReplicationTarget create(final PersistenceTarget<Binary> delegate,
                                                      final AeronReplicationWriteCoordinator coordinator) {
        return create(delegate, coordinator, null, () -> true);
    }

    /// Creates a target with the given callbacks and no writer-side index check.
    static AeronStorageBinaryReplicationTarget create(final PersistenceTarget<Binary> delegate,
                                                      final AeronReplicationWriteCoordinator coordinator,
                                                      final TypeDictionaryOutbox dictionarySource,
                                                      final BooleanSupplier distributionEnabled) {
        return new AeronStorageBinaryReplicationTarget(delegate, () -> coordinator,
                new AeronStorageBinaryReplicationTarget.TargetCallbacks(dictionarySource,
                        distributionEnabled, null, VALIDATE_EVERYTHING));
    }
}
