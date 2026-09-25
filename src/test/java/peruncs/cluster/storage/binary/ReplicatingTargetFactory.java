package peruncs.cluster.storage.binary;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;

/// Test target factory mirroring the production pattern: wrap the local target
/// so committed binaries reach the distributor while the local write and its
/// channel marks stay untouched.
///
/// Replaces the deleted generic replication target, whose
/// local-commit-then-distribute semantics were the uncoordinated durability
/// path; every production wiring now uses the transport's coordinated factory.
///
/// @param distributor downstream distributor receiving committed binaries
public record ReplicatingTargetFactory(ReplicationPublisher distributor)
        implements java.util.function.UnaryOperator<PersistenceTarget<Binary>> {

    @Override
    public PersistenceTarget<Binary> apply(final PersistenceTarget<Binary> delegate) {
        return new PersistenceTarget<>() {
            @Override
            public void write(final Binary data) {
                data.iterateChannelChunks(Binary::mark);
                try {
                    delegate.write(data);
                } finally {
                    data.iterateChannelChunks(Binary::reset);
                }
                ReplicatingTargetFactory.this.distributor.distributeData(data);
            }

            @Override
            public boolean isWritable() {
                return delegate.isWritable();
            }
        };
    }
}
