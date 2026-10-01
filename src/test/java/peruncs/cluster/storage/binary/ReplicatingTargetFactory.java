package peruncs.cluster.storage.binary;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;

import java.util.function.BiConsumer;

/// Test target factory mirroring the production pattern: wrap the local target
/// so each committed binary reaches the sink together with the type dictionary
/// staged for it, while the local write and its channel marks stay untouched.
///
/// Replaces the deleted generic replication target, whose
/// local-commit-then-distribute semantics were the uncoordinated durability
/// path; every production wiring now uses the transport's coordinated factory.
///
/// @param outbox type dictionaries staged by the Store's dictionary exporter
/// @param sink   receives the staged dictionary (or `null`) and the committed binary
public record ReplicatingTargetFactory(TypeDictionaryOutbox outbox, BiConsumer<String, Binary> sink)
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
                ReplicatingTargetFactory.this.sink.accept(ReplicatingTargetFactory.this.outbox.consume(), data);
            }

            @Override
            public boolean isWritable() {
                return delegate.isWritable();
            }
        };
    }
}
