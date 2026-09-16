package peruncs.datagrid.cluster.node.aeron;

import org.eclipse.serializer.persistence.binary.types.Binary;
import peruncs.datagrid.cluster.node.NodeLibraryPropertiesProvider;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;

/// The Aeron distributor state shared with the Store integration.
/// Data publication is intentionally rejected here. Aeron Store writes must use
/// the provider's persistence-target factory so local acceptance, Archive
/// publication, and checkpoint fencing share one transaction owner.
final class AeronDistributor implements StorageBinaryDataDistributor {
    private final BooleanSupplier writer;
    private final LongConsumer sequenceSynchronizer;
    private final AtomicLong index = new AtomicLong(-1L);
    private final AtomicBoolean ignored = new AtomicBoolean();
    private final AtomicReference<String> dictionary = new AtomicReference<>();

    AeronDistributor(final BooleanSupplier writer, final LongConsumer sequenceSynchronizer) {
        this.writer = Objects.requireNonNull(writer, NodeLibraryPropertiesProvider.WRITER_ROLE);
        this.sequenceSynchronizer = Objects.requireNonNull(sequenceSynchronizer, "sequenceSynchronizer");
    }

    @Override
    public void messageIndex(final long value) {
        if (!this.writer.getAsBoolean()) {
            throw new IllegalStateException("Aeron replication message index is writable only by the writer");
        }
        if (value < -1 || value == Long.MAX_VALUE)
            throw new IllegalArgumentException("message index must be in [-1, Long.MAX_VALUE)");
        this.index.set(value);
        this.sequenceSynchronizer.accept(value + 1);
    }

    @Override
    public long messageIndex() {
        return this.index.get();
    }

    @Override
    public void ignoreDistribution(final boolean value) {
        this.ignored.set(value);
    }

    @Override
    public boolean ignoreDistribution() {
        return this.ignored.get();
    }

    @Override
    public void distributeTypeDictionary(final String value) {
        this.dictionary.set(value);
    }

    @Override
    public String consumeTypeDictionary() {
        return this.dictionary.getAndSet(null);
    }

    @Override
    public void distributeData(final Binary data) {
        Objects.requireNonNull(data, "data");
        if (this.ignored.get()) {
            throw new IllegalStateException("Aeron replication distribution is disabled during startup");
        }
        if (!this.writer.getAsBoolean()) throw new IllegalStateException("Aeron replication distributor is writer-only");
        throw new IllegalStateException(
                "Aeron Store binaries must be written through the replication persistence target");
    }

        /// The provider owns the shared Aeron/archive runtime.
    @Override
    public void dispose() {
    }
}
