package peruncs.cluster.node.aeron;

import org.eclipse.serializer.persistence.binary.types.Binary;
import peruncs.cluster.api.NodeSettingsSource;
import peruncs.cluster.storage.binary.ReplicationPublisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;

/// Guards the Aeron distribution state shared with the Store integration.
/// Direct data publication is intentionally rejected here. Aeron Store writes must use
/// the provider's persistence-target factory so local acceptance, Archive
/// publication, and checkpoint fencing share one transaction owner.
final class AeronDistributionGate implements ReplicationPublisher {
    private final BooleanSupplier writer;
    private final LongConsumer sequenceSynchronizer;
    private final AtomicLong index = new AtomicLong(-1L);
    private final AtomicBoolean ignored = new AtomicBoolean();
    private final AtomicReference<String> dictionary = new AtomicReference<>();

    AeronDistributionGate(final BooleanSupplier writer, final LongConsumer sequenceSynchronizer) {
        this.writer = Objects.requireNonNull(writer, NodeSettingsSource.WRITER_ROLE);
        this.sequenceSynchronizer = Objects.requireNonNull(sequenceSynchronizer, "sequenceSynchronizer");
    }

    /// Records the Store message index for the writer.
    ///
    /// @param value store message index
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

    /// Returns the last recorded Store message index.
    ///
    /// @return message index, or `-1` when none was recorded
    @Override
    public long messageIndex() {
        return this.index.get();
    }

    /// Enables or disables distribution without changing the recorded index.
    ///
    /// @param value `true` while startup must not distribute
    @Override
    public void ignoreDistribution(final boolean value) {
        this.ignored.set(value);
    }

    /// Reports whether distribution is currently ignored.
    ///
    /// @return `true` while distribution is disabled
    @Override
    public boolean ignoreDistribution() {
        return this.ignored.get();
    }

    /// Stores the newest type dictionary for the next consumer.
    ///
    /// @param value exported type dictionary
    @Override
    public void distributeTypeDictionary(final String value) {
        this.dictionary.set(value);
    }

    /// Consumes the stored type dictionary, if any.
    ///
    /// @return type dictionary, or `null` when none is pending
    @Override
    public String consumeTypeDictionary() {
        return this.dictionary.getAndSet(null);
    }

    /// Rejects direct Store data publication.
    ///
    /// Aeron writes flow through the provider's persistence target so local
    /// acceptance and checkpoint fencing stay one operation.
    ///
    /// @param data committed binary data
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
