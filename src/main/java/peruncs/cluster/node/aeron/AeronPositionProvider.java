package peruncs.cluster.node.aeron;

import peruncs.cluster.errors.NodeException;
import peruncs.cluster.errors.internal.ReplicationPositionUnavailableException;
import peruncs.cluster.node.replication.ReplicationPositionProvider;
import peruncs.cluster.storage.ReplicationPosition;

import java.util.Objects;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/// Cached Aeron writer-boundary view. A reader's applied cursor is deliberately
/// not exposed as the latest log position because it cannot prove the writer's
/// durable boundary without a control channel.
///
/// Callers must invoke [#init()] before querying a writer. Initialization
/// establishes the writer-side runtime; [#latest()] only reads the
/// already-published boundary and never starts transport resources implicitly.
/// Readers remain unavailable because they cannot establish a writer boundary
/// locally.
final class AeronPositionProvider implements ReplicationPositionProvider {
    private final BooleanSupplier writer;
    private final BooleanSupplier initialized;
    private final Runnable ensureWriter;
    private final Supplier<AeronWriterRecoveryBoundary> writerBoundary;
    private final Supplier<UUID> clusterId;
    private final Supplier<UUID> nodeId;
    private final Supplier<UUID> storeGeneration;
    private final LongSupplier epoch;
    private final LongSupplier fencingToken;

    AeronPositionProvider(final BooleanSupplier writer, final BooleanSupplier initialized,
                          final Runnable ensureWriter,
                          final Supplier<AeronWriterRecoveryBoundary> writerBoundary, final Supplier<UUID> clusterId,
                          final Supplier<UUID> nodeId, final Supplier<UUID> storeGeneration, final LongSupplier epoch,
                          final LongSupplier fencingToken) {
        this.writer = Objects.requireNonNull(writer, "writer");
        this.initialized = Objects.requireNonNull(initialized, "initialized");
        this.ensureWriter = Objects.requireNonNull(ensureWriter, "ensureWriter");
        this.writerBoundary = Objects.requireNonNull(writerBoundary, "writerBoundary");
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.storeGeneration = Objects.requireNonNull(storeGeneration, "storeGeneration");
        this.epoch = Objects.requireNonNull(epoch, "epoch");
        this.fencingToken = Objects.requireNonNull(fencingToken, "fencingToken");
    }

    @Override
    public void init() throws NodeException {
        if (this.writer.getAsBoolean()) this.ensureWriter.run();
    }

    @Override
    public ReplicationPosition latest() throws NodeException {
        if (!this.writer.getAsBoolean()) {
            throw new ReplicationPositionUnavailableException("Aeron reader cannot establish the writer's latest durable boundary without watermark delivery");
        }
        if (!this.initialized.getAsBoolean())
            throw new ReplicationPositionUnavailableException("Aeron writer position is unavailable until the position provider is initialized");
        final long fencingToken;
        try {
            fencingToken = this.fencingToken.getAsLong();
        } catch (final ReplicationPositionUnavailableException unavailable) {
            throw unavailable;
        } catch (final RuntimeException failure) {
            throw new ReplicationPositionUnavailableException(
                    "Aeron writer Store mark is not available; no writer position can be established", failure);
        }
        if (fencingToken <= 0) {
            throw new ReplicationPositionUnavailableException(
                    "Aeron writer Store mark is not available; no writer position can be established");
        }
        final AeronWriterRecoveryBoundary boundary = this.writerBoundary.get();
        if (boundary.recordingId() < 0 || boundary.position() < 0) {
            throw new ReplicationPositionUnavailableException(
                    "Aeron writer has no resolved Archive position to report");
        }
        final UUID generation = this.storeGeneration.get();
        return new ReplicationPosition(this.clusterId.get(), generation, this.epoch.getAsLong(),
                boundary.recordingId(), boundary.sequence(), boundary.position(), fencingToken, this.nodeId.get());
    }

        /// Releases nothing: the provider reads the transport's published
        /// boundary and never owns reader or writer resources.
    @Override
    public void close() {
    }
}
