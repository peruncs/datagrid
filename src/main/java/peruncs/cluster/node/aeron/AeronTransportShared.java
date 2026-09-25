package peruncs.cluster.node.aeron;

import peruncs.cluster.errors.ReseedRequiredException;

import java.util.function.Supplier;

/// Shared lifecycle state every [AeronTransport] owner synchronizes on.
///
/// Runtime, writer, reader, and retention owners synchronize their compound
/// lifecycle steps on this instance itself, preserving the single transport
/// monitor of the original provider: ownership of state moved to the owners,
/// the lock domain did not. The collector and capacity are shared between the
/// writer and retention owners, so the facade keeps exactly one copy here
/// instead of duplicating them.
final class AeronTransportShared {
    /* Reader delivery callbacks run on the reader polling thread; this scope
     * marks that thread so transport re-entry from a callback fails fast. */
    private static final ScopedValue<Boolean> DELIVERY_CALLBACK = ScopedValue.newInstance();

    private final AeronArchiveCapacity archiveCapacity;
    /// Logical stream claimed on first use; one transport owns exactly one stream.
    private String distributorStream;
    /* Reader-watermark channel and its fan-in state, shared between the reader
     * and retention owners. Installed by the facade after the owners exist. */
    private volatile WatermarkCollector watermarks;
    private volatile boolean closed;
    private volatile boolean closing;

    AeronTransportShared(final AeronArchiveCapacity archiveCapacity) {
        this.archiveCapacity = archiveCapacity;
    }

    /// Installs the watermark collector once all owners are wired.
    ///
    /// @param watermarks collector shared by the reader and retention owners
    void installWatermarks(final WatermarkCollector watermarks) {
        this.watermarks = watermarks;
    }

    /// Returns the shared watermark collector.
    ///
    /// @return watermark collector
    WatermarkCollector watermarks() {
        return this.watermarks;
    }

    /// Returns the shared Archive capacity view.
    ///
    /// @return archive capacity
    AeronArchiveCapacity capacity() {
        return this.archiveCapacity;
    }

    /// Claims the transport's single configured replication stream.
    ///
    /// @param streamName logical stream name, claimed on first use
    void claimStream(final String streamName) {
        if (streamName == null || streamName.isBlank()) {
            throw new IllegalArgumentException("Aeron replication stream name must not be blank");
        }
        if (this.distributorStream == null) {
            this.distributorStream = streamName;
        } else if (!this.distributorStream.equals(streamName)) {
            throw new IllegalArgumentException(
                    "Aeron transport is configured for stream %s, not %s".formatted(this.distributorStream, streamName));
        }
    }

    /// Releases the claimed stream during full teardown.
    void clearStreamClaim() {
        this.distributorStream = null;
    }

    /// Returns the claimed stream name, or `null` before the first claim.
    ///
    /// @return claimed stream name, or `null`
    String claimedStream() {
        return this.distributorStream;
    }

    boolean closed() {
        return this.closed;
    }

    boolean closing() {
        return this.closing;
    }

    void closing(final boolean value) {
        this.closing = value;
    }

    void closed(final boolean value) {
        this.closed = value;
    }

    /// Fails fast once the transport is closing or closed, and rejects
    /// re-entry from a reader delivery callback.
    void ensureOpen() {
        this.ensureNotDeliveryCallback();
        if (this.closed || this.closing) {
            throw new IllegalStateException(this.closed ? "Aeron transport is closed" : "Aeron transport is closing");
        }
    }

    /// Rejects any transport operation attempted from inside a reader
    /// delivery callback.
    void ensureNotDeliveryCallback() {
        if (DELIVERY_CALLBACK.isBound()) {
            throw new IllegalStateException(
                    "Aeron transport cannot be re-entered from a reader delivery callback");
        }
    }

    /// Runs the action marked as a reader delivery callback.
    ///
    /// @param action callback action
    void runInDeliveryCallback(final Runnable action) {
        if (DELIVERY_CALLBACK.isBound()) {
            throw new IllegalStateException("nested Aeron reader delivery callback");
        }
        ScopedValue.where(DELIVERY_CALLBACK, Boolean.TRUE).run(action);
    }

    /// Runs the supplier marked as a reader delivery callback.
    ///
    /// @param action callback supplier
    /// @param <T>    result type
    /// @return supplier result
    <T> T callInDeliveryCallback(final Supplier<T> action) {
        if (DELIVERY_CALLBACK.isBound()) {
            throw new IllegalStateException("nested Aeron reader delivery callback");
        }
        return ScopedValue.where(DELIVERY_CALLBACK, Boolean.TRUE).call(action::get);
    }

    /// Builds a typed reseed failure, keeping a `null` cause out of the stack.
    ///
    /// @param message failure message
    /// @param cause   optional cause
    /// @return reseed failure
    static ReseedRequiredException reseedRequired(final String message, final Throwable cause) {
        return cause == null ? new ReseedRequiredException(message) :
                new ReseedRequiredException(message, cause);
    }
}
