package peruncs.datagrid.cluster.storage.aeron.config;

import io.aeron.driver.Configuration;
import io.aeron.logbuffer.FrameDescriptor;
import org.agrona.BitUtil;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.datagrid.cluster.storage.types.ReplicationDurabilityMode;

import java.util.Objects;

/// Immutable framing, delivery, and durability limits shared by one writer and
/// its readers.
///
/// Both sides must use the same values to interpret the stream. Construction
/// rejects values that could create a frame the writer cannot publish or the
/// reader cannot assemble. The builder is a convenience over the canonical
/// record constructor; the record itself carries the validated values.
///
/// @param termLength                 Aeron term length shared by publication and subscription
/// @param mtuLength                  publication MTU
/// @param chunkSize                  logical Store-data chunk size carried by one envelope
/// @param maxTransactionBytes        largest complete Store transaction accepted
/// @param offerTimeoutNanos          bounded wait for publication and Archive progress
/// @param recordingStartTimeoutNanos bounded wait for an Archive recording to become active
/// @param recordedPositionTimeoutNanos bounded wait for the Archive to report a recorded position
/// @param recordingStopTimeoutNanos  bounded wait for an Archive recording to stop
/// @param readerStopTimeoutNanos     bounded wait for a reader to stop at a resolved boundary
/// @param readerFragmentsPerPoll     fragments a reader consumes per poll call while replaying
/// @param durabilityMode             local-versus-Archive ordering used by the writer
/// @param retryPolicy                idle pacing and probe spacing for bounded retry loops
public record AeronReplicationConfiguration(
        int termLength,
        int mtuLength,
        int chunkSize,
        int maxTransactionBytes,
        long offerTimeoutNanos,
        long recordingStartTimeoutNanos,
        long recordedPositionTimeoutNanos,
        long recordingStopTimeoutNanos,
        long readerStopTimeoutNanos,
        int readerFragmentsPerPoll,
        ReplicationDurabilityMode durabilityMode,
        AeronRetryPolicy retryPolicy
) {
        /// Default Aeron term length in bytes.
    public static final int DEFAULT_TERM_LENGTH = 16 * 1024 * 1024;
        /// Default publication MTU in bytes.
    public static final int DEFAULT_MTU_LENGTH = 1408;
        /// Default logical Store-data chunk size in bytes.
    public static final int DEFAULT_CHUNK_SIZE = 1024 * 1024;
        /// Default largest accepted transaction in bytes.
    public static final int DEFAULT_MAX_TRANSACTION_BYTES = 64 * 1024 * 1024;
        /// Hard upper bound for the largest accepted transaction in bytes.
    public static final int MAX_SUPPORTED_TRANSACTION_BYTES = AeronReplicationEnvelope.MAX_TRANSACTION_PAYLOAD_BYTES;
        /// Default fragments consumed per reader poll; a replay backlog drains in a few polls instead of thousands.
    public static final int DEFAULT_READER_FRAGMENTS_PER_POLL = 256;
    private static final long DEFAULT_OFFER_TIMEOUT_NANOS = 30_000_000_000L;
    private static final long DEFAULT_RECORDING_START_TIMEOUT_NANOS = 30_000_000_000L;
    private static final long DEFAULT_RECORDED_POSITION_TIMEOUT_NANOS = 30_000_000_000L;
    private static final long DEFAULT_RECORDING_STOP_TIMEOUT_NANOS = 30_000_000_000L;
    private static final long DEFAULT_READER_STOP_TIMEOUT_NANOS = 30_000_000_000L;

        /// Validates every framing, timeout, and delivery limit.
    ///
    /// @throws IllegalArgumentException when the limits cannot describe a valid
    ///                                  Aeron envelope
    public AeronReplicationConfiguration {
        if (durabilityMode == null || retryPolicy == null) {
            throw new IllegalArgumentException("durabilityMode and retryPolicy must be set");
        }
        if (!BitUtil.isPowerOfTwo(termLength) || termLength < 64 * 1024) {
            throw new IllegalArgumentException("termLength must be a power of two >= 64 KiB");
        }
        try {
            Configuration.validateMtuLength(mtuLength);
        } catch (final RuntimeException failure) {
            throw new IllegalArgumentException("Invalid Aeron MTU length: " + mtuLength, failure);
        }
        if (maxTransactionBytes <= 0 || maxTransactionBytes > MAX_SUPPORTED_TRANSACTION_BYTES) {
            throw new IllegalArgumentException(
                    "maxTransactionBytes must be between 1 and %s".formatted(MAX_SUPPORTED_TRANSACTION_BYTES));
        }
        if (chunkSize <= 0 || chunkSize > maxTransactionBytes) {
            throw new IllegalArgumentException("chunkSize must be positive and <= maxTransactionBytes");
        }
        final long packetCount = (maxTransactionBytes + (long) chunkSize - 1L) / chunkSize;
        if (packetCount > AeronReplicationEnvelope.MAX_PACKET_COUNT) {
            throw new IllegalArgumentException(
                    "maxTransactionBytes requires more than %s packets".formatted(AeronReplicationEnvelope.MAX_PACKET_COUNT));
        }
        if (offerTimeoutNanos <= 0 || recordingStartTimeoutNanos <= 0 ||
            recordedPositionTimeoutNanos <= 0 || recordingStopTimeoutNanos <= 0 ||
            readerStopTimeoutNanos <= 0) {
            throw new IllegalArgumentException("all Aeron timeouts must be positive");
        }
        if (readerFragmentsPerPoll <= 0) {
            throw new IllegalArgumentException("readerFragmentsPerPoll must be positive");
        }
        final int maxMessageLength = maxMessageLengthForTermLength(termLength);
        if ((long) chunkSize + AeronReplicationEnvelope.HEADER_LENGTH > maxMessageLength) {
            throw new IllegalArgumentException(
                    "chunkSize plus envelope exceeds Aeron maxMessageLength=%s".formatted(maxMessageLength));
        }
    }

        /// Returns the validated default configuration.
    ///
    /// @return default configuration
    public static AeronReplicationConfiguration defaults() {
        return builder().build();
    }

        /// Starts a builder with the documented defaults.
    ///
    /// @return new configuration builder
    public static Builder builder() {
        return new Builder();
    }

    private static int maxMessageLengthForTermLength(final int termLength) {
        return Math.min(FrameDescriptor.computeMaxMessageLength(termLength), 16 * 1024 * 1024);
    }

        /// Returns the largest envelope message that this publication may offer.
    /// Aeron fragments that message according to the MTU; the logical chunk must
    /// still fit within this publication limit.
    ///
    /// @return maximum envelope length in bytes
    public int maxMessageLength() {
        return maxMessageLengthForTermLength(this.termLength);
    }

        /// Builds an immutable Aeron replication configuration with the
    /// documented defaults, then applies only the setter values.
    public static final class Builder {
        private int termLength = DEFAULT_TERM_LENGTH;
        private int mtuLength = DEFAULT_MTU_LENGTH;
        private int chunkSize = DEFAULT_CHUNK_SIZE;
        private int maxTransactionBytes = DEFAULT_MAX_TRANSACTION_BYTES;
        private long offerTimeoutNanos = DEFAULT_OFFER_TIMEOUT_NANOS;
        private long recordingStartTimeoutNanos = DEFAULT_RECORDING_START_TIMEOUT_NANOS;
        private long recordedPositionTimeoutNanos = DEFAULT_RECORDED_POSITION_TIMEOUT_NANOS;
        private long recordingStopTimeoutNanos = DEFAULT_RECORDING_STOP_TIMEOUT_NANOS;
        private long readerStopTimeoutNanos = DEFAULT_READER_STOP_TIMEOUT_NANOS;
        private int readerFragmentsPerPoll = DEFAULT_READER_FRAGMENTS_PER_POLL;
        private ReplicationDurabilityMode durabilityMode = ReplicationDurabilityMode.ARCHIVE_FIRST;
        private AeronRetryPolicy retryPolicy = AeronRetryPolicy.Default();

                /// Creates a builder initialized with the documented defaults.
        public Builder() {
        }

                /// Sets the term length; it must be a power of two of at least 64 KiB.
        ///
        /// @param value term length in bytes
        /// @return this builder
        public Builder termLength(final int value) {
            this.termLength = value;
            return this;
        }

                /// Sets the aligned network MTU used by the publication.
        ///
        /// @param value MTU in bytes
        /// @return this builder
        public Builder mtuLength(final int value) {
            this.mtuLength = value;
            return this;
        }

                /// Sets the logical data chunk size.
        ///
        /// @param value chunk size in bytes
        /// @return this builder
        public Builder chunkSize(final int value) {
            this.chunkSize = value;
            return this;
        }

                /// Sets the largest complete Store transaction accepted.
        ///
        /// @param value maximum transaction size in bytes
        /// @return this builder
        public Builder maxTransactionBytes(final int value) {
            this.maxTransactionBytes = value;
            return this;
        }

                /// Sets the maximum wait for publication or Archive progress.
        ///
        /// @param value wait in nanoseconds
        /// @return this builder
        public Builder offerTimeoutNanos(final long value) {
            this.offerTimeoutNanos = value;
            return this;
        }

                /// Sets the maximum wait for an Archive recording to become active.
        ///
        /// @param value wait in nanoseconds
        /// @return this builder
        public Builder recordingStartTimeoutNanos(final long value) {
            this.recordingStartTimeoutNanos = value;
            return this;
        }

                /// Sets the maximum wait for the Archive to report a recorded position.
        ///
        /// @param value wait in nanoseconds
        /// @return this builder
        public Builder recordedPositionTimeoutNanos(final long value) {
            this.recordedPositionTimeoutNanos = value;
            return this;
        }

                /// Sets the maximum wait for an Archive recording to stop.
        ///
        /// @param value wait in nanoseconds
        /// @return this builder
        public Builder recordingStopTimeoutNanos(final long value) {
            this.recordingStopTimeoutNanos = value;
            return this;
        }

                /// Sets the maximum wait for a reader to stop at a resolved boundary.
        ///
        /// @param value wait in nanoseconds
        /// @return this builder
        public Builder readerStopTimeoutNanos(final long value) {
            this.readerStopTimeoutNanos = value;
            return this;
        }

                /// Sets the fragments a reader consumes per poll call.
        ///
        /// A larger value drains a replay backlog in fewer polls; the default
        /// of [#DEFAULT_READER_FRAGMENTS_PER_POLL] balances replay throughput
        /// against live-tail latency.
        ///
        /// @param value fragments per poll; must be positive
        /// @return this builder
        public Builder readerFragmentsPerPoll(final int value) {
            this.readerFragmentsPerPoll = value;
            return this;
        }

                /// Sets the local-versus-Archive ordering used by the writer.
        ///
        /// @param value durability mode
        /// @return this builder
        public Builder durabilityMode(final ReplicationDurabilityMode value) {
            this.durabilityMode = Objects.requireNonNull(value, "durabilityMode");
            return this;
        }

                /// Sets the idle pacing and probe spacing for bounded retry loops.
        ///
        /// @param value retry policy
        /// @return this builder
        public Builder retryPolicy(final AeronRetryPolicy value) {
            this.retryPolicy = Objects.requireNonNull(value, "retryPolicy");
            return this;
        }

                /// Validates and creates the immutable configuration.
        ///
        /// @return validated configuration
        /// @throws IllegalArgumentException if the limits cannot describe a valid
        ///                                  Aeron envelope
        public AeronReplicationConfiguration build() {
            return new AeronReplicationConfiguration(
                    this.termLength,
                    this.mtuLength,
                    this.chunkSize,
                    this.maxTransactionBytes,
                    this.offerTimeoutNanos,
                    this.recordingStartTimeoutNanos,
                    this.recordedPositionTimeoutNanos,
                    this.recordingStopTimeoutNanos,
                    this.readerStopTimeoutNanos,
                    this.readerFragmentsPerPoll,
                    this.durabilityMode,
                    this.retryPolicy
            );
        }
    }
}
