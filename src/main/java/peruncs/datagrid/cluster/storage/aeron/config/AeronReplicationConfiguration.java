package peruncs.datagrid.cluster.storage.aeron.config;

import io.aeron.driver.Configuration;
import io.aeron.logbuffer.FrameDescriptor;
import org.agrona.BitUtil;
import peruncs.datagrid.cluster.storage.aeron.wire.AeronReplicationEnvelope;
import peruncs.datagrid.cluster.storage.types.ReplicationDurabilityMode;

import java.util.Objects;

/// Immutable framing and durability limits shared by one writer and its
/// readers.
///
/// Both sides must use the same values to interpret the stream. The builder
/// rejects values that could create a frame the writer cannot publish or the
/// reader cannot assemble.
public final class AeronReplicationConfiguration {
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
    private static final long DEFAULT_OFFER_TIMEOUT_NANOS = 30_000_000_000L;
    private static final long DEFAULT_RECORDING_START_TIMEOUT_NANOS = 30_000_000_000L;
    private static final long DEFAULT_RECORDED_POSITION_TIMEOUT_NANOS = 30_000_000_000L;
    private static final long DEFAULT_RECORDING_STOP_TIMEOUT_NANOS = 30_000_000_000L;
    private static final long DEFAULT_READER_STOP_TIMEOUT_NANOS = 30_000_000_000L;
    private final int termLength;
    private final int mtuLength;
    private final int chunkSize;
    private final int maxTransactionBytes;
    private final long offerTimeoutNanos;
    private final long recordingStartTimeoutNanos;
    private final long recordedPositionTimeoutNanos;
    private final long recordingStopTimeoutNanos;
    private final long readerStopTimeoutNanos;
    private final ReplicationDurabilityMode durabilityMode;
    private final AeronRetryPolicy retryPolicy;

    private AeronReplicationConfiguration(
            final int termLength,
            final int mtuLength,
            final int chunkSize,
            final int maxTransactionBytes,
            final long offerTimeoutNanos,
            final long recordingStartTimeoutNanos,
            final long recordedPositionTimeoutNanos,
            final long recordingStopTimeoutNanos,
            final long readerStopTimeoutNanos,
            final ReplicationDurabilityMode durabilityMode,
            final AeronRetryPolicy retryPolicy
    ) {
        this.termLength = termLength;
        this.mtuLength = mtuLength;
        this.chunkSize = chunkSize;
        this.maxTransactionBytes = maxTransactionBytes;
        this.offerTimeoutNanos = offerTimeoutNanos;
        this.recordingStartTimeoutNanos = recordingStartTimeoutNanos;
        this.recordedPositionTimeoutNanos = recordedPositionTimeoutNanos;
        this.recordingStopTimeoutNanos = recordingStopTimeoutNanos;
        this.readerStopTimeoutNanos = readerStopTimeoutNanos;
        this.durabilityMode = durabilityMode;
        this.retryPolicy = retryPolicy;
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

        /// Returns the Aeron term length in bytes.
    ///
    /// @return term length in bytes
    public int termLength() {
        return this.termLength;
    }

        /// Returns the transport MTU used when the publication is created.
    ///
    /// @return MTU in bytes
    public int mtuLength() {
        return this.mtuLength;
    }

        /// Returns the largest logical Store-data chunk in one envelope.
    ///
    /// @return chunk size in bytes
    public int chunkSize() {
        return this.chunkSize;
    }

        /// Returns the largest Store transaction accepted by the writer.
    ///
    /// @return maximum transaction size in bytes
    public int maxTransactionBytes() {
        return this.maxTransactionBytes;
    }

        /// Returns the deadline used for publication and Archive progress waits.
    ///
    /// @return wait in nanoseconds
    public long offerTimeoutNanos() {
        return this.offerTimeoutNanos;
    }

        /// Returns the bounded wait for an Archive recording to become active.
    ///
    /// @return wait in nanoseconds
    public long recordingStartTimeoutNanos() {
        return this.recordingStartTimeoutNanos;
    }

        /// Returns the bounded wait for the Archive to report a recorded position.
    ///
    /// @return wait in nanoseconds
    public long recordedPositionTimeoutNanos() {
        return this.recordedPositionTimeoutNanos;
    }

        /// Returns the bounded wait for an Archive recording to stop.
    ///
    /// @return wait in nanoseconds
    public long recordingStopTimeoutNanos() {
        return this.recordingStopTimeoutNanos;
    }

        /// Returns the bounded wait used when a reader is stopped at the live tail.
    ///
    /// @return wait in nanoseconds
    public long readerStopTimeoutNanos() {
        return this.readerStopTimeoutNanos;
    }

        /// Returns the order in which local acceptance and Archive publication occur.
    ///
    /// @return selected durability mode
    public ReplicationDurabilityMode durabilityMode() {
        return this.durabilityMode;
    }

        /// Returns the idle pacing and probe spacing for bounded retry loops.
    ///
    /// @return retry policy
    public AeronRetryPolicy retryPolicy() {
        return this.retryPolicy;
    }

        /// Returns the largest envelope message that this publication may offer.
    /// Aeron fragments that message according to the MTU; the logical chunk must
    /// still fit within this publication limit.
    ///
    /// @return maximum envelope length in bytes
    public int maxMessageLength() {
        return maxMessageLengthForTermLength(this.termLength);
    }

        /// Builds an immutable Aeron replication configuration.
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
            if (!BitUtil.isPowerOfTwo(this.termLength) || this.termLength < 64 * 1024) {
                throw new IllegalArgumentException("termLength must be a power of two >= 64 KiB");
            }
            try {
                Configuration.validateMtuLength(this.mtuLength);
            } catch (final RuntimeException failure) {
                throw new IllegalArgumentException("Invalid Aeron MTU length: " + this.mtuLength, failure);
            }
            if (this.maxTransactionBytes <= 0 || this.maxTransactionBytes > MAX_SUPPORTED_TRANSACTION_BYTES) {
                throw new IllegalArgumentException(
                        "maxTransactionBytes must be between 1 and %s".formatted(MAX_SUPPORTED_TRANSACTION_BYTES));
            }
            if (this.chunkSize <= 0 || this.chunkSize > this.maxTransactionBytes) {
                throw new IllegalArgumentException("chunkSize must be positive and <= maxTransactionBytes");
            }
            final long packetCount = (this.maxTransactionBytes + (long) this.chunkSize - 1L) / this.chunkSize;
            if (packetCount > AeronReplicationEnvelope.MAX_PACKET_COUNT) {
                throw new IllegalArgumentException(
                        "maxTransactionBytes requires more than %s packets".formatted(AeronReplicationEnvelope.MAX_PACKET_COUNT));
            }
            if (this.offerTimeoutNanos <= 0 || this.recordingStartTimeoutNanos <= 0 ||
                this.recordedPositionTimeoutNanos <= 0 || this.recordingStopTimeoutNanos <= 0 ||
                this.readerStopTimeoutNanos <= 0 || this.durabilityMode == null || this.retryPolicy == null) {
                throw new IllegalArgumentException(
                        "all Aeron timeouts must be positive and durabilityMode/retryPolicy must be set");
            }
            final int maxMessageLength = maxMessageLengthForTermLength(this.termLength);
            if ((long) this.chunkSize + AeronReplicationEnvelope.HEADER_LENGTH > maxMessageLength) {
                throw new IllegalArgumentException(
                        "chunkSize plus envelope exceeds Aeron maxMessageLength=%s".formatted(maxMessageLength));
            }
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
                    this.durabilityMode,
                    this.retryPolicy
            );
        }
    }
}
