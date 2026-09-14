package org.eclipse.datagrid.storage.distributed.aeron.config;

import org.agrona.BitUtil;
import org.eclipse.datagrid.storage.distributed.aeron.wire.AeronReplicationEnvelope;
import org.eclipse.datagrid.storage.distributed.types.ReplicationDurabilityMode;
import org.eclipse.datagrid.storage.distributed.types.ReplicationLimits;

import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
 * %%
 * Copyright (C) 2025 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

/**
 * Immutable framing and durability limits shared by one writer and its
 * readers.
 *
 * <p>Both sides must use the same values to interpret the stream. The builder
 * rejects values that could create a frame the writer cannot publish or the
 * reader cannot assemble.</p>
 */
public final class AeronReplicationConfiguration
{
	private static final String PREFIX = "eclipsestore.distribution.aeron.";
	/** Property that sets the Aeron term length in bytes. */
	public static final String TERM_LENGTH_PROPERTY = PREFIX + "term-length";
	/** Property that sets the publication MTU in bytes. */
	public static final String MTU_LENGTH_PROPERTY = PREFIX + "mtu-length";
	/** Property that sets the logical Store-data chunk size in bytes. */
	public static final String CHUNK_SIZE_PROPERTY = PREFIX + "chunk-size";
	/** Property that sets the largest accepted transaction in bytes. */
	public static final String MAX_TRANSACTION_BYTES_PROPERTY = PREFIX + "max-transaction-bytes";
	/** Property that sets the publication wait in nanoseconds. */
	public static final String OFFER_TIMEOUT_NANOS_PROPERTY = PREFIX + "offer-timeout-nanos";
	/** Property that sets the wait for an Archive recording to become visible. */
	public static final String RECORDING_START_TIMEOUT_NANOS_PROPERTY = PREFIX + "recording-start-timeout-nanos";
	/** Property that sets the wait for an Archive position to become durable. */
	public static final String RECORDED_POSITION_TIMEOUT_NANOS_PROPERTY = PREFIX + "recorded-position-timeout-nanos";
	/** Property that sets the wait for an Archive recording to stop. */
	public static final String RECORDING_STOP_TIMEOUT_NANOS_PROPERTY = PREFIX + "recording-stop-timeout-nanos";
	/** Property that sets the reader stop wait in nanoseconds. */
	public static final String READER_STOP_TIMEOUT_NANOS_PROPERTY = PREFIX + "reader-stop-timeout-nanos";
	private static final long DEFAULT_OFFER_TIMEOUT_NANOS = 30_000_000_000L;
	private static final long DEFAULT_RECORDING_START_TIMEOUT_NANOS = 30_000_000_000L;
	private static final long DEFAULT_RECORDED_POSITION_TIMEOUT_NANOS = 30_000_000_000L;
	private static final long DEFAULT_RECORDING_STOP_TIMEOUT_NANOS = 30_000_000_000L;
	private static final long DEFAULT_READER_STOP_TIMEOUT_NANOS = 30_000_000_000L;
	/** Property that selects the local and Archive durability order. */
	public static final String DURABILITY_MODE_PROPERTY = PREFIX + "durability-mode";
	/** Default Aeron term length in bytes. */
	public static final int DEFAULT_TERM_LENGTH = 16 * 1024 * 1024;
	/** Default publication MTU in bytes. */
	public static final int DEFAULT_MTU_LENGTH = 1408;
	/** Default logical Store-data chunk size in bytes. */
	public static final int DEFAULT_CHUNK_SIZE = 1024 * 1024;
	/** Default largest accepted transaction in bytes. */
	public static final int DEFAULT_MAX_TRANSACTION_BYTES = 64 * 1024 * 1024;
	/** Hard upper bound for the largest accepted transaction in bytes. */
	public static final int MAX_SUPPORTED_TRANSACTION_BYTES = ReplicationLimits.MAX_MESSAGE_BYTES;

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
		final ReplicationDurabilityMode durabilityMode
	)
	{
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
	}

	/**
	 * Returns the validated default configuration.
	 *
	 * @return default configuration
	 */
	public static AeronReplicationConfiguration defaults()
	{
		return builder().build();
	}

	/**
	 * Starts a builder with the documented defaults.
	 *
	 * @return new configuration builder
	 */
	public static Builder builder()
	{
		return new Builder();
	}

	/**
	 * Builds a configuration from properties. Missing values use the defaults;
	 * malformed values fail before any Aeron resource is opened.
	 *
	 * @param properties source properties
	 * @return validated configuration
	 * @throws NullPointerException if {@code properties} is {@code null}
	 * @throws IllegalArgumentException if a value is malformed or unsafe
	 */
	public static AeronReplicationConfiguration from(final Properties properties)
	{
		Objects.requireNonNull(properties, "properties");
		validateKnownProperties(properties);
		return builder()
			.termLength(integer(properties, TERM_LENGTH_PROPERTY, DEFAULT_TERM_LENGTH))
			.mtuLength(integer(properties, MTU_LENGTH_PROPERTY, DEFAULT_MTU_LENGTH))
			.chunkSize(integer(properties, CHUNK_SIZE_PROPERTY, DEFAULT_CHUNK_SIZE))
			.maxTransactionBytes(integer(properties, MAX_TRANSACTION_BYTES_PROPERTY, DEFAULT_MAX_TRANSACTION_BYTES))
			.offerTimeoutNanos(longValue(properties, OFFER_TIMEOUT_NANOS_PROPERTY, DEFAULT_OFFER_TIMEOUT_NANOS))
			.recordingStartTimeoutNanos(longValue(properties, RECORDING_START_TIMEOUT_NANOS_PROPERTY,
				DEFAULT_RECORDING_START_TIMEOUT_NANOS))
			.recordedPositionTimeoutNanos(longValue(properties, RECORDED_POSITION_TIMEOUT_NANOS_PROPERTY,
				DEFAULT_RECORDED_POSITION_TIMEOUT_NANOS))
			.recordingStopTimeoutNanos(longValue(properties, RECORDING_STOP_TIMEOUT_NANOS_PROPERTY,
				DEFAULT_RECORDING_STOP_TIMEOUT_NANOS))
			.readerStopTimeoutNanos(longValue(properties, READER_STOP_TIMEOUT_NANOS_PROPERTY,
				DEFAULT_READER_STOP_TIMEOUT_NANOS))
			.durabilityMode(mode(properties.getProperty(DURABILITY_MODE_PROPERTY)))
			.build();
	}

	private static void validateKnownProperties(final Properties properties)
	{
		final Set<String> known = Set.of(
			TERM_LENGTH_PROPERTY,
			MTU_LENGTH_PROPERTY,
			CHUNK_SIZE_PROPERTY,
			MAX_TRANSACTION_BYTES_PROPERTY,
			OFFER_TIMEOUT_NANOS_PROPERTY,
			RECORDING_START_TIMEOUT_NANOS_PROPERTY,
			RECORDED_POSITION_TIMEOUT_NANOS_PROPERTY,
			RECORDING_STOP_TIMEOUT_NANOS_PROPERTY,
			READER_STOP_TIMEOUT_NANOS_PROPERTY,
			DURABILITY_MODE_PROPERTY
		);
		for (final String name : properties.stringPropertyNames())
		{
			if (name.startsWith(PREFIX) && !known.contains(name))
			{
				throw new IllegalArgumentException("Unknown Aeron replication property: " + name);
			}
		}
	}

	private static long longValue(final Properties properties, final String key, final long fallback)
	{
		final String value = properties.getProperty(key);
		if (value == null) return fallback;
		try
		{
			return Long.parseLong(value.trim());
		}
		catch (final NumberFormatException failure)
		{
			throw new IllegalArgumentException("Invalid long for " + key + ": " + value, failure);
		}
	}

	private static int integer(final Properties properties, final String key, final int fallback)
	{
		final String value = properties.getProperty(key);
		if (value == null) return fallback;
		try
		{
			return Integer.parseInt(value.trim());
		}
		catch (final NumberFormatException failure)
		{
			throw new IllegalArgumentException("Invalid integer for " + key + ": " + value, failure);
		}
	}

	private static ReplicationDurabilityMode mode(final String value)
	{
		if (value == null || value.isBlank()) return ReplicationDurabilityMode.ARCHIVE_FIRST;
		return switch (value.trim().toLowerCase(java.util.Locale.ROOT))
		{
			case "archive-first", "archive_first" -> ReplicationDurabilityMode.ARCHIVE_FIRST;
			case "enqueue-then-archive", "enqueue_then_archive" -> ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE;
			default -> throw new IllegalArgumentException("Unknown replication durability mode: " + value);
		};
	}

	/**
	 * Returns the Aeron term length in bytes.
	 *
	 * @return term length in bytes
	 */
	public int termLength()
	{
		return this.termLength;
	}

	/**
	 * Returns the transport MTU used when the publication is created.
	 *
	 * @return MTU in bytes
	 */
	public int mtuLength()
	{
		return this.mtuLength;
	}

	/**
	 * Returns the largest logical Store-data chunk in one envelope.
	 *
	 * @return chunk size in bytes
	 */
	public int chunkSize()
	{
		return this.chunkSize;
	}

	/**
	 * Returns the largest Store transaction accepted by the writer.
	 *
	 * @return maximum transaction size in bytes
	 */
	public int maxTransactionBytes()
	{
		return this.maxTransactionBytes;
	}

	/**
	 * Returns the deadline used for publication and Archive progress waits.
	 *
	 * @return wait in nanoseconds
	 */
	public long offerTimeoutNanos()
	{
		return this.offerTimeoutNanos;
	}

	/**
	 * Returns the bounded wait for an Archive recording to become active.
	 *
	 * @return wait in nanoseconds
	 */
	public long recordingStartTimeoutNanos()
	{
		return this.recordingStartTimeoutNanos;
	}

	/**
	 * Returns the bounded wait for the Archive to report a recorded position.
	 *
	 * @return wait in nanoseconds
	 */
	public long recordedPositionTimeoutNanos()
	{
		return this.recordedPositionTimeoutNanos;
	}

	/**
	 * Returns the bounded wait for an Archive recording to stop.
	 *
	 * @return wait in nanoseconds
	 */
	public long recordingStopTimeoutNanos()
	{
		return this.recordingStopTimeoutNanos;
	}

	/**
	 * Returns the bounded wait used when a reader is stopped at the live tail.
	 *
	 * @return wait in nanoseconds
	 */
	public long readerStopTimeoutNanos()
	{
		return this.readerStopTimeoutNanos;
	}

	/**
	 * Returns the order in which local acceptance and Archive publication occur.
	 *
	 * @return selected durability mode
	 */
	public ReplicationDurabilityMode durabilityMode()
	{
		return this.durabilityMode;
	}

	/**
	 * Returns the largest envelope message that this publication may offer.
	 * Aeron fragments that message according to the MTU; the logical chunk must
	 * still fit within this publication limit.
	 *
	 * @return maximum envelope length in bytes
	 */
	public int maxMessageLength()
	{
		return maxMessageLengthForTermLength(this.termLength);
	}

	private static int maxMessageLengthForTermLength(final int termLength)
	{
		return Math.min(termLength / 8, 16 * 1024 * 1024);
	}

	/** Builds an immutable Aeron replication configuration. */
	public static final class Builder
	{
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

		/** Creates a builder initialized with the documented defaults. */
		public Builder()
		{
		}

		/**
		 * Sets the term length; it must be a power of two of at least 64 KiB.
		 *
		 * @param value term length in bytes
		 * @return this builder
		 */
		public Builder termLength(final int value)
		{
			this.termLength = value;
			return this;
		}

		/**
		 * Sets the aligned network MTU used by the publication.
		 *
		 * @param value MTU in bytes
		 * @return this builder
		 */
		public Builder mtuLength(final int value)
		{
			this.mtuLength = value;
			return this;
		}

		/**
		 * Sets the logical data chunk size.
		 *
		 * @param value chunk size in bytes
		 * @return this builder
		 */
		public Builder chunkSize(final int value)
		{
			this.chunkSize = value;
			return this;
		}

		/**
		 * Sets the largest complete Store transaction accepted.
		 *
		 * @param value maximum transaction size in bytes
		 * @return this builder
		 */
		public Builder maxTransactionBytes(final int value)
		{
			this.maxTransactionBytes = value;
			return this;
		}

		/**
		 * Sets the maximum wait for publication or Archive progress.
		 *
		 * @param value wait in nanoseconds
		 * @return this builder
		 */
		public Builder offerTimeoutNanos(final long value)
		{
			this.offerTimeoutNanos = value;
			return this;
		}

		/**
		 * Sets the maximum wait for an Archive recording to become active.
		 *
		 * @param value wait in nanoseconds
		 * @return this builder
		 */
		public Builder recordingStartTimeoutNanos(final long value)
		{
			this.recordingStartTimeoutNanos = value;
			return this;
		}

		/**
		 * Sets the maximum wait for the Archive to report a recorded position.
		 *
		 * @param value wait in nanoseconds
		 * @return this builder
		 */
		public Builder recordedPositionTimeoutNanos(final long value)
		{
			this.recordedPositionTimeoutNanos = value;
			return this;
		}

		/**
		 * Sets the maximum wait for an Archive recording to stop.
		 *
		 * @param value wait in nanoseconds
		 * @return this builder
		 */
		public Builder recordingStopTimeoutNanos(final long value)
		{
			this.recordingStopTimeoutNanos = value;
			return this;
		}

		/**
		 * Sets the maximum wait for a reader to stop at a resolved boundary.
		 *
		 * @param value wait in nanoseconds
		 * @return this builder
		 */
		public Builder readerStopTimeoutNanos(final long value)
		{
			this.readerStopTimeoutNanos = value;
			return this;
		}

		/**
		 * Sets the local-versus-Archive ordering used by the writer.
		 *
		 * @param value durability mode
		 * @return this builder
		 */
		public Builder durabilityMode(final ReplicationDurabilityMode value)
		{
			this.durabilityMode = Objects.requireNonNull(value, "durabilityMode");
			return this;
		}

		/**
		 * Validates and creates the immutable configuration.
		 *
		 * @return validated configuration
		 * @throws IllegalArgumentException if the limits cannot describe a valid
		 *         Aeron envelope
		 */
		public AeronReplicationConfiguration build()
		{
			if (!BitUtil.isPowerOfTwo(this.termLength) || this.termLength < 64 * 1024)
			{
				throw new IllegalArgumentException("termLength must be a power of two >= 64 KiB");
			}
			if (this.mtuLength < 512 || this.mtuLength > 64 * 1024 || (this.mtuLength & 7) != 0)
			{
				throw new IllegalArgumentException("mtuLength must be an aligned value between 512 and 65536");
			}
			if (this.maxTransactionBytes <= 0 || this.maxTransactionBytes > MAX_SUPPORTED_TRANSACTION_BYTES)
			{
				throw new IllegalArgumentException(
					"maxTransactionBytes must be between 1 and " + MAX_SUPPORTED_TRANSACTION_BYTES);
			}
			if (this.chunkSize <= 0 || this.chunkSize > this.maxTransactionBytes)
			{
				throw new IllegalArgumentException("chunkSize must be positive and <= maxTransactionBytes");
			}
			final long packetCount = (this.maxTransactionBytes + (long)this.chunkSize - 1L) / this.chunkSize;
			if (packetCount > ReplicationLimits.MAX_PACKET_COUNT)
			{
				throw new IllegalArgumentException(
					"maxTransactionBytes requires more than " + ReplicationLimits.MAX_PACKET_COUNT + " packets");
			}
			if (this.offerTimeoutNanos <= 0 || this.recordingStartTimeoutNanos <= 0 ||
				this.recordedPositionTimeoutNanos <= 0 || this.recordingStopTimeoutNanos <= 0 ||
				this.readerStopTimeoutNanos <= 0 || this.durabilityMode == null)
			{
				throw new IllegalArgumentException(
					"all Aeron timeouts must be positive and durabilityMode must be set");
			}
			final int maxMessageLength = maxMessageLengthForTermLength(this.termLength);
			if ((long)this.chunkSize + AeronReplicationEnvelope.HEADER_LENGTH > maxMessageLength)
			{
				throw new IllegalArgumentException(
					"chunkSize plus envelope exceeds Aeron maxMessageLength=" + maxMessageLength);
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
				this.durabilityMode
			);
		}
	}
}
