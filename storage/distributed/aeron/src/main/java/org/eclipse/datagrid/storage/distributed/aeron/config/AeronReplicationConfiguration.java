package org.eclipse.datagrid.storage.distributed.aeron.config;

import org.agrona.BitUtil;
import org.eclipse.datagrid.storage.distributed.aeron.wire.AeronReplicationEnvelope;
import org.eclipse.datagrid.storage.distributed.types.ReplicationDurabilityMode;

import java.util.Objects;
import java.util.Properties;

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
	public static final String TERM_LENGTH_PROPERTY = PREFIX + "term-length";
	public static final String MTU_LENGTH_PROPERTY = PREFIX + "mtu-length";
	public static final String CHUNK_SIZE_PROPERTY = PREFIX + "chunk-size";
	public static final String MAX_TRANSACTION_BYTES_PROPERTY = PREFIX + "max-transaction-bytes";
	public static final String OFFER_TIMEOUT_NANOS_PROPERTY = PREFIX + "offer-timeout-nanos";
	public static final String READER_STOP_TIMEOUT_NANOS_PROPERTY = PREFIX + "reader-stop-timeout-nanos";
	private static final long DEFAULT_OFFER_TIMEOUT_NANOS = 30_000_000_000L;
	private static final long DEFAULT_READER_STOP_TIMEOUT_NANOS = 30_000_000_000L;
	public static final String DURABILITY_MODE_PROPERTY = PREFIX + "durability-mode";
	public static final int DEFAULT_TERM_LENGTH = 16 * 1024 * 1024;
	public static final int DEFAULT_MTU_LENGTH = 1408;
	public static final int DEFAULT_CHUNK_SIZE = 1024 * 1024;
	public static final int DEFAULT_MAX_TRANSACTION_BYTES = 64 * 1024 * 1024;
	/** Hard upper bound prevents a configuration from requesting near-Integer.MAX_VALUE native buffers. */
	public static final int MAX_SUPPORTED_TRANSACTION_BYTES = 1024 * 1024 * 1024;

	private final int termLength;
	private final int mtuLength;
	private final int chunkSize;
	private final int maxTransactionBytes;
	private final long offerTimeoutNanos;
	private final long readerStopTimeoutNanos;
	private final ReplicationDurabilityMode durabilityMode;

	private AeronReplicationConfiguration(
		final int termLength,
		final int mtuLength,
		final int chunkSize,
		final int maxTransactionBytes,
		final long offerTimeoutNanos,
		final long readerStopTimeoutNanos,
		final ReplicationDurabilityMode durabilityMode
	)
	{
		this.termLength = termLength;
		this.mtuLength = mtuLength;
		this.chunkSize = chunkSize;
		this.maxTransactionBytes = maxTransactionBytes;
		this.offerTimeoutNanos = offerTimeoutNanos;
		this.readerStopTimeoutNanos = readerStopTimeoutNanos;
		this.durabilityMode = durabilityMode;
	}

	/** Returns the validated default configuration. */
	public static AeronReplicationConfiguration defaults()
	{
		return builder().build();
	}

	/** Starts a builder with the documented defaults. */
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
		final String offerTimeout = properties.getProperty(OFFER_TIMEOUT_NANOS_PROPERTY);
		final long offerTimeoutNanos;
		if (offerTimeout == null)
		{
			offerTimeoutNanos = DEFAULT_OFFER_TIMEOUT_NANOS;
		}
		else
		{
			try
			{
				offerTimeoutNanos = Long.parseLong(offerTimeout.trim());
			}
			catch (final NumberFormatException failure)
			{
				throw new IllegalArgumentException(
					"Invalid long for " + OFFER_TIMEOUT_NANOS_PROPERTY + ": " + offerTimeout, failure);
			}
		}
		final long readerStopTimeoutNanos;
		final String readerStopTimeout = properties.getProperty(READER_STOP_TIMEOUT_NANOS_PROPERTY);
		if (readerStopTimeout == null)
		{
			readerStopTimeoutNanos = DEFAULT_READER_STOP_TIMEOUT_NANOS;
		}
		else
		{
			try
			{
				readerStopTimeoutNanos = Long.parseLong(readerStopTimeout.trim());
			}
			catch (final NumberFormatException failure)
			{
				throw new IllegalArgumentException(
					"Invalid long for " + READER_STOP_TIMEOUT_NANOS_PROPERTY + ": " + readerStopTimeout, failure);
			}
		}
		return builder()
			.termLength(integer(properties, TERM_LENGTH_PROPERTY, DEFAULT_TERM_LENGTH))
			.mtuLength(integer(properties, MTU_LENGTH_PROPERTY, DEFAULT_MTU_LENGTH))
			.chunkSize(integer(properties, CHUNK_SIZE_PROPERTY, DEFAULT_CHUNK_SIZE))
			.maxTransactionBytes(integer(properties, MAX_TRANSACTION_BYTES_PROPERTY, DEFAULT_MAX_TRANSACTION_BYTES))
			.offerTimeoutNanos(offerTimeoutNanos)
			.readerStopTimeoutNanos(readerStopTimeoutNanos)
			.durabilityMode(mode(properties.getProperty(DURABILITY_MODE_PROPERTY)))
			.build();
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
			case "local-durable-first", "local_durable_first" -> ReplicationDurabilityMode.LOCAL_DURABLE_FIRST;
			default -> throw new IllegalArgumentException("Unknown replication durability mode: " + value);
		};
	}

	/** Returns the Aeron term length in bytes. */
	public int termLength()
	{
		return this.termLength;
	}

	/** Returns the transport MTU used when the publication is created. */
	public int mtuLength()
	{
		return this.mtuLength;
	}

	/** Returns the largest logical Store-data chunk in one envelope. */
	public int chunkSize()
	{
		return this.chunkSize;
	}

	/** Returns the largest Store transaction accepted by the writer. */
	public int maxTransactionBytes()
	{
		return this.maxTransactionBytes;
	}

	/** Returns the deadline used for publication and Archive progress waits. */
	public long offerTimeoutNanos()
	{
		return this.offerTimeoutNanos;
	}

	/** Returns the bounded wait used when a reader is stopped at the live tail. */
	public long readerStopTimeoutNanos()
	{
		return this.readerStopTimeoutNanos;
	}

	/** Returns the order in which local acceptance and Archive publication occur. */
	public ReplicationDurabilityMode durabilityMode()
	{
		return this.durabilityMode;
	}

	/**
	 * Returns the largest envelope message that this publication may offer.
	 * Aeron fragments that message according to the MTU; the logical chunk must
	 * still fit within this publication limit.
	 */
	public int maxMessageLength()
	{
		return Math.min(this.termLength / 8, 16 * 1024 * 1024);
	}

	/** Builds an immutable Aeron replication configuration. */
	public static final class Builder
	{
		private int termLength = DEFAULT_TERM_LENGTH;
		private int mtuLength = DEFAULT_MTU_LENGTH;
		private int chunkSize = DEFAULT_CHUNK_SIZE;
		private int maxTransactionBytes = DEFAULT_MAX_TRANSACTION_BYTES;
		private long offerTimeoutNanos = DEFAULT_OFFER_TIMEOUT_NANOS;
		private long readerStopTimeoutNanos = DEFAULT_READER_STOP_TIMEOUT_NANOS;
		private ReplicationDurabilityMode durabilityMode = ReplicationDurabilityMode.ARCHIVE_FIRST;

		/** Sets the term length; it must be a power of two of at least 64 KiB. */
		public Builder termLength(final int value)
		{
			this.termLength = value;
			return this;
		}

		/** Sets the aligned network MTU used by the publication. */
		public Builder mtuLength(final int value)
		{
			this.mtuLength = value;
			return this;
		}

		/** Sets the logical data chunk size. */
		public Builder chunkSize(final int value)
		{
			this.chunkSize = value;
			return this;
		}

		/** Sets the largest complete Store transaction accepted. */
		public Builder maxTransactionBytes(final int value)
		{
			this.maxTransactionBytes = value;
			return this;
		}

		/** Sets the maximum wait for publication or Archive progress. */
		public Builder offerTimeoutNanos(final long value)
		{
			this.offerTimeoutNanos = value;
			return this;
		}

		/** Sets the maximum wait for a reader to stop at a resolved boundary. */
		public Builder readerStopTimeoutNanos(final long value)
		{
			this.readerStopTimeoutNanos = value;
			return this;
		}

		/** Sets the local-versus-Archive ordering used by the writer. */
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
			if (this.offerTimeoutNanos <= 0 || this.readerStopTimeoutNanos <= 0 || this.durabilityMode == null)
			{
				throw new IllegalArgumentException(
					"offerTimeoutNanos and readerStopTimeoutNanos must be positive and durabilityMode must be set");
			}
			final int maxMessageLength = Math.min(this.termLength / 8, 16 * 1024 * 1024);
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
				this.readerStopTimeoutNanos,
				this.durabilityMode
			);
		}
	}
}
