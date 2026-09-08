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

/** Immutable transport limits shared by a writer and all readers. */
public final class AeronReplicationConfiguration
{
	private static final String PREFIX = "eclipsestore.distribution.aeron.";
	public static final String TERM_LENGTH_PROPERTY = PREFIX + "term-length";
	public static final String MTU_LENGTH_PROPERTY = PREFIX + "mtu-length";
	public static final String CHUNK_SIZE_PROPERTY = PREFIX + "chunk-size";
	public static final String MAX_TRANSACTION_BYTES_PROPERTY = PREFIX + "max-transaction-bytes";
	public static final String OFFER_TIMEOUT_NANOS_PROPERTY = PREFIX + "offer-timeout-nanos";
	public static final String DURABILITY_MODE_PROPERTY = PREFIX + "durability-mode";
	public static final int DEFAULT_TERM_LENGTH = 16 * 1024 * 1024;
	public static final int DEFAULT_MTU_LENGTH = 1408;
	public static final int DEFAULT_CHUNK_SIZE = 1024 * 1024;
	public static final int DEFAULT_MAX_TRANSACTION_BYTES = 64 * 1024 * 1024;

	private final int termLength;
	private final int mtuLength;
	private final int chunkSize;
	private final int maxTransactionBytes;
	private final long offerTimeoutNanos;
	private final ReplicationDurabilityMode durabilityMode;

	private AeronReplicationConfiguration(
		final int termLength,
		final int mtuLength,
		final int chunkSize,
		final int maxTransactionBytes,
		final long offerTimeoutNanos,
		final ReplicationDurabilityMode durabilityMode
	)
	{
		this.termLength = termLength;
		this.mtuLength = mtuLength;
		this.chunkSize = chunkSize;
		this.maxTransactionBytes = maxTransactionBytes;
		this.offerTimeoutNanos = offerTimeoutNanos;
		this.durabilityMode = durabilityMode;
	}

	public static AeronReplicationConfiguration defaults()
	{
		return builder().build();
	}

	public static Builder builder()
	{
		return new Builder();
	}

	public static AeronReplicationConfiguration from(final Properties properties)
	{
		Objects.requireNonNull(properties, "properties");
		return builder()
			.termLength(integer(properties, TERM_LENGTH_PROPERTY, DEFAULT_TERM_LENGTH))
			.mtuLength(integer(properties, MTU_LENGTH_PROPERTY, DEFAULT_MTU_LENGTH))
			.chunkSize(integer(properties, CHUNK_SIZE_PROPERTY, DEFAULT_CHUNK_SIZE))
			.maxTransactionBytes(integer(properties, MAX_TRANSACTION_BYTES_PROPERTY, DEFAULT_MAX_TRANSACTION_BYTES))
			.offerTimeoutNanos(longValue(properties, OFFER_TIMEOUT_NANOS_PROPERTY, 30_000_000_000L))
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

	public int termLength()
	{
		return this.termLength;
	}

	public int mtuLength()
	{
		return this.mtuLength;
	}

	public int chunkSize()
	{
		return this.chunkSize;
	}

	public int maxTransactionBytes()
	{
		return this.maxTransactionBytes;
	}

	public long offerTimeoutNanos()
	{
		return this.offerTimeoutNanos;
	}

	public ReplicationDurabilityMode durabilityMode()
	{
		return this.durabilityMode;
	}

	/**
	 * The Aeron publication limit is termLength / 8, capped by 16 MiB. The
	 * application envelope must fit within that limit because one logical chunk
	 * is offered as one Aeron message and Aeron then fragments it on the wire.
	 */
	public int maxMessageLength()
	{
		return Math.min(this.termLength / 8, 16 * 1024 * 1024);
	}

	public static final class Builder
	{
		private int termLength = DEFAULT_TERM_LENGTH;
		private int mtuLength = DEFAULT_MTU_LENGTH;
		private int chunkSize = DEFAULT_CHUNK_SIZE;
		private int maxTransactionBytes = DEFAULT_MAX_TRANSACTION_BYTES;
		private long offerTimeoutNanos = 30_000_000_000L;
		private ReplicationDurabilityMode durabilityMode = ReplicationDurabilityMode.ARCHIVE_FIRST;

		public Builder termLength(final int value)
		{
			this.termLength = value;
			return this;
		}

		public Builder mtuLength(final int value)
		{
			this.mtuLength = value;
			return this;
		}

		public Builder chunkSize(final int value)
		{
			this.chunkSize = value;
			return this;
		}

		public Builder maxTransactionBytes(final int value)
		{
			this.maxTransactionBytes = value;
			return this;
		}

		public Builder offerTimeoutNanos(final long value)
		{
			this.offerTimeoutNanos = value;
			return this;
		}

		public Builder durabilityMode(final ReplicationDurabilityMode value)
		{
			this.durabilityMode = Objects.requireNonNull(value, "durabilityMode");
			return this;
		}

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
			if (this.chunkSize <= 0 || this.chunkSize > this.maxTransactionBytes)
			{
				throw new IllegalArgumentException("chunkSize must be positive and <= maxTransactionBytes");
			}
            if (this.offerTimeoutNanos <= 0 || this.durabilityMode == null)
			{
				throw new IllegalArgumentException("offerTimeoutNanos must be positive and durabilityMode must be set");
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
				this.durabilityMode
			);
		}
	}
}
