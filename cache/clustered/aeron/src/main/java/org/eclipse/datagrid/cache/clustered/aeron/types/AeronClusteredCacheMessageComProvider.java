package org.eclipse.datagrid.cache.clustered.aeron.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered Aeron
 * %%
 * Copyright (C) 2025 - 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageAcceptor;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageComProvider;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageReceiver;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageSender;
import org.eclipse.serializer.Serializer;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.eclipse.datagrid.cache.clustered.types.ClusteredCachePropertyParsers.*;

/**
 * This provider builds the Aeron sender and receiver for clustered cache
 * invalidation.
 *
 * <p>One provider owns one Aeron client, one publication, and one subscription
 * for a node. The sender and receiver share a 16-byte sender identity carried
 * in every frame, so a node ignores its own invalidations, and both use the
 * same channel and stream id, so every node in a cluster sees every other
 * node's updates.</p>
 *
 * <p>The provider is created reflectively by Hibernate through its
 * no-argument constructor and reads {@link AeronClusteredConfigurationPropertyNames}
 * when the sender and receiver are requested. It provides the same synchronous,
 * fail-on-error contract as the Kafka provider, so the transport can be swapped
 * without changing cache semantics.</p>
 *
 * <p>One provider owns at most one sender and one receiver. Repeated requests
 * return the same handle; a request with a different serializer, acceptor, or
 * transport limit is rejected instead of creating a second handle that could
 * close the shared publication or subscription underneath the first.</p>
 *
 * <p>Topology: the default channel is {@code aeron:ipc}, which is single-host.
 * Multi-host deployments must configure a UDP channel with
 * {@code control-mode=dynamic} (Aeron MDC), which the provider validates;
 * plain unicast UDP and loopback UDP would silently drop cross-node traffic or
 * never leave the host. The channel is assumed to be on an isolated network;
 * like the Kafka adapter it carries no authentication.</p>
 *
 * <p>Self-suppression compares the 16-byte sender identity in each frame
 * against the identity shared by this provider's sender and receiver. The
 * identity is a random UUID per provider unless {@code node-id} is configured;
 * configure the same node id on every provider of one node (for example
 * several Hibernate session factories) so they suppress each other's
 * invalidations. The identity is fixed on first use; a later call with a
 * different node id is rejected.</p>
 *
 * <p>Failure latency differs from Kafka: the sender waits at most the
 * configured offer timeout (default 5 s) for the publication to accept a
 * frame, while the Kafka sender waits for {@code delivery.timeout.ms} (default
 * 120 s). Both fail the local cache operation, but after different delays.
 * The offer timeout bounds the publication handshake; the driver timeout
 * bounds the Aeron client's connection to the MediaDriver separately.</p>
 */
public class AeronClusteredCacheMessageComProvider implements ClusteredCacheMessageComProvider
{
	private static final System.Logger LOGGER =
		System.getLogger(AeronClusteredCacheMessageComProvider.class.getName());

	private static final String DEFAULT_CHANNEL = "aeron:ipc";
	private static final int DEFAULT_STREAM_ID = 2001;
	private static final long DEFAULT_OFFER_TIMEOUT_MILLIS = 5_000L;
	private static final long DEFAULT_DRIVER_TIMEOUT_MILLIS = 10_000L;
	private static final int DEFAULT_MAX_PAYLOAD_BYTES = 1 << 20;
	private static final int MAX_PAYLOAD_BYTES = Integer.MAX_VALUE - AeronClusteredCacheMessageCodec.HEADER_LENGTH;
	/* A configured node id identifies all cache providers in one JVM, while the
	 * random incarnation in the wire identity changes after a process restart.
	 * Deriving the identity directly avoids an unbounded process-wide map keyed by
	 * configuration strings while preserving same-node self suppression. */
	private static final UUID PROCESS_INCARNATION = UUID.randomUUID();

	private AeronClusteredCacheResources resources;
	private byte[] senderId;
	private AeronClusteredCacheSenderSequence.SequenceLease sequenceLease;
	private Object sequenceLock;
	private String configuredNodeId;
	private ClusteredCacheMessageSender<Object, Object> sender;
	private Serializer<byte[]> senderSerializer;
	private long senderOfferTimeoutNanos;
	private int senderMaxPayloadBytes;
	private ClusteredCacheMessageReceiver receiver;
	private Serializer<byte[]> receiverSerializer;
	private ClusteredCacheMessageAcceptor receiverAcceptor;
	private int receiverMaxPayloadBytes;
	private boolean senderSequenceReleased;
	private boolean receiverSequenceReleased;

	/** Creates a provider with no Aeron resources yet. */
	public AeronClusteredCacheMessageComProvider()
	{
	}

	/**
	 * {@inheritDoc}
	 *
	 * <p>The sender is synchronous and fails the local cache operation when the
	 * invalidation cannot be published.</p>
	 */
	@Override
	public synchronized ClusteredCacheMessageSender<Object, Object> provideUpdateTimestampsCacheMessageSender(
		@SuppressWarnings("rawtypes") final Map properties,
		final Serializer<byte[]> serializer
	)
	{
		if (properties == null)
		{
			throw new NullPointerException("properties");
		}
		Objects.requireNonNull(serializer, "serializer");
		this.ensureProviderOpen();
		final byte[] senderId = this.ensureSenderId(properties);
		final long offerTimeoutMillis = longProperty(properties,
			AeronClusteredConfigurationPropertyNames.OFFER_TIMEOUT_MILLIS, DEFAULT_OFFER_TIMEOUT_MILLIS, 0L);
		final long offerTimeoutNanos;
		try
		{
			offerTimeoutNanos = Math.multiplyExact(offerTimeoutMillis, 1_000_000L);
		}
		catch (final ArithmeticException failure)
		{
			throw new IllegalArgumentException(
				AeronClusteredConfigurationPropertyNames.OFFER_TIMEOUT_MILLIS +
					" is too large: " + offerTimeoutMillis, failure);
		}
		final int maxPayloadBytes = maxPayloadBytes(properties);
		if (this.receiverSerializer != null && this.receiverSerializer != serializer)
		{
			throw new IllegalArgumentException(
				"The Aeron clustered-cache sender and receiver must use the same serializer");
		}
		if (this.sender != null)
		{
			if (this.sender instanceof AeronClusteredCacheMessageSender existingSender && existingSender.isDisposed())
			{
				throw new IllegalStateException(
					"The Aeron clustered-cache sender is single-use and has been disposed; create a new provider");
			}
			if (this.senderSerializer != serializer ||
				this.senderOfferTimeoutNanos != offerTimeoutNanos || this.senderMaxPayloadBytes != maxPayloadBytes)
			{
				throw new IllegalArgumentException(
					"The Aeron clustered-cache provider already owns a sender with different serializer or limits");
			}
			return this.sender;
		}
		/* Validate every value that can reject the binding before starting an
		 * embedded driver or acquiring the shared sequence lease. A malformed
		 * first request must not leak a live Aeron runtime that no handle owns. */
		final AeronClusteredCacheResources resources = this.ensureResources(properties);
		final AeronClusteredCacheSenderSequence.SequenceLease sequence;
		try
		{
			sequence = this.ensureSequence(properties);
		}
		catch (final RuntimeException | Error failure)
		{
			this.releaseSequenceIfUnused();
			this.closeUnboundResources(failure);
			throw failure;
		}
		final ClusteredCacheMessageSender<Object, Object> created;
		try
		{
			created = AeronClusteredCacheMessageSender.UpdateTimestamps(
				resources,
				senderId,
				sequence,
				this.sequenceLock,
				this::senderClosed,
				serializer,
				offerTimeoutNanos,
				maxPayloadBytes);
		}
		catch (final RuntimeException | Error failure)
		{
			/* A provider owns no usable handle yet. Do not leave a sequence lease or
			 * lazily-created resources stranded when a future sender constructor gains
			 * validation or allocation that can fail. */
			this.releaseSequenceIfUnused();
			this.closeUnboundResources(failure);
			throw failure;
		}
		this.senderSerializer = serializer;
		this.senderOfferTimeoutNanos = offerTimeoutNanos;
		this.senderMaxPayloadBytes = maxPayloadBytes;
		this.sender = created;
		return this.sender;
	}

	/**
	 * {@inheritDoc}
	 *
 * <p>The receiver fails closed on malformed or undeserializable frames, on a
 * valid message that cannot be applied, and on a sender sequence gap. A
 * volatile invalidation stream cannot prove that a bad frame was harmless or
 * repair a missed message, so continuing would expose stale cache entries.</p>
	 */
	@Override
	public synchronized ClusteredCacheMessageReceiver provideMessageReceiver(
		@SuppressWarnings("rawtypes") final Map properties,
		final Serializer<byte[]> serializer,
		final ClusteredCacheMessageAcceptor messageAcceptor
	)
	{
		if (properties == null)
		{
			throw new NullPointerException("properties");
		}
		Objects.requireNonNull(serializer, "serializer");
		Objects.requireNonNull(messageAcceptor, "messageAcceptor");
		this.ensureProviderOpen();
		/* Validate the node id before creating resources so a conflicting second
		 * call fails with IllegalArgumentException and cannot leave closed
		 * resources behind for a retry. */
		final byte[] senderId = this.ensureSenderId(properties);
		final int maxPayloadBytes = maxPayloadBytes(properties);
		if (this.senderSerializer != null && this.senderSerializer != serializer)
		{
			throw new IllegalArgumentException(
				"The Aeron clustered-cache sender and receiver must use the same serializer");
		}
		if (this.receiver != null)
		{
			if (this.receiver instanceof AeronClusteredCacheMessageReceiver existingReceiver && existingReceiver.isDisposed())
			{
				throw new IllegalStateException(
					"The Aeron clustered-cache receiver is single-use and has been disposed; create a new provider");
			}
			if (this.receiverSerializer != serializer || this.receiverAcceptor != messageAcceptor ||
				this.receiverMaxPayloadBytes != maxPayloadBytes)
			{
				throw new IllegalArgumentException(
					"The Aeron clustered-cache provider already owns a receiver with different configuration");
			}
			return this.receiver;
		}
		final AeronClusteredCacheResources resources = this.ensureResources(properties);
		final ClusteredCacheMessageReceiver created;
		try
		{
			created = new AeronClusteredCacheMessageReceiver(
				resources,
				senderId,
				this::receiverClosed,
				serializer,
				messageAcceptor,
				maxPayloadBytes);
		}
		catch (final RuntimeException | Error failure)
		{
			this.closeUnboundResources(failure);
			throw failure;
		}
		this.receiverSerializer = serializer;
		this.receiverAcceptor = messageAcceptor;
		this.receiverMaxPayloadBytes = maxPayloadBytes;
		this.receiver = created;
		return this.receiver;
	}

	private static int maxPayloadBytes(@SuppressWarnings("rawtypes") final Map properties)
	{
		final int value = intProperty(properties, AeronClusteredConfigurationPropertyNames.MAX_PAYLOAD_BYTES,
			DEFAULT_MAX_PAYLOAD_BYTES, 1);
		if (value > MAX_PAYLOAD_BYTES)
		{
			throw new IllegalArgumentException(
				AeronClusteredConfigurationPropertyNames.MAX_PAYLOAD_BYTES + " must be at most " + MAX_PAYLOAD_BYTES);
		}
		return value;
	}

	private void ensureProviderOpen()
	{
		if (this.resources != null && this.resources.isClosed())
		{
			throw new IllegalStateException(
				"Aeron clustered-cache provider resources are closed; create a new provider for a new lifecycle");
		}
	}

	/**
	 * Returns the 16-byte sender identity shared by this provider's sender and
	 * receiver. It is derived from the configured {@code node-id} UUID, or
	 * generated once per provider instance when no node id is configured. A
	 * configured node id maps to one random process incarnation shared by all
	 * providers in this JVM; a new JVM gets a new incarnation so its sequence
	 * restart cannot be mistaken for a lost frame. The identity is fixed on
	 * first use; a later call with a different node id is rejected.
	 */
	private byte[] ensureSenderId(@SuppressWarnings("rawtypes") final Map properties)
	{
		final String configured = stringProperty(
			properties, AeronClusteredConfigurationPropertyNames.NODE_ID, null);
		final UUID configuredUuid = configured == null ? null : parseNodeId(configured);
		final String normalized = configuredUuid == null ? null : configuredUuid.toString();
		if (this.senderId == null)
		{
			this.configuredNodeId = normalized;
			this.senderId = configured == null
				? uuidBytes(UUID.randomUUID())
				: uuidBytes(new UUID(
					PROCESS_INCARNATION.getMostSignificantBits() ^ configuredUuid.getMostSignificantBits(),
					PROCESS_INCARNATION.getLeastSignificantBits() ^ configuredUuid.getLeastSignificantBits()));
		}
		else if (!Objects.equals(this.configuredNodeId, normalized))
		{
			throw new IllegalArgumentException(
				"Conflicting " + AeronClusteredConfigurationPropertyNames.NODE_ID +
					": the provider is already bound to node id " + this.configuredNodeId +
					", requested " + configured);
		}
		return this.senderId;
	}

	private static byte[] uuidBytes(final UUID value)
	{
		final byte[] bytes = new byte[Long.BYTES * 2];
		putLong(bytes, 0, value.getMostSignificantBits());
		putLong(bytes, Long.BYTES, value.getLeastSignificantBits());
		return bytes;
	}

	private static void putLong(final byte[] target, final int offset, final long value)
	{
		for (int index = 0; index < Long.BYTES; index++)
		{
			target[offset + index] = (byte)(value >>> (Long.BYTES - 1 - index) * Byte.SIZE);
		}
	}

	/**
	 * Returns the sequence source for this provider's identity. Configured node
	 * identities use a process-wide sequence shared by every provider on the
	 * same channel. The entry survives provider disposal so sequence continuity
	 * is preserved while remote receivers remain attached.
	 */
	private AeronClusteredCacheSenderSequence.SequenceLease ensureSequence(
		@SuppressWarnings("rawtypes") final Map properties
	)
	{
		if (this.sequenceLease == null)
		{
			final String channel = stringProperty(
				properties, AeronClusteredConfigurationPropertyNames.CHANNEL, DEFAULT_CHANNEL);
			final int streamId = intProperty(
				properties, AeronClusteredConfigurationPropertyNames.STREAM_ID, DEFAULT_STREAM_ID, 0);
				if (this.configuredNodeId == null)
				{
					this.sequenceLease = AeronClusteredCacheSenderSequence.SequenceLease.local();
					this.sequenceLock = new Object();
				}
				else
				{
					this.sequenceLease = AeronClusteredCacheSenderSequence.acquire(this.senderId, channel, streamId);
					this.sequenceLock = this.sequenceLease.lock();
				}
			}
		return this.sequenceLease;
	}

	private synchronized void senderClosed()
	{
		this.senderSequenceReleased = true;
		this.releaseSequenceIfUnused();
	}

	private synchronized void receiverClosed()
	{
		this.receiverSequenceReleased = true;
		this.releaseSequenceIfUnused();
	}

	private void releaseSequenceIfUnused()
	{
		if (this.sequenceLease != null &&
			(this.sender == null || this.senderSequenceReleased) &&
			(this.receiver == null || this.receiverSequenceReleased))
		{
			this.sequenceLease.close();
			this.sequenceLease = null;
		}
	}

	private void closeUnboundResources(final Throwable primary)
	{
		if (this.sender != null || this.receiver != null || this.resources == null) return;
		try
		{
			this.resources.close();
			this.resources = null;
		}
		catch (final RuntimeException cleanupFailure)
		{
			if (primary != cleanupFailure) primary.addSuppressed(cleanupFailure);
		}
	}

	private static UUID parseNodeId(final String configured)
	{
		try
		{
			final UUID identity = UUID.fromString(configured.trim());
			if (identity.equals(new UUID(0L, 0L)))
			{
				throw new IllegalArgumentException("the zero UUID is not a valid node id");
			}
			return identity;
		}
		catch (final IllegalArgumentException failure)
		{
			throw new IllegalArgumentException(
				AeronClusteredConfigurationPropertyNames.NODE_ID + " must be a UUID: " + configured, failure);
		}
	}

	private AeronClusteredCacheResources ensureResources(@SuppressWarnings("rawtypes") final Map properties)
	{
		final String channel = stringProperty(
			properties, AeronClusteredConfigurationPropertyNames.CHANNEL, DEFAULT_CHANNEL);
		final int streamId = intProperty(
			properties, AeronClusteredConfigurationPropertyNames.STREAM_ID, DEFAULT_STREAM_ID, 0);
		final String directory = stringProperty(
			properties, AeronClusteredConfigurationPropertyNames.DIRECTORY, null);
		final long driverTimeoutMillis = longProperty(properties,
			AeronClusteredConfigurationPropertyNames.DRIVER_TIMEOUT_MILLIS, DEFAULT_DRIVER_TIMEOUT_MILLIS, 1L);
		final boolean embeddedDriver = booleanProperty(
			properties, AeronClusteredConfigurationPropertyNames.EMBEDDED_DRIVER, false);

		if (this.resources == null)
		{
			if (DEFAULT_CHANNEL.equals(channel))
			{
				LOGGER.log(System.Logger.Level.WARNING, "No " +
					AeronClusteredConfigurationPropertyNames.CHANNEL +
					" configured; defaulting to aeron:ipc, which is single-host. " +
					"Multi-host deployments must configure a UDP channel with control-mode=dynamic.");
			}
			if (embeddedDriver && directory == null)
			{
				LOGGER.log(System.Logger.Level.WARNING, "No " +
					AeronClusteredConfigurationPropertyNames.DIRECTORY + " configured with " +
					AeronClusteredConfigurationPropertyNames.EMBEDDED_DRIVER +
					" enabled; the embedded MediaDriver will use a generated private directory.");
			}
			validateChannel(channel, embeddedDriver);
			this.resources = new AeronClusteredCacheResources(
				directory, channel, streamId, driverTimeoutMillis, embeddedDriver);
			LOGGER.log(System.Logger.Level.DEBUG, "Bound Aeron clustered-cache resources to channel=" +
				channel + ", streamId=" + streamId);
		}
		else if (this.resources.isClosed())
		{
			throw new IllegalStateException(
				"Aeron clustered-cache provider resources are closed; create a new provider for a new lifecycle");
		}
		else if (!this.resources.matches(channel, streamId, directory, driverTimeoutMillis, embeddedDriver))
		{
			throw new IllegalArgumentException(
				"Conflicting Aeron clustered-cache configuration: the provider is already bound to " +
					this.resources.describe() +
					"; requested channel=" + channel + ", streamId=" + streamId +
					", directory=" + directory + ", driverTimeoutMillis=" + driverTimeoutMillis +
					", embeddedDriver=" + embeddedDriver);
		}
		return this.resources;
	}

	/**
	 * Validates the channel topology before any Aeron connection: UDP channels
	 * must use dynamic MDC so every node in the cluster sees every other node's
	 * frames, wildcard endpoints are rejected because the channel is expected to
	 * sit on an isolated, controlled network, and loopback UDP is rejected
	 * unless an embedded driver is used, because it would never leave the host.
	 */
	private static void validateChannel(final String channel, final boolean embeddedDriver)
	{
		final ChannelUri uri;
		try
		{
			uri = ChannelUri.parse(channel);
		}
		catch (final RuntimeException failure)
		{
			throw new IllegalArgumentException(
				"Invalid " + AeronClusteredConfigurationPropertyNames.CHANNEL + ": " + channel, failure);
		}
		if (uri.isUdp())
		{
			final String controlMode = uri.get(CommonContext.MDC_CONTROL_MODE_PARAM_NAME);
			if (!CommonContext.MDC_CONTROL_MODE_DYNAMIC.equalsIgnoreCase(controlMode))
			{
				throw new IllegalArgumentException(
					AeronClusteredConfigurationPropertyNames.CHANNEL +
						" must use control-mode=dynamic for N-to-N invalidation: " + channel +
						" (or use aeron:ipc for single-host deployments)");
			}
			final String endpoint = uri.get(CommonContext.ENDPOINT_PARAM_NAME);
			final String endpointHost = endpointHost(endpoint);
			final String controlHost = endpointHost(uri.get(CommonContext.MDC_CONTROL_PARAM_NAME));
			if (isWildcardHost(endpointHost) || isWildcardHost(controlHost))
			{
				throw new IllegalArgumentException(
					AeronClusteredConfigurationPropertyNames.CHANNEL +
						" must not bind a wildcard endpoint: " + channel);
			}
			if (!embeddedDriver && (isLoopbackHost(endpointHost) || isLoopbackHost(controlHost)))
			{
				throw new IllegalArgumentException(
					AeronClusteredConfigurationPropertyNames.CHANNEL +
						" must not use a loopback endpoint outside an embedded driver: " + channel);
			}
		}
	}

	private static String endpointHost(final String endpoint)
	{
		if (endpoint == null || endpoint.isBlank())
		{
			return null;
		}
		final String value = endpoint.trim();
		if (value.charAt(0) == '[')
		{
			final int closing = value.indexOf(']');
			return closing > 0 ? value.substring(1, closing) : value;
		}
		final int colon = value.lastIndexOf(':');
		return colon > 0 ? value.substring(0, colon) : value;
	}

	private static boolean isWildcardHost(final String host)
	{
		if (host == null)
		{
			return false;
		}
		final String normalized = host.trim().toLowerCase(java.util.Locale.ROOT);
		return normalized.equals("*") || normalized.equals("0.0.0.0") || normalized.equals("::") ||
			normalized.equals("0:0:0:0:0:0:0:0");
	}

	private static boolean isLoopbackHost(final String host)
	{
		if (host == null)
		{
			return false;
		}
		final String normalized = host.trim().toLowerCase(java.util.Locale.ROOT);
		return normalized.equals("localhost") || normalized.equals("127.0.0.1") ||
			normalized.equals("::1") || normalized.equals("0:0:0:0:0:0:0:1");
	}
}
