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

import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageAcceptor;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageComProvider;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageReceiver;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCacheMessageSender;
import org.eclipse.datagrid.cache.clustered.types.ClusteredCachePropertyParsers;
import org.eclipse.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.aeron.ChannelUri;
import io.aeron.CommonContext;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.eclipse.datagrid.cache.clustered.types.ClusteredCachePropertyParsers.booleanProperty;
import static org.eclipse.datagrid.cache.clustered.types.ClusteredCachePropertyParsers.intProperty;
import static org.eclipse.datagrid.cache.clustered.types.ClusteredCachePropertyParsers.longProperty;
import static org.eclipse.datagrid.cache.clustered.types.ClusteredCachePropertyParsers.stringProperty;

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
	private static final Logger logger = LoggerFactory.getLogger(AeronClusteredCacheMessageComProvider.class);

	private static final String DEFAULT_CHANNEL = "aeron:ipc";
	private static final int DEFAULT_STREAM_ID = 2001;
	private static final long DEFAULT_OFFER_TIMEOUT_MILLIS = 5_000L;
	private static final long DEFAULT_DRIVER_TIMEOUT_MILLIS = 10_000L;
	private static final int DEFAULT_MAX_PAYLOAD_BYTES = 1 << 20;

	private AeronClusteredCacheResources resources;
	private byte[] senderId;
	private AtomicLong sequence;
	private String configuredNodeId;

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
		final byte[] senderId = this.ensureSenderId(properties);
		final AeronClusteredCacheResources resources = this.ensureResources(properties);
		final AtomicLong sequence = this.ensureSequence(properties);
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
		return AeronClusteredCacheMessageSender.UpdateTimestamps(
			resources,
			senderId,
			sequence,
			serializer,
			offerTimeoutNanos,
			intProperty(properties, AeronClusteredConfigurationPropertyNames.MAX_PAYLOAD_BYTES,
				DEFAULT_MAX_PAYLOAD_BYTES, 1));
	}

	/**
	 * {@inheritDoc}
	 *
	 * <p>The receiver is a best-effort consumer: a malformed frame is logged and
	 * skipped so later invalidations still arrive, and a gap in one sender's
	 * sequence is reported so a lost burst is observable.</p>
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
		/* Validate the node id before creating resources so a conflicting second
		 * call fails with IllegalArgumentException and cannot leave closed
		 * resources behind for a retry. */
		final byte[] senderId = this.ensureSenderId(properties);
		return new AeronClusteredCacheMessageReceiver(
			this.ensureResources(properties),
			senderId,
			serializer,
			messageAcceptor,
			intProperty(properties, AeronClusteredConfigurationPropertyNames.MAX_PAYLOAD_BYTES,
				DEFAULT_MAX_PAYLOAD_BYTES, 1));
	}

	/**
	 * Returns the 16-byte sender identity shared by this provider's sender and
	 * receiver. It is derived from the configured {@code node-id} UUID, or
	 * generated once per provider instance when no node id is configured. The
	 * identity is fixed on first use; a later call with a different node id is
	 * rejected.
	 */
	private byte[] ensureSenderId(@SuppressWarnings("rawtypes") final Map properties)
	{
		final String configured = stringProperty(
			properties, AeronClusteredConfigurationPropertyNames.NODE_ID, null);
		if (this.senderId == null)
		{
			final UUID identity = configured == null
				? UUID.randomUUID()
				: parseNodeId(configured);
			this.configuredNodeId = configured;
			this.senderId = ByteBuffer.allocate(Long.BYTES * 2).order(ByteOrder.BIG_ENDIAN)
				.putLong(identity.getMostSignificantBits())
				.putLong(identity.getLeastSignificantBits())
				.array();
		}
		else if (!Objects.equals(this.configuredNodeId, configured))
		{
			throw new IllegalArgumentException(
				"Conflicting " + AeronClusteredConfigurationPropertyNames.NODE_ID +
					": the provider is already bound to node id " + this.configuredNodeId +
					", requested " + configured);
		}
		return this.senderId;
	}

	/**
	 * Returns the sequence source for this provider's identity: a per-provider
	 * counter when the identity is random, or the process-wide counter shared
	 * by every provider configured with the same {@code node-id} on the same
	 * channel.
	 */
	private AtomicLong ensureSequence(@SuppressWarnings("rawtypes") final Map properties)
	{
		if (this.sequence == null)
		{
			final String channel = stringProperty(
				properties, AeronClusteredConfigurationPropertyNames.CHANNEL, DEFAULT_CHANNEL);
			final int streamId = intProperty(
				properties, AeronClusteredConfigurationPropertyNames.STREAM_ID, DEFAULT_STREAM_ID, 0);
			this.sequence = this.configuredNodeId == null
				? new AtomicLong()
				: AeronClusteredCacheSenderSequence.shared(this.senderId, channel, streamId);
		}
		return this.sequence;
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
				logger.warn("No {} configured; defaulting to aeron:ipc, which is single-host. " +
					"Multi-host deployments must configure a UDP channel with control-mode=dynamic.",
					AeronClusteredConfigurationPropertyNames.CHANNEL);
			}
			if (embeddedDriver && directory == null)
			{
				logger.warn("No {} configured with {} enabled; the embedded MediaDriver will use a " +
					"generated private directory.",
					AeronClusteredConfigurationPropertyNames.DIRECTORY,
					AeronClusteredConfigurationPropertyNames.EMBEDDED_DRIVER);
			}
			validateChannel(channel, embeddedDriver);
			this.resources = new AeronClusteredCacheResources(
				directory, channel, streamId, driverTimeoutMillis, embeddedDriver);
			logger.debug("Bound Aeron clustered-cache resources to channel={}, streamId={}", channel, streamId);
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
			if (channel.contains("=0.0.0.0:") || channel.contains("=*:") || channel.contains("=[::]:"))
			{
				throw new IllegalArgumentException(
					AeronClusteredConfigurationPropertyNames.CHANNEL +
						" must not bind a wildcard endpoint: " + channel);
			}
			if (!embeddedDriver &&
				(channel.contains("=localhost:") || channel.contains("=127.0.0.1:") || channel.contains("=[::1]:")))
			{
				throw new IllegalArgumentException(
					AeronClusteredConfigurationPropertyNames.CHANNEL +
						" must not use a loopback endpoint outside an embedded driver: " + channel);
			}
		}
	}
}