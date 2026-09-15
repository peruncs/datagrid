package peruncs.datagrid.cache.aeron;

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import peruncs.datagrid.cache.types.ClusteredCacheMessageAcceptor;

import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

/// This provider builds the Aeron sender and receiver for clustered cache
/// invalidation.
///
/// One provider owns one Aeron client, one publication, and one subscription
/// for a node. The sender and receiver share a 16-byte sender identity carried
/// in every frame, so a node ignores its own invalidations, and both use the
/// same channel and stream id, so every node in a cluster sees every other
/// node's updates.
///
/// The region factory creates one provider per session factory. The provider
/// takes an injected [AeronClusteredCacheConfiguration] when the sender and
/// receiver are requested. It provides a synchronous, fail-on-error contract so
/// cache semantics do not depend on transport timing.
///
/// One provider owns at most one sender and one receiver. Repeated requests
/// return the same handle; a request with a different acceptor or transport
/// limit is rejected instead of creating a second handle that could close the
/// shared publication or subscription underneath the first.
///
/// Topology: the default channel is `aeron:ipc`, which is single-host.
/// Multi-host deployments must configure a UDP channel with
/// `control-mode=dynamic` (Aeron MDC), which the provider validates;
/// plain unicast UDP and loopback UDP would silently drop cross-node traffic or
/// never leave the host. The channel is assumed to be on an isolated network;
/// it carries no authentication.
///
/// Self-suppression compares the 16-byte sender identity in each frame
/// against the identity shared by this provider's sender and receiver. The
/// identity is a random UUID per provider unless `node-id` is configured;
/// configure the same node id on every provider of one node (for example
/// several Hibernate session factories) so they suppress each other's
/// invalidations. The identity is fixed on first use; a later call with a
/// different node id is rejected.
///
/// The sender waits at most the configured offer timeout (default 5 s) for the
/// publication to accept a frame and fails the local cache operation on timeout.
/// The offer timeout bounds the publication handshake; the driver timeout
/// bounds the Aeron client's connection to the MediaDriver separately.
public class AeronClusteredCacheMessageCommunicationProvider {
    private static final System.Logger LOGGER =
            System.getLogger(AeronClusteredCacheMessageCommunicationProvider.class.getName());

    /* A configured node id identifies all cache providers in one JVM, while the
     * random incarnation in the wire identity changes after a process restart.
     * Deriving the identity directly avoids an unbounded process-wide map keyed by
     * configuration strings while preserving same-node self suppression. */
    private static final LazyConstant<UUID> PROCESS_INCARNATION = LazyConstant.of(UUID::randomUUID);

    private AeronClusteredCacheResources resources;
    private byte[] senderId;
    private AeronClusteredCacheSenderSequence.SequenceLease sequenceLease;
    private Object sequenceLock;
    private String configuredNodeId;
    private AeronClusteredCacheMessageSender sender;
    private long senderOfferTimeoutNanos;
    private int senderMaxPayloadBytes;
    private AeronClusteredCacheMessageReceiver receiver;
    private ClusteredCacheMessageAcceptor receiverAcceptor;
    private int receiverMaxPayloadBytes;
    private boolean senderSequenceReleased;
    private boolean receiverSequenceReleased;

        /// Creates a provider with no Aeron resources yet.
    public AeronClusteredCacheMessageCommunicationProvider() {
    }

    private static byte[] uuidBytes(final UUID value) {
        final byte[] bytes = new byte[Long.BYTES * 2];
        putLong(bytes, 0, value.getMostSignificantBits());
        putLong(bytes, Long.BYTES, value.getLeastSignificantBits());
        return bytes;
    }

    private static void putLong(final byte[] target, final int offset, final long value) {
        for (int index = 0; index < Long.BYTES; index++) {
            target[offset + index] = (byte) (value >>> (Long.BYTES - 1 - index) * Byte.SIZE);
        }
    }

        /// Validates the channel topology before any Aeron connection: UDP channels
    /// must use dynamic MDC so every node in the cluster sees every other node's
    /// frames, wildcard endpoints are rejected because the channel is expected to
    /// sit on an isolated, controlled network, and loopback UDP is rejected
    /// unless an embedded driver is used, because it would never leave the host.
    private static void validateChannel(final String channel, final boolean embeddedDriver) {
        final ChannelUri uri;
        try {
            uri = ChannelUri.parse(channel);
        } catch (final RuntimeException failure) {
            throw new IllegalArgumentException(
                    "Invalid channel: %s".formatted(channel), failure);
        }
        if (uri.isUdp()) {
            final String controlMode = uri.get(CommonContext.MDC_CONTROL_MODE_PARAM_NAME);
            if (!CommonContext.MDC_CONTROL_MODE_DYNAMIC.equalsIgnoreCase(controlMode)) {
                throw new IllegalArgumentException(
                        "Channel must use control-mode=dynamic for N-to-N invalidation: %s (or use aeron:ipc for single-host deployments)".formatted(channel));
            }
            final String endpoint = uri.get(CommonContext.ENDPOINT_PARAM_NAME);
            final String endpointHost = endpointHost(endpoint);
            final String controlHost = endpointHost(uri.get(CommonContext.MDC_CONTROL_PARAM_NAME));
            if (isWildcardHost(endpointHost) || isWildcardHost(controlHost)) {
                throw new IllegalArgumentException(
                        "Channel must not bind a wildcard endpoint: %s".formatted(channel));
            }
            if (!embeddedDriver && (isLoopbackHost(endpointHost) || isLoopbackHost(controlHost))) {
                throw new IllegalArgumentException(
                        "Channel must not use a loopback endpoint outside an embedded driver: %s".formatted(channel));
            }
        }
    }

    private static String endpointHost(final String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            return null;
        }
        final String value = endpoint.trim();
        if (value.charAt(0) == '[') {
            final int closing = value.indexOf(']');
            return closing > 0 ? value.substring(1, closing) : value;
        }
        final int colon = value.lastIndexOf(':');
        return colon > 0 ? value.substring(0, colon) : value;
    }

    private static boolean isWildcardHost(final String host) {
        if (host == null) {
            return false;
        }
        final String normalized = host.trim().toLowerCase(Locale.ROOT);
        return normalized.equals("*") || normalized.equals("0.0.0.0") || normalized.equals("::") ||
               normalized.equals("0:0:0:0:0:0:0:0");
    }

    private static boolean isLoopbackHost(final String host) {
        if (host == null) {
            return false;
        }
        final String normalized = host.trim().toLowerCase(Locale.ROOT);
        return normalized.equals("localhost") || normalized.equals("127.0.0.1") ||
               normalized.equals("::1") || normalized.equals("0:0:0:0:0:0:0:1");
    }

        /// Creates the sender for timestamp cache events.
    ///
    /// The sender is synchronous and fails the local cache operation when the
    /// invalidation cannot be published.
    ///
    /// One provider owns at most one sender. A repeat call with the same
    /// transport configuration and limits returns the existing sender; a
    /// different channel, stream, directory, node id, or limits is rejected,
    /// and a disposed sender is never resurrected — create a new provider
    /// instead. Every value is validated before any Aeron resource or sequence
    /// lease is acquired, so a rejected call cannot leak a live runtime, and a
    /// failed construction releases the lease and closes still-unbound
    /// resources again.
    ///
    /// @param configuration injected Aeron configuration
    /// @return sender for timestamp cache events
    public synchronized AeronClusteredCacheMessageSender provideUpdateTimestampsCacheMessageSender(
            final AeronClusteredCacheConfiguration configuration
    ) {
        Objects.requireNonNull(configuration, "configuration");
        this.ensureProviderOpen();
        final byte[] senderId = this.ensureSenderId(configuration);
        final long offerTimeoutNanos = configuration.offerTimeoutNanos();
        final int maxPayloadBytes = configuration.maxPayloadBytes();
        /* Validate the transport binding even on a repeat call: a second call
         * with a different channel or stream must be rejected, not silently
         * bound to the resources created by the first call. */
        final AeronClusteredCacheResources resources = this.ensureResources(configuration);
        if (this.sender != null) {
            if (this.sender.isDisposed()) {
                throw new IllegalStateException(
                        "The Aeron clustered-cache sender is single-use and has been disposed; create a new provider");
            }
            if (this.senderOfferTimeoutNanos != offerTimeoutNanos || this.senderMaxPayloadBytes != maxPayloadBytes) {
                throw new IllegalArgumentException(
                        "The Aeron clustered-cache provider already owns a sender with different limits");
            }
            return this.sender;
        }
        final AeronClusteredCacheSenderSequence.SequenceLease sequence;
        try {
            sequence = this.ensureSequence(configuration);
        } catch (final RuntimeException | Error failure) {
            this.releaseSequenceIfUnused();
            this.closeUnboundResources(failure);
            throw failure;
        }
        final AeronClusteredCacheMessageSender created;
        try {
            created = AeronClusteredCacheMessageSender.New(
                    resources,
                    senderId,
                    sequence,
                    this.sequenceLock,
                    this::senderClosed,
                    offerTimeoutNanos,
                    maxPayloadBytes);
        } catch (final RuntimeException | Error failure) {
            /* A provider owns no usable handle yet. Do not leave a sequence lease or
             * lazily-created resources stranded when a future sender constructor gains
             * validation or allocation that can fail. */
            this.releaseSequenceIfUnused();
            this.closeUnboundResources(failure);
            throw failure;
        }
        this.senderOfferTimeoutNanos = offerTimeoutNanos;
        this.senderMaxPayloadBytes = maxPayloadBytes;
        this.sender = created;
        return this.sender;
    }

        /// Creates the receiver that passes remote messages to the acceptor.
    ///
    /// The receiver fails closed on malformed or undeserializable frames, on a
    /// valid message that cannot be applied, and on a sender sequence gap. A
    /// volatile invalidation stream cannot prove that a bad frame was harmless or
    /// repair a missed message, so continuing would expose stale cache entries.
    ///
    /// One provider owns at most one receiver, mirroring the sender rules:
    /// a repeat call with the same transport configuration, acceptor, and
    /// limits returns the existing receiver, anything different is rejected,
    /// and a disposed receiver requires a new provider. The node identity and
    /// transport binding are validated before any resource is created so a
    /// conflicting call cannot strand a live runtime behind it.
    ///
    /// @param configuration   injected Aeron configuration
    /// @param messageAcceptor target for received messages
    /// @return receiver for remote cache messages
    public synchronized AeronClusteredCacheMessageReceiver provideMessageReceiver(
            final AeronClusteredCacheConfiguration configuration,
            final ClusteredCacheMessageAcceptor messageAcceptor
    ) {
        Objects.requireNonNull(configuration, "configuration");
        Objects.requireNonNull(messageAcceptor, "messageAcceptor");
        this.ensureProviderOpen();
        /* Validate the node id and transport binding before creating resources
         * so a conflicting second call fails with IllegalArgumentException and
         * cannot leave closed resources behind for a retry. */
        final byte[] senderId = this.ensureSenderId(configuration);
        final int maxPayloadBytes = configuration.maxPayloadBytes();
        final AeronClusteredCacheResources resources = this.ensureResources(configuration);
        if (this.receiver != null) {
            if (this.receiver.isDisposed()) {
                throw new IllegalStateException(
                        "The Aeron clustered-cache receiver is single-use and has been disposed; create a new provider");
            }
            if (this.receiverAcceptor != messageAcceptor || this.receiverMaxPayloadBytes != maxPayloadBytes) {
                throw new IllegalArgumentException(
                        "The Aeron clustered-cache provider already owns a receiver with different configuration");
            }
            return this.receiver;
        }
        final AeronClusteredCacheMessageReceiver created;
        try {
            created = new AeronClusteredCacheMessageReceiver(
                    resources,
                    senderId,
                    this::receiverClosed,
                    messageAcceptor,
                    maxPayloadBytes);
        } catch (final RuntimeException | Error failure) {
            this.closeUnboundResources(failure);
            throw failure;
        }
        this.receiverAcceptor = messageAcceptor;
        this.receiverMaxPayloadBytes = maxPayloadBytes;
        this.receiver = created;
        return this.receiver;
    }

    private void ensureProviderOpen() {
        if (this.resources != null && this.resources.isClosed()) {
            throw new IllegalStateException(
                    "Aeron clustered-cache provider resources are closed; create a new provider for a new lifecycle");
        }
    }

        /// Returns the 16-byte sender identity shared by this provider's sender and
    /// receiver. It is derived from the configured `node-id` UUID, or
    /// generated once per provider instance when no node id is configured. A
    /// configured node id maps to one random process incarnation shared by all
    /// providers in this JVM; a new JVM gets a new incarnation so its sequence
    /// restart cannot be mistaken for a lost frame. The identity is fixed on
    /// first use; a later call with a different node id is rejected.
    private byte[] ensureSenderId(final AeronClusteredCacheConfiguration configuration) {
        final UUID configuredUuid = configuration.nodeId();
        final String normalized = configuredUuid == null ? null : configuredUuid.toString();
        if (this.senderId == null) {
            this.configuredNodeId = normalized;
            final UUID processIncarnation = PROCESS_INCARNATION.get();
            this.senderId = configuredUuid == null
                    ? uuidBytes(UUID.randomUUID())
                    : uuidBytes(new UUID(
                    processIncarnation.getMostSignificantBits() ^ configuredUuid.getMostSignificantBits(),
                    processIncarnation.getLeastSignificantBits() ^ configuredUuid.getLeastSignificantBits()));
        } else if (!Objects.equals(this.configuredNodeId, normalized)) {
            throw new IllegalArgumentException(
                    "Conflicting node-id: the provider is already bound to node id %s, requested %s".formatted(this.configuredNodeId, normalized));
        }
        return this.senderId;
    }

        /// Returns the sequence source for this provider's identity. Configured node
    /// identities use a process-wide sequence shared by every provider on the
    /// same channel. The entry survives provider disposal so sequence continuity
    /// is preserved while remote receivers remain attached.
    private AeronClusteredCacheSenderSequence.SequenceLease ensureSequence(
            final AeronClusteredCacheConfiguration configuration
    ) {
        if (this.sequenceLease == null) {
            final String channel = configuration.channel();
            final int streamId = configuration.streamId();
            if (this.configuredNodeId == null) {
                this.sequenceLease = AeronClusteredCacheSenderSequence.SequenceLease.local();
                this.sequenceLock = new Object();
            } else {
                this.sequenceLease = AeronClusteredCacheSenderSequence.acquire(this.senderId, channel, streamId);
                this.sequenceLock = this.sequenceLease.lock();
            }
        }
        return this.sequenceLease;
    }

    private synchronized void senderClosed() {
        this.senderSequenceReleased = true;
        this.releaseSequenceIfUnused();
    }

    private synchronized void receiverClosed() {
        this.receiverSequenceReleased = true;
        this.releaseSequenceIfUnused();
    }

    private void releaseSequenceIfUnused() {
        if (this.sequenceLease != null &&
            (this.sender == null || this.senderSequenceReleased) &&
            (this.receiver == null || this.receiverSequenceReleased)) {
            this.sequenceLease.close();
            this.sequenceLease = null;
        }
    }

    private void closeUnboundResources(final Throwable primary) {
        if (this.sender != null || this.receiver != null || this.resources == null) return;
        try {
            this.resources.close();
            this.resources = null;
        } catch (final Throwable cleanupFailure) {
            if (primary != cleanupFailure) primary.addSuppressed(cleanupFailure);
        }
    }

    private AeronClusteredCacheResources ensureResources(final AeronClusteredCacheConfiguration configuration) {
        final String channel = configuration.channel();
        final int streamId = configuration.streamId();
        final String directory = configuration.directory();
        final long driverTimeoutMillis = configuration.driverTimeoutMillis();
        final boolean embeddedDriver = configuration.embeddedDriver();

        if (this.resources == null) {
            if (AeronClusteredCacheConfiguration.DEFAULT_CHANNEL.equals(channel)) {
                LOGGER.log(System.Logger.Level.WARNING, "No channel configured; defaulting to aeron:ipc, which is single-host. Multi-host deployments must configure a UDP channel with control-mode=dynamic.");
            }
            if (embeddedDriver && directory == null) {
                LOGGER.log(System.Logger.Level.WARNING, "No directory configured with embedded driver enabled; the embedded MediaDriver will use a generated private directory.");
            }
            validateChannel(channel, embeddedDriver);
            this.resources = new AeronClusteredCacheResources(
                    directory, channel, streamId, driverTimeoutMillis, embeddedDriver);
            LOGGER.log(System.Logger.Level.DEBUG, "Bound Aeron clustered-cache resources to channel=%s, streamId=%s".formatted(channel, streamId));
        } else if (this.resources.isClosed()) {
            throw new IllegalStateException(
                    "Aeron clustered-cache provider resources are closed; create a new provider for a new lifecycle");
        } else if (!this.resources.matches(channel, streamId, directory, driverTimeoutMillis, embeddedDriver)) {
            throw new IllegalArgumentException(
                    "Conflicting Aeron clustered-cache configuration: the provider is already bound to %s; requested channel=%s, streamId=%s, directory=%s, driverTimeoutMillis=%s, embeddedDriver=%s".formatted(this.resources.describe(), channel, streamId, directory, driverTimeoutMillis, embeddedDriver));
        }
        return this.resources;
    }
}
