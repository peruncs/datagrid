package peruncs.datagrid.cache.aeron;

import peruncs.datagrid.cache.types.ClusteredConfigurationPropertyNames;

/// Names of the Aeron settings used by clustered-cache messages.
///
/// These settings belong to the cache invalidation adapter and are
/// deliberately disjoint from the Store replication Aeron settings
/// (`eclipsestore.distribution.aeron.*`): the cache broadcast stream is
/// independent of the Store replication stream and may use different framing,
/// channels, and drivers.
public interface AeronClusteredConfigurationPropertyNames {
        /// Prefix shared by Aeron clustered-cache properties.
    String PREFIX = "%saeron.".formatted(ClusteredConfigurationPropertyNames.PREFIX);

        /// Property containing the Aeron channel shared by all participants.
    /// Defaults to `aeron:ipc`, which is single-host; multi-host
    /// deployments must configure a UDP channel with `control-mode=dynamic`.
    String CHANNEL = "%schannel".formatted(PREFIX);
        /// Property containing the Aeron stream id shared by all participants.
    String STREAM_ID = "%sstream-id".formatted(PREFIX);
        /// Optional property fixing the node identity shared by every provider of one node; defaults to a random UUID per provider.
    String NODE_ID = "%snode-id".formatted(PREFIX);
        /// Optional property containing the Aeron driver directory; defaults to Aeron's configured directory.
    String DIRECTORY = "%sdirectory".formatted(PREFIX);
        /// Property that launches a private embedded MediaDriver; the configured directory must be exclusive to this provider.
    String EMBEDDED_DRIVER = "%sembedded-driver".formatted(PREFIX);
        /// Property bounding how long a sender waits for the publication to accept a frame before failing the cache write.
    String OFFER_TIMEOUT_MILLIS = "%soffer-timeout-millis".formatted(PREFIX);
        /// Property bounding how long the Aeron client waits for a driver connection.
    String DRIVER_TIMEOUT_MILLIS = "%sdriver-timeout-millis".formatted(PREFIX);
        /// Property bounding the accepted serialized payload size.
    String MAX_PAYLOAD_BYTES = "%smax-payload-bytes".formatted(PREFIX);
}
