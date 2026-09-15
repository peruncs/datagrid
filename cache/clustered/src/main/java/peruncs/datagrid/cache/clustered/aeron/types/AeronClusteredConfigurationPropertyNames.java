package peruncs.datagrid.cache.clustered.aeron.types;

import peruncs.datagrid.cache.clustered.types.ClusteredConfigurationPropertyNames;

/**
 * Names of the Aeron settings used by clustered-cache messages.
 *
 * <p>These settings belong to the cache invalidation adapter and are
 * deliberately disjoint from the Store replication Aeron settings
 * ({@code eclipsestore.distribution.aeron.*}): the cache broadcast stream is
 * independent of the Store replication stream and may use different framing,
 * channels, and drivers.</p>
 */
public interface AeronClusteredConfigurationPropertyNames
{
	/** Prefix shared by Aeron clustered-cache properties. */
	String PREFIX = ClusteredConfigurationPropertyNames.PREFIX + "aeron.";

	/**
	 * Property containing the Aeron channel shared by all participants.
	 * Defaults to {@code aeron:ipc}, which is single-host; multi-host
	 * deployments must configure a UDP channel with {@code control-mode=dynamic}.
	 */
	String CHANNEL = PREFIX + "channel";
	/** Property containing the Aeron stream id shared by all participants. */
	String STREAM_ID = PREFIX + "stream-id";
	/** Optional property fixing the node identity shared by every provider of one node; defaults to a random UUID per provider. */
	String NODE_ID = PREFIX + "node-id";
	/** Optional property containing the Aeron driver directory; defaults to Aeron's configured directory. */
	String DIRECTORY = PREFIX + "directory";
	/** Property that launches a private embedded MediaDriver; the configured directory must be exclusive to this provider. */
	String EMBEDDED_DRIVER = PREFIX + "embedded-driver";
	/** Property bounding how long a sender waits for the publication to accept a frame before failing the cache write. */
	String OFFER_TIMEOUT_MILLIS = PREFIX + "offer-timeout-millis";
	/** Property bounding how long the Aeron client waits for a driver connection. */
	String DRIVER_TIMEOUT_MILLIS = PREFIX + "driver-timeout-millis";
	/** Property bounding the accepted serialized payload size. */
	String MAX_PAYLOAD_BYTES = PREFIX + "max-payload-bytes";
}
