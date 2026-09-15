package peruncs.datagrid.cache.clustered.types;

import peruncs.datagrid.cache.clustered.aeron.types.AeronClusteredCacheMessageComProvider;
import peruncs.datagrid.cache.clustered.aeron.types.AeronClusteredCacheMessageSender;
import peruncs.datagrid.cache.clustered.aeron.types.AeronClusteredConfigurationPropertyNames;
import org.junit.jupiter.api.Test;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.EventType;
import java.util.HashMap;
import java.util.Map;

import static peruncs.datagrid.cache.clustered.test.ClusteredCacheTestSupport.publish;
import static peruncs.datagrid.cache.clustered.test.ClusteredCacheTestSupport.serializer;
import static org.junit.jupiter.api.Assertions.*;

/** Verifies the listener configuration owns its sender and disposes it once. */
class ClusteredCacheEntryListenerConfigurationTest
{
	@Test
	void disposeIsIdempotentAndFailsClosed()
	{
		final AeronClusteredCacheMessageComProvider provider = new AeronClusteredCacheMessageComProvider();
		final AeronClusteredCacheMessageSender sender =
			provider.provideUpdateTimestampsCacheMessageSender(properties(), serializer());
		final ClusteredCacheEntryListenerConfiguration configuration =
			new ClusteredCacheEntryListenerConfiguration(sender);

		configuration.dispose();
		configuration.dispose();

		assertThrows(CacheEntryListenerException.class,
			() -> publish(sender, EventType.CREATED, "cache", "table", 1L),
			"a disposed sender must reject further publishes");
	}

	@Test
	void listenerFactoryReturnsTheSender()
	{
		final AeronClusteredCacheMessageComProvider provider = new AeronClusteredCacheMessageComProvider();
		final AeronClusteredCacheMessageSender sender =
			provider.provideUpdateTimestampsCacheMessageSender(properties(), serializer());
		final ClusteredCacheEntryListenerConfiguration configuration =
			new ClusteredCacheEntryListenerConfiguration(sender);
		try
		{
			final CacheEntryListenerConfiguration<Object, Object> listener =
				configuration.getUpdateTimestampsCacheEntryListenerConfiguration();
			assertSame(sender, listener.getCacheEntryListenerFactory().create());
			assertTrue(listener.isSynchronous(), "clustered invalidations must be synchronous");
			assertFalse(listener.isOldValueRequired());
			assertNull(listener.getCacheEntryEventFilterFactory());
		}
		finally
		{
			configuration.dispose();
		}
	}

	@Test
	void nullSenderIsRejected()
	{
		assertThrows(NullPointerException.class,
			() -> new ClusteredCacheEntryListenerConfiguration(null));
	}

	private static Map<String, Object> properties()
	{
		final Map<String, Object> properties = new HashMap<>();
		properties.put(AeronClusteredConfigurationPropertyNames.CHANNEL, "aeron:ipc");
		properties.put(AeronClusteredConfigurationPropertyNames.STREAM_ID, "2001");
		properties.put(AeronClusteredConfigurationPropertyNames.EMBEDDED_DRIVER, "true");
		properties.put(AeronClusteredConfigurationPropertyNames.OFFER_TIMEOUT_MILLIS, "10000");
		properties.put(AeronClusteredConfigurationPropertyNames.DRIVER_TIMEOUT_MILLIS, "10000");
		return properties;
	}
}
