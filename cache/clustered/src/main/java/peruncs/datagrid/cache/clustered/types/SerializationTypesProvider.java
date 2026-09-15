package peruncs.datagrid.cache.clustered.types;

import java.util.Collection;
import java.util.List;

/**
 * This provider lists types that the clustered-cache serializer must know.
 *
 * <p>The clustered-cache serializer only carries timestamp invalidation
 * messages and the sender identity string, so the default provider registers
 * only the message type. Applications that extend the message set can supply
 * their own provider.</p>
 */
public interface SerializationTypesProvider
{
	/** Returns all types that must be registered before messages are serialized.
	 *
	 * @return types required by the serializer
	 */
	Collection<Class<?>> provideTypes();

	/** The built-in set of types used by the cache integration. */
	class Default implements SerializationTypesProvider
	{
		private static final Collection<Class<?>> TYPES = List.of(TimestampsRegionUpdateMessage.class);

		/** Creates the default provider. */
		public Default()
		{
		}

		@Override
        public Collection<Class<?>> provideTypes()
        {
            return TYPES;
        }
    }
}
