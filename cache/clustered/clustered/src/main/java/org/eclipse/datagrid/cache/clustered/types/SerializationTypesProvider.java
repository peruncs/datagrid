package org.eclipse.datagrid.cache.clustered.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered
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

import org.hibernate.cache.internal.BasicCacheKeyImplementation;
import org.hibernate.cache.internal.CacheKeyImplementation;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * This provider lists types that the clustered-cache serializer must know.
 *
 * <p>Applications can add their own cache key or message types. The default
 * provider contains the Hibernate key types, the timestamp message, and the
 * common identifier type used by the built-in cache integration.</p>
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
		/** Creates the default provider. */
		public Default()
		{
		}

		@Override
        public Collection<Class<?>> provideTypes()
        {
            return List.of(
                // Hibernate cache key types
                CacheKeyImplementation.class,
                BasicCacheKeyImplementation.class,
                // message types
                TimestampsRegionUpdateMessage.class,
                // common cache key id types
                UUID.class
            );
        }
    }
}
