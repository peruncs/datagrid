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
