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

import org.eclipse.store.cache.hibernate.types.ConfigurationPropertyNames;

/** Names of the neutral clustered-cache configuration properties. */
public interface ClusteredConfigurationPropertyNames
{
    /** Prefix shared by clustered-cache properties. */
    String PREFIX = ConfigurationPropertyNames.PREFIX + "clustered.";
    /** Property that selects the serializer type provider. */
    String SERIALIZATION_TYPES_PROVIDER = PREFIX + "serialization-types-provider";
    /** Property that selects the message communication provider. */
    String COM_PROVIDER = PREFIX + "com-provider";
}
