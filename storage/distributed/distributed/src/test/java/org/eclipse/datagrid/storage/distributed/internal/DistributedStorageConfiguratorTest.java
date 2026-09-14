package org.eclipse.datagrid.storage.distributed.internal;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
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

import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionary;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryExporter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** Verifies that one Store object implementing both extension SPIs keeps both contracts. */
class DistributedStorageConfiguratorTest
{
	@Test
	void preservesTargetAndDictionaryExporterContracts()
	{
		final DistributedStorageConfigurator configurator = new DistributedStorageConfigurator(new NoOpDistributor());
		final Object decorated = configurator.apply(new BothContracts());

		assertInstanceOf(PersistenceTarget.class, decorated);
		assertInstanceOf(PersistenceTypeDictionaryExporter.class, decorated);
	}

	private static final class BothContracts
		implements PersistenceTarget<Binary>, PersistenceTypeDictionaryExporter
	{
		@Override public void write(final Binary data) { }
		@Override public boolean isWritable() { return true; }
		@Override public void exportTypeDictionary(final PersistenceTypeDictionary typeDictionary) { }
	}

	private static final class NoOpDistributor implements StorageBinaryDataDistributor
	{
		@Override public void distributeData(final Binary data) { }
		@Override public void distributeTypeDictionary(final String typeDictionaryData) { }
		@Override public void dispose() { }
	}
}
