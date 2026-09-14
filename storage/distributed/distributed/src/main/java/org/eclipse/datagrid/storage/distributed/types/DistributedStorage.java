package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
 * %%
 * Copyright (C) 2025 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 * 
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */


import org.eclipse.datagrid.storage.distributed.internal.DistributedStorageConfigurator;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageConnectionFoundation;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageFoundation;

import java.util.function.UnaryOperator;

/**
 * This utility installs distributed writing into an embedded Store foundation.
 *
 * <p>The configured foundation keeps its normal local target and type
 * dictionary, then wraps both so committed binary data and type definitions
 * reach the supplied distributor. The utility changes the foundation in place
 * and returns it for fluent setup.</p>
 */
	public final class DistributedStorage
{
		/** Adds distributed writing to an embedded storage foundation.
		 *
		 * @param foundation foundation to configure
		 * @param distributor destination for committed data
		 * @return the configured foundation
		 */
		public static EmbeddedStorageFoundation<?> configureWriting(
		final EmbeddedStorageFoundation<?> foundation,
		final StorageBinaryDataDistributor distributor
	)
	{
		final EmbeddedStorageConnectionFoundation<?> connectionFoundation = foundation.getConnectionFoundation();
		connectionFoundation.setInstanceDispatcher(new DistributedStorageConfigurator(
			distributor,
			delegate -> StorageBinaryTargetDistributing.New(delegate, distributor)
		));
		return foundation;
	}

		/** Adds distributed writing with a custom local target wrapper.
		 *
		 * @param foundation foundation to configure
		 * @param distributor destination for committed data
		 * @param targetFactory wrapper for the local persistence target
		 * @return the configured foundation
		 */
		public static EmbeddedStorageFoundation<?> configureWriting(
		final EmbeddedStorageFoundation<?> foundation,
		final StorageBinaryDataDistributor distributor,
		final UnaryOperator<PersistenceTarget<Binary>> targetFactory
	)
	{
		final EmbeddedStorageConnectionFoundation<?> connectionFoundation = foundation.getConnectionFoundation();
		connectionFoundation.setInstanceDispatcher(new DistributedStorageConfigurator(
			distributor, targetFactory));
		return foundation;
	}

	private DistributedStorage()
	{
		throw new UnsupportedOperationException();
	}
}
