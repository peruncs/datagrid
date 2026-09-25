package org.eclipse.datagrid.storage.distributed.internal;

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


import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryTargetDistributing;
import org.eclipse.datagrid.storage.distributed.types.StorageTypeDictionaryExporterDistributing;
import org.eclipse.serializer.functional.InstanceDispatcherLogic;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryExporter;

import java.util.function.UnaryOperator;

import static org.eclipse.serializer.util.X.notNull;

/**
 * This dispatcher wraps Store components with distributed-writing behavior.
 *
 * <p>Persistence targets distribute committed binary data, and type dictionary
 * exporters distribute type definitions. Other objects pass through unchanged
 * so the normal Store foundation keeps its existing behavior.</p>
 */
public class DistributedStorageConfigurator implements InstanceDispatcherLogic
{
	private final StorageBinaryDataDistributor distributor;
	private final UnaryOperator<PersistenceTarget<Binary>> targetFactory;

	public DistributedStorageConfigurator(final StorageBinaryDataDistributor distributor)
	{
		this(distributor, delegate -> StorageBinaryTargetDistributing.New(delegate, distributor));
	}

	public DistributedStorageConfigurator(
		final StorageBinaryDataDistributor distributor,
		final UnaryOperator<PersistenceTarget<Binary>> targetFactory
	)
	{
		super();
		this.distributor = notNull(distributor);
		this.targetFactory = notNull(targetFactory);
	}

	@SuppressWarnings("unchecked") // Store supplies the Binary persistence target to this typed factory
	@Override
	public <T> T apply(final T subject)
	{
		if (subject == null) return null;
		if (subject instanceof PersistenceTarget<?> target &&
			subject instanceof PersistenceTypeDictionaryExporter dictionaryExporter)
		{
			/* Store foundations may expose one object through both SPIs. Returning
			 * only the target decorator silently drops dictionary publication, so
			 * preserve both contracts in one adapter. */
			return (T)new TargetAndDictionaryExporter(
				this.targetFactory.apply((PersistenceTarget<Binary>)target),
				StorageTypeDictionaryExporterDistributing.New(dictionaryExporter, this.distributor)
			);
		}
		if (subject instanceof PersistenceTarget<?> target)
		{
			return (T)this.targetFactory.apply((PersistenceTarget<Binary>)target);
		}
		if (subject instanceof PersistenceTypeDictionaryExporter dictionaryExporter)
		{
			return (T)StorageTypeDictionaryExporterDistributing.New(
				dictionaryExporter,
				this.distributor
			);
		}

		return subject;
	}

	/** Combines the two Store extension contracts when one subject implements both. */
	private static final class TargetAndDictionaryExporter
		implements PersistenceTarget<Binary>, PersistenceTypeDictionaryExporter
	{
		private final PersistenceTarget<Binary> target;
		private final PersistenceTypeDictionaryExporter dictionaryExporter;

		private TargetAndDictionaryExporter(
			final PersistenceTarget<Binary> target,
			final PersistenceTypeDictionaryExporter dictionaryExporter
		)
		{
			this.target = notNull(target);
			this.dictionaryExporter = notNull(dictionaryExporter);
		}

		@Override
		public void write(final Binary data)
		{
			this.target.write(data);
		}

		@Override
		public boolean isWritable()
		{
			return this.target.isWritable();
		}

		@Override
		public boolean isStoringEnabled()
		{
			return this.target.isStoringEnabled();
		}

		@Override
		public void validateIsWritable()
		{
			this.target.validateIsWritable();
		}

		@Override
		public void validateIsStoringEnabled()
		{
			this.target.validateIsStoringEnabled();
		}

		@Override
		public void prepareTarget()
		{
			this.target.prepareTarget();
		}

		@Override
		public void closeTarget()
		{
			this.target.closeTarget();
		}

		@Override
		public void exportTypeDictionary(final org.eclipse.serializer.persistence.types.PersistenceTypeDictionary typeDictionary)
		{
			this.dictionaryExporter.exportTypeDictionary(typeDictionary);
		}
	}

}
