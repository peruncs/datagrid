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


import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.typing.Disposable;

import static org.eclipse.serializer.util.X.notNull;

/**
 * Provider-neutral sink for Store binary data and optional type dictionaries.
 * Implementations decide how bytes are transported; callers only require that
 * dictionary data precede the matching binary transaction.
 */
public interface StorageBinaryDataDistributor extends Disposable
{
	public void distributeData(Binary data);

	public void distributeTypeDictionary(String typeDictionaryData);

	/** Returns and clears a dictionary staged for the next binary transaction. */
	default String consumeTypeDictionary()
	{
		return null;
	}

	public static StorageBinaryDataDistributor Caching(final StorageBinaryDataDistributor delegate)
	{
		return new StorageBinaryDataDistributor.Caching(
			notNull(delegate)
		);
	}

	/*
	 * Only distribute optional new type dictionary before actual data to minimize
	 * traffic.
	 */
	public static class Caching implements StorageBinaryDataDistributor
	{
		private final StorageBinaryDataDistributor delegate;
		/*
		 * BinaryStorer exports its dictionary and writes the matching Binary on the
		 * committing thread. Keeping the snapshot per thread prevents concurrent
		 * commits from attaching one transaction's dictionary to another.
		 */
		private final ThreadLocal<String> typeDictionaryData = new ThreadLocal<>();

		Caching(final StorageBinaryDataDistributor delegate)
		{
			super();
			this.delegate = delegate;
		}

		@Override
		public void distributeData(final Binary data)
		{
			final String dictionary = this.typeDictionaryData.get();
			if (dictionary != null)
			{
				try
				{
					this.delegate.distributeTypeDictionary(dictionary);
				}
				finally
				{
					this.typeDictionaryData.remove();
				}
			}
			this.delegate.distributeData(data);
		}

		@Override
		public String consumeTypeDictionary()
		{
			final String value = this.typeDictionaryData.get();
			this.typeDictionaryData.remove();
			return value;
		}

		@Override
		public void distributeTypeDictionary(final String typeDictionaryData)
		{
			if (typeDictionaryData == null)
			{
				this.typeDictionaryData.remove();
			}
			else
			{
				this.typeDictionaryData.set(typeDictionaryData);
			}
		}

		@Override
		public void dispose()
		{
			this.typeDictionaryData.remove();
			this.delegate.dispose();
		}

	}

}
