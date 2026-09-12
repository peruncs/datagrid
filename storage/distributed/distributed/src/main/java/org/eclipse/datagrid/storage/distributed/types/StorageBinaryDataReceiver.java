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

/**
 * Receives complete Store binaries and type dictionaries from a provider.
 *
 * <p>The ordinary callback is borrowed and synchronous.  A transport that has
 * already assembled a native binary may override {@link #receiveDataOwned(Binary)}
 * to transfer buffer ownership, followed by {@link #awaitApplied()} when the
 * receiver performs deferred materialisation.  The owned callback must return
 * {@code true} only after the receiver has taken responsibility for releasing
 * every direct buffer in the supplied binary.</p>
 */
public interface StorageBinaryDataReceiver
{
	/**
	 * Receives a complete binary. The callback must consume the supplied binary
	 * synchronously; transports may release its native buffers immediately after
	 * this method returns to avoid retaining off-heap memory.
	 */
    void receiveData(Binary data);

	/**
	 * Delivers a complete binary while allowing an implementation to take
	 * ownership of its direct buffers. The default keeps the historical borrowed
	 * callback contract and therefore returns {@code false}; transports may then
	 * release their buffers immediately after this method returns.
	 *
	 * @param data complete binary whose direct buffers may be transferred
	 * @return {@code true} when the receiver owns the buffers after return
	 */
	default boolean receiveDataOwned(final Binary data)
	{
		this.receiveData(data);
		return false;
	}

	/**
	 * Completes the durable application of the most recently accepted binary.
	 * Implementations that only consume the binary synchronously may leave this
	 * method as a no-op.  A transport adapter uses it to wait for deferred Store
	 * materialisation after ownership has already transferred, which prevents a
	 * failure during the wait from causing the caller to free the same buffers a
	 * second time.
	 */
	default void awaitApplied()
	{
	}

	void receiveTypeDictionary(String typeDictionaryData);
}
