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

/** Receives complete Store binaries and type dictionaries from a provider. */
public interface StorageBinaryDataReceiver
{
	/**
	 * Receives a complete binary. The callback must consume the supplied binary
	 * synchronously; transports may release its native buffers immediately after
	 * this method returns to avoid retaining off-heap memory.
	 */
    void receiveData(Binary data);

	void receiveTypeDictionary(String typeDictionaryData);
}
