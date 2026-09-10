package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
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

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataClient;

/**
 * Transport-neutral reader lifecycle. Provider modules supply the
 * implementation and map their cursor/offset model to {@link MessageInfo}.
 * A client must not report a message as consumed until the Store merger has
 * accepted the complete committed binary.
 */
public interface ClusterStorageBinaryDataClient extends StorageBinaryDataClient
{
	void stopAtLatestMessage();

	MessageInfo messageInfo();

	boolean isRunning();

	/** Returns a terminal reader failure, or {@code null} while the client is healthy. */
	default RuntimeException failure()
	{
		return null;
	}

	default boolean isLive()
	{
		return isRunning();
	}

	void resume() throws NodelibraryException;

	static ClusterStorageBinaryDataClient NoOp(
		final ReplicationCursor startingCursor,
		final AfterDataMessageConsumedListener listener
	)
	{
		final ReplicationCursor cursor = startingCursor == null
			? new ReplicationCursor("none", null, -1, new byte[0])
			: startingCursor;
		return new ClusterStorageBinaryDataClient()
		{
			private final MessageInfo info = MessageInfo.New(
				cursor.logicalSequence(), cursor.transport(), cursor.storeGeneration(), cursor.providerPosition()
			);

			@Override public void start() { }
			@Override public void stopAtLatestMessage() { }
			@Override public MessageInfo messageInfo() { return this.info; }
			@Override public boolean isRunning() { return false; }
			@Override public void resume() { }
			@Override public void dispose() { }
		};
	}
}
