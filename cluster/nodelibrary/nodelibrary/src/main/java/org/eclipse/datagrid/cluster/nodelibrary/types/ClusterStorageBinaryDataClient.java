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

	/**
	 * Returns the latest lifecycle result. Implementations that can distinguish a
	 * resolved transaction boundary should override this method; the fallback
	 * preserves the historic "stopped means complete" contract of simple clients.
	 */
	default StopOutcome stopOutcome()
	{
		if (this.failure() != null) return StopOutcome.FAILED;
		return this.isRunning() ? StopOutcome.RUNNING : StopOutcome.RESOLVED_BOUNDARY;
	}

	/** Returns the stop outcome together with the last resolved cursor. */
	default StopResult stopResult()
	{
		final MessageInfo info = this.messageInfo();
		return new StopResult(this.stopOutcome(), info.messageIndex(), -1L);
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
