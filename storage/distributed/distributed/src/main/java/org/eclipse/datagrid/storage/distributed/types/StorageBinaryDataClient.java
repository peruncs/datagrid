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


import org.eclipse.serializer.typing.Disposable;

/** Minimal lifecycle contract for a reader-side binary replication client. */
public interface StorageBinaryDataClient extends Disposable
{
	/** Result of a requested stop-at-latest operation. */
	enum StopOutcome
	{
		NOT_STARTED,
		RUNNING,
		STOPPING,
		RESOLVED_BOUNDARY,
		TIMED_OUT,
		FAILED,
		STOPPED,
		CLOSED
	}

	/** Immutable result of a stop-at-latest request. */
	record StopResult(StopOutcome outcome, long sequence, long position)
	{
		public StopResult
		{
			if (outcome == null) throw new NullPointerException("outcome");
		}
	}

	/** Returns the most recent stop outcome, or {@link StopOutcome#NOT_STARTED}. */
	default StopOutcome stopOutcome()
	{
		return StopOutcome.NOT_STARTED;
	}

	/** Returns the stop outcome together with the last resolved cursor. */
	default StopResult stopResult()
	{
		return new StopResult(this.stopOutcome(), -1L, -1L);
	}

	void start();
}
