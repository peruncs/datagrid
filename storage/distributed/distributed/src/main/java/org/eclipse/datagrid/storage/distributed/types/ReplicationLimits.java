package org.eclipse.datagrid.storage.distributed.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
 * %%
 * Copyright (C) 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

/** Shared hard limits for every transport-neutral replication frame. */
public final class ReplicationLimits
{
	/** Maximum bytes in one replicated message. */
	public static final int MAX_MESSAGE_BYTES = 256 * 1024 * 1024;
	/** Maximum packets in one replicated message. */
	public static final int MAX_PACKET_COUNT = 1_000_000;

	private ReplicationLimits()
	{
	}
}
