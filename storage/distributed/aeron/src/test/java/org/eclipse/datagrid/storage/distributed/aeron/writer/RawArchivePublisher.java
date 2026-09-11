package org.eclipse.datagrid.storage.distributed.aeron.writer;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
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

import java.nio.ByteBuffer;

/** Test-only bridge for crash fixtures that need raw Archive frames. */
public final class RawArchivePublisher
{
	private RawArchivePublisher()
	{
	}

	/** Publishes a fixture transaction without exposing the raw path in production APIs. */
	public static void publish(
		final AeronArchiveReplicationPublisher publisher,
		final byte[] dictionary,
		final ByteBuffer[] data
	)
	{
		publisher.publishTransaction(dictionary, data);
	}
}
