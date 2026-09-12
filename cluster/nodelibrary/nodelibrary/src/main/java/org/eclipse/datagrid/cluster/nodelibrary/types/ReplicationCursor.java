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

import java.util.Arrays;
import java.util.UUID;

/**
 * Transport-neutral durable replication position.
 *
 * <p>{@code logicalSequence} is the Data Grid ordering value. The opaque
 * {@code providerPosition} is interpreted only by the selected transport (for
 * example Kafka partition offsets or an Aeron recording id/position pair).</p>
 *
 * @param transport selected provider id
 * @param storeGeneration immutable Store image identity, or {@code null} for legacy cursors
 * @param logicalSequence last fully resolved transaction, or {@code -1} before the first one
 * @param providerPosition provider-specific position bytes
 */
public record ReplicationCursor(
	String transport,
	UUID storeGeneration,
	long logicalSequence,
	byte[] providerPosition
)
{
	/** Validates and copies the provider position.
	 *
	 * @param transport selected provider id
	 * @param storeGeneration Store generation
	 * @param logicalSequence last resolved transaction
	 * @param providerPosition provider position bytes
	 */
	public ReplicationCursor
	{
		if (transport == null || transport.isBlank())
		{
			throw new IllegalArgumentException("transport must not be blank");
		}
		if (logicalSequence < -1)
		{
			throw new IllegalArgumentException("logicalSequence must be >= -1");
		}
		providerPosition = providerPosition == null ? new byte[0] : providerPosition.clone();
	}

	/** Returns a copy of the provider position.
	 * @return provider position copy
	 */
	public byte[] providerPosition()
	{
		return this.providerPosition.clone();
	}

	/** Compatibility name used by the original Kafka lifecycle.
	 * @return logical sequence
	 */
	public long messageIndex()
	{
		return this.logicalSequence;
	}

	@Override
	public boolean equals(final Object other)
	{
		if (!(other instanceof ReplicationCursor cursor))
		{
			return false;
		}
		return this.logicalSequence == cursor.logicalSequence
			&& java.util.Objects.equals(this.transport, cursor.transport)
			&& java.util.Objects.equals(this.storeGeneration, cursor.storeGeneration)
			&& Arrays.equals(this.providerPosition, cursor.providerPosition);
	}

	@Override
	public int hashCode()
	{
		return java.util.Objects.hash(this.transport, this.storeGeneration, this.logicalSequence,
			Arrays.hashCode(this.providerPosition));
	}
}
