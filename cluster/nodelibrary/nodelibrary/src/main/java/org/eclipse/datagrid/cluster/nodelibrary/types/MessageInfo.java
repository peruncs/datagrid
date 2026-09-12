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

import static org.eclipse.serializer.util.X.notNull;

/**
 * Backward-compatible replication position view used by the nodelibrary.
 * Provider-specific offsets remain opaque bytes so adding Aeron does not
 * change the existing Kafka-facing lifecycle API.
 */
public interface MessageInfo
{
	/** Returns the logical message index.
	 * @return message index
	 */
	long messageIndex();

	/** Returns the transport name.
	 * @return transport name
	 */
	String transport();

	/** Returns the Store generation.
	 * @return Store generation, or {@code null}
	 */
	UUID storeGeneration();

	/** Returns the provider cursor bytes.
	 * @return provider cursor copy
	 */
	byte[] providerPosition();

	/** Creates complete message information.
	 *
	 * @param messageIndex logical message index
	 * @param transport transport name
	 * @param storeGeneration Store generation
	 * @param providerPosition provider cursor bytes
	 * @return message information
	 */
	static MessageInfo New(
		final long messageIndex,
		final String transport,
		final UUID storeGeneration,
		final byte[] providerPosition
	)
	{
		return new Default(messageIndex, notNull(transport), storeGeneration, providerPosition);
	}

	/** Creates legacy message information.
	 *
	 * @param messageIndex logical message index
	 * @return message information
	 */
	static MessageInfo New(final long messageIndex)
	{
		return New(messageIndex, "unknown", null, new byte[0]);
	}

	/** Stores an immutable copy of a provider position. */
	final class Default implements MessageInfo
	{
		private final long messageIndex;
		private final String transport;
		private final UUID storeGeneration;
		private final byte[] providerPosition;

		private Default(
			final long messageIndex,
			final String transport,
			final UUID storeGeneration,
			final byte[] providerPosition
		)
		{
			this.messageIndex = messageIndex;
			this.transport = transport;
			this.storeGeneration = storeGeneration;
			this.providerPosition = providerPosition == null ? new byte[0] : providerPosition.clone();
		}

		@Override
		public String transport()
		{
			return this.transport;
		}

		@Override
		public UUID storeGeneration()
		{
			return this.storeGeneration;
		}

		@Override
		public byte[] providerPosition()
		{
			return this.providerPosition.clone();
		}

		@Override
		public long messageIndex()
		{
			return this.messageIndex;
		}

		@Override
		public String toString()
		{
			return "MessageInfo{messageIndex=" + this.messageIndex + ",transport=" + this.transport +
				",storeGeneration=" + this.storeGeneration + ",providerPositionBytes=" +
				Arrays.toString(this.providerPosition) + '}';
		}
	}
}
