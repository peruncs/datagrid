package org.eclipse.datagrid.cluster.nodelibrary.aeron;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Aeron Provider
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

import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterStorageBinaryDataDistributor;
import org.eclipse.datagrid.storage.distributed.aeron.writer.AeronReplicationWriteCoordinator;
import org.eclipse.serializer.persistence.binary.types.Binary;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * The Aeron distributor state shared with the Store integration.
 * Direct distribution uses the coordinator's archive-first fence. Store writes
 * should still use the persistence-target path when local acceptance is part of
 * the same operation.
 */
final class AeronDistributor implements ClusterStorageBinaryDataDistributor
{
	private final BooleanSupplier writer;
	private final LongConsumer sequenceSynchronizer;
	private final Supplier<AeronReplicationWriteCoordinator> coordinatorSupplier;
	private volatile long index = -1L;
	private volatile boolean ignored;
	private String dictionary;

	AeronDistributor(final BooleanSupplier writer, final LongConsumer sequenceSynchronizer,
		final Supplier<AeronReplicationWriteCoordinator> coordinatorSupplier)
	{
		this.writer = Objects.requireNonNull(writer, "writer");
		this.sequenceSynchronizer = Objects.requireNonNull(sequenceSynchronizer, "sequenceSynchronizer");
		this.coordinatorSupplier = Objects.requireNonNull(coordinatorSupplier, "coordinatorSupplier");
	}

	@Override
	public void messageIndex(final long value)
	{
		if (value < -1 || value == Long.MAX_VALUE)
			throw new IllegalArgumentException("message index must be in [-1, Long.MAX_VALUE)");
		this.index = value;
		this.sequenceSynchronizer.accept(value + 1);
	}

	@Override public long messageIndex() { return this.index; }
	@Override public void ignoreDistribution(final boolean value) { this.ignored = value; }
	@Override public boolean ignoreDistribution() { return this.ignored; }

	@Override
	public synchronized void distributeTypeDictionary(final String value)
	{
		this.dictionary = value;
	}

	@Override
	public synchronized String consumeTypeDictionary()
	{
		final String value = this.dictionary;
		this.dictionary = null;
		return value;
	}

	@Override
	public synchronized void distributeData(final Binary data)
	{
		if (this.ignored) return;
		if (!this.writer.getAsBoolean()) throw new IllegalStateException("Aeron replication distributor is writer-only");
		final AeronReplicationWriteCoordinator coordinator =
			Objects.requireNonNull(this.coordinatorSupplier.get(), "coordinator");
		final String pendingDictionary = this.dictionary;
		if (pendingDictionary != null)
		{
			coordinator.distributeTypeDictionary(pendingDictionary);
		}
		coordinator.distributeData(Objects.requireNonNull(data, "data"));
		this.dictionary = null;
	}

	/** The provider owns the shared Aeron/archive runtime. */
	@Override public void dispose() { }
}
