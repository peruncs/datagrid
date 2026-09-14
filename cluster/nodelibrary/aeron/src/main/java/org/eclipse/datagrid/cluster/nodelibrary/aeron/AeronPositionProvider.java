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

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.ReplicationPositionUnavailableException;
import org.eclipse.datagrid.cluster.nodelibrary.types.ReplicationCursor;
import org.eclipse.datagrid.cluster.nodelibrary.types.ReplicationPositionProvider;
import org.eclipse.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCursor;

import java.util.Objects;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Cached Aeron writer-boundary view. A reader's applied cursor is deliberately
 * not exposed as the latest log position because it cannot prove the writer's
 * durable boundary without a control channel.
 *
 * <p>Callers must invoke {@link #init()} before querying a writer. Initialization
 * establishes the writer-side runtime; {@link #latest()} only reads the
 * already-published boundary and never starts transport resources implicitly.
 * Readers remain unavailable because they cannot establish a writer boundary
 * locally.</p>
 */
final class AeronPositionProvider implements ReplicationPositionProvider
{
	private final BooleanSupplier writer;
	private final BooleanSupplier initialized;
	private final Runnable ensureWriter;
	private final Supplier<AeronWriterBoundary> writerBoundary;
	private final Supplier<UUID> clusterId;
	private final Supplier<UUID> nodeId;
	private final Supplier<UUID> storeGeneration;
	private final LongSupplier epoch;

	AeronPositionProvider(final BooleanSupplier writer, final BooleanSupplier initialized,
		final Runnable ensureWriter,
		final Supplier<AeronWriterBoundary> writerBoundary, final Supplier<UUID> clusterId,
		final Supplier<UUID> nodeId, final Supplier<UUID> storeGeneration, final LongSupplier epoch)
	{
		this.writer = Objects.requireNonNull(writer, "writer");
		this.initialized = Objects.requireNonNull(initialized, "initialized");
		this.ensureWriter = Objects.requireNonNull(ensureWriter, "ensureWriter");
		this.writerBoundary = Objects.requireNonNull(writerBoundary, "writerBoundary");
		this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
		this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
		this.storeGeneration = Objects.requireNonNull(storeGeneration, "storeGeneration");
		this.epoch = Objects.requireNonNull(epoch, "epoch");
	}

	@Override
	public void init() throws NodelibraryException
	{
		if (this.writer.getAsBoolean()) this.ensureWriter.run();
	}

	@Override
	public ReplicationCursor latest() throws NodelibraryException
	{
		if (!this.writer.getAsBoolean())
		{
			throw new ReplicationPositionUnavailableException("Aeron reader cannot establish the writer's latest durable boundary without watermark delivery");
		}
		if (!this.initialized.getAsBoolean())
			throw new ReplicationPositionUnavailableException("Aeron writer position is unavailable until the position provider is initialized");
		final AeronWriterBoundary boundary = this.writerBoundary.get();
		final UUID generation = this.storeGeneration.get();
		final byte[] encoded = boundary.recordingId() < 0 || boundary.position() < 0
			? new byte[0]
			: new AeronReplicationCursor(this.clusterId.get(), this.nodeId.get(), generation,
				this.epoch.getAsLong(), boundary.recordingId(), boundary.position(), boundary.sequence()).encode();
		return new ReplicationCursor("aeron", generation, boundary.sequence(), encoded);
	}

	@Override public void close() { }
}
