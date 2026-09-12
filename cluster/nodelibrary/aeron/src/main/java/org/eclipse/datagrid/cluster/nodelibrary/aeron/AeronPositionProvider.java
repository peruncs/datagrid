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

import org.eclipse.datagrid.cluster.nodelibrary.types.ReplicationCursor;
import org.eclipse.datagrid.cluster.nodelibrary.types.ReplicationPositionProvider;

import java.util.Objects;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Cached Aeron position view. Writer callers receive the last durable terminal
 * boundary. Reader callers receive their own durable cursor because that is the
 * only latest position they can establish without a control channel.
 */
final class AeronPositionProvider implements ReplicationPositionProvider
{
	private final BooleanSupplier writer;
	private final Runnable ensureWriter;
	private final Supplier<AeronWriterBoundary> writerBoundary;
	private final Supplier<UUID> storeGeneration;
	private final Supplier<ReplicationCursor> readerCursor;

	AeronPositionProvider(final BooleanSupplier writer, final Runnable ensureWriter,
		final Supplier<AeronWriterBoundary> writerBoundary, final Supplier<UUID> storeGeneration,
		final Supplier<ReplicationCursor> readerCursor)
	{
		this.writer = Objects.requireNonNull(writer, "writer");
		this.ensureWriter = Objects.requireNonNull(ensureWriter, "ensureWriter");
		this.writerBoundary = Objects.requireNonNull(writerBoundary, "writerBoundary");
		this.storeGeneration = Objects.requireNonNull(storeGeneration, "storeGeneration");
		this.readerCursor = Objects.requireNonNull(readerCursor, "readerCursor");
	}

	@Override public void init() { }

	@Override
	public synchronized ReplicationCursor latest()
	{
		if (!this.writer.getAsBoolean())
		{
			return Objects.requireNonNull(this.readerCursor.get(), "readerCursor");
		}
		this.ensureWriter.run();
		final AeronWriterBoundary boundary = this.writerBoundary.get();
		return new ReplicationCursor("aeron", this.storeGeneration.get(), boundary.sequence(), encodePosition(boundary));
	}

	private static byte[] encodePosition(final AeronWriterBoundary boundary)
	{
		if (boundary.recordingId() < 0 || boundary.position() < 0) return new byte[0];
		return java.nio.ByteBuffer.allocate(Long.BYTES * 2)
			.putLong(boundary.recordingId()).putLong(boundary.position()).array();
	}

	@Override public void close() { }
}
