package org.eclipse.datagrid.storage.distributed.aeron.writer;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
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

import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.exceptions.PersistenceExceptionTransfer;
import org.eclipse.serializer.persistence.types.PersistenceTarget;

import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;

import static org.eclipse.serializer.util.X.notNull;

/** Persistence target that keeps local enqueue and Aeron publication ordered. */
public final class AeronStorageBinaryTargetDistributing implements PersistenceTarget<Binary>
{
	private final PersistenceTarget<Binary> delegate;
	private final AeronReplicationWriteCoordinator coordinator;
	private final StorageBinaryDataDistributor dictionarySource;
	private final LongConsumer committedSequence;
	private final BooleanSupplier distributionEnabled;

	public AeronStorageBinaryTargetDistributing(final PersistenceTarget<Binary> delegate,
		final AeronReplicationWriteCoordinator coordinator)
	{
		this(delegate, coordinator, null, ignored -> { }, () -> true);
	}

	public AeronStorageBinaryTargetDistributing(final PersistenceTarget<Binary> delegate,
		final AeronReplicationWriteCoordinator coordinator, final StorageBinaryDataDistributor dictionarySource,
		final LongConsumer committedSequence, final BooleanSupplier distributionEnabled)
	{
		this.delegate = notNull(delegate);
		this.coordinator = notNull(coordinator);
		this.dictionarySource = dictionarySource;
		this.committedSequence = notNull(committedSequence);
		this.distributionEnabled = notNull(distributionEnabled);
	}

	@Override
	public void write(final Binary data) throws PersistenceExceptionTransfer
	{
		synchronized (this.coordinator)
		{
			if (!this.distributionEnabled.getAsBoolean())
			{
				data.iterateChannelChunks(Binary::mark);
				try { this.delegate.write(data); }
				finally { data.iterateChannelChunks(Binary::reset); }
				return;
			}
			if (this.dictionarySource != null)
			{
				final String dictionary = this.dictionarySource.consumeTypeDictionary();
				if (dictionary != null) this.coordinator.distributeTypeDictionary(dictionary);
			}
			data.iterateChannelChunks(Binary::mark);
			if (this.coordinator.durabilityMode() == org.eclipse.datagrid.storage.distributed.types.ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE)
			{
				try
				{
					this.delegate.write(data);
				}
				finally
				{
					data.iterateChannelChunks(Binary::reset);
				}
				final AeronReplicationPublisher.PreparedTransaction prepared;
				try
				{
					prepared = this.coordinator.prepare(data);
				}
				catch (final RuntimeException | Error failure)
				{
					try { this.coordinator.markEnqueueWithoutArchive(); }
					catch (final RuntimeException | Error markerFailure) { failure.addSuppressed(markerFailure); }
					throw new IllegalStateException(
						"local Store write completed but Aeron publication could not prepare; reseed is required", failure);
				}
				try (prepared)
				{
					this.coordinator.markEnqueued(prepared);
					this.commitAndNotify(prepared);
				}
				return;
			}

			try (final AeronReplicationPublisher.PreparedTransaction prepared = this.coordinator.prepare(data))
			{
				try
				{
					this.delegate.write(data);
				}
				catch (final RuntimeException | Error failure)
				{
					try { this.coordinator.abort(prepared); }
					catch (final RuntimeException | Error abortFailure) { failure.addSuppressed(abortFailure); }
					throw failure;
				}
				finally
				{
					data.iterateChannelChunks(Binary::reset);
				}
				this.coordinator.markEnqueued(prepared);
				this.commitAndNotify(prepared);
			}
		}
	}

	/** Commits a prepared transaction and preserves the fail-closed uncertainty marker. */
	private void commitAndNotify(final AeronReplicationPublisher.PreparedTransaction prepared)
	{
		this.coordinator.commitOrMarkUncertain(prepared);
		this.committedSequence.accept(prepared.sequence());
	}

	@Override
	public boolean isWritable()
	{
		return this.delegate.isWritable();
	}
}
