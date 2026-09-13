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

/**
 * Store target that couples local acceptance to Aeron replication.
 *
 * <p>The selected durability mode decides which side is attempted first. A
 * failed terminal step records an uncertain state and stops further writes;
 * this is safer than allowing the local Store and Archive to drift silently.</p>
 */
public final class AeronStorageBinaryTargetDistributing implements PersistenceTarget<Binary>
{
	private static void crashPoint(final String name, final long sequence)
	{
		CrashHook.invoke(name, sequence);
	}
	private final PersistenceTarget<Binary> delegate;
	private final AeronReplicationWriteCoordinator coordinator;
	private final StorageBinaryDataDistributor dictionarySource;
	private final LongConsumer committedSequence;
	private final BooleanSupplier distributionEnabled;

	/**
	 * Creates a target with replication enabled for every write.
	 *
	 * @param delegate local Store target
	 * @param coordinator Aeron transaction coordinator
	 */
	public AeronStorageBinaryTargetDistributing(final PersistenceTarget<Binary> delegate,
		final AeronReplicationWriteCoordinator coordinator)
	{
		this(delegate, coordinator, null, ignored -> { }, () -> true);
	}

	/**
	 * Creates a target with the provider-owned dictionary, sequence and admission
	 * callbacks.  The callbacks are deliberately supplied by the provider so that
	 * local Store acceptance and the Aeron checkpoint transition remain one owner-
	 * serialized operation.
	 *
	 * @param delegate local Store target
	 * @param coordinator Aeron transaction coordinator
	 * @param dictionarySource source of staged type dictionaries
	 * @param committedSequence callback for the committed sequence
	 * @param distributionEnabled predicate that enables replication
	 */
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

	/** Writes locally and completes the matching Aeron transaction. */
	@Override
	public void write(final Binary data) throws PersistenceExceptionTransfer
	{
		/* The target lock is the transaction boundary: prepare, local Store
		 * acceptance, and terminal publication must not interleave with another
		 * writer. Coordinator methods remain synchronized because they are also
		 * used directly by the provider and tests. */
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
				if (dictionary != null)
				{
					/* consumeTypeDictionary() only transfers ownership to the coordinator.
					 * The coordinator deliberately retains the bytes until commit(), so a
					 * local rejection or uncertain publication can retry the same dictionary
					 * even though the source has already cleared its staging slot. */
					this.coordinator.distributeTypeDictionary(dictionary);
				}
			}
			data.iterateChannelChunks(Binary::mark);
			if (this.coordinator.durabilityMode() == org.eclipse.datagrid.storage.distributed.types.ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE)
			{
				try
				{
					final long localSequence = this.coordinator.markLocalEnqueue(data);
					this.delegate.write(data);
					crashPoint("AFTER_ENQUEUE_BEFORE_PREPARE", localSequence);
				}
				catch (final RuntimeException | Error failure)
				{
					try { this.coordinator.clearLocalEnqueue(); }
					catch (final RuntimeException | Error clearFailure) { failure.addSuppressed(clearFailure); }
					throw failure;
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

			final AeronReplicationPublisher.PreparedTransaction prepared;
			try
			{
				prepared = this.coordinator.prepare(data);
			}
			catch (final RuntimeException | Error failure)
			{
				data.iterateChannelChunks(Binary::reset);
				throw failure;
			}
			try (prepared)
			{
				crashPoint("AFTER_PREPARE_BEFORE_LOCAL_WRITE", prepared.sequence());
				try
				{
					this.delegate.write(data);
					crashPoint("AFTER_LOCAL_WRITE_BEFORE_COMMIT", prepared.sequence());
				}
				catch (final RuntimeException | Error failure)
				{
					try { this.coordinator.abort(prepared); }
					catch (final RuntimeException | Error abortFailure) { failure.addSuppressed(abortFailure); }
					throw failure;
				}
				finally { data.iterateChannelChunks(Binary::reset); }
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

	/** Returns whether the local persistence target can accept a write. */
	@Override
	public boolean isWritable()
	{
		return this.delegate.isWritable();
	}
}
