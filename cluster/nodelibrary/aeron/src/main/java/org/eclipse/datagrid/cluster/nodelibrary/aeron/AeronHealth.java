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

import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterReplicationTransport.StorageControllerAdapter;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterStorageBinaryDataClient;
import org.eclipse.datagrid.cluster.nodelibrary.types.ReplicationHealth;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Cached health view for one Aeron provider client and storage controller. */
final class AeronHealth implements ReplicationHealth
{
	private final StorageControllerAdapter storage;
	private final ClusterStorageBinaryDataClient client;
	private final BooleanSupplier closed;
	private final BooleanSupplier driverFailed;
	private final BooleanSupplier capacityAvailable;
	private final BooleanSupplier writerReady;
	private final BooleanSupplier writerRole;
	private final Supplier<ReplicationHealth.State> checkpointState;
	private final LongSupplier archiveUsableSpace;
	private final LongSupplier writerDurablePosition;
	private final LongSupplier writerDurableSequence;
	private final LongSupplier appliedSequence;
	private volatile boolean active = true;

	AeronHealth(final StorageControllerAdapter storage, final ClusterStorageBinaryDataClient client,
		final BooleanSupplier closed, final BooleanSupplier driverFailed, final BooleanSupplier capacityAvailable,
		final BooleanSupplier writerReady, final BooleanSupplier writerRole,
		final Supplier<ReplicationHealth.State> checkpointState, final LongSupplier archiveUsableSpace,
		final LongSupplier writerDurablePosition, final LongSupplier writerDurableSequence,
		final LongSupplier appliedSequence)
	{
		this.storage = Objects.requireNonNull(storage, "storage");
		this.client = client;
		this.closed = Objects.requireNonNull(closed, "closed");
		this.driverFailed = Objects.requireNonNull(driverFailed, "driverFailed");
		this.capacityAvailable = Objects.requireNonNull(capacityAvailable, "capacityAvailable");
		this.writerReady = Objects.requireNonNull(writerReady, "writerReady");
		this.writerRole = Objects.requireNonNull(writerRole, "writerRole");
		this.checkpointState = Objects.requireNonNull(checkpointState, "checkpointState");
		this.archiveUsableSpace = Objects.requireNonNull(archiveUsableSpace, "archiveUsableSpace");
		this.writerDurablePosition = Objects.requireNonNull(writerDurablePosition, "writerDurablePosition");
		this.writerDurableSequence = Objects.requireNonNull(writerDurableSequence, "writerDurableSequence");
		this.appliedSequence = Objects.requireNonNull(appliedSequence, "appliedSequence");
	}

	boolean matches(final StorageControllerAdapter storage, final ClusterStorageBinaryDataClient client)
	{
		return this.storage == storage && this.client == client;
	}

	@Override public void init() { }
	@Override public long archiveUsableSpaceBytes() { return this.archiveUsableSpace.getAsLong(); }
	@Override public long writerDurablePosition() { return this.writerDurablePosition.getAsLong(); }
	@Override public long writerDurableSequence() { return this.writerDurableSequence.getAsLong(); }
	@Override public long appliedSequence() { return this.appliedSequence.getAsLong(); }

	@Override
	public boolean isReady()
	{
		return this.ready(true);
	}

	@Override
	public boolean isHealthy()
	{
		return this.ready(false);
	}

	private boolean ready(final boolean requireLive)
	{
		/* Do not invoke a lifecycle supplier after this view has been closed.  In
		 * particular, writerReady may initialise an Archive; a health object that
		 * has already been disposed must be a pure, side-effect-free failure view. */
		if (!this.active || this.closed.getAsBoolean()) return false;
		/* Evaluate writer readiness before the retained recovery state.  A freshly
		 * created writer is STARTING until the first health probe initialises its
		 * Archive; short-circuiting on that state would make readiness permanently
		 * false even though startup is otherwise healthy. */
		final boolean writerIsReady = this.writerReady.getAsBoolean();
		return this.storage.isReady()
			&& !this.driverFailed.getAsBoolean() && this.capacityAvailable.getAsBoolean()
			&& this.checkpointState.get() == null
			&& (writerIsReady || this.client != null && this.client.failure() == null
				&& this.client.isRunning() && (!requireLive || this.client.isLive()));
	}

	@Override
	public ReplicationHealth.State state()
	{
		if (!this.active || this.closed.getAsBoolean() || this.driverFailed.getAsBoolean())
		{
			return ReplicationHealth.State.FAILED;
		}
		if (this.writerRole.getAsBoolean())
		{
			/* Writers intentionally have no reader client.  Treating that null client
			 * as a failure made every healthy writer report FAILED, even though its
			 * publication and terminal checkpoint were ready. */
			final ReplicationHealth.State checkpoint = this.checkpointState.get();
			if (checkpoint == ReplicationHealth.State.RESEED_REQUIRED ||
				checkpoint == ReplicationHealth.State.FAILED)
			{
				return checkpoint;
			}
			if (!this.capacityAvailable.getAsBoolean()) return ReplicationHealth.State.DEGRADED_ARCHIVE;
			return this.writerReady.getAsBoolean()
				? ReplicationHealth.State.LIVE : ReplicationHealth.State.STARTING;
		}
		final ReplicationHealth.State checkpoint = this.checkpointState.get();
		if (checkpoint != null) return checkpoint;
		if (this.client == null)
		{
			/* A reader is not failed merely because the provider has not created its
			 * subscription yet.  This is the normal state between provider creation
			 * and ClusterFoundation's client wiring. */
			return ReplicationHealth.State.STARTING;
		}
		if (this.client.failure() != null) return ReplicationHealth.State.FAILED;
		return this.client.isLive() ? ReplicationHealth.State.LIVE : ReplicationHealth.State.REPLAYING;
	}

	@Override public void close() { this.active = false; }
}
