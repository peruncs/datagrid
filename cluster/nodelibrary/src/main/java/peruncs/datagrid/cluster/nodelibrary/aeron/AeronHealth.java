package peruncs.datagrid.cluster.nodelibrary.aeron;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import peruncs.datagrid.cluster.nodelibrary.replication.ClusterReplicationTransport;
import peruncs.datagrid.cluster.nodelibrary.replication.ClusterReplicationTransport.StorageControllerAdapter;
import peruncs.datagrid.cluster.nodelibrary.replication.ClusterStorageBinaryDataClient;
import peruncs.datagrid.cluster.nodelibrary.replication.ReplicationHealth;

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
	private final BooleanSupplier watermarkFailed;
	private volatile boolean active = true;

	AeronHealth(final StorageControllerAdapter storage, final ClusterStorageBinaryDataClient client,
		final BooleanSupplier closed, final BooleanSupplier driverFailed, final BooleanSupplier capacityAvailable,
		final BooleanSupplier writerReady, final BooleanSupplier writerRole,
		final Supplier<ReplicationHealth.State> checkpointState, final LongSupplier archiveUsableSpace,
		final LongSupplier writerDurablePosition, final LongSupplier writerDurableSequence,
		final LongSupplier appliedSequence, final BooleanSupplier watermarkFailed)
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
		this.watermarkFailed = Objects.requireNonNull(watermarkFailed, "watermarkFailed");
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
		if (!this.active || this.closed.getAsBoolean() || this.watermarkFailed.getAsBoolean()) return false;
		/* Writer readiness is a side-effect-free lifecycle snapshot. */
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
		if (!this.active || this.closed.getAsBoolean() || this.driverFailed.getAsBoolean() ||
			this.watermarkFailed.getAsBoolean())
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
		if (!this.client.isRunning()) return ReplicationHealth.State.STARTING;
		return this.client.isLive() ? ReplicationHealth.State.LIVE : ReplicationHealth.State.REPLAYING;
	}

	@Override public void close() { this.active = false; }
}
