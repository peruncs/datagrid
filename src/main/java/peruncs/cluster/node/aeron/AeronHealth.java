package peruncs.cluster.node.aeron;

import peruncs.cluster.api.ReplicationState;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.node.replication.ClusterReplicationTransport.StorageControllerAdapter;
import peruncs.cluster.node.replication.ReplicationHealth;
import peruncs.cluster.storage.binary.ReplicationApplier;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.WARNING;

/// Cached health view for one Aeron provider client and storage controller.
///
/// The view holds only suppliers: it never closes transport resources and it
/// is replaced, not reused, when the provider client or storage adapter
/// changes. A supplier that throws is treated as an unhealthy probe and logged
/// at debug level, so a monitoring scrape reports FAILED instead of
/// propagating an exception into the monitoring path.
final class AeronHealth implements ReplicationHealth {
    private static final System.Logger LOGGER = System.getLogger(AeronHealth.class.getName());
    private final StorageControllerAdapter storage;
    private final ReplicationApplier client;
    private final BooleanSupplier closed;
    private final BooleanSupplier driverFailed;
    private final BooleanSupplier capacityAvailable;
    private final BooleanSupplier writerReady;
    private final BooleanSupplier writerRole;
    private final Supplier<ReplicationState> writerState;
    private final LongSupplier archiveUsableSpace;
    private final LongSupplier writerDurablePosition;
    private final LongSupplier writerDurableSequence;
    private final LongSupplier appliedSequence;
    private final BooleanSupplier watermarkFailed;
    private volatile boolean active = true;
    /* First failure of each probe is logged at warning level so a persistent
     * supplier bug (not a transient network probe) surfaces at the default
     * log level; repeats stay at debug to keep a degraded-but-known node from
     * flooding the operator. */
    private final Set<String> warnedProbes = ConcurrentHashMap.newKeySet();

    /// Failure and capacity probes shared by every role.
    ///
    /// @param closed             the transport is closed
    /// @param driverFailed       the MediaDriver failed
    /// @param capacityAvailable  the Archive can take another maximum-size transaction
    /// @param watermarkFailed    the reader-to-writer watermark channel failed
    record Signals(BooleanSupplier closed, BooleanSupplier driverFailed, BooleanSupplier capacityAvailable,
                   BooleanSupplier watermarkFailed) {
    }

    /// Writer-side probes.
    ///
    /// @param ready the installed writer can accept durable writes
    /// @param role  this node is the writer
    /// @param state recovery or publication state, or `null` while healthy
    record WriterProbes(BooleanSupplier ready, BooleanSupplier role, Supplier<ReplicationState> state) {
    }

    /// Positions and capacity reported in the status.
    ///
    /// @param archiveUsableSpace    usable Archive bytes
    /// @param writerDurablePosition durable writer Archive position
    /// @param writerDurableSequence durable writer sequence
    /// @param appliedSequence       last sequence applied by the reader
    record Positions(LongSupplier archiveUsableSpace, LongSupplier writerDurablePosition,
                     LongSupplier writerDurableSequence, LongSupplier appliedSequence) {
    }

    AeronHealth(final StorageControllerAdapter storage, final ReplicationApplier client,
                final Signals signals, final WriterProbes writer, final Positions positions) {
        this.storage = Objects.requireNonNull(storage, "storage");
        this.client = client;
        this.closed = Objects.requireNonNull(signals.closed(), "closed");
        this.driverFailed = Objects.requireNonNull(signals.driverFailed(), "driverFailed");
        this.capacityAvailable = Objects.requireNonNull(signals.capacityAvailable(), "capacityAvailable");
        this.watermarkFailed = Objects.requireNonNull(signals.watermarkFailed(), "watermarkFailed");
        this.writerReady = Objects.requireNonNull(writer.ready(), "writerReady");
        this.writerRole = Objects.requireNonNull(writer.role(), "writerRole");
        this.writerState = Objects.requireNonNull(writer.state(), "writerState");
        this.archiveUsableSpace = Objects.requireNonNull(positions.archiveUsableSpace(), "archiveUsableSpace");
        this.writerDurablePosition = Objects.requireNonNull(positions.writerDurablePosition(), "writerDurablePosition");
        this.writerDurableSequence = Objects.requireNonNull(positions.writerDurableSequence(), "writerDurableSequence");
        this.appliedSequence = Objects.requireNonNull(positions.appliedSequence(), "appliedSequence");
    }

    /// Reports whether this view belongs to the supplied provider pair.
    ///
    /// @param storage storage adapter identity
    /// @param client  client identity
    /// @return `true` when this view was created for both
    boolean matches(final StorageControllerAdapter storage, final ReplicationApplier client) {
        return this.storage == storage && this.client == client;
    }

    @Override
    public long archiveUsableSpaceBytes() {
        return this.archiveUsableSpace.getAsLong();
    }

    @Override
    public long writerDurablePosition() {
        return this.writerDurablePosition.getAsLong();
    }

    @Override
    public long writerDurableSequence() {
        return this.writerDurableSequence.getAsLong();
    }

    @Override
    public long appliedSequence() {
        return this.appliedSequence.getAsLong();
    }

    @Override
    public boolean isReady() {
        return this.ready(true);
    }

    @Override
    public boolean isHealthy() {
        return this.ready(false);
    }

    private boolean ready(final boolean requireLive) {
        /* Do not invoke a lifecycle supplier after this view has been closed.  In
         * particular, writerReady may initialise an Archive, so it is invoked at
         * most once per evaluation, only on the live path, and the snapshot is
         * cached in a local for every check below. A health object that has
         * already been disposed stays a pure, side-effect-free failure view. */
        try {
            if (!this.active || this.closed.getAsBoolean() || this.watermarkFailed.getAsBoolean()) return false;
            /* Writer readiness may perform I/O; the single cached snapshot below is
             * the only invocation for this evaluation. */
            final boolean writerIsReady = this.writerReady.getAsBoolean();
            return this.storage.isReady()
                   && !this.driverFailed.getAsBoolean() && this.capacityAvailable.getAsBoolean()
                   && this.writerState.get() == null
                   && (writerIsReady || this.client != null && this.client.failure() == null
                                       && this.client.isRunning() && (!requireLive || this.client.isLive()));
        } catch (final RuntimeException probeFailure) {
            this.logProbeFailure("readiness", probeFailure);
            return false;
        }
    }

    /// Reports the replication lifecycle state.
    ///
    /// Writers have no reader client by design, so a missing client is
    /// starting rather than failed; only driver, watermark, writer, or
    /// client failures report failed. A writer without Archive capacity
    /// reports degraded instead of failed so it stays scrutable while
    /// refusing new writes.
    ///
    /// @return current replication state
    @Override
    public ReplicationState state() {
        try {
            return this.stateUnchecked();
        } catch (final RuntimeException probeFailure) {
            this.logProbeFailure("state", probeFailure);
            return ReplicationState.FAILED;
        }
    }

    /// Logs one probe failure at warning and every later one at debug.
    ///
    /// @param probe probe name used in the message and the once-per-probe set
    /// @param probeFailure failure thrown by a supplier
    private void logProbeFailure(final String probe, final RuntimeException probeFailure) {
        if (this.warnedProbes.add(probe)) {
            LOGGER.log(WARNING,
                    "Aeron replication %s probe failed; reporting the degraded state, later failures log at debug"
                            .formatted(probe), probeFailure);
            return;
        }
        LOGGER.log(DEBUG,
                "Aeron replication %s probe failed again".formatted(probe), probeFailure);
    }

    private ReplicationState stateUnchecked() {
        if (!this.active || this.closed.getAsBoolean() || this.driverFailed.getAsBoolean() ||
            this.watermarkFailed.getAsBoolean()) {
            return ReplicationState.FAILED;
        }
        if (this.writerRole.getAsBoolean()) {
            /* Writers intentionally have no reader client.  Treating that null client
             * as a failure made every healthy writer report FAILED, even though its
             * publication and writer boundary were ready. */
            final ReplicationState state = this.writerState.get();
            if (state == ReplicationState.RESEED_REQUIRED || state == ReplicationState.FAILED) {
                return state;
            }
            if (!this.capacityAvailable.getAsBoolean()) return ReplicationState.DEGRADED;
            return this.writerReady.getAsBoolean()
                    ? ReplicationState.LIVE : ReplicationState.STARTING;
        }
        final ReplicationState state = this.writerState.get();
        if (state != null) return state;
        if (this.client == null) {
            /* A reader is not failed merely because the provider has not created its
             * subscription yet.  This is the normal state between provider creation
             * and NodeLifecycle's client wiring. */
            return ReplicationState.STARTING;
        }
        if (this.client.failure() instanceof ReseedRequiredException) {
            /* The recording no longer covers the reader's Store mark, so
             * retrying the same mark would fail again: report the typed
             * reseed signal. An unreachable Archive is a plain failure. */
            return ReplicationState.RESEED_REQUIRED;
        }
        if (this.client.failure() != null) return ReplicationState.FAILED;
        if (!this.client.isRunning()) return ReplicationState.STARTING;
        return this.client.isLive() ? ReplicationState.LIVE : ReplicationState.REPLAYING;
    }

    /// Marks this view inactive so later probes report a stable failure
    /// without invoking any lifecycle supplier.
    @Override
    public void close() {
        this.active = false;
    }
}
