package peruncs.cluster.node.aeron;

import io.aeron.archive.client.ArchiveException;
import io.aeron.exceptions.AeronException;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.types.StorageConnection;
import peruncs.cluster.api.ReplicationState;
import peruncs.cluster.errors.ReplicationPositionUnavailableException;
import peruncs.cluster.errors.ReplicationUnavailableException;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.errors.WriterFencedException;
import peruncs.cluster.node.NodeSettingsSource;
import peruncs.cluster.node.replication.ReplicationPositionProvider;
import peruncs.cluster.node.store.RejectingPersistenceTarget;
import peruncs.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.cluster.storage.aeron.checkpoint.AeronReplicationCheckpointStore;
import peruncs.cluster.storage.aeron.writer.*;
import peruncs.cluster.storage.binary.ReplicationPublisher;
import peruncs.cluster.storage.index.ClusterStoreIndexes;
import peruncs.cluster.storage.io.AtomicFileWriter;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static peruncs.cluster.node.aeron.AeronTransportShared.reseedRequired;

/// Owns the writer side of one transport: the Archive publication, write
/// coordinator, fencing lease, and the durable writer checkpoint boundary.
///
/// The writer publication is installed only after its checkpoint recovery has
/// fully succeeded, so a partially recovered writer is never visible to the
/// position provider or health. A recovery failure is classified as typed
/// reseed/unavailable failures and remembered as a terminal state, so later
/// health probes keep reporting it instead of restarting silently. Compound
/// writer steps synchronize on the shared transport monitor; the position
/// boundary itself is published as one immutable snapshot.
final class AeronWriterTransport {
    /* Test-only, dynamically scoped seam used by the forked crash harness. */
    private static final ScopedValue<Long> CHECKPOINT_SEQUENCE = ScopedValue.newInstance();
    /// Default heartbeat staleness bound for the writer fencing lease.
    private static final long DEFAULT_LEASE_STALENESS_MILLIS = 30_000L;

    private final AeronTransport facade;
    private final Path leaseDirectory;
    /// Heartbeat staleness bound for the writer fencing lease.
    private final long leaseStalenessMillis;
    private volatile WriterFencingLease writerLease;
    /* Fencing token captured when the lease is acquired. Writer checkpoints
     * and the position supplier read this snapshot instead of performing a
     * live lease lookup that would throw a bare exception when unleased. */
    private volatile long writerFencingTokenSnapshot = -1L;
    private final AtomicLong nextSequence = new AtomicLong();
    /// Recording identity selected at writer startup; retained when RecordingPos is briefly unavailable.
    private final AtomicLong writerRecordingId = new AtomicLong();
    private volatile AeronArchiveReplicationPublisher writer;
    private volatile AeronReplicationWriteCoordinator coordinator;
    /* Published together after the terminal checkpoint is durable.  Readers of
     * positionProvider() must never combine fields from two transactions. */
    private volatile AeronWriterBoundary writerBoundary = new AeronWriterBoundary(-1, -1, -1);
    private volatile boolean writerRecoveryInProgress;
    private volatile ReplicationState writerRecoveryState;
    /* The classified failure behind writerRecoveryState. A later ensureWriter
     * rethrows it instead of retrying recovery: a RESEED_REQUIRED or terminal
     * FAILED outcome is sticky by contract, and the operator restart is the
     * only retry. */
    private volatile RuntimeException writerRecoveryFailure;
    private ReplicationPublisher distributor;
    private ReplicationPositionProvider positionProvider;

    AeronWriterTransport(final AeronTransport facade, final Path leaseDirectory, final Long leaseStalenessMillis) {
        this.facade = facade;
        this.leaseDirectory = leaseDirectory;
        /* Heartbeat staleness bound for the fencing lease: configurable so
         * deployments with slow shared volumes can widen it, while the
         * default keeps takeover within half a minute. */
        this.leaseStalenessMillis = leaseStalenessMillis == null || leaseStalenessMillis <= 0L
                ? DEFAULT_LEASE_STALENESS_MILLIS : leaseStalenessMillis;
        this.writerRecordingId.set(facade.settings().topology().recordingId());
    }

    private AeronSettings settings() {
        return this.facade.settings();
    }

    private AeronTransportShared shared() {
        return this.facade.shared();
    }

    private AeronRuntimeOwner runtime() {
        return this.facade.runtimeOwner();
    }

    /// Maps a writer recovery failure onto the typed transport failure taxonomy.
    ///
    /// @param failure failure thrown while recovering the writer
    /// @return typed replication failure, or the original runtime failure
    static RuntimeException writerRecoveryFailure(final RuntimeException failure) {
        if (failure instanceof ArchiveException archive) {
            return new ReplicationUnavailableException("Aeron Archive writer recovery failed", archive,
                    archive.errorCode());
        }
        if (failure instanceof AeronException) {
            return new ReplicationUnavailableException("Aeron writer recovery failed", failure);
        }
        return failure;
    }

    /// Returns the sequence of the checkpoint currently being persisted, or `-1`.
    ///
    /// @return in-flight checkpoint sequence for the crash harness
    static long currentCheckpointSequence() {
        return CHECKPOINT_SEQUENCE.orElse(-1L);
    }

    /// Atomically clears a failed writer candidate, reporting close noise as suppressed.
    private static void closeFailedWriter(final AeronArchiveReplicationPublisher writer,
                                          final Throwable failure) {
        if (writer == null) return;
        try {
            writer.close();
        } catch (final Throwable cleanupFailure) {
            if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
        }
    }

    /// Returns the replication publisher for the transport's single replication stream.
    ///
    /// The returned handle carries dictionaries and lifecycle control
    /// only: data publication is deliberately unavailable through it and
    /// flows exclusively through the persistence-target factory, where
    /// local Store acceptance and checkpoint fencing are one serialized
    /// operation. A second stream name is rejected — one transport owns
    /// exactly one stream.
    ///
    /// @param streamName logical stream name, claimed on first use
    /// @return replication publisher
    ReplicationPublisher distributor(final String streamName) {
        /* Aeron data publication is intentionally available only through the
         * persistence target below, where local acceptance and checkpoint fencing
         * are one serialized operation. The publisher remains the dictionary and
         * lifecycle control object required by the neutral Store integration. */
        final AeronTransportShared shared = shared();
        synchronized (shared) {
            shared.ensureOpen();
            shared.claimStream(streamName);
            if (this.distributor != null && Objects.equals(shared.claimedStream(), streamName)) {
                return this.distributor;
            }
            if (this.distributor != null) {
                throw new IllegalStateException("Aeron transport supports one replication stream per provider");
            }
            this.distributor = new AeronDistributionGate(
                    () -> settings().topology().role().isWriter(),
                    value ->
                    {
                        this.nextSequence.accumulateAndGet(value, Math::max);
                        if (this.writer != null) this.writer.synchronizeNextSequence(value);
                    });
            return this.distributor;
        }
    }

    /// Raises the next write sequence after a reader applied a transaction.
    ///
    /// Reader replay of newly written history must advance the same sequence a
    /// local Store write would, so a recovered writer never reuses it.
    ///
    /// @param value next sequence observed by a reader
    void advanceSequence(final long value) {
        this.nextSequence.accumulateAndGet(value, Math::max);
    }

    /// Clears the publisher reference after a completed transport close.
    void clearDistributor() {
        this.distributor = null;
    }

    /// Builds the write-path persistence target for the single stream.
    ///
    /// @param streamName    logical stream name
    /// @param distributor   publisher created for the stream
    /// @param writerStorage writer-side storage connection supplier
    /// @return persistence-target decorator
    UnaryOperator<PersistenceTarget<Binary>> persistenceTargetFactory(
            final String streamName, final ReplicationPublisher distributor,
            final Supplier<StorageConnection> writerStorage) {
        final AeronTransportShared shared = shared();
        synchronized (shared) {
            shared.ensureOpen();
            shared.claimStream(streamName);
            if (!settings().topology().role().isWriter()) {
                /* Reader roles must reproduce the writer's history through
                 * the replication import path, which bypasses this target.
                 * Any locally originated write is rejected instead of
                 * persisting an unreplicated divergence. */
                return RejectingPersistenceTarget::create;
            }
        }
        final AeronReplicationWriteCoordinator coordinator = this.ensureCoordinator();
        return delegate -> new AeronStorageBinaryReplicationTarget(
                delegate,
                coordinator,
                distributor,
                sequence ->
                {
                    if (distributor instanceof ReplicationPublisher cluster) {
                        cluster.messageIndex(sequence);
                    }
                },
                () -> !(distributor instanceof ReplicationPublisher cluster) || !cluster.ignoreDistribution(),
                /* The reachable index topology can change without replacing
                 * a Store root, so every distributed write validates the
                 * live graph. The storage connection does not exist during
                 * wiring; a null connection during root creation skips it. */
                () ->
                {
                    final StorageConnection connection = writerStorage == null ? null : writerStorage.get();
                    if (connection != null) {
                        ClusterStoreIndexes.validateStorageRoots(connection);
                    }
                }
        );
    }

    /// Returns the lazily created position provider for the stream.
    ///
    /// @param streamName logical stream name, claimed on first use
    /// @return writer position provider
    ReplicationPositionProvider positionProvider(final String streamName) {
        final AeronTransportShared shared = shared();
        synchronized (shared) {
            shared.ensureOpen();
            shared.claimStream(streamName);
            if (this.positionProvider == null) {
                this.positionProvider = new AeronPositionProvider(
                        () -> settings().topology().role().isWriter(),
                        () -> this.writer != null && !this.writerRecoveryInProgress && !shared.closed(),
                        this::ensureWriter,
                        () -> this.writerBoundary,
                        settings().topology()::clusterId,
                        () -> settings().topology().identity().nodeId(),
                        () -> settings().topology().identity().storeGeneration(),
                        settings().topology()::epoch,
                        this::positionWriterFencingToken);
            }
            return this.positionProvider;
        }
    }

    /// Reports whether the installed writer can accept durable writes.
    ///
    /// @return `true` while a healthy writer is installed and not recovering
    boolean writerReady() {
        return settings().topology().role().isWriter() && !this.writerRecoveryInProgress &&
               this.writerRecoveryState == null && this.writer != null && !this.writer.isFailed() &&
               this.runtime().driverFailure() == null;
    }

    /// Reports current writer readiness without starting the runtime. Lifecycle
    /// startup belongs to an explicit client or write operation, not a health
    /// probe.
    ReplicationState writerCheckpointState() {
        if (this.runtime().driverFailure() != null) {
            return ReplicationState.FAILED;
        }
        if (!settings().topology().role().isWriter()) {
            return null;
        }
        /* Preserve a failed startup result even though the writer object was never
         * installed. Returning STARTING for a null writer would hide a permanent
         * RESEED_REQUIRED/FAILED state from health probes. */
        if (this.writerRecoveryState != null) return this.writerRecoveryState;
        /* Capture once: the close stages can discard the writer between the
         * null check and the dereference, which would surface as a probe NPE. */
        final AeronArchiveReplicationPublisher current = this.writer;
        if (this.writerRecoveryInProgress || current == null) {
            return ReplicationState.STARTING;
        }
        /* A live writer can fail closed after an offer, await, or checkpoint
         * error without being discarded immediately.  Do not advertise LIVE
         * while that publisher can no longer accept a durable transaction. */
        return current.isFailed() ? ReplicationState.FAILED : null;
    }

    /// Returns the last published writer boundary snapshot.
    ///
    /// @return immutable writer boundary
    AeronWriterBoundary writerBoundary() {
        return this.writerBoundary;
    }

    /// Returns the current writer recording identity.
    ///
    /// @return recording id, or a negative value while unassigned
    long writerRecordingId() {
        return this.writerRecordingId.get();
    }

    /// Reports whether a writer publication is installed.
    ///
    /// @return `true` while a writer exists
    boolean hasWriter() {
        return this.writer != null;
    }

    /// Reports whether a write coordinator is installed.
    ///
    /// @return `true` while a coordinator exists
    boolean hasCoordinator() {
        return this.coordinator != null;
    }

    /// Reports whether the writer fencing lease is held.
    ///
    /// @return `true` while a lease exists
    boolean hasLease() {
        return this.writerLease != null;
    }

    private void ensureWriter() {
        final AeronTransportShared shared = shared();
        synchronized (shared) {
            this.ensureWriterLocked();
        }
        /* Retention callbacks acquire their own monitor. Do not invoke them
         * while the transport monitor is held, or retention->transport and
         * transport->retention paths can deadlock during writer recovery. */
        this.facade.retentionOwner().drainDeferredWatermarks();
    }

    private AeronArchiveReplicationPublisher ensureWriterLocked() {
        final AeronTransportShared shared = shared();
        if (shared.closing() || shared.closed()) {
            throw new IllegalStateException("Aeron writer cannot start while transport is closing");
        }
        if (this.writer != null) {
            if (this.writerRecoveryState != null) {
                throw new IllegalStateException("Aeron writer recovery did not complete");
            }
            return this.writer;
        }
        if (this.writerRecoveryState != null) {
            /* A previous recovery attempt failed terminally. Never retry from
             * scratch and never return a partially recovered writer: the same
             * typed failure is rethrown so health probes and callers keep
             * reporting the original cause. */
            throw this.writerRecoveryFailure;
        }
        /* Fence before any publication path exists: a second writer for
         * the same cluster/generation fails here while the first writer's
         * heartbeat is fresh, instead of publishing concurrently. */
        this.ensureWriterLease();
        this.writerRecoveryInProgress = true;
        AeronArchiveReplicationPublisher candidate = null;
        try {
            this.runtime().ensure();
            final AeronReplicationCheckpoint checkpoint = this.loadWriterCheckpoint();
            CrashHook.invoke("AFTER_RECOVERY_CHECKPOINT_READ",
                    checkpoint == null ? -1L : checkpoint.transactionSequence());
            final long initialSequence = checkpoint == null ? this.nextSequence.get() :
                    checkpoint.transactionSequence() + 1;
            this.nextSequence.set(initialSequence);
            final long recordingId = checkpoint != null ? checkpoint.recordingId() : settings().topology().recordingId();
            if (recordingId >= 0) {
                this.validateRecordingBoundary(recordingId, checkpoint);
                try {
                    candidate = (settings().archivePolicy().externalArchive()
                            ? AeronArchiveReplicationPublisher.extendRemote(this.runtime().archive(), recordingId,
                            settings().topology().streamId(), settings().replication(), settings().topology().clusterId(),
                            settings().topology().epoch(), initialSequence, settings().authentication().wireNonce())
                            : AeronArchiveReplicationPublisher.extend(this.runtime().archive(), recordingId,
                            settings().topology().streamId(), settings().replication(), settings().topology().clusterId(),
                            settings().topology().epoch(), initialSequence, settings().authentication().wireNonce()));
                } catch (final RuntimeException failure) {
                    throw reseedRequired("cannot extend configured recording %s after restart; the Archive recording is not safely reusable".formatted(recordingId), failure);
                }
            } else {
                if (checkpoint != null && checkpoint.transactionSequence() >= 0) {
                    throw reseedRequired("writer checkpoint has no recording identity", null);
                }
                candidate = (settings().archivePolicy().externalArchive()
                        ? AeronArchiveReplicationPublisher.createRemote(this.runtime().archive(), settings().topology().channels().live(),
                        settings().topology().streamId(), settings().replication(), settings().topology().clusterId(),
                        settings().topology().epoch(), initialSequence, settings().authentication().wireNonce())
                        : AeronArchiveReplicationPublisher.create(this.runtime().archive(), settings().topology().channels().live(),
                        settings().topology().streamId(), settings().replication(), settings().topology().clusterId(),
                        settings().topology().epoch(), initialSequence, settings().authentication().wireNonce()));
            }
            final long discoveredRecordingId = candidate.recordingId();
            final long recoveredRecordingId = discoveredRecordingId >= 0 ? discoveredRecordingId : recordingId;
            AeronWriterBoundary recoveredBoundary = this.writerBoundary;
            if (checkpoint == null && recoveredRecordingId >= 0) {
                long startPosition = -1L;
                try {
                    startPosition = !this.runtime().isStarted() ? -1L :
                            this.runtime().getStartPosition(recoveredRecordingId);
                } catch (final ArchiveException failure) {
                    if (failure.errorCode() != ArchiveException.UNKNOWN_RECORDING) {
                        throw reseedRequired("cannot inspect new Aeron recording %s".formatted(recoveredRecordingId), failure);
                    }
                    /* A fresh recording may not expose its catalog position until the
                     * first image is connected. Keep the boundary sequence explicit and
                     * leave the position unknown rather than inventing a byte offset. */
                }
                recoveredBoundary = new AeronWriterBoundary(initialSequence - 1,
                        recoveredRecordingId, startPosition);
            }
            CrashHook.invoke("AFTER_RECOVERY_PUBLISHER_CREATED", initialSequence);
            /* Claim the fencing token before the writer becomes visible: no
             * publication path can exist while the publisher still carries the
             * neutral default token, and every subsequent recovery milestone
             * runs against a candidate that is fenced exactly like the
             * published writer would be. */
            candidate.claimFencingToken(this.heldWriterFencingToken());
            this.writerRecordingId.set(recoveredRecordingId);
            this.writerBoundary = recoveredBoundary;
            this.writer = candidate;
            candidate = null;
            this.writerRecoveryState = null;
            this.writerRecoveryFailure = null;
            /* Publish the recovered boundary before the outer method drains
             * deferred reader watermarks. */
            this.writerRecoveryInProgress = false;
            return this.writer;
        } catch (final RuntimeException failure) {
            final RuntimeException classified = writerRecoveryFailure(failure);
            closeFailedWriter(candidate, classified);
            this.writerRecoveryState = classified instanceof ReseedRequiredException
                    ? ReplicationState.RESEED_REQUIRED : ReplicationState.FAILED;
            this.writerRecoveryFailure = classified;
            throw classified;
        } catch (final Error failure) {
            closeFailedWriter(candidate, failure);
            this.writerRecoveryState = ReplicationState.FAILED;
            this.writerRecoveryFailure =
                    new IllegalStateException("Aeron writer recovery failed", failure);
            throw failure;
        } finally {
            this.writerRecoveryInProgress = false;
        }
    }

    /// Acquires the writer fencing lease, failing when another writer holds it.
    ///
    /// The lease carries the monotonically increasing token every envelope,
    /// checkpoint, and cursor publishes; readers reject stale tokens so a
    /// deposed writer cannot interleave history. Acquisition happens before
    /// the writer publication path exists, so a second writer for the same
    /// cluster/generation fails here instead of publishing concurrently.
    private void ensureWriterLease() {
        if (this.writerLease == null) {
            if (this.leaseDirectory == null) {
                throw new IllegalStateException(
                        "a writer requires a shared lease directory (%s) so it can fence against another writer for the same cluster/generation".formatted(
                                NodeSettingsSource.Env.EnvKeys.BACKUP_PATH));
            }
            final Duration staleness = Duration.ofMillis(this.leaseStalenessMillis);
            final WriterFencingLease acquired = WriterFencingLease.acquire(
                    this.leaseDirectory, settings().topology().clusterId(), settings().topology().identity().storeGeneration(),
                    settings().topology().identity().nodeId(), staleness,
                    Duration.ofMillis(settings().timeouts().leaseAcquireLockTimeoutMillis()));
            this.writerLease = acquired;
            /* Capture the token once, while the lease is known held. Every
             * later checkpoint and cursor reuses this snapshot instead of a
             * live lookup that would fail once the lease is gone. */
            this.writerFencingTokenSnapshot = acquired.fencingToken();
        }
    }

    /// Returns the fencing token captured at lease acquisition.
    ///
    /// Write paths always run after [ensureWriterLease], so the snapshot is
    /// set; a missing snapshot is a defensive fail-closed, never a live-path
    /// lookup that could observe the lease disappearing mid-write.
    private long heldWriterFencingToken() {
        final long token = this.writerFencingTokenSnapshot;
        if (token <= 0) {
            throw new IllegalStateException("writer publishes without a fencing lease; a restart is required");
        }
        return token;
    }

    /// Returns the captured fencing token for the writer position supplier.
    ///
    /// The position path must report a typed unavailability instead of the
    /// bare lease exception used on internal write paths.
    private long positionWriterFencingToken() {
        final long token = this.writerFencingTokenSnapshot;
        if (token <= 0) {
            throw new ReplicationPositionUnavailableException(
                    "Aeron writer fencing lease is not held; no writer position can be established");
        }
        return token;
    }

    /// Reports whether the held writer lease is still current.
    ///
    /// Write admission deliberately bypasses the lease freshness cache:
    /// commit fencing must observe a takeover immediately, not after the
    /// background-check interval expires.
    private boolean writerFencingTokenValid() {
        final WriterFencingLease lease = this.writerLease;
        return lease != null && lease.isCurrent();
    }

    private WriterLeaseGate writerLeaseGate() {
        return new WriterLeaseGate() {
            @Override
            public boolean isValid() {
                return writerFencingTokenValid();
            }

            @Override
            public long terminalOfferBudgetNanos() {
                final WriterFencingLease lease = writerLease;
                return lease == null ? 1L : lease.terminalOfferBudgetNanos();
            }

            @Override
            public RuntimeException terminalFailure() {
                return runtime().driverFailure();
            }

            @Override
            public long offerUnderOwnership(final WriterLeaseGate.OwnedOffer offer) {
                final WriterFencingLease lease = writerLease;
                if (lease == null) {
                    throw new WriterFencedException(
                            "writer fencing lease lost before commit; restart required");
                }
                return lease.executeUnderOwnership(offer);
            }
        };
    }

    /// Pauses writes and purges Archive segments up to the retention boundary.
    ///
    /// @param boundary reader-acknowledged writable boundary
    /// @return purge result of the paused section
    long purgeWithWritesPaused(final long boundary) {
        final AeronReplicationWriteCoordinator currentCoordinator = this.coordinator;
        final AeronArchiveReplicationPublisher currentWriter = this.writer;
        if (currentCoordinator == null || currentWriter == null) {
            throw new IllegalStateException(
                    "Aeron retention requires the coordinator-backed Store writer");
        }
        final RuntimeException terminalFailure = this.runtime().driverFailure();
        if (terminalFailure != null) {
            throw new ReplicationUnavailableException(
                    "Aeron MediaDriver is unavailable; retention is closed", terminalFailure);
        }
        return currentCoordinator.withWritesPaused(
                () -> currentWriter.purgeSegmentsWhileWritesPaused(boundary));
    }

    private AeronReplicationWriteCoordinator ensureCoordinator() {
        final AeronTransportShared shared = shared();
        final AeronReplicationWriteCoordinator coordinator;
        synchronized (shared) {
            if (this.coordinator == null) {
                final AeronArchiveReplicationPublisher.CheckpointWriter checkpointWriter =
                        new AeronArchiveReplicationPublisher.CheckpointWriter() {
                            @Override
                            public void onState(final AeronReplicationCheckpoint.State state, final long sequence,
                                                final int dataLength, final int dataChunkCount, final int dataCrc32c, final long position) {
                                persistWriterCheckpoint(state, sequence, dataLength, dataChunkCount, dataCrc32c, position);
                            }

                            @Override
                            public void clearInFlightFence() {
                                try {
                                    AtomicFileWriter.delete(inFlightCheckpointPath());
                                } catch (final IOException failure) {
                                    throw new ReseedRequiredException(
                                            "cannot clear in-flight writer checkpoint %s".formatted(inFlightCheckpointPath()),
                                            failure);
                                }
                            }
                        };
                final AeronArchiveReplicationPublisher writer = this.ensureWriterLocked();
                /* ensureWriterLocked claimed the lease token before the writer
                 * was published; the re-claim below is an idempotent assertion
                 * of the same token. Lease validity is checked before Archive
                 * capacity so a fenced writer fails with a distinct
                 * lease-lost error, never a misleading capacity message. */
                writer.claimFencingToken(this.heldWriterFencingToken());
                this.coordinator = writer.newWriteCoordinator(
                        checkpointWriter,
                        requiredBytes -> {
                            final RuntimeException terminalFailure = runtime().driverFailure();
                            if (terminalFailure != null) {
                                throw new ReplicationUnavailableException(
                                        "Aeron MediaDriver is unavailable; writer admission is closed",
                                        terminalFailure);
                            }
                            return shared.capacity().available(requiredBytes);
                        },
                        this.writerLeaseGate());
            }
            coordinator = this.coordinator;
        }
        this.facade.retentionOwner().drainDeferredWatermarks();
        return coordinator;
    }

    private AeronReplicationCheckpoint loadWriterCheckpoint() {
        final AeronReplicationCheckpoint checkpoint = this.readWriterCheckpoint();
        if (checkpoint != null) {
            if (checkpoint.recordingId() >= 0) this.writerRecordingId.set(checkpoint.recordingId());
            this.writerBoundary = new AeronWriterBoundary(
                    checkpoint.transactionSequence(), checkpoint.recordingId(), checkpoint.recordingPosition());
        }
        return checkpoint;
    }

    /// Reads and validates checkpoint state without changing live transport state.
    private AeronReplicationCheckpoint readWriterCheckpoint() {
        final Path inFlightPath = this.inFlightCheckpointPath();
        final AeronReplicationCheckpoint inFlight = this.readOptionalCheckpoint(inFlightPath);
        if (inFlight != null) {
            final AeronReplicationCheckpoint terminal = this.readOptionalCheckpoint(settings().topology().directories().checkpointPath());
            if (terminal == null) {
                throw reseedRequired("in-flight writer transaction has no terminal checkpoint: %s".formatted(inFlightPath), null);
            }
            this.validateWriterCheckpointIdentity(inFlight);
            this.validateWriterCheckpointIdentity(terminal);
            if ((terminal.state() == AeronReplicationCheckpoint.State.COMMITTED ||
                 terminal.state() == AeronReplicationCheckpoint.State.REJECTED) &&
                terminal.transactionSequence() >= inFlight.transactionSequence()) {
                if (terminal.transactionSequence() == inFlight.transactionSequence() &&
                    (terminal.dataLength() != inFlight.dataLength() ||
                     terminal.dataChunkCount() != inFlight.dataChunkCount() ||
                     terminal.resolutionCrc32c() != inFlight.resolutionCrc32c() ||
                     terminal.recordingId() != inFlight.recordingId())) {
                    throw reseedRequired("terminal checkpoint does not match the in-flight fence", null);
                }
                try {
                    AtomicFileWriter.delete(inFlightPath);
                } catch (final IOException failure) {
                    throw reseedRequired("cannot clear covered in-flight writer checkpoint", failure);
                }
            } else {
                throw reseedRequired("in-flight writer transaction is not covered by a terminal checkpoint %s".formatted(inFlightPath), null);
            }
        }
        final AeronReplicationCheckpoint checkpoint = this.readOptionalCheckpoint(settings().topology().directories().checkpointPath());
        if (checkpoint == null) return null;
        this.validateWriterCheckpointIdentity(checkpoint);
        if (checkpoint.state() != AeronReplicationCheckpoint.State.COMMITTED &&
            checkpoint.state() != AeronReplicationCheckpoint.State.REJECTED) {
            throw new ReseedRequiredException(
                    "writer checkpoint is not restartable (state=%s, sequence=%s, path=%s)".formatted(checkpoint.state(), checkpoint.transactionSequence(), settings().topology().directories().checkpointPath()));
        }
        return checkpoint;
    }

    private AeronReplicationCheckpoint readOptionalCheckpoint(final Path path) {
        try {
            return AeronReplicationCheckpointStore.read(path);
        } catch (final NoSuchFileException absent) {
            return null;
        } catch (final IOException failure) {
            throw reseedRequired("cannot read Aeron writer checkpoint %s".formatted(path), failure);
        }
    }

    private void validateWriterCheckpointIdentity(final AeronReplicationCheckpoint checkpoint) {
        if (checkpoint.recordType() != AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT ||
            !checkpoint.clusterId().equals(settings().topology().clusterId()) ||
            !checkpoint.nodeId().equals(settings().topology().identity().nodeId()) ||
            !checkpoint.storeGeneration().equals(settings().topology().identity().storeGeneration()) ||
            checkpoint.writerEpoch() != settings().topology().epoch()) {
            throw reseedRequired("writer checkpoint identity does not match Aeron configuration", null);
        }
        if (settings().topology().recordingId() >= 0 && checkpoint.recordingId() >= 0 &&
            settings().topology().recordingId() != checkpoint.recordingId()) {
            throw reseedRequired("configured recording does not match writer checkpoint", null);
        }
        final long heldToken = this.heldWriterFencingToken();
        if (checkpoint.fencingToken() <= 0L || checkpoint.fencingToken() > heldToken) {
            throw reseedRequired(
                    "writer checkpoint fencing token %s is not valid for held token %s"
                            .formatted(checkpoint.fencingToken(), heldToken), null);
        }
    }

    /// Verifies the Archive stop-position boundary against the durable writer
    /// checkpoint. Extending beyond the last persisted terminal checkpoint can
    /// reuse a sequence already present in the Archive, so this scalar fence
    /// fails closed until explicit tail replay/truncation is implemented. It
    /// does not scan envelope frames or prove that an orphan tail is replayable.
    private void validateRecordingBoundary(final long recordingId,
                                           final AeronReplicationCheckpoint checkpoint) {
        if (checkpoint == null) {
            this.validateEmptyRecordingBoundary(recordingId);
            return;
        }
        if (checkpoint.recordingPosition() < 0) {
            this.validateEmptyRecordingBoundary(recordingId);
            return;
        }
        final long stopPosition;
        try {
            stopPosition = this.runtime().getStopPosition(recordingId);
        } catch (final RuntimeException failure) {
            throw reseedRequired("cannot inspect recording tail %s".formatted(recordingId), failure);
        }
        new AeronWriterBoundary(checkpoint.transactionSequence(), recordingId,
                checkpoint.recordingPosition()).validateArchiveStop(stopPosition);
    }

    private void validateEmptyRecordingBoundary(final long recordingId) {
        /* A recording without a terminal checkpoint is safe only when it is
         * genuinely empty; otherwise the next writer could reuse an orphaned
         * sequence. */
        try {
            final long start = this.runtime().getStartPosition(recordingId);
            final long stop = this.runtime().getStopPosition(recordingId);
            if (stop < 0 || start < 0 || stop > start) {
                throw reseedRequired("recording has data but no terminal writer checkpoint", null);
            }
        } catch (final ReseedRequiredException failure) {
            throw failure;
        } catch (final RuntimeException failure) {
            throw reseedRequired("cannot inspect empty recording boundary %s".formatted(recordingId), failure);
        }
    }

    private void persistWriterCheckpoint(final AeronReplicationCheckpoint.State state,
                                         final long sequence, final int dataLength, final int dataChunkCount,
                                         final int dataCrc32c, final long position) {
        if (state == AeronReplicationCheckpoint.State.PREPARING ||
            state == AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN) {
            this.writeCheckpoint(this.inFlightCheckpointPath(), state, sequence, dataLength,
                    dataChunkCount, dataCrc32c, position);
            return;
        }
        if (state != AeronReplicationCheckpoint.State.COMMITTED &&
            state != AeronReplicationCheckpoint.State.REJECTED) {
            return;
        }
        this.writeCheckpoint(settings().topology().directories().checkpointPath(), state, sequence, dataLength,
                dataChunkCount, dataCrc32c, position);
        CrashHook.invoke("AFTER_CHECKPOINT_WRITE_BEFORE_COMMITTED_SEQUENCE_UPDATE", sequence);
        /* The terminal checkpoint is the durable boundary. Publish it to
         * readers before best-effort cleanup of the diagnostic fence so a
         * cleanup failure cannot make a durable commit look unavailable. */
        this.writerBoundary = new AeronWriterBoundary(sequence, this.writerRecordingId.get(), position);
        try {
            AtomicFileWriter.delete(this.inFlightCheckpointPath());
        } catch (final IOException failure) {
            /* Self-healing cleanup: restart deletes a covered in-flight fence
             * before resuming, so a transient deletion failure must not make a
             * durable commit look unavailable. */
            System.getLogger(AeronWriterTransport.class.getName()).log(
                    java.lang.System.Logger.Level.WARNING,
                    "cannot clear in-flight writer checkpoint %s; restart recovery will retry".formatted(
                            this.inFlightCheckpointPath()), failure);
        }
    }

    private Path inFlightCheckpointPath() {
        final Path path = settings().topology().directories().checkpointPath();
        return path.resolveSibling("%s.inflight".formatted(path.getFileName()));
    }

    private void writeCheckpoint(final Path path, final AeronReplicationCheckpoint.State state,
                                 final long sequence, final int dataLength, final int dataChunkCount,
                                 final int dataCrc32c, final long position) {
        this.writeCheckpoint(path, this.newWriterCheckpoint(
                state, sequence, dataLength, dataChunkCount, dataCrc32c, position));
    }

    private void writeCheckpoint(final Path path, final AeronReplicationCheckpoint checkpoint) {
        ScopedValue.where(CHECKPOINT_SEQUENCE, checkpoint.transactionSequence()).run(() ->
        {
            try {
                AeronReplicationCheckpointStore.write(path, checkpoint);
            } catch (final IOException failure) {
                throw new ReseedRequiredException(
                        "cannot persist writer checkpoint %s; restart must fail closed".formatted(path), failure);
            }
        });
    }

    private AeronReplicationCheckpoint newWriterCheckpoint(
            final AeronReplicationCheckpoint.State state, final long sequence,
            final int dataLength, final int dataChunkCount, final int dataCrc32c,
            final long position) {
        final long writerRecordingId = this.writerRecordingId.get();
        final long recordingId = writerRecordingId >= 0 ? writerRecordingId : settings().topology().recordingId();
        return new AeronReplicationCheckpoint(
                AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT,
                state, settings().topology().clusterId(), settings().topology().identity().nodeId(), settings().topology().identity().storeGeneration(),
                recordingId, settings().topology().epoch(), this.heldWriterFencingToken(), sequence, position,
                dataLength, dataChunkCount, dataCrc32c);
    }

    /// Closes the write coordinator as an ordered transport close stage.
    void closeCoordinatorStage() {
        this.coordinator.dispose();
        this.coordinator = null;
    }

    /// Closes the writer publication as an ordered transport close stage.
    void closeWriterStage() {
        try {
            this.writer.close();
            this.writer = null;
        } catch (final Throwable writerFailure) {
            /* A released publication is not proof that the Archive
             * recording stopped. Keep the wrapper until isClosed()
             * confirms the stop postcondition; otherwise a transient
             * external-Archive failure would abandon an active
             * recording and make a retry impossible. If the Archive
             * connection is definitively gone, however, retaining the
             * wrapper also retains the local driver forever and
             * prevents the failed process from exiting. The
             * checkpoint remains fail-closed, so release the local
             * runtime in that terminal case. */
            if (this.writer.isClosed() || this.runtime().driverFailure() != null ||
                (settings().archivePolicy().externalArchive() && AeronArchiveFailures.unavailable(writerFailure))) {
                this.writer = null;
            }
            throw writerFailure;
        }
    }

    /// Releases the writer fencing lease as the final ordered close stage.
    void closeLeaseStage() {
        final WriterFencingLease lease;
        synchronized (shared()) {
            lease = this.writerLease;
            this.writerLease = null;
        }
        if (lease != null) lease.close();
    }
}
