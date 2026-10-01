package peruncs.cluster.node.aeron;

import io.aeron.archive.client.ArchiveException;
import io.aeron.exceptions.AeronException;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.serializer.persistence.types.Storer;
import org.eclipse.serializer.persistence.types.PersistenceTypeDictionaryAssembler;
import org.eclipse.serializer.reference.Swizzling;
import org.eclipse.store.storage.types.StorageConnection;
import peruncs.cluster.api.ClusterStorageManager;
import peruncs.cluster.api.ReplicationState;
import peruncs.cluster.errors.NodeException;
import peruncs.cluster.errors.ReplicationPendingException;
import peruncs.cluster.errors.ReplicationUnavailableException;
import peruncs.cluster.errors.ReseedRequiredException;
import peruncs.cluster.errors.ReplicationPositionUnavailableException;
import peruncs.cluster.node.replication.ReplicationPositionProvider;
import peruncs.cluster.node.store.RejectingPersistenceTarget;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;
import peruncs.cluster.storage.aeron.writer.AeronArchiveReplicationPublisher;
import peruncs.cluster.storage.aeron.writer.AeronReplicationWriteCoordinator;
import peruncs.cluster.storage.aeron.writer.AeronStorageBinaryReplicationTarget;
import peruncs.cluster.storage.binary.TypeDictionaryOutbox;
import peruncs.cluster.storage.index.ClusterStoreIndexes;
import peruncs.cluster.storage.io.FaultInjection;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.function.UnaryOperator;

import static peruncs.cluster.node.aeron.AeronTransportShared.reseedRequired;

/// Owns the writer side of one transport: the Archive publication, write
/// coordinator, and recovered Archive boundary.
///
/// The writer publication is installed only after tail recovery has
/// fully succeeded, so a partially recovered writer is never visible to the
/// position provider or health. A recovery failure is classified as typed
/// reseed/unavailable failures and remembered as a terminal state, so later
/// health probes keep reporting it instead of restarting silently. Compound
/// writer steps synchronize on the shared transport monitor; the position
/// boundary itself is published as one immutable snapshot.
final class AeronWriterTransport {
    private final AeronTransport facade;
    private final int indexValidationMaxObjects;
    /* The next Store-mark token is fixed before recovery starts. */
    private volatile long writerFencingTokenSnapshot = -1L;
    /// Recording identity selected at writer startup; retained when RecordingPos is briefly unavailable.
    private final AtomicLong writerRecordingId = new AtomicLong();
    private volatile AeronArchiveReplicationPublisher writer;
    private volatile AeronReplicationWriteCoordinator coordinator;
    /* Readers must never combine fields from two terminal markers. */
    private volatile AeronWriterRecoveryBoundary writerBoundary = new AeronWriterRecoveryBoundary(-1, -1, -1);
    /* A recovered ABORT may put the Archive boundary one sequence beyond the
     * Store mark until the startup fencing commit advances the mark. */
    private volatile long recoveredAbortSequence = -1L;
    private volatile boolean writerRecoveryInProgress;
    /* Consecutive transient recovery failures; guarded by the transport monitor. */
    private int consecutiveTransientFailures;
    private volatile ReplicationState writerRecoveryState;
    /* The classified failure behind writerRecoveryState. A later ensureWriter
     * rethrows it instead of retrying recovery: a RESEED_REQUIRED or terminal
     * FAILED outcome is sticky by contract, and the operator restart is the
     * only retry. */
    private volatile RuntimeException writerRecoveryFailure;
    private ReplicationPositionProvider positionProvider;

    AeronWriterTransport(final AeronTransport facade, final int indexValidationMaxObjects) {
        this.facade = facade;
        this.indexValidationMaxObjects = indexValidationMaxObjects;
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
            return new ReplicationUnavailableException("Aeron Archive writer recovery failed", archive);
        }
        if (failure instanceof AeronException) {
            return new ReplicationUnavailableException("Aeron writer recovery failed", failure);
        }
        return failure;
    }

    /// Separates a transient Archive outage from proof that the recording cannot serve this Store.
    ///
    /// Only a missing recording, an incompatible recording or corrupt history requires a reseed;
    /// a timeout or an unreachable Archive leaves the recording and the Store mark intact, so the
    /// failure is reported as unavailable and the next attempt may recover. Any other failure is a
    /// defect, not evidence about the recording: it is returned unchanged and latches `FAILED`
    /// instead of sending operators to reseed an intact recording.
    ///
    /// @param context what the writer was doing
    /// @param failure the failure thrown by the Archive or the recovery scan
    /// @return the typed failure to throw
    static RuntimeException classifyArchiveFailure(final String context, final RuntimeException failure) {
        if (failure instanceof NodeException) {
            return failure;
        }
        if (failure instanceof ArchiveException archive) {
            return archive.errorCode() == ArchiveException.UNKNOWN_RECORDING
                    ? reseedRequired(context + ": the recording is missing", archive)
                    : new ReplicationUnavailableException(context, archive);
        }
        if (failure instanceof IllegalArgumentException) {
            /* The recording does not match the configured stream or framing: proof it is unusable. */
            return reseedRequired(context, failure);
        }
        if (failure instanceof AeronException || failure instanceof IllegalStateException) {
            return new ReplicationUnavailableException(context, failure);
        }
        return failure;
    }

    /// Clears a failed writer candidate, reporting close noise as suppressed.
    ///
    /// @return `true` when the candidate was closed cleanly, so its recording is not left active
    private static boolean closeFailedWriter(final AeronArchiveReplicationPublisher writer,
                                             final Throwable failure) {
        if (writer == null) return true;
        try {
            writer.close();
            return true;
        } catch (final Throwable cleanupFailure) {
            if (cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
            return false;
        }
    }

    /// Builds the write-path persistence target for the single stream.
    ///
    /// @param outbox              receives the type dictionaries exported by Store commits
    /// @param distributionEnabled whether writes are currently replicated
    /// @param writerStorage       writer-side storage connection supplier
    /// @return persistence-target decorator
    UnaryOperator<PersistenceTarget<Binary>> persistenceTargetFactory(
            final TypeDictionaryOutbox outbox,
            final BooleanSupplier distributionEnabled,
            final Supplier<StorageConnection> writerStorage) {
        final AeronTransportShared shared = shared();
        synchronized (shared) {
            shared.ensureOpen();
            if (!settings().topology().role().isWriter()) {
                /* Reader roles must reproduce the writer's history through
                 * the replication import path, which bypasses this target.
                 * Any locally originated write is rejected instead of
                 * persisting an unreplicated divergence. */
                return RejectingPersistenceTarget::create;
            }
        }
        final boolean hasWriterStorage = writerStorage != null;
        final Runnable writerIndexValidation = !hasWriterStorage
                ? () -> {
                }
                : ClusterStoreIndexes.writerValidator(
                        writerStorage, this.facade.typeHandlers(), this.indexValidationMaxObjects);
        final LongSupplier replicationMarkObjectId = !hasWriterStorage ? null : () -> {
            final StorageConnection storage = writerStorage.get();
            return storage == null ? -1L : storage.persistenceManager().objectRegistry()
                    .lookupObjectId(this.facade.replicationMarkForWriter());
        };
        final ToIntFunction<Binary> commitScan;
        if (!hasWriterStorage) {
            /* Without Store access every commit is validated and accepted as a bootstrap write. */
            commitScan = binary -> ClusterStoreIndexes.COMMIT_HAS_REPLICATION_MARK |
                                   ClusterStoreIndexes.COMMIT_TOUCHES_INDEXES | ClusterStoreIndexes.COMMIT_BOOTSTRAP;
        } else {
            final ToIntFunction<Binary> scan =
                    ClusterStoreIndexes.writerCommitScan(writerStorage, replicationMarkObjectId);
            final AtomicLong dictionaryDefinitionCount = new AtomicLong(-1L);
            final PersistenceTypeDictionaryAssembler assembler = PersistenceTypeDictionaryAssembler.New();
            commitScan = binary -> {
                final int result = scan.applyAsInt(binary);
                final StorageConnection storage = writerStorage.get();
                if (storage != null && distributionEnabled.getAsBoolean()) {
                    final var typeDictionary = storage.persistenceManager().typeDictionary();
                    final long currentDefinitionCount = typeDictionary.allTypeDefinitions().size();
                    /* Store can register an internal handler during commit assembly
                     * without exporting it through the configured dictionary hook.
                     * Type ids are append-only, so a count change needs one snapshot. */
                    if (dictionaryDefinitionCount.get() != currentDefinitionCount) {
                        outbox.stageIncremental(assembler.assemble(typeDictionary));
                        dictionaryDefinitionCount.set(currentDefinitionCount);
                    }
                }
                return result;
            };
        }
        return delegate -> new AeronStorageBinaryReplicationTarget(delegate, this::ensureCoordinator,
                new AeronStorageBinaryReplicationTarget.TargetCallbacks(
                        outbox,
                        sequence -> {
                        },
                        distributionEnabled,
                        writerIndexValidation,
                        commitScan));
    }

    /// Returns the lazily created position provider.
    /// @return writer position provider
    ReplicationPositionProvider positionProvider() {
        final AeronTransportShared shared = shared();
        synchronized (shared) {
            shared.ensureOpen();
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
        final AeronReplicationWriteCoordinator current = this.coordinator;
        return settings().topology().role().isWriter() && !this.writerRecoveryInProgress &&
               this.writerRecoveryState == null && this.writer != null && !this.writer.isFailed() &&
               (current == null || current.failure() == null) && this.runtime().driverFailure() == null;
    }

    void retryPendingCommit() {
        final AeronReplicationWriteCoordinator current = this.coordinator;
        if (current != null) current.retryPendingCommit();
    }

    Duration pendingCommitRetryInterval() {
        return Duration.ofNanos(Math.max(1_000_000L, this.settings().replication().offerTimeoutNanos()));
    }

    private RuntimeException pendingCommitFailure() {
        final AeronReplicationWriteCoordinator current = this.coordinator;
        return current == null ? null : current.failure();
    }

    /// Reports current writer readiness without starting the runtime. Lifecycle
    /// startup belongs to an explicit client or write operation, not a health
    /// probe.
    ReplicationState writerState() {
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
        final AeronReplicationWriteCoordinator writeCoordinator = this.coordinator;
        if (writeCoordinator != null) {
            final RuntimeException coordinatorFailure = writeCoordinator.failure();
            if (coordinatorFailure instanceof ReplicationPendingException) {
                return ReplicationState.REPLICATION_SUSPENDED;
            }
            if (coordinatorFailure != null) return ReplicationState.FAILED;
        }
        /* A live writer can fail closed after an offer or Archive wait
         * error without being discarded immediately.  Do not advertise LIVE
         * while that publisher can no longer accept a durable transaction. */
        return current.isFailed() ? ReplicationState.FAILED : null;
    }

    /// Returns the last published writer boundary snapshot.
    ///
    /// @return immutable writer boundary
    AeronWriterRecoveryBoundary writerBoundary() {
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

    void prepareReplicationCommit(final ReplicationMark mark) {
        final AeronReplicationWriteCoordinator currentCoordinator = this.ensureCoordinator();
        currentCoordinator.reserveStoreCommit(mark, this.writer);
    }

    void cancelReplicationCommit(final ReplicationMark mark) {
        final AeronReplicationWriteCoordinator current = this.coordinator;
        if (current != null) current.cancelStoreCommit(mark);
    }

    void ensureStoreMark(final ReplicationMark mark, final StorageConnection storage) {
        Objects.requireNonNull(mark, "mark");
        Objects.requireNonNull(storage, "storage");
        this.validateMarkIdentity(mark);
        final var registry = storage.persistenceManager().objectRegistry();
        final long markObjectId = registry.lookupObjectId(mark);
        if (mark.sequence() >= 0L) {
            if (Swizzling.isNotFoundId(markObjectId)) {
                throw reseedRequired("replication mark has no Store object id", null);
            }
            this.ensureWriter();
            this.validateLoadedMark(mark);
            if (mark.fencingToken() < this.heldWriterFencingToken()) {
                this.commitMark(mark, storage);
                this.recoveredAbortSequence = -1L;
                this.validateLoadedMark(mark);
            }
            return;
        }
        final boolean recoveredInitialAbort = this.writerBoundary.sequence() == 0L &&
                this.recoveredAbortSequence == 0L;
        if (this.writerBoundary.sequence() >= 0L && !recoveredInitialAbort) {
            throw reseedRequired("writer Store has committed data but no replication mark", null);
        }

        AeronArchiveReplicationPublisher current = this.writer;
        if (current == null) {
            this.ensureWriter();
            current = this.writer;
        }
        final long recordingId = current.recordingId();
        if (recordingId < 0L) {
            throw reseedRequired("empty writer recording has no identity", null);
        }
        final long startPosition;
        try {
            startPosition = this.runtime().getStartPosition(recordingId);
        } catch (final RuntimeException failure) {
            throw reseedRequired("cannot inspect the empty writer recording", failure);
        }
        if (recordingId < 0L || startPosition < 0L ||
            (!recoveredInitialAbort && current.currentPosition() != startPosition)) {
            throw reseedRequired("writer Store has no mark and the recording is not empty", null);
        }

        this.commitMark(mark, storage);
        if (mark.sequence() < 0L || Swizzling.isNotFoundId(registry.lookupObjectId(mark))) {
            throw reseedRequired("writer failed to persist its initial replication mark", null);
        }
        this.recoveredAbortSequence = -1L;
        this.validateLoadedMark(mark);
    }

    private void commitMark(final ReplicationMark mark, final StorageConnection storage) {
        final Storer storer = storage.createStorer();
        if (storage instanceof ClusterStorageManager<?>) {
            storer.store(mark);
            storer.commit();
            return;
        }
        this.prepareReplicationCommit(mark);
        try {
            storer.store(mark);
            storer.commit();
        } finally {
            this.cancelReplicationCommit(mark);
        }
    }

    private void validateMarkIdentity(final ReplicationMark mark) {
        final var topology = settings().topology();
        if (!topology.clusterId().equals(mark.clusterId()) ||
            !topology.identity().storeGeneration().equals(mark.storeGeneration()) ||
            topology.epoch() != mark.epoch() || mark.sequence() < -1L || mark.sequence() == Long.MAX_VALUE ||
            mark.recordingId() < -1L || mark.fencingToken() < 0L ||
            mark.sequence() >= 0L && (mark.recordingId() < 0L || mark.fencingToken() <= 0L ||
                                    mark.prepareStartPosition() < 0L)) {
            throw reseedRequired("replication mark identity does not match writer configuration", null);
        }
    }

    private void validateLoadedMark(final ReplicationMark mark) {
        final AeronWriterRecoveryBoundary boundary = this.writerBoundary;
        final boolean resolvedAbortAfterMark = mark.sequence() < Long.MAX_VALUE &&
                mark.sequence() + 1L == this.recoveredAbortSequence &&
                this.recoveredAbortSequence == boundary.sequence();
        if ((mark.sequence() != boundary.sequence() && !resolvedAbortAfterMark) ||
            mark.recordingId() != this.writerRecordingId.get() ||
            mark.fencingToken() <= 0L || mark.fencingToken() > this.heldWriterFencingToken() ||
            mark.prepareStartPosition() < 0L || mark.prepareStartPosition() > boundary.position()) {
            throw reseedRequired("replication mark does not match the recovered writer boundary", null);
        }
    }

    /// Reports whether a write coordinator is installed.
    ///
    /// @return `true` while a coordinator exists
    boolean hasCoordinator() {
        return this.coordinator != null;
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
        this.writerRecoveryInProgress = true;
        AeronArchiveReplicationPublisher candidate = null;
        try {
            final ReplicationMark mark = Objects.requireNonNull(
                    this.facade.replicationMarkForWriter(), "writer replication mark");
            this.validateMarkIdentity(mark);
            this.writerFencingTokenSnapshot = nextFencingToken(mark);
            this.runtime().ensure();
            long recordingId = mark.recordingId() >= 0L
                    ? mark.recordingId() : settings().topology().recordingId();
            if (recordingId < 0L) {
                recordingId = AeronArchiveReplicationPublisher.latestRecordingId(
                        this.runtime().archive(), settings().topology().streamId());
            }
            final AeronWriterTailRecovery.Result recovery = recordingId >= 0L
                    ? this.inspectWriterTail(mark, recordingId) : null;
            final long initialSequence = recovery == null ? Math.max(0L, mark.sequence() + 1L)
                    : recovery.nextSequence();
            if (recordingId >= 0L) {
                try {
                    candidate = AeronArchiveReplicationPublisher.extend(this.runtime().archive(), recordingId, settings().topology().streamId(), new AeronArchiveReplicationPublisher.PublisherSetup(settings().replication(), settings().topology().clusterId(), settings().topology().epoch(), settings().wireNonce()), initialSequence);
                } catch (final RuntimeException failure) {
                    throw classifyArchiveFailure(
                            "cannot extend Archive recording %s after writer recovery".formatted(recordingId), failure);
                }
            } else {
                if (mark.sequence() >= 0L) {
                    throw reseedRequired("committed Store mark has no Archive recording identity", null);
                }
                candidate = AeronArchiveReplicationPublisher.create(this.runtime().archive(), settings().topology().channels().live(), settings().topology().streamId(), new AeronArchiveReplicationPublisher.PublisherSetup(settings().replication(), settings().topology().clusterId(), settings().topology().epoch(), settings().wireNonce()), initialSequence);
            }
            final long discoveredRecordingId = candidate.recordingId();
            final long recoveredRecordingId = discoveredRecordingId >= 0 ? discoveredRecordingId : recordingId;
            candidate.claimFencingToken(this.heldWriterFencingToken());
            AeronWriterRecoveryBoundary recoveredBoundary;
            if (recovery != null) {
                final long markToken = mark.sequence() >= 0L ? mark.fencingToken() : this.heldWriterFencingToken();
                long boundaryPosition = recovery.boundaryPosition();
                if (recovery.markedCommit() != null) {
                    boundaryPosition = candidate.appendRecoveryMarker(
                            recovery.markedCommit().sequence(), recovery.markedCommit().kind(),
                            recovery.markedCommit().payloadLength(), recovery.markedCommit().dataChunkCount(),
                            recovery.markedCommit().dataCrc32c(), markToken);
                }
                if (recovery.nextAbort() != null) {
                    boundaryPosition = candidate.appendRecoveryMarker(
                            recovery.nextAbort().sequence(), recovery.nextAbort().kind(),
                            recovery.nextAbort().payloadLength(), recovery.nextAbort().dataChunkCount(),
                            recovery.nextAbort().dataCrc32c(), markToken);
                }
                if (boundaryPosition < 0L) {
                    throw reseedRequired("writer tail recovery did not establish a terminal boundary", null);
                }
                recoveredBoundary = new AeronWriterRecoveryBoundary(
                        recovery.boundarySequence(), recoveredRecordingId, boundaryPosition);
            } else {
                long startPosition = -1L;
                try {
                    startPosition = !this.runtime().isStarted() ? -1L :
                            this.runtime().getStartPosition(recoveredRecordingId);
                } catch (final ArchiveException failure) {
                    if (failure.errorCode() != ArchiveException.UNKNOWN_RECORDING) {
                        throw classifyArchiveFailure(
                                "cannot inspect new Aeron recording %s".formatted(recoveredRecordingId), failure);
                    }
                    /* A fresh recording may not expose its catalog position until the
                     * first image is connected. Keep the boundary sequence explicit and
                     * leave the position unknown rather than inventing a byte offset. */
                }
                recoveredBoundary = new AeronWriterRecoveryBoundary(initialSequence - 1,
                        recoveredRecordingId, startPosition);
            }
            FaultInjection.invoke(FaultInjection.Point.AFTER_RECOVERY_PUBLISHER_CREATED, initialSequence);
            /* Recovery offers carry the Store-mark token claimed before the
             * writer becomes visible. */
            this.writerRecordingId.set(recoveredRecordingId);
            this.writerBoundary = recoveredBoundary;
            this.recoveredAbortSequence = recovery != null && mark.sequence() < Long.MAX_VALUE &&
                    recovery.boundarySequence() == mark.sequence() + 1L ? recovery.boundarySequence() : -1L;
            this.writer = candidate;
            candidate = null;
            this.writerRecoveryState = null;
            this.writerRecoveryFailure = null;
            this.consecutiveTransientFailures = 0;
            /* Publish the recovered boundary before the outer method drains
             * deferred reader watermarks. */
            this.writerRecoveryInProgress = false;
            return this.writer;
        } catch (final RuntimeException failure) {
            final RuntimeException classified = writerRecoveryFailure(failure);
            final boolean cleanClose = closeFailedWriter(candidate, classified);
            /* A transient outage is not latched: the next call recovers from scratch, which is
             * safe because the tail scan is read-only and a recovery marker is only appended
             * after the scan. It is retried only a configured number of consecutive times, so a
             * failure that is not really transient cannot rescan on every write forever. A failed
             * close may leave the recording active, so it latches at once. */
            final boolean transientOutage = classified instanceof ReplicationUnavailableException && cleanClose &&
                    ++this.consecutiveTransientFailures < settings().timeouts().writerRecoveryAttempts();
            if (!transientOutage) {
                if (classified instanceof ReplicationUnavailableException) {
                    System.getLogger(AeronWriterTransport.class.getName()).log(System.Logger.Level.WARNING,
                            "Writer recovery failed %d consecutive time(s); latching FAILED"
                                    .formatted(this.consecutiveTransientFailures), classified);
                }
                this.writerRecoveryState = classified instanceof ReseedRequiredException
                        ? ReplicationState.RESEED_REQUIRED : ReplicationState.FAILED;
                this.writerRecoveryFailure = classified;
            }
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

    private AeronWriterTailRecovery.Result inspectWriterTail(
            final ReplicationMark mark, final long recordingId) {
        final long startPosition;
        final long stopPosition;
        try {
            startPosition = this.runtime().getStartPosition(recordingId);
            stopPosition = this.runtime().getStopPosition(recordingId);
        } catch (final RuntimeException failure) {
            throw classifyArchiveFailure(
                    "cannot inspect Archive recording %s before writer recovery".formatted(recordingId), failure);
        }
        if (startPosition < 0L || stopPosition < 0L) {
            /* The Archive has not stopped the recording yet (a crashed writer's recording is
             * stopped when the Archive notices); that resolves by itself, so it is a retry. */
            throw new ReplicationUnavailableException("writer Archive recording is not stopped for recovery");
        }
        final long maxToken = this.heldWriterFencingToken();
        try {
            return AeronWriterTailRecovery.inspect(new AeronWriterTailRecovery.Request(
                    this.runtime().archive(), mark, settings().topology().clusterId(),
                    settings().topology().epoch(), settings().wireNonce(), maxToken, recordingId,
                    startPosition, stopPosition, settings().topology().streamId() + 1,
                    settings().replication().recordedPositionTimeoutNanos(),
                    new AeronWriterTailRecovery.Framing(settings().replication().maxTransactionBytes(),
                            settings().replication().chunkSize(), settings().replication().mtuLength(),
                            settings().replication().termLength())));
        } catch (final RuntimeException failure) {
            throw classifyArchiveFailure("writer Archive tail could not be reconciled with its Store mark", failure);
        }
    }

    private static long nextFencingToken(final ReplicationMark mark) {
        if (mark.sequence() < 0L) return 1L;
        try {
            return Math.addExact(mark.fencingToken(), 1L);
        } catch (final ArithmeticException overflow) {
            throw reseedRequired("writer fencing token is exhausted", overflow);
        }
    }

    /// Returns the token selected from the persisted Store mark.
    private long heldWriterFencingToken() {
        final long token = this.writerFencingTokenSnapshot;
        if (token <= 0) {
            throw new IllegalStateException("writer fencing token is unavailable before Store recovery");
        }
        return token;
    }

    /// Position reads report a typed unavailability before Store recovery.
    private long positionWriterFencingToken() {
        final long token = this.writerFencingTokenSnapshot;
        if (token <= 0) {
            throw new ReplicationPositionUnavailableException(
                    "Aeron writer fencing token is unavailable; no writer position can be established");
        }
        return token;
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
                final AeronArchiveReplicationPublisher writer = this.ensureWriterLocked();
                /* Re-claiming the same Store-mark token is an idempotent
                 * assertion before the coordinator becomes visible. */
                writer.claimFencingToken(this.heldWriterFencingToken());
                this.coordinator = writer.newWriteCoordinator(
                        this::recordTerminal,
                        requiredBytes -> {
                            final RuntimeException terminalFailure = runtime().driverFailure();
                            if (terminalFailure != null) {
                                throw new ReplicationUnavailableException(
                                        "Aeron MediaDriver is unavailable; writer admission is closed",
                                        terminalFailure);
                            }
                            return shared.capacity().available(requiredBytes);
                        });
            }
            coordinator = this.coordinator;
        }
        this.facade.retentionOwner().drainDeferredWatermarks();
        return coordinator;
    }

    private void recordTerminal(final long position) {
        final AeronWriterRecoveryBoundary boundary = this.writerBoundary;
        this.writerBoundary = new AeronWriterRecoveryBoundary(
                boundary.sequence() + 1L, boundary.recordingId(), position);
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
            /* A released publication is not proof that the embedded Archive
             * recording stopped. Keep the wrapper until isClosed() confirms
             * the stop postcondition; a failed driver is terminal. */
            if (this.writer.isClosed() || this.runtime().driverFailure() != null) {
                this.writer = null;
            }
            throw writerFailure;
        }
    }

}
