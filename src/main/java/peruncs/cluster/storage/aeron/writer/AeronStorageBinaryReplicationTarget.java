package peruncs.cluster.storage.aeron.writer;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.exceptions.PersistenceExceptionTransfer;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import peruncs.cluster.errors.WriteRejectedException;
import peruncs.cluster.storage.binary.TypeDictionaryOutbox;
import peruncs.cluster.storage.index.ClusterStoreIndexes;
import peruncs.cluster.storage.io.FaultInjection;

import java.util.function.*;


import static org.eclipse.serializer.util.X.notNull;

/// Store target that couples local acceptance to Aeron replication.
///
/// The Archive preparation always happens before the local Store write. A
/// failed terminal step records an uncertain state and stops further writes
/// instead of letting the Store and Archive drift silently.
///
/// The index check, Aeron preparation (offers and the wait for the Archive to
/// record them) and the local Store write run inside one
/// [AeronReplicationWriteCoordinator#prepareWriteAtomically] section. The
/// COMMIT offer then runs outside that section and does not wait for Archive
/// recording, so health, maintenance and dispose are not blocked by it.
public final class AeronStorageBinaryReplicationTarget implements PersistenceTarget<Binary> {
    /// Provider callbacks associated with one target.
    ///
    /// @param dictionarySource      outbox of staged type dictionaries, or `null`
    /// @param distributionEnabled   whether this target currently replicates writes
    /// @param writerIndexValidation full writer graph check, or `null` to accept silently
    /// @param commitScan            one pass over the commit returning `ClusterStoreIndexes.COMMIT_*` flags
    public record TargetCallbacks(
            TypeDictionaryOutbox dictionarySource,
            BooleanSupplier distributionEnabled,
            Runnable writerIndexValidation,
            ToIntFunction<Binary> commitScan
    ) {
        /// Validates the required callbacks.
        public TargetCallbacks {
            distributionEnabled = notNull(distributionEnabled);
            commitScan = notNull(commitScan);
        }
    }

    private final PersistenceTarget<Binary> delegate;
    private final LazyConstant<AeronReplicationWriteCoordinator> coordinator;
    private final TypeDictionaryOutbox dictionarySource;
    private final BooleanSupplier distributionEnabled;
    private final Runnable writerIndexValidation;
    private final ToIntFunction<Binary> commitScan;

    /// Creates a target that opens its writer only on the first Store operation.
    ///
    /// @param delegate           local Store target
    /// @param coordinatorFactory opens the Aeron transaction coordinator on first use
    /// @param callbacks          writer callbacks
    public AeronStorageBinaryReplicationTarget(final PersistenceTarget<Binary> delegate,
                                               final Supplier<AeronReplicationWriteCoordinator> coordinatorFactory,
                                               final TargetCallbacks callbacks) {
        this.delegate = notNull(delegate);
        final Supplier<AeronReplicationWriteCoordinator> factory = notNull(coordinatorFactory);
        this.coordinator = LazyConstant.of(() -> notNull(factory.get()));
        final TargetCallbacks checked = notNull(callbacks);
        this.dictionarySource = checked.dictionarySource();
        this.distributionEnabled = checked.distributionEnabled();
        this.writerIndexValidation = checked.writerIndexValidation();
        this.commitScan = checked.commitScan();
    }

    private AeronReplicationWriteCoordinator coordinator() {
        return this.coordinator.get();
    }

    /// Runs the writer-side index check now.
    ///
    /// Call at writer startup for fail-fast enforcement; the distributed
    /// commit path invokes the same check before every publication. A target
    /// built without a validation hook accepts silently.
    ///
    /// @throws RuntimeException if the writer graph violates the index policy
    void validateWriterState() {
        final Runnable validation = this.writerIndexValidation;
        if (validation != null) validation.run();
    }

    /// Writes locally and completes the matching Aeron transaction.
    @Override
    public void write(final Binary data) throws PersistenceExceptionTransfer {
        final PreparedTransaction prepared =
                this.coordinator().prepareWriteAtomically(() -> this.prepareWrite(data));
        if (prepared == null) {
            return;
        }
        /* The COMMIT offer runs outside write admission and does not wait for
         * Archive recording. On bounded back-pressure, the Store mark and open
        * token remain recovery evidence; close must not emit a contradictory ABORT. */
        try (prepared) {
            this.coordinator().commitAcceptedStore(prepared);
        }
    }

    /// Runs the fast write phase under write admission and returns the
    /// prepared Aeron transaction, or `null` when distribution is disabled.
    ///
    /// The returned token must be committed outside the write lock; the caller
    /// owns its lifetime.
    private PreparedTransaction prepareWrite(final Binary data) {
        final int scan = this.commitScan.applyAsInt(data);
        if (!this.distributionEnabled.getAsBoolean()) {
            if ((scan & ClusterStoreIndexes.COMMIT_BOOTSTRAP) == 0) {
                throw new WriteRejectedException("replication is disabled for guarded Store commits");
            }
            data.iterateChannelChunks(Binary::mark);
            try {
                this.delegate.write(data);
            } finally {
                data.iterateChannelChunks(Binary::reset);
            }
            return null;
        }
        if ((scan & ClusterStoreIndexes.COMMIT_HAS_REPLICATION_MARK) == 0) {
            throw new WriteRejectedException("replicated Store commit does not contain its replication mark");
        }
        /* Writer-side index enforcement before publication: an external
         * registration is rejected before local Store acceptance. */
        if ((scan & ClusterStoreIndexes.COMMIT_TOUCHES_INDEXES) != 0) this.rejectInvalidIndexWrite();
        if (this.dictionarySource != null) {
            final String dictionary = this.dictionarySource.consume();
            if (dictionary != null) {
                /* consume() only transfers ownership to the coordinator.
                 * The coordinator deliberately retains the bytes until commit(), so a
                 * local rejection or uncertain publication can retry the same dictionary
                 * even though the source has already cleared its staging slot. */
                this.coordinator().distributeTypeDictionary(dictionary);
            }
        }
        data.iterateChannelChunks(Binary::mark);
        final PreparedTransaction prepared;
        try {
            prepared = this.coordinator().prepare(data);
        } catch (final RuntimeException failure) {
            data.iterateChannelChunks(Binary::reset);
            throw failure;
        }
        boolean handedOff = false;
        try {
            FaultInjection.invoke(FaultInjection.Point.AFTER_PREPARE_BEFORE_LOCAL_WRITE, prepared.sequence());
            try {
                this.delegate.write(data);
                prepared.markLocallyAccepted();
                FaultInjection.invoke(FaultInjection.Point.AFTER_LOCAL_WRITE_BEFORE_COMMIT, prepared.sequence());
            } catch (final Error failure) {
                /* The local Store may already have accepted the bytes.  Abandon the
                 * token without emitting a contradictory ABORT; the open prepared
                 * transaction remains the fail-closed recovery evidence. */
                prepared.abandonWithoutAbort();
                throw failure;
            } catch (final RuntimeException failure) {
                /* A failing delegate write is not proof of rejection: Store
                 * enqueue-then-wait can surface the exception while the queued
                 * transaction still completes. Do NOT publish ABORT: a
                 * recorded ABORT would permanently drop acknowledged bytes.
                 * Mark the commit uncertain and abandon the token so restart
                 * recovery fails closed on the open transaction (only a caught
                 * failure after entering local persistence reaches this). */
                try {
                    this.coordinator().markStoreOutcomeUncertain(prepared);
                } catch (final RuntimeException maskFailure) {
                    failure.addSuppressed(maskFailure);
                }
                prepared.abandonWithoutAbort();
                throw failure;
            }
            handedOff = true;
            return prepared;
        } finally {
            try {
                data.iterateChannelChunks(Binary::reset);
            } finally {
                /* Any path that created a token but did not return it must detach
                 * it: a later writer must not inherit a prepared transaction it
                 * cannot commit, and the publisher must fail closed rather than
                 * continue. */
                if (!handedOff) prepared.abandonWithoutAbort();
            }
        }
    }

    private void rejectInvalidIndexWrite() {
        try {
            this.validateWriterState();
        } catch (final IllegalArgumentException | IllegalStateException rejected) {
            throw new WriteRejectedException("writer index validation rejected the Store write", rejected);
        }
    }

    /// Returns whether the local persistence target can accept a write.
    @Override
    public boolean isWritable() {
        return this.delegate.isWritable() && this.coordinator().isWritable();
    }
}
