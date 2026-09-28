package peruncs.cluster.storage.aeron.writer;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.exceptions.PersistenceExceptionTransfer;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import peruncs.cluster.errors.WriteRejectedException;
import peruncs.cluster.storage.binary.ReplicationPublisher;

import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import java.util.function.Predicate;

import static org.eclipse.serializer.util.X.notNull;

/// Store target that couples local acceptance to Aeron replication.
///
/// The Archive preparation always happens before the local Store write. A
/// failed terminal step records an uncertain state and stops further writes
/// instead of letting the Store and Archive drift silently.
///
/// The local write and the Aeron preparation run inside one
/// [AeronReplicationWriteCoordinator#prepareWriteAtomically] section; the
/// commit then waits for its Archive acknowledgement with no coordinator lock
/// held, so a slow Archive never blocks health, maintenance, or dispose.
public final class AeronStorageBinaryReplicationTarget implements PersistenceTarget<Binary> {
    /// Provider callbacks associated with one target.
    ///
    /// @param dictionarySource      source of staged type dictionaries, or `null`
    /// @param committedSequence     callback for committed sequence numbers
    /// @param distributionEnabled   whether this target currently replicates writes
    /// @param writerIndexValidation full writer graph check, or `null`
    /// @param commitTouchesIndexes  entity type pre-filter, or `null` to validate every write
    public record TargetCallbacks(
            ReplicationPublisher dictionarySource,
            LongConsumer committedSequence,
            BooleanSupplier distributionEnabled,
            Runnable writerIndexValidation,
            Predicate<Binary> commitTouchesIndexes
    ) {
        public TargetCallbacks {
            committedSequence = notNull(committedSequence);
            distributionEnabled = notNull(distributionEnabled);
        }
    }

    private final PersistenceTarget<Binary> delegate;
    private final AeronReplicationWriteCoordinator coordinator;
    private final ReplicationPublisher dictionarySource;
    private final LongConsumer committedSequence;
    private final BooleanSupplier distributionEnabled;
    private final Runnable writerIndexValidation;
    private final Predicate<Binary> commitTouchesIndexes;

    /// Creates a target with its provider-owned callbacks.
    ///
    /// The provider keeps Store acceptance and checkpoint transitions under
    /// the same writer owner.
    ///
    /// @param delegate    local Store target
    /// @param coordinator Aeron transaction coordinator
    /// @param callbacks   writer callbacks
    public AeronStorageBinaryReplicationTarget(final PersistenceTarget<Binary> delegate,
                                               final AeronReplicationWriteCoordinator coordinator,
                                               final TargetCallbacks callbacks) {
        this.delegate = notNull(delegate);
        this.coordinator = notNull(coordinator);
        final TargetCallbacks checked = notNull(callbacks);
        this.dictionarySource = checked.dictionarySource();
        this.committedSequence = checked.committedSequence();
        this.distributionEnabled = checked.distributionEnabled();
        this.writerIndexValidation = checked.writerIndexValidation();
        this.commitTouchesIndexes = checked.commitTouchesIndexes();
    }

        /// Creates a target with replication enabled for every write and no
    /// dictionary staging.
    ///
    /// @param delegate    local Store target
    /// @param coordinator Aeron transaction coordinator
    /// @return target that replicates every write
    static AeronStorageBinaryReplicationTarget create(final PersistenceTarget<Binary> delegate,
                                                   final AeronReplicationWriteCoordinator coordinator) {
        return new AeronStorageBinaryReplicationTarget(delegate, coordinator,
                new TargetCallbacks(null, ignored -> {
                }, () -> true, null, null));
    }

        /// Creates a target with the provider-owned callbacks but no writer-side
    /// index check.
    ///
    /// @param delegate            local Store target
    /// @param coordinator         Aeron transaction coordinator
    /// @param dictionarySource    source of staged type dictionaries
    /// @param committedSequence   callback for the committed sequence
    /// @param distributionEnabled predicate that enables replication
    /// @return target using the supplied callbacks
    static AeronStorageBinaryReplicationTarget create(final PersistenceTarget<Binary> delegate,
                                                   final AeronReplicationWriteCoordinator coordinator,
                                                   final ReplicationPublisher dictionarySource,
                                                   final LongConsumer committedSequence,
                                                   final BooleanSupplier distributionEnabled) {
        return new AeronStorageBinaryReplicationTarget(delegate, coordinator,
                new TargetCallbacks(dictionarySource, committedSequence, distributionEnabled, null, null));
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
        final AeronReplicationPublisher.PreparedTransaction prepared =
                this.coordinator.prepareWriteAtomically(() -> this.prepareWrite(data));
        if (prepared == null) {
            return;
        }
        /* The slow commit wait runs outside write admission. A failure recorded
         * as uncertain leaves the publisher terminal, so the token close is a
         * no-op; a refusal before any publication still aborts through close. */
        try (prepared) {
            this.coordinator.commitOrMarkUncertain(prepared);
            this.committedSequence.accept(prepared.sequence());
        }
    }

        /// Runs the fast write phase under write admission and returns the
    /// prepared Aeron transaction, or `null` when distribution is disabled.
    ///
    /// The returned token must be committed outside the write lock; the caller
    /// owns its lifetime.
    private AeronReplicationPublisher.PreparedTransaction prepareWrite(final Binary data) {
        if (!this.distributionEnabled.getAsBoolean()) {
            data.iterateChannelChunks(Binary::mark);
            try {
                this.delegate.write(data);
            } finally {
                data.iterateChannelChunks(Binary::reset);
            }
            return null;
        }
        /* Writer-side index enforcement before any publication step: a direct
         * external registration fails here, ahead of the local Store write
         * and the Aeron prepare/commit below. */
        if (this.commitTouchesIndexes == null || this.commitTouchesIndexes.test(data)) {
            try {
                this.validateWriterState();
            } catch (final IllegalArgumentException | IllegalStateException rejected) {
                throw new WriteRejectedException("writer index validation rejected the Store write", rejected);
            }
        }
        if (this.dictionarySource != null) {
            final String dictionary = this.dictionarySource.consumeTypeDictionary();
            if (dictionary != null) {
                /* consumeTypeDictionary() only transfers ownership to the coordinator.
                 * The coordinator deliberately retains the bytes until commit(), so a
                 * local rejection or uncertain publication can retry the same dictionary
                 * even though the source has already cleared its staging slot. */
                this.coordinator.distributeTypeDictionary(dictionary);
            }
        }
        data.iterateChannelChunks(Binary::mark);
        final AeronReplicationPublisher.PreparedTransaction prepared;
        try {
            prepared = this.coordinator.prepare(data);
        } catch (final RuntimeException failure) {
            data.iterateChannelChunks(Binary::reset);
            throw failure;
        }
        boolean handedOff = false;
        try {
            CrashHook.invoke("AFTER_PREPARE_BEFORE_LOCAL_WRITE", prepared.sequence());
            try {
                this.delegate.write(data);
                CrashHook.invoke("AFTER_LOCAL_WRITE_BEFORE_COMMIT", prepared.sequence());
            } catch (final Error failure) {
                /* The local Store may already have accepted the bytes.  Abandon the
                 * token without emitting a contradictory ABORT; the PREPARING fence
                 * remains the fail-closed recovery evidence. */
                prepared.abandonWithoutAbort();
                throw failure;
            } catch (final RuntimeException failure) {
                /* A failing delegate write is not proof of rejection: Store
                 * enqueue-then-wait can surface the exception while the queued
                 * transaction still completes. do NOT publish ABORT —
                 * recording REJECTED would permanently drop acknowledged
                 * bytes. Mark the commit uncertain and abandon the token so
                 * restart recovery fails closed on the in-flight fence (front
                 * and back admission rejection stay safe: only a caught
                 * failure after entering local persistence reaches this). */
                try {
                    this.coordinator.markCommittingUncertain(prepared);
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

        /// Returns whether the local persistence target can accept a write.
    @Override
    public boolean isWritable() {
        return this.delegate.isWritable() && this.coordinator.isWritable();
    }
}
