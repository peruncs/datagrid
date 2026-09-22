package peruncs.datagrid.cluster.storage.aeron.writer;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.exceptions.PersistenceExceptionTransfer;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;

import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;

import static org.eclipse.serializer.util.X.notNull;

/// Store target that couples local acceptance to Aeron replication.
///
/// The Archive preparation always happens before the local Store write. A
/// failed terminal step records an uncertain state and stops further writes
/// instead of letting the Store and Archive drift silently.
///
/// The local write and the Aeron preparation run inside one
/// [AeronReplicationWriteCoordinator#executeWriteAtomically] section; the
/// commit then waits for its Archive acknowledgement with no coordinator lock
/// held, so a slow Archive never blocks health, maintenance, or dispose.
public final class AeronStorageBinaryReplicationTarget implements PersistenceTarget<Binary> {
    private final PersistenceTarget<Binary> delegate;
    private final AeronReplicationWriteCoordinator coordinator;
    private final StorageBinaryDataDistributor dictionarySource;
    private final LongConsumer committedSequence;
    private final BooleanSupplier distributionEnabled;
    private final Runnable writerIndexValidation;

        /// Creates a target with the provider-owned dictionary, sequence and
    /// admission callbacks.
    ///
    /// The callbacks are deliberately supplied by the provider so that local
    /// Store acceptance and the Aeron checkpoint transition remain one owner-
    /// serialized operation.
    ///
    /// @param delegate              local Store target
    /// @param coordinator           Aeron transaction coordinator
    /// @param dictionarySource      source of staged type dictionaries
    /// @param committedSequence     callback for the committed sequence
    /// @param distributionEnabled   predicate that enables replication
    /// @param writerIndexValidation writer index check, or `null` to skip
    public AeronStorageBinaryReplicationTarget(final PersistenceTarget<Binary> delegate,
                                               final AeronReplicationWriteCoordinator coordinator,
                                               final StorageBinaryDataDistributor dictionarySource,
                                               final LongConsumer committedSequence,
                                               final BooleanSupplier distributionEnabled,
                                               final Runnable writerIndexValidation) {
        this.delegate = notNull(delegate);
        this.coordinator = notNull(coordinator);
        this.dictionarySource = dictionarySource;
        this.committedSequence = notNull(committedSequence);
        this.distributionEnabled = notNull(distributionEnabled);
        this.writerIndexValidation = writerIndexValidation;
    }

        /// Creates a target with replication enabled for every write and no
    /// dictionary staging.
    ///
    /// @param delegate    local Store target
    /// @param coordinator Aeron transaction coordinator
    /// @return target that replicates every write
    static AeronStorageBinaryReplicationTarget New(final PersistenceTarget<Binary> delegate,
                                                   final AeronReplicationWriteCoordinator coordinator) {
        return new AeronStorageBinaryReplicationTarget(delegate, coordinator, null, ignored -> {
        }, () -> true, null);
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
    static AeronStorageBinaryReplicationTarget New(final PersistenceTarget<Binary> delegate,
                                                   final AeronReplicationWriteCoordinator coordinator,
                                                   final StorageBinaryDataDistributor dictionarySource,
                                                   final LongConsumer committedSequence,
                                                   final BooleanSupplier distributionEnabled) {
        return new AeronStorageBinaryReplicationTarget(delegate, coordinator, dictionarySource,
                committedSequence, distributionEnabled, null);
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
        this.validateWriterState();
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
            boolean localAccepted = false;
            try {
                this.delegate.write(data);
                localAccepted = true;
                CrashHook.invoke("AFTER_LOCAL_WRITE_BEFORE_COMMIT", prepared.sequence());
            } catch (final Error failure) {
                /* The local Store may already have accepted the bytes.  Abandon the
                 * token without emitting a contradictory ABORT; the PREPARING fence
                 * remains the fail-closed recovery evidence. */
                prepared.abandonWithoutAbort();
                throw failure;
            } catch (final RuntimeException failure) {
                try {
                    if (localAccepted) {
                        try {
                            this.coordinator.markCommittingUncertain(prepared);
                        } finally {
                            /* Never let try-with-resources manufacture an ABORT after
                             * local Store acceptance, even when the uncertainty marker
                             * itself cannot be persisted. */
                            prepared.abandonWithoutAbort();
                        }
                    } else this.coordinator.abort(prepared);
                } catch (final RuntimeException abortFailure) {
                    failure.addSuppressed(abortFailure);
                }
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
