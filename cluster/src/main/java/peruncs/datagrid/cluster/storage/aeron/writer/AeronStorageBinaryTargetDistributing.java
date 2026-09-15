package peruncs.datagrid.cluster.storage.aeron.writer;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.exceptions.PersistenceExceptionTransfer;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;
import peruncs.datagrid.cluster.storage.types.ReplicationDurabilityMode;

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
public final class AeronStorageBinaryTargetDistributing implements PersistenceTarget<Binary> {
    private final PersistenceTarget<Binary> delegate;
    private final AeronReplicationWriteCoordinator coordinator;
    private final StorageBinaryDataDistributor dictionarySource;
    private final LongConsumer committedSequence;
    private final BooleanSupplier distributionEnabled;

    /**
     * Creates a target with replication enabled for every write.
     *
     * @param delegate    local Store target
     * @param coordinator Aeron transaction coordinator
     */
    public AeronStorageBinaryTargetDistributing(final PersistenceTarget<Binary> delegate,
                                                final AeronReplicationWriteCoordinator coordinator) {
        this(delegate, coordinator, null, ignored -> {
        }, () -> true);
    }

    /**
     * Creates a target with the provider-owned dictionary, sequence and admission
     * callbacks.  The callbacks are deliberately supplied by the provider so that
     * local Store acceptance and the Aeron checkpoint transition remain one owner-
     * serialized operation.
     *
     * @param delegate            local Store target
     * @param coordinator         Aeron transaction coordinator
     * @param dictionarySource    source of staged type dictionaries
     * @param committedSequence   callback for the committed sequence
     * @param distributionEnabled predicate that enables replication
     */
    public AeronStorageBinaryTargetDistributing(final PersistenceTarget<Binary> delegate,
                                                final AeronReplicationWriteCoordinator coordinator, final StorageBinaryDataDistributor dictionarySource,
                                                final LongConsumer committedSequence, final BooleanSupplier distributionEnabled) {
        this.delegate = notNull(delegate);
        this.coordinator = notNull(coordinator);
        this.dictionarySource = dictionarySource;
        this.committedSequence = notNull(committedSequence);
        this.distributionEnabled = notNull(distributionEnabled);
    }

    /** Writes locally and completes the matching Aeron transaction. */
    @Override
    public void write(final Binary data) throws PersistenceExceptionTransfer {
        this.coordinator.executeWriteAtomically(() -> writeInternal(data));
    }

    private void writeInternal(final Binary data) {
        if (!this.distributionEnabled.getAsBoolean()) {
            data.iterateChannelChunks(Binary::mark);
            try {
                this.delegate.write(data);
            } finally {
                data.iterateChannelChunks(Binary::reset);
            }
            return;
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
        if (this.coordinator.durabilityMode() == ReplicationDurabilityMode.ENQUEUE_THEN_ARCHIVE) {
            boolean localAccepted = false;
            try {
                final long localSequence = this.coordinator.markLocalEnqueue(data);
                this.delegate.write(data);
                localAccepted = true;
                CrashHook.invoke("AFTER_ENQUEUE_BEFORE_PREPARE", localSequence);
            } catch (final Error failure) {
                /* Preserve the durable ENQUEUED fence.  Error cleanup can allocate or
                 * perform I/O and is unsafe when the JVM is already fatally failing. */
                throw failure;
            } catch (final RuntimeException failure) {
                try {
                    if (localAccepted) this.coordinator.markEnqueueWithoutArchive();
                    else this.coordinator.clearLocalEnqueue();
                } catch (final RuntimeException clearFailure) {
                    failure.addSuppressed(clearFailure);
                }
                throw failure;
            } finally {
                data.iterateChannelChunks(Binary::reset);
            }
            final AeronReplicationPublisher.PreparedTransaction prepared;
            try {
                prepared = this.coordinator.prepare(data);
            } catch (final Error failure) {
                /* The local Store write completed before preparation failed. Leave the
                 * durable ENQUEUED fence unresolved and avoid allocating a wrapper or
                 * performing checkpoint I/O from a fatal Error path. */
                throw failure;
            } catch (final RuntimeException failure) {
                try {
                    this.coordinator.markEnqueueWithoutArchive();
                } catch (final RuntimeException markerFailure) {
                    failure.addSuppressed(markerFailure);
                }
                throw new IllegalStateException(
                        "local Store write completed but Aeron publication could not prepare; reseed is required", failure);
            }
            try (prepared) {
                this.coordinator.markEnqueued(prepared);
                this.commitAndNotify(prepared);
            }
            return;
        }

        final AeronReplicationPublisher.PreparedTransaction prepared;
        try {
            prepared = this.coordinator.prepare(data);
        } catch (final Error failure) {
            /* Preserve the durable ENQUEUED fence.  Error cleanup can allocate or
             * perform I/O and is unsafe when the JVM is already fatally failing. */
            throw failure;
        } catch (final RuntimeException failure) {
            data.iterateChannelChunks(Binary::reset);
            throw failure;
        }
        try (prepared) {
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
            } finally {
                data.iterateChannelChunks(Binary::reset);
            }
            this.coordinator.markEnqueued(prepared);
            this.commitAndNotify(prepared);
        }
    }

    /** Commits a prepared transaction and preserves the fail-closed uncertainty marker. */
    private void commitAndNotify(final AeronReplicationPublisher.PreparedTransaction prepared) {
        this.coordinator.commitOrMarkUncertain(prepared);
        this.committedSequence.accept(prepared.sequence());
    }

    /** Returns whether the local persistence target can accept a write. */
    @Override
    public boolean isWritable() {
        return this.delegate.isWritable() && this.coordinator.isWritable();
    }
}
