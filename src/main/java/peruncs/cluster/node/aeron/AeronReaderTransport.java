package peruncs.cluster.node.aeron;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.archive.client.PersistentSubscription;
import org.eclipse.serializer.persistence.binary.types.Binary;
import peruncs.cluster.node.replication.CommitAppliedListener;
import peruncs.cluster.storage.ReplicationCursor;
import peruncs.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.cluster.storage.aeron.checkpoint.AeronReplicationCheckpointStore;
import peruncs.cluster.storage.aeron.checkpoint.AeronReplicationCursor;
import peruncs.cluster.storage.aeron.reader.AeronArchiveReader;
import peruncs.cluster.storage.aeron.reader.CursorSnapshot;
import peruncs.cluster.storage.aeron.reader.ReaderDeliveryListener;
import peruncs.cluster.storage.binary.ReplicationApplier;
import peruncs.cluster.storage.binary.StorageBinaryDataReceiver;
import peruncs.cluster.storage.io.AtomicFileWriter;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static peruncs.cluster.node.aeron.AeronTransportShared.reseedRequired;

/// Owns the reader side of one transport: the Archive replay client, its
/// replacement slot, and its durable cursor publications.
///
/// Reader replacement and disposal run under the slot's own lifecycle lock,
/// never under the shared transport monitor: a reader dispose can join its
/// polling thread for up to the reader-stop timeout, and health and failure
/// callbacks must keep observing the transport while that join is in flight.
/// Cursor validation is strict — wrong transport, unexpected provider state,
/// generation mismatch, or a sequence without a recording position all fail
/// closed — and a restarted reader re-advertises its watermark immediately so
/// the writer can rebuild its retention quorum without waiting for traffic.
final class AeronReaderTransport {
    private final AeronTransport facade;
    /* Reader replacement and disposal run under this slot's own lifecycle
     * lock, never under the transport monitor: a reader dispose can join
     * its polling thread for up to the reader-stop timeout, and health()/
     * failure callbacks must keep observing the transport while that join
     * is in flight. The slot publishes the replacement reference last. */
    private final CurrentReader<AeronArchiveReader> readers = new CurrentReader<>();
    private final AtomicLong readerRecordingId = new AtomicLong(Aeron.NULL_VALUE);

    AeronReaderTransport(final AeronTransport facade) {
        this.facade = facade;
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

    /// Creates the reader-side client starting at the supplied durable cursor.
    ///
    /// A writer never reads, not even against an external Archive: it gets
    /// a no-op client instead, because a second subscription against its
    /// own recording would break the one-writer topology and leak a
    /// half-initialized reader into the writer's health path. A new call
    /// disposes and replaces any existing reader. The starting cursor is
    /// validated strictly — wrong transport, unexpected provider state on
    /// an uninitialized cursor, generation mismatch, or a sequence without
    /// a recording position all fail fast — and a resolved cursor
    /// re-advertises its watermark immediately so a restarted writer can
    /// rebuild its retention quorum without waiting for new traffic.
    ///
    /// @param receiver destination for received binaries and dictionaries
    /// @param streamName logical stream name
    /// @param cursorListener callback after data is applied
    /// @param startingCursor durable starting cursor
    /// @param commitPosition whether reader positions are committed
    /// @return binary data client
    ReplicationApplier client(
            final StorageBinaryDataReceiver receiver,
            final String streamName,
            final CommitAppliedListener cursorListener,
            final ReplicationCursor startingCursor,
            final boolean commitPosition
    ) {
        final AeronTransportShared shared = shared();
        synchronized (shared) {
            shared.ensureOpen();
            shared.claimStream(streamName);
            /* A writer owns publication only. Even with an external Archive it must not
             * create a second subscription against its own recording; that would violate
             * the one-writer/N-reader topology and expose a partially initialized reader
             * through the writer's health path. Configure a separate reader/backup-reader
             * node when replay is required. */
            if (settings().topology().role().isWriter()) {
                return ReplicationApplier.noOp(startingCursor);
            }
        }
        Objects.requireNonNull(receiver, "receiver");
        this.runtime().ensure();
        final long recordingId = settings().topology().recordingId() >= 0
                ? settings().topology().recordingId() : this.discoverReaderRecordingId();
        this.rejectUncertainReaderImport(recordingId);
        final ReplicationCursor cursor = startingCursor == null
                ? new ReplicationCursor("aeron", null, -1, "") : startingCursor;
        final boolean aeronCursor = "aeron".equalsIgnoreCase(cursor.transport());
        if (!aeronCursor && !"none".equalsIgnoreCase(cursor.transport())) {
            throw new IllegalArgumentException("cursor belongs to transport %s".formatted(cursor.transport()));
        }
        if (!aeronCursor && cursor.hasProviderPosition()) {
            throw new IllegalArgumentException("an uninitialized cursor cannot carry Aeron provider state");
        }
        if (!aeronCursor && cursor.logicalSequence() >= 0) {
            /* Claims prior history without Aeron provider identity: neither
             * a new reader nor a resumable cursor. Fail closed instead of
             * silently restarting from scratch over existing Store files. */
            throw reseedRequired(
                    "non-Aeron cursor claims sequence %s without provider identity; manual reseed is required".formatted(
                            cursor.logicalSequence()),
                    null);
        }
        if (aeronCursor && cursor.storeGeneration() != null &&
            !settings().topology().identity().storeGeneration().equals(cursor.storeGeneration())) {
            throw new IllegalArgumentException("cursor store generation does not match Aeron configuration");
        }
        if (aeronCursor && cursor.logicalSequence() >= 0 && cursor.storeGeneration() == null) {
            throw new IllegalArgumentException("resolved Aeron cursor must identify its Store generation");
        }
        final AtomicReference<AeronArchiveReader> readerRef = new AtomicReference<>();
        /* Decode once and validate the token together with the recording
         * identity, so the seed floor below can never come from a cursor
         * whose identity checks failed. */
        final AeronReplicationCursor validatedCursor = this.validatedAeronCursor(cursor, aeronCursor, recordingId);
        final long cursorPosition = validatedCursor == null ? -1L : validatedCursor.recordingPosition();
        final AeronArchiveReader replacement;
        /* Replacement runs through the reader slot, which takes its own
         * lifecycle lock, disposes the previous reader, creates the
         * replacement, and publishes it last. Health and failure callbacks
         * read the slot without waiting for a dispose. */
        shared.ensureOpen();
        replacement = this.readers.replace(() -> {
            /* Re-check under the slot lock: close() may have started after
             * the lock-free prologue above. */
            shared.ensureOpen();
            final AeronArchiveReader created = AeronArchiveReader.create(
                    AeronArchiveReader.Configuration.builder()
                    .aeron(this.runtime().aeron())
                    .archiveContext(this.runtime().archiveContext())
                    .recordingId(recordingId)
                    .startPosition(aeronCursor && cursorPosition >= 0
                            ? cursorPosition : PersistentSubscription.FROM_START)
                    .liveChannel(settings().topology().channels().live())
                    .liveStreamId(settings().topology().streamId())
                    .replayChannel(settings().topology().channels().replay())
                    .replayStreamId(settings().topology().streamId() + 1)
                    .replicationConfiguration(settings().replication())
                    .clusterId(settings().topology().clusterId())
                    .wireNonce(settings().authentication().wireNonce())
                    .epoch(settings().topology().epoch())
                    .initialSequence(aeronCursor ? cursor.logicalSequence() : -1)
                    .initialPosition(aeronCursor ? cursorPosition : -1)
                    .receiver(new ReceiverAdapter(shared, receiver))
                    .transactionResolved(snapshot -> {
                        final AeronArchiveReader current = readerRef.get();
                        if (current != null && current == this.readers.current()) {
                            this.facade.writerTransport().advanceSequence(snapshot.sequence() + 1);
                            /* A retention watermark is proof of a durable recovery
                             * cursor, not merely an applied Store update. Persist the
                             * barrier tail first and publish exactly that boundary.
                             * Readers without a cursor sink cannot join retention. */
                            if (cursorListener != null) {
                                final byte[] position = new AeronReplicationCursor(
                                        settings().topology().clusterId(), settings().topology().identity().nodeId(), settings().topology().identity().storeGeneration(),
                                        settings().topology().epoch(), this.currentFencingToken(), recordingId,
                                        snapshot.position(), snapshot.sequence()).encode();
                                shared.runInDeliveryCallback(() -> cursorListener.onApplied(ReplicationCursor.of(
                                        "aeron", settings().topology().identity().storeGeneration(), snapshot.sequence(), position)));
                                this.publishReaderWatermark(snapshot, recordingId);
                            }
                        }
                    })
                    .deliveryListener(this.readerDeliveryListener())
                    .build());
            this.readerRecordingId.set(recordingId);
            /* Seed the stale-token floor from the validated cursor before the
             * reader accepts any frame, so a restart never re-accepts history
             * from a writer its persisted cursor already moved past. The token
             * comes from the same validated cursor as the replay position
             * above, never from a separately decoded copy. */
            created.seedFencingToken(validatedCursor == null ? 0L : validatedCursor.fencingToken());
            return created;
        });
        readerRef.set(replacement);
        /* Re-advertise the durable cursor when a reader restarts even if no new
         * transaction arrives. Otherwise a restarted writer cannot rebuild its
         * retention quorum until unrelated Store traffic happens. */
        if (aeronCursor && cursor.logicalSequence() >= 0) {
            this.publishReaderWatermark(new CursorSnapshot(cursor.logicalSequence(), cursorPosition), recordingId);
        }
        return new ClientAdapter(replacement, recordingId, settings().topology().clusterId(),
                settings().topology().identity().nodeId(), settings().topology().identity().storeGeneration(), settings().topology().epoch());
    }

    /// Decodes the durable cursor and validates its recording identity together
    /// with its fencing token. A `null` result is the explicit new-reader
    /// case (no provider position, sequence `-1`); anything else that is
    /// absent or mismatched fails closed because the Store may already hold
    /// history the cursor can no longer address.
    ///
    /// @param cursor              durable starting cursor
    /// @param aeronCursor         whether the cursor belongs to this transport
    /// @param expectedRecordingId recording the reader will replay
    /// @return validated Aeron cursor, or `null` for a new reader
    private AeronReplicationCursor validatedAeronCursor(
            final ReplicationCursor cursor, final boolean aeronCursor, final long expectedRecordingId) {
        if (!aeronCursor) {
            return null;
        }
        final byte[] positionBytes = cursor.providerPositionBytes();
        if (positionBytes.length == 0) {
            if (cursor.logicalSequence() >= 0) {
                throw reseedRequired("Aeron cursor has a sequence but no recording position", null);
            }
            return null;
        }
        final AeronReplicationCursor aeron;
        try {
            aeron = AeronReplicationCursor.decode(positionBytes);
        } catch (final IllegalArgumentException failure) {
            throw reseedRequired("invalid Aeron cursor", failure);
        }
        if (!aeron.clusterId().equals(settings().topology().clusterId()) ||
            !aeron.storeGeneration().equals(settings().topology().identity().storeGeneration()) ||
            aeron.epoch() != settings().topology().epoch() || aeron.recordingId() != expectedRecordingId ||
            aeron.sequence() != cursor.logicalSequence()) {
            throw reseedRequired("Aeron cursor identity does not match the configured reader or writer", null);
        }
        return aeron;
    }

    private long discoverReaderRecordingId() {
        final ChannelUri live = ChannelUri.parse(settings().topology().channels().live());
        final String alias = live.get(CommonContext.ALIAS_PARAM_NAME);
        if (alias == null || alias.isBlank()) {
            throw new IllegalStateException("Aeron recording discovery requires alias= on ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL");
        }
        final AtomicLong discovered = new AtomicLong(Aeron.NULL_VALUE);
        final int count = this.runtime().listRecordingsForUri("alias=%s".formatted(alias), settings().topology().streamId(),
                (_, _, recordingId, _,
                 _, _, _, _, _,
                 _, _, _, _, _, _,
                 _) -> discovered.set(recordingId));
        if (count != 1 || discovered.get() < 0) {
            throw new IllegalStateException("Aeron recording discovery requires exactly one alias=%s recording for stream %s; found %s".formatted(alias, settings().topology().streamId(), count));
        }
        return discovered.get();
    }

    private Path readerUncertaintyPath() {
        return settings().topology().directories().checkpointPath().resolveSibling("%s.reader-inflight".formatted(settings().topology().directories().checkpointPath().getFileName()));
    }

    private void rejectUncertainReaderImport(final long recordingId) {
        final Path path = this.readerUncertaintyPath();
        try {
            final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(path);
            if (checkpoint.recordType() != AeronReplicationCheckpoint.RecordType.READER_CURSOR ||
                checkpoint.state() != AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN ||
                !checkpoint.clusterId().equals(settings().topology().clusterId()) ||
                !checkpoint.nodeId().equals(settings().topology().identity().nodeId()) ||
                !checkpoint.storeGeneration().equals(settings().topology().identity().storeGeneration()) ||
                checkpoint.recordingId() != recordingId ||
                checkpoint.writerEpoch() != settings().topology().epoch()) {
                throw reseedRequired(
                        "reader import marker identity does not match Aeron configuration: %s".formatted(path),
                        null);
            }
            throw reseedRequired("reader Store import is uncertain at sequence %s; manual reseed is required: %s".formatted(checkpoint.transactionSequence(), path), null);
        } catch (final NoSuchFileException absent) {
            /* The marker may be removed by a prior reader shutdown between the
             * existence check and this open.  Absence is the safe state; only a
             * present but unreadable marker requires reseeding. */
        } catch (final IOException failure) {
            throw reseedRequired("cannot read uncertain reader import marker; manual reseed is required: %s".formatted(path),
                    failure);
        }
    }

    private ReaderDeliveryListener readerDeliveryListener() {
        final Path path = this.readerUncertaintyPath();
        final AeronTransportShared shared = shared();
        return new ReaderDeliveryListener() {
            @Override
            public void beforeStoreImport(final long sequence, final long position, final int dataLength,
                                          final int dataChunkCount, final int crc32c) {
                shared.runInDeliveryCallback(() -> {
                    try {
                        final AeronReplicationCheckpoint checkpoint = new AeronReplicationCheckpoint(
                                AeronReplicationCheckpoint.RecordType.READER_CURSOR,
                                AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN,
                                settings().topology().clusterId(), settings().topology().identity().nodeId(), settings().topology().identity().storeGeneration(), readerRecordingId.get(),
                                settings().topology().epoch(), currentFencingToken(), sequence, position,
                                dataLength, dataChunkCount, crc32c);
                        AeronReplicationCheckpointStore.write(path, checkpoint);
                    } catch (final IOException failure) {
                        throw reseedRequired("cannot persist uncertain reader import marker: %s".formatted(path), failure);
                    }
                });
            }

            @Override
            public void afterStoreImport() {
                shared.runInDeliveryCallback(() -> {
                    try {
                        /* The cursor has already been forced successfully. A directory force
                         * here only makes marker removal durable; omitting it is safe because
                         * a stale marker fails closed and requests a reseed after a crash. */
                        AtomicFileWriter.delete(path, false);
                    } catch (final IOException failure) {
                        throw reseedRequired("cannot clear uncertain reader import marker: %s".formatted(path), failure);
                    }
                });
            }
        };
    }

    private void publishReaderWatermark(final CursorSnapshot snapshot, final long recordingId) {
        shared().watermarks().publish(snapshot, recordingId);
    }

    /// Returns the newest writer fencing token this reader has accepted.
    long currentFencingToken() {
        final AeronArchiveReader current = this.readers.current();
        return current == null ? 0L : current.fencingToken();
    }

    /// Returns the last applied reader sequence, or `-1` before/without a reader.
    ///
    /// @return last applied sequence
    long appliedSequence() {
        /* Capture once: a concurrent replace/dispose can clear the
         * slot between two separate current() reads and turn this
         * supplier into an NPE. */
        final AeronArchiveReader current = this.readers.current();
        return current == null ? -1L : current.lastAppliedSequence();
    }

    /// Fails the installed reader from the driver error handler, lock-free.
    ///
    /// @param failure terminal driver failure
    void failCurrent(final RuntimeException failure) {
        final AeronArchiveReader current = this.readers.current();
        if (current != null) {
            current.fail(failure);
        }
    }

    /// Whether a reader is installed or a replacement is in progress.
    ///
    /// @return `true` while the slot is occupied
    boolean occupied() {
        return this.readers.occupied();
    }

    /// Disposes the current reader as an ordered transport close stage.
    void disposeReader() {
        this.readers.dispose();
    }

    /// Adapts complete Aeron data to the neutral binary receiver.
    private record ReceiverAdapter(AeronTransportShared shared, StorageBinaryDataReceiver receiver)
            implements StorageBinaryDataReceiver {
        public void receiveTypeDictionary(final String value) {
            this.shared().runInDeliveryCallback(() -> this.receiver().receiveTypeDictionary(value));
        }

        public void receiveData(final Binary value) {
            this.shared().runInDeliveryCallback(() -> {
                this.receiver().receiveData(value);
                this.receiver().awaitApplied();
            });
        }

        @Override
        public boolean receiveDataOwned(final Binary value) {
            return this.shared().callInDeliveryCallback(() -> this.receiver().receiveDataOwned(value));
        }

        @Override
        public boolean canReceiveDataOwned() {
            return this.receiver().canReceiveDataOwned();
        }

        @Override
        public void awaitApplied() {
            this.shared().runInDeliveryCallback(this.receiver()::awaitApplied);
        }
    }

    /// Adds the neutral message-position view to the Aeron client.
    private record ClientAdapter(
            AeronArchiveReader delegate,
            long recordingId,
            UUID clusterId,
            UUID nodeId,
            UUID storeGeneration,
            long epoch) implements ReplicationApplier {
        public void start() {
            this.delegate().start();
        }

        public void stopAtLatestMessage() {
            this.delegate().stopAtLatestMessage();
        }

        public ReplicationCursor cursor() {
            final CursorSnapshot snapshot = this.delegate().cursorSnapshot();
            return ReplicationCursor.of("aeron", this.storeGeneration(), snapshot.sequence(),
                    new AeronReplicationCursor(this.clusterId(), this.nodeId(), this.storeGeneration(),
                            this.epoch(), this.delegate().fencingToken(), this.recordingId(), snapshot.position(),
                            snapshot.sequence()).encode());
        }

        public boolean isRunning() {
            return this.delegate().isRunning();
        }

        public boolean isLive() {
            return this.delegate().isLive();
        }

        public RuntimeException failure() {
            return this.delegate().failure();
        }

        public ReplicationApplier.StopOutcome stopOutcome() {
            return this.delegate().stopOutcome();
        }

        public ReplicationApplier.StopResult stopResult() {
            return this.delegate().stopResult();
        }

        public void resume() {
            this.delegate().resume();
        }

        public void dispose() {
            this.delegate().dispose();
        }
    }
}
