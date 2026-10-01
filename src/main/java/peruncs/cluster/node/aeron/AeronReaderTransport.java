package peruncs.cluster.node.aeron;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.archive.client.PersistentSubscription;
import org.eclipse.serializer.persistence.binary.types.Binary;
import peruncs.cluster.storage.ReplicationPosition;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;
import peruncs.cluster.storage.aeron.reader.AeronArchiveReader;
import peruncs.cluster.storage.aeron.reader.CursorSnapshot;
import peruncs.cluster.storage.binary.ReplicationApplier;
import peruncs.cluster.storage.binary.StorageBinaryDataReceiver;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static peruncs.cluster.node.aeron.AeronTransportShared.reseedRequired;

/// Owns the reader side of one transport: the Archive replay client and its
/// replacement slot.
///
/// Reader replacement and disposal run under the slot's own lifecycle lock,
/// never under the shared transport monitor: a reader dispose can join its
/// polling thread for up to the reader-stop timeout, and health and failure
/// callbacks must keep observing the transport while that join is in flight.
/// The Store mark is validated against the configured cluster and recording,
/// and a restarted reader re-advertises its retention watermark immediately.
final class AeronReaderTransport {
    private final AeronTransport facade;
    /* Reader replacement and disposal run under this slot's own lifecycle
     * lock, never under the transport monitor: a reader dispose can join
     * its polling thread for up to the reader-stop timeout, and health()/
     * failure callbacks must keep observing the transport while that join
     * is in flight. The slot publishes the replacement reference last. */
    private final CurrentReader<AeronArchiveReader> readers = new CurrentReader<>();
    /* One Archive stream keeps its recording identity across reader restarts. */
    private volatile long discoveredRecordingId = Aeron.NULL_VALUE;

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

    /// Creates the reader-side client starting at the Store-resident mark.
    ///
    /// A writer gets a no-op client instead, because a second subscription
    /// against its own recording would break the one-writer topology and leak a
    /// half-initialized reader into the writer's health path. A new call
    /// disposes and replaces any existing reader. The starting mark is checked
    /// against the configured Store and recording, and a resolved mark
    /// re-advertises its watermark immediately so a restarted writer can
    /// rebuild its retention quorum without waiting for new traffic.
    ///
    /// @param receiver destination for received binaries and dictionaries
    /// @param startingMark Store-resident starting boundary
    /// @return binary data client
    ReplicationApplier clientFromMark(
            final StorageBinaryDataReceiver receiver,
            final ReplicationMark startingMark
    ) {
        return this.createClient(receiver, Objects.requireNonNull(startingMark, "startingMark"));
    }

    private ReplicationApplier createClient(
            final StorageBinaryDataReceiver receiver,
            final ReplicationMark startingMark
    ) {
        final AeronTransportShared shared = shared();
        synchronized (shared) {
            shared.ensureOpen();
            /* A writer owns publication only; a second subscription against its own
             * recording would violate the one-writer/N-reader topology. */
            if (settings().topology().role().isWriter()) {
                return ReplicationApplier.noOp();
            }
        }
        Objects.requireNonNull(receiver, "receiver");
        this.runtime().ensure();
        final long recordingId = settings().topology().recordingId() >= 0
                ? settings().topology().recordingId() : this.discoverReaderRecordingId();
        validateStartingMark(startingMark, recordingId);
        final long initialSequence = startingMark.sequence();
        final long cursorPosition = startingMark.prepareStartPosition();
        final AtomicReference<AeronArchiveReader> readerRef = new AtomicReference<>();
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
                    .startPosition(initialSequence >= 0
                            ? cursorPosition : PersistentSubscription.FROM_START)
                    .liveChannel(settings().topology().channels().live())
                    .liveStreamId(settings().topology().streamId())
                    .replayChannel(settings().topology().channels().replay())
                    .replayStreamId(settings().topology().streamId() + 1)
                    .replicationConfiguration(settings().replication())
                    .clusterId(settings().topology().clusterId())
                    .wireNonce(settings().wireNonce())
                    .epoch(settings().topology().epoch())
                    .initialSequence(initialSequence)
                    .initialPosition(cursorPosition)
                    .receiver(new ReceiverAdapter(shared, receiver))
                    .transactionResolved(snapshot -> {
                        final AeronArchiveReader current = readerRef.get();
                        if (current != null && current == this.readers.current()) {
                            /* The imported Store mark is the durable boundary, and a restart
                             * resumes replay at its prepare start. Publish exactly that
                             * position, not the end of the commit frame, so the writer's
                             * retention never purges the segment a restart still needs. An
                             * ABORT-only barrier leaves the mark unchanged, so the watermark
                             * simply repeats. */
                            final long durableSequence = startingMark.sequence();
                            if (durableSequence >= 0L) {
                                this.publishReaderWatermark(new CursorSnapshot(durableSequence,
                                        startingMark.prepareStartPosition()), recordingId);
                            }
                        }
                    })
                    .build());
            created.seedFencingToken(startingMark.fencingToken());
            return created;
        });
        readerRef.set(replacement);
        /* Re-advertise the durable mark when a reader restarts even if no new
         * transaction arrives. Otherwise a restarted writer cannot rebuild its
         * retention quorum until unrelated Store traffic happens. */
        if (initialSequence >= 0) {
            this.publishReaderWatermark(new CursorSnapshot(initialSequence, cursorPosition), recordingId);
        }
        return new ClientAdapter(replacement, startingMark, recordingId, settings().topology().clusterId(),
                settings().topology().identity().nodeId(), settings().topology().identity().storeGeneration(), settings().topology().epoch());
    }

    /// Rejects a Store mark that belongs to another replication history.
    private void validateStartingMark(final ReplicationMark mark, final long expectedRecordingId) {
        if (!Objects.equals(mark.clusterId(), settings().topology().clusterId()) ||
            !Objects.equals(mark.storeGeneration(), settings().topology().identity().storeGeneration()) ||
            mark.epoch() != settings().topology().epoch() || mark.recordingId() != expectedRecordingId ||
            mark.sequence() < -1L || mark.fencingToken() < 0L ||
            (mark.sequence() >= 0L && mark.prepareStartPosition() < 0L)) {
            throw reseedRequired("Store replication mark does not match the configured reader or recording", null);
        }
    }

    private long discoverReaderRecordingId() {
        long cachedRecordingId = this.discoveredRecordingId;
        if (cachedRecordingId >= 0L) return cachedRecordingId;
        synchronized (this) {
            cachedRecordingId = this.discoveredRecordingId;
            if (cachedRecordingId >= 0L) return cachedRecordingId;
            final ChannelUri live = ChannelUri.parse(settings().topology().channels().live());
            final String alias = live.get(CommonContext.ALIAS_PARAM_NAME);
            if (alias == null || alias.isBlank()) {
                throw new IllegalStateException("Aeron recording discovery requires alias= on PERUNCS_AERON_LIVE_CHANNEL");
            }
            final AtomicLong discovered = new AtomicLong(Aeron.NULL_VALUE);
            final int count = this.runtime().listRecordingsForUri("alias=%s".formatted(alias), settings().topology().streamId(),
                    (_, _, recordingId, _,
                     _, _, _, _, _,
                     _, _, _, _, _, _,
                     _) -> discovered.set(recordingId));
            if (count != 1 || discovered.get() < 0) {
                throw new IllegalStateException("Aeron recording discovery requires exactly one alias=%s recording for stream %s; found %s"
                        .formatted(alias, settings().topology().streamId(), count));
            }
            this.discoveredRecordingId = discovered.get();
            return this.discoveredRecordingId;
        }
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
        @Override
        public ByteBuffer allocateNativeBuffer(final int minimumCapacity) {
            return this.receiver().allocateNativeBuffer(minimumCapacity);
        }

        @Override
        public void releaseNativeBuffer(final ByteBuffer buffer) {
            this.receiver().releaseNativeBuffer(buffer);
        }

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
        public boolean canAcceptOwnedData(final long payloadBytes) {
            return this.receiver().canAcceptOwnedData(payloadBytes);
        }

        @Override
        public void awaitApplied() {
            this.shared().runInDeliveryCallback(this.receiver()::awaitApplied);
        }
    }

    /// Adds the neutral message-position view to the Aeron client.
    private record ClientAdapter(
            AeronArchiveReader delegate,
            ReplicationMark mark,
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

        public ReplicationPosition position() {
            /* The boundary a Store image can be restarted from: its mark, not the end of the last
             * commit frame the assembler resolved. Before the first applied transaction the mark is
             * empty and the position is reported as unknown (-1), never the assembler's cursor. */
            final CursorSnapshot snapshot = this.mark().sequence() >= 0L
                    ? new CursorSnapshot(this.mark().sequence(), this.mark().prepareStartPosition())
                    : new CursorSnapshot(-1L, -1L);
            return new ReplicationPosition(this.clusterId(), this.storeGeneration(), this.epoch(),
                    this.recordingId(), snapshot.sequence(), snapshot.position(),
                    this.delegate().fencingToken(), this.nodeId());
        }

        @Override
        public long currentSequence() {
            return this.delegate().lastAppliedSequence();
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
