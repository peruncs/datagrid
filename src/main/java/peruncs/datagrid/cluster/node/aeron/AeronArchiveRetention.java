package peruncs.datagrid.cluster.node.aeron;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import org.eclipse.serializer.functional.Action;
import org.eclipse.serializer.functional.Producer;
import peruncs.datagrid.cluster.node.replication.ReplicationLogRetention;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReaderWatermark;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCursor;
import peruncs.datagrid.cluster.storage.types.AtomicFileWriter;
import peruncs.datagrid.cluster.storage.types.ReplicationCursor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;

/// Writer-owned Archive retention controller.
///
/// Watermarks are trusted by network isolation and configured identity, not by
/// signatures: only watermarks from configured readers naming this writer are
/// accepted. This component deliberately owns no transport lifecycle. The
/// provider supplies a small writer access view, so retention cannot
/// accidentally close or replace the publication while validating a reader
/// watermark.
///
/// All retention decisions run on one dedicated agent thread behind a command
/// queue: callers submit their operation and wait for its outcome. Segment
/// purges and recording restarts therefore never run concurrently on polling
/// threads, and no caller lock is held across Archive calls.
final class AeronArchiveRetention implements ReplicationLogRetention {
    /* Versions 1 through 3 were development-only layouts, including the signed
     * watermark encoding. There is no migration contract, so the unsigned
     * watermark layout starts at version 4. */
    private static final int STATE_VERSION = 4;
    /* Aeron 1.53's purge guard reports an active-recording purge as
     * ACTIVE_RECORDING, but its detach guard for an in-progress replay sends
     * GENERIC with this producer-owned prefix. There is no dedicated error
     * code for the replay case, so the probe is string-based on top of the
     * code and pinned by a test. */
    private static final String REPLAY_IN_PROGRESS_DETACH_MESSAGE = "invalid detach: replay in progress";

    private final Set<UUID> configuredReaders;
    private final Runnable ensureWriter;
    private final RecordingPositions recordingPositions;
    private final LongSupplier recordingId;
    private final Supplier<AeronWriterBoundary> writerBoundary;
    private final LongUnaryOperator segmentPurger;
    private final UUID clusterId;
    private final UUID storeGeneration;
    private final long writerEpoch;
    private final IntSupplier termLength;
    private final IntSupplier segmentLength;
    private final BooleanSupplier watermarkDeliveryAvailable;
    private final Path statePath;
    /* Replaced atomically when durable state is restored. Keeping the live quorum
     * immutable during parsing prevents a malformed state file from installing a
     * partial retirement/acknowledgement set. All access runs on the single
     * retention agent thread; closed and cleanupComplete are volatile so any
     * thread observes shutdown and whether a retry is still owed. */
    private AeronReaderWatermark.Quorum quorum;
    private volatile boolean closed;
    private volatile boolean cleanupComplete;
    private boolean stateRestored;
    private AeronReaderWatermark persistedBoundary;
    private final ThreadPoolExecutor agent;
    private final ThreadLocal<Boolean> onAgentThread = ThreadLocal.withInitial(() -> Boolean.FALSE);
    private final AtomicReference<RuntimeException> terminalFailure = new AtomicReference<>();
    private static final System.Logger LOGGER = System.getLogger(AeronArchiveRetention.class.getName());

        /// Default bound for one queued retention command, in milliseconds.
    /// Archive segment purges run under the writer-paused fence and can take
    /// a while on large histories; callers wait at most this long before the
    /// wait itself fails.
    public static final long DEFAULT_OPERATION_TIMEOUT_MILLIS = 60_000L;

    private final long operationTimeoutMillis;

    AeronArchiveRetention(
            final Set<UUID> readers,
            final Runnable ensureWriter,
            final RecordingPositions recordingPositions,
            final LongSupplier recordingId,
            final Supplier<AeronWriterBoundary> writerBoundary,
            final LongUnaryOperator segmentPurger,
            final UUID clusterId,
            final UUID storeGeneration,
            final long writerEpoch,
            final IntSupplier termLength,
            final IntSupplier segmentLength,
            final BooleanSupplier watermarkDeliveryAvailable,
            final Path statePath,
            final long operationTimeoutMillis
    ) {
        Objects.requireNonNull(readers, "readers");
        Objects.requireNonNull(ensureWriter, "ensureWriter");
        Objects.requireNonNull(recordingPositions, "recordingPositions");
        Objects.requireNonNull(recordingId, "recordingId");
        Objects.requireNonNull(writerBoundary, "writerBoundary");
        Objects.requireNonNull(segmentPurger, "segmentPurger");
        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(storeGeneration, "storeGeneration");
        Objects.requireNonNull(termLength, "termLength");
        Objects.requireNonNull(segmentLength, "segmentLength");
        Objects.requireNonNull(watermarkDeliveryAvailable, "watermarkDeliveryAvailable");
        this.configuredReaders = Set.copyOf(readers);
        this.quorum = new AeronReaderWatermark.Quorum(readers);
        this.ensureWriter = ensureWriter;
        this.recordingPositions = recordingPositions;
        this.recordingId = recordingId;
        this.writerBoundary = writerBoundary;
        this.segmentPurger = segmentPurger;
        this.clusterId = clusterId;
        this.storeGeneration = storeGeneration;
        this.writerEpoch = writerEpoch;
        this.termLength = termLength;
        this.segmentLength = segmentLength;
        this.watermarkDeliveryAvailable = watermarkDeliveryAvailable;
        this.statePath = statePath;
        if (operationTimeoutMillis <= 0) {
            throw new IllegalArgumentException("operationTimeoutMillis must be positive");
        }
        this.operationTimeoutMillis = operationTimeoutMillis;
        this.agent = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(Math.max(16, readers.size() * 2)),
                Thread.ofVirtual().name("datagrid-retention-agent", 0L).factory(),
                new ThreadPoolExecutor.AbortPolicy());
    }

        /// Runs one retention command on the single agent thread.
    ///
    /// Calls already on the agent thread (nested retention calls) run
    /// directly; every other caller queues behind ongoing Archive work and
    /// waits at most the configured operation timeout for its outcome.
    ///
    /// A timed-out wait does not interrupt the agent thread: Archive control
    /// RPCs are not interrupt-safe and interrupting one mid-protocol can leave
    /// the AeronArchive client in an unknown state. Instead the wait fails,
    /// the future is cancelled without interruption (so a command that has not
    /// started yet never runs), and a running command is allowed to finish.
    /// The controller becomes terminal after a timeout because the Archive
    /// client's protocol state and any held writer-maintenance fence are no
    /// longer safe to reuse.
    /// Failures keep their original type so policy rejections stay
    /// distinguishable from transport faults.
    private <T> T onAgent(final Producer<T> operation) {
        final RuntimeException failed = this.terminalFailure.get();
        if (failed != null) throw new IllegalStateException("Aeron retention is unavailable", failed);
        if (Boolean.TRUE.equals(this.onAgentThread.get())) return operation.produce();
        final Future<T> submitted;
        try {
            submitted = this.agent.submit(() -> {
                this.onAgentThread.set(Boolean.TRUE);
                try {
                    return operation.produce();
                } finally {
                    this.onAgentThread.remove();
                }
            });
        } catch (final RejectedExecutionException rejected) {
            throw new IllegalStateException("Aeron retention is closed", rejected);
        }
        try {
            return submitted.get(this.operationTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (final TimeoutException timeout) {
            submitted.cancel(false);
            final IllegalStateException terminal = new IllegalStateException(
                    "Timed out waiting for Aeron retention after %s ms".formatted(this.operationTimeoutMillis),
                    timeout);
            this.terminalFailure.compareAndSet(null, terminal);
            throw terminal;
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for Aeron retention", interrupted);
        } catch (final ExecutionException failure) {
            final Throwable cause = failure.getCause();
            if (cause instanceof Error error) throw error;
            if (cause instanceof RuntimeException runtime) throw runtime;
            throw new IllegalStateException("Aeron retention failed", cause);
        }
    }

    /// Returns the terminal retention failure, or `null` while available.
    RuntimeException failure() {
        return this.terminalFailure.get();
    }

        /// Runs one void retention command on the single agent thread.
    private void onAgent(final Action operation) {
        this.<Void>onAgent(() -> {
            operation.execute();
            return null;
        });
    }

    @Override
    public boolean isSupported() {
        if (this.closed) return false;
        return this.onAgent(() -> {
            if (this.closed || !this.watermarkDeliveryAvailable.getAsBoolean()) return false;
            this.ensureStateRestored();
            return this.quorum.isComplete();
        });
    }

    /// Deletes Archive history through the requested cursor, gated by the reader quorum.
    ///
    /// Nothing is deleted unless the complete configured reader set has
    /// acknowledged at least the requested sequence, the watermark recording
    /// matches the active writer, and the watermark stays within the durable
    /// writer boundary: the terminal commit checkpoint, extended by the
    /// Archive's durably recorded position so a truthful reader acknowledgement
    /// is not refused just because the checkpoint fsync trails the live
    /// recording (see [#requireWithinDurableBoundary]). Deletion stops at
    /// complete segment boundaries; when a live replay still uses a selected
    /// segment the request defers instead. Any Archive failure fails closed.
    /// A cursor that has not crossed a full segment reports nothing to delete
    /// rather than deleting partially.
    ///
    /// @param cursor durable boundary to delete through
    /// @return deletion outcome with the boundary position
    @Override
    public MaintenanceResult deleteThrough(final ReplicationCursor cursor) {
        return this.onAgent(() -> this.deleteThroughOnAgent(cursor));
    }

    private MaintenanceResult deleteThroughOnAgent(final ReplicationCursor cursor) {
        if (this.closed) throw new IllegalStateException("Aeron retention is closed");
        if (!this.watermarkDeliveryAvailable.getAsBoolean()) {
            throw new UnsupportedOperationException("Aeron retention requires a deployed reader-to-writer watermark channel");
        }
        this.ensureStateRestored();
        if (cursor == null || !"aeron".equalsIgnoreCase(cursor.transport()))
            throw new IllegalArgumentException("Aeron retention requires an Aeron cursor");
        if (cursor.logicalSequence() < 0)
            throw new IllegalArgumentException("retention cursor must name a resolved sequence");
        try {
            if (!this.quorum.isComplete()) {
                throw new IllegalStateException(
                        "Aeron reader quorum has not acknowledged the requested boundary; missing=%s".formatted(this.quorum.missingReaders()));
            }
            final AeronReaderWatermark quorumWatermark = this.quorum.aggregate();
            final AeronWriterBoundary requested = this.requestedBoundary(cursor);
            if (quorumWatermark.sequence() < requested.sequence() ||
                quorumWatermark.sequence() == requested.sequence() &&
                quorumWatermark.position() < requested.position())
                throw new IllegalStateException("Aeron reader quorum has not reached the requested sequence");
            final long targetPosition = Math.min(quorumWatermark.position(), requested.position());
            this.ensureWriter.run();
            this.requireWithinDurableBoundary(requested.sequence(), targetPosition);
            final long activeRecordingId = this.recordingId.getAsLong();
            if (requested.recordingId() != activeRecordingId || quorumWatermark.recordingId() != activeRecordingId)
                throw new IllegalArgumentException("retention watermark recording does not match the active writer");
            final long start = this.recordingPositions.startPosition().applyAsLong(activeRecordingId);
            final long boundary = AeronArchive.segmentFileBasePosition(start, targetPosition,
                    this.termLength.getAsInt(), this.segmentLength.getAsInt());
            if (boundary <= start) {
                return new MaintenanceResult(MaintenanceResult.Status.NOTHING_TO_DELETE, start,
                        "reader quorum has not crossed a complete Archive segment");
            }
            final long recorded = this.recordedDurablePosition(activeRecordingId);
            if (recorded < 0) throw new IllegalStateException("Aeron recording has no durable position");
            if (boundary > recorded)
                throw new IllegalArgumentException("retention watermark does not cover a complete Archive segment");
            try {
                this.segmentPurger.applyAsLong(boundary);
            } catch (final ArchiveException failure) {
                if (isReplayInProgressDetach(failure)) {
                    return new MaintenanceResult(MaintenanceResult.Status.DEFERRED_ACTIVE_REPLAY, boundary,
                            "Archive replay still uses a segment selected for retention");
                }
                throw failure;
            }
            return new MaintenanceResult(MaintenanceResult.Status.DELETED, boundary,
                    "Archive segments purged through boundary");
        } catch (final IllegalArgumentException | IllegalStateException failure) {
            throw failure;
        } catch (final RuntimeException failure) {
            if (failure instanceof ArchiveException archiveFailure) {
                throw new IllegalStateException(
                        "Aeron Archive retention failed closed (errorCode=%s)".formatted(archiveFailure.errorCode()),
                        archiveFailure);
            }
            throw new IllegalStateException("Aeron Archive retention failed closed", failure);
        }
    }

        /// Permanently retires a reader, deleting its quorum entry and state.
    ///
    /// @param readerId permanently retired reader identity
    @Override
    public void retireReader(final UUID readerId) {
        this.onAgent(() -> this.retireReaderOnAgent(readerId));
    }

    private void retireReaderOnAgent(final UUID readerId) {
        if (this.closed) throw new IllegalStateException("Aeron retention is closed");
        if (!this.watermarkDeliveryAvailable.getAsBoolean()) {
            throw new UnsupportedOperationException(
                    "Aeron reader retirement requires a deployed reader-to-writer watermark channel");
        }
        this.ensureStateRestored();
        Objects.requireNonNull(readerId, "readerId");
        final AeronReaderWatermark previous = this.quorum.latest(readerId);
        if (!this.quorum.retire(readerId)) return;
        try {
            this.persistState();
        } catch (final RuntimeException failure) {
            this.quorum.reinstate(readerId);
            this.quorum.restore(readerId, previous);
            throw failure;
        }
    }

    /// Records one reader's durable boundary into the retention quorum.
    ///
    /// The cursor must carry a watermark naming the same sequence and Store
    /// generation as the cursor itself; anything else is rejected as a
    /// mismatched boundary rather than counted toward deletion.
    ///
    /// @param cursor reader cursor carrying the watermark
    @Override
    public void recordReaderWatermark(final ReplicationCursor cursor) {
        this.onAgent(() -> this.recordReaderWatermarkOnAgent(cursor));
    }

    private void recordReaderWatermarkOnAgent(final ReplicationCursor cursor) {
        if (this.closed) throw new IllegalStateException("Aeron retention is closed");
        if (!this.watermarkDeliveryAvailable.getAsBoolean()) {
            throw new UnsupportedOperationException(
                    "Aeron retention watermark delivery is not configured");
        }
        this.ensureStateRestored();
        if (cursor == null || !"aeron".equalsIgnoreCase(cursor.transport()))
            throw new IllegalArgumentException("Aeron retention requires an Aeron cursor");
        if (cursor.logicalSequence() < 0)
            throw new IllegalArgumentException("reader watermark must name a resolved sequence");
        final AeronReaderWatermark watermark;
        try {
            watermark = AeronReaderWatermark.decode(cursor.providerPositionBytes());
        } catch (final RuntimeException failure) {
            throw new IllegalArgumentException("reader cursor has no Aeron watermark", failure);
        }
        if (watermark.sequence() != cursor.logicalSequence() ||
            !this.storeGeneration.equals(cursor.storeGeneration())) {
            throw new IllegalArgumentException("reader cursor and watermark do not name one boundary");
        }
        this.recordReaderWatermark(watermark);
    }

        /// Accepts a watermark already decoded by the Aeron control subscription.
    void recordReaderWatermark(final AeronReaderWatermark watermark) {
        this.onAgent(() -> this.recordReaderWatermarkOnAgent(watermark));
    }

    /// Queues reader progress without blocking the Aeron polling thread.
    boolean offerReaderWatermark(final AeronReaderWatermark watermark) {
        if (this.closed || this.terminalFailure.get() != null) return false;
        try {
            this.agent.execute(() -> {
                this.onAgentThread.set(Boolean.TRUE);
                try {
                    this.recordReaderWatermarkOnAgent(watermark);
                } catch (final RuntimeException failure) {
                    LOGGER.log(System.Logger.Level.WARNING, "Aeron reader watermark update failed", failure);
                } finally {
                    this.onAgentThread.remove();
                }
            });
            return true;
        } catch (final RejectedExecutionException fullOrClosed) {
            return false;
        }
    }

    private void recordReaderWatermarkOnAgent(final AeronReaderWatermark watermark) {
        if (this.closed) throw new IllegalStateException("Aeron retention is closed");
        if (!this.watermarkDeliveryAvailable.getAsBoolean()) {
            throw new UnsupportedOperationException(
                    "Aeron retention watermark delivery is not configured");
        }
        this.ensureStateRestored();
        if (watermark == null || this.quorum.rejectsReader(watermark.readerId()) ||
            watermark.sequence() < 0 || watermark.position() < 0 ||
            this.differsFromWriter(watermark) ||
            (this.recordingId.getAsLong() >= 0 && watermark.recordingId() != this.recordingId.getAsLong())) {
            throw new IllegalArgumentException("reader watermark identity is invalid");
        }
        /* Resolve a lazily created recording only after the identity checks.
         * This prevents a misconfigured caller from forcing writer startup while
         * still ensuring that a valid token is checked against the actual recording. */
        this.ensureWriter.run();
        if (watermark.recordingId() != this.recordingId.getAsLong()) {
            throw new IllegalArgumentException("reader watermark recording does not match the active writer");
        }
        this.requireWithinDurableBoundary(watermark.sequence(), watermark.position());
        final AeronReaderWatermark previous = this.quorum.latest(watermark.readerId());
        this.quorum.accept(watermark);
        final AeronReaderWatermark completeBoundary = this.completeBoundary(this.quorum);
        /* A watermark that does not advance the complete quorum cannot authorize a
         * new deletion. Leaving the older file in place is deliberately conservative
         * after a crash: the writer may retain extra Archive segments, but it cannot
         * delete a segment on the strength of an unpersisted boundary. */
        if (!this.advancesPersistedBoundary(completeBoundary)) return;
        try {
            this.persistState();
        } catch (final RuntimeException failure) {
            /* The in-memory quorum must never advance beyond the durable quorum file.
             * Otherwise a failed write could authorize deletion that disappears on
             * restart. Restore the prior acknowledgement before propagating failure. */
            this.quorum.restore(watermark.readerId(), previous);
            throw failure;
        }
    }

    @Override
    public void close() {
        /* `closed` stops new commands and is set immediately. `cleanupComplete`
         * is set only after the agent has terminated and the quorum is closed,
         * so a close that timed out or was interrupted before quorum cleanup
         * stays retryable: a later close() finishes termination and quorum
         * cleanup instead of returning early. */
        this.closed = true;
        if (this.cleanupComplete) return;
        /* Queued commands drain first: shutdown() lets the running and queued
         * retention work finish, so no purge is abandoned mid-decision. */
        this.agent.shutdown();
        try {
            if (!this.agent.awaitTermination(30, TimeUnit.SECONDS)) {
                this.agent.shutdownNow();
                if (!this.agent.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOGGER.log(System.Logger.Level.WARNING,
                            "Aeron retention agent did not terminate after shutdown");
                    return;
                }
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            this.agent.shutdownNow();
            return;
        }
        this.quorum.close();
        /* Published only after both steps succeed: a quorum.close() failure
         * stays retryable like a termination failure. */
        this.cleanupComplete = true;
    }

    /// Validates reader-claimed progress against the writer's durable boundary.
    ///
    /// The boundary is the terminal commit checkpoint extended by the Archive's
    /// durably recorded position. Progress at or below the terminal checkpoint
    /// is trivially covered. Progress past it is still admissible on a
    /// continuously appending writer: a reader resolves a commit from the live
    /// stream while the writer is between the commit's Archive acknowledgement
    /// and the checkpoint fsync, so a truthful watermark can name a sequence
    /// the checkpoint file has not reached yet. Such progress is accepted only
    /// when it occupies new bytes — strictly beyond the checkpoint position —
    /// that already lie inside the durably recorded region of the active
    /// recording. A future sequence without new bytes, and any position beyond
    /// the recorded position, is genuinely impossible for a reader to know and
    /// fails closed.
    private void requireWithinDurableBoundary(final long sequence, final long position) {
        final AeronWriterBoundary terminal = this.writerBoundary.get();
        if (terminal == null || terminal.sequence() < 0 || terminal.position() < 0 ||
            sequence < terminal.sequence() ||
            sequence == terminal.sequence() && position <= terminal.position()) {
            return;
        }
        /* Past the terminal checkpoint. The recording question below runs only
         * in this slow path, so a watermark accepted at or below the checkpoint
         * never pays for an Archive control round-trip. */
        final long recorded = this.recordedDurablePosition(this.recordingId.getAsLong());
        if (position <= terminal.position() || recorded < 0 || position > recorded) {
            throw new IllegalStateException("reader watermark is ahead of the durable writer boundary");
        }
    }

    /// Returns the durably recorded position of one recording: the stop
    /// position of a finished recording, otherwise its live recording position.
    private long recordedDurablePosition(final long recordingId) {
        final long stop = this.recordingPositions.stopPosition().applyAsLong(recordingId);
        return stop >= 0 ? stop : this.recordingPositions.recordingPosition().applyAsLong(recordingId);
    }

    private AeronWriterBoundary requestedBoundary(final ReplicationCursor cursor) {
        if (!this.storeGeneration.equals(cursor.storeGeneration())) {
            throw new IllegalArgumentException("retention cursor belongs to another Store generation");
        }
        try {
            final AeronReplicationCursor requested = AeronReplicationCursor.decode(cursor.providerPositionBytes());
            if (!requested.clusterId().equals(this.clusterId) ||
                !requested.storeGeneration().equals(this.storeGeneration) ||
                requested.epoch() != this.writerEpoch || requested.sequence() != cursor.logicalSequence() ||
                requested.recordingPosition() < 0) {
                throw new IllegalArgumentException("retention cursor identity does not match the active writer");
            }
            return new AeronWriterBoundary(
                    requested.sequence(), requested.recordingId(), requested.recordingPosition());
        } catch (final RuntimeException failure) {
            throw new IllegalArgumentException("retention cursor must carry a valid Aeron replication cursor", failure);
        }
    }

    private void restoreState() {
        if (this.statePath == null) return;
        try {
            final byte[] encoded;
            /* Open with NOFOLLOW_LINKS and keep the handle for the complete read. A
             * separate isSymbolicLink/size/readAllBytes sequence is TOCTOU-prone: an
             * attacker could replace the state path with a symlink between checks. */
            try (SeekableByteChannel channel = Files.newByteChannel(
                    this.statePath, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
                final long size = channel.size();
                if (size > 1_048_576L || size < 0L)
                    throw new IOException("retention state is too large");
                encoded = new byte[(int) size];
                final ByteBuffer source = ByteBuffer.wrap(encoded);
                while (source.hasRemaining()) {
                    final int read = channel.read(source);
                    if (read <= 0) throw new IOException("retention state read made no progress");
                }
            }
            final ByteBuffer buffer = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
            if (buffer.remaining() < Integer.BYTES * 2)
                throw new IOException("truncated retention state");
            final int version = buffer.getInt();
            if (version != STATE_VERSION)
                throw new IOException("unsupported retention state version");
            final int count = buffer.getInt();
            if (count < 0 || count > 1024)
                throw new IOException("invalid retention state count");
            final ArrayList<AeronReaderWatermark> watermarks = new ArrayList<>(count);
            final HashSet<UUID> watermarkReaders = new HashSet<>();
            for (int i = 0; i < count; i++) {
                if (buffer.remaining() < Integer.BYTES)
                    throw new IOException("truncated retention state");
                final int length = buffer.getInt();
                if (length <= 0 || length > buffer.remaining())
                    throw new IOException("invalid retention token length");
                final byte[] token = new byte[length];
                buffer.get(token);
                final AeronReaderWatermark watermark = AeronReaderWatermark.decode(token);
                if (this.differsFromWriter(watermark)) {
                    throw new IOException("retention state belongs to another Aeron writer");
                }
                if (watermark.sequence() < 0 || watermark.position() < 0)
                    throw new IOException("retention state contains an unresolved watermark");
                if (!watermarkReaders.add(watermark.readerId())) {
                    throw new IOException("retention state contains duplicate reader watermark");
                }
                if (this.quorum.rejectsReader(watermark.readerId())) {
                    throw new IOException(
                            "retention state contains a watermark for an unconfigured or retired reader: %s".formatted(watermark.readerId()));
                }
                watermarks.add(watermark);
            }
            if (buffer.remaining() < Integer.BYTES)
                throw new IOException("truncated retirement state");
            final int retiredCount = buffer.getInt();
            if (retiredCount < 0 || retiredCount > 1024 || buffer.remaining() != retiredCount * 16)
                throw new IOException("invalid retirement state count");
            final ArrayList<UUID> retiredReaders = new ArrayList<>(retiredCount);
            final HashSet<UUID> retiredReaderSet = new HashSet<>();
            for (int i = 0; i < retiredCount; i++) {
                final UUID readerId = new UUID(buffer.getLong(), buffer.getLong());
                if (!this.configuredReaders.contains(readerId))
                    throw new IOException("retention state contains an unconfigured retired reader %s".formatted(readerId));
                if (!retiredReaderSet.add(readerId))
                    throw new IOException("retention state contains duplicate retired reader %s".formatted(readerId));
                if (watermarkReaders.contains(readerId))
                    throw new IOException("retention state contains both a watermark and retirement for %s".formatted(readerId));
                retiredReaders.add(readerId);
            }
            if (buffer.hasRemaining()) throw new IOException("trailing retention state bytes");
            /* Build a replacement quorum only after the complete file has been parsed.
             * Mutating the live quorum here used to make a later runtime validation
             * failure observable as partially restored state. The replacement is
             * published in one assignment, so retries always start from a clean view. */
            final AeronReaderWatermark.Quorum restored =
                    new AeronReaderWatermark.Quorum(this.configuredReaders);
            for (final UUID readerId : retiredReaders) {
                if (!restored.retire(readerId))
                    throw new IOException("retention state contains duplicate retired reader %s".formatted(readerId));
            }
            for (final AeronReaderWatermark watermark : watermarks) restored.accept(watermark);
            final AeronReaderWatermark.Quorum previous = this.quorum;
            this.quorum = restored;
            this.persistedBoundary = this.completeBoundary(restored);
            previous.close();
        } catch (final NoSuchFileException ignored) {
            /* The state file is optional on first startup. */
        } catch (final IOException | RuntimeException failure) {
            throw new IllegalStateException("cannot load Aeron retention state %s".formatted(this.statePath), failure);
        }
    }

        /// Restores durable quorum state only once the watermark channel is usable.
    private void ensureStateRestored() {
        if (!this.stateRestored && this.watermarkDeliveryAvailable.getAsBoolean()) {
            this.restoreState();
            this.stateRestored = true;
        }
    }

    private boolean differsFromWriter(final AeronReaderWatermark watermark) {
        return !watermark.clusterId().equals(this.clusterId) ||
               !watermark.storeGeneration().equals(this.storeGeneration) ||
               watermark.writerEpoch() != this.writerEpoch;
    }

    /// Reports whether one purge failure means a live replay still uses a
    /// selected segment, in which case retention defers instead of failing.
    ///
    /// @param failure Archive purge failure
    /// @return `true` when the purge must be deferred for an active replay
    static boolean isReplayInProgressDetach(final ArchiveException failure) {
        return failure.errorCode() == ArchiveException.ACTIVE_RECORDING ||
               failure.errorCode() == ArchiveException.GENERIC && failure.getMessage() != null &&
               failure.getMessage().contains(REPLAY_IN_PROGRESS_DETACH_MESSAGE);
    }

    private void persistState() {
        if (this.statePath == null) return;
        try {
            final var snapshot = this.quorum
                    .snapshot()
                    .entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).toList();
            final var retired = this.quorum.retiredReaders().stream().sorted().toList();
            /* Encode each token once. Retention updates are infrequent, but encoding
             * twice still produced two short-lived arrays for every reader on
             * every durable state replacement. */
            final byte[][] encodedTokens = new byte[snapshot.size()][];
            int length = Integer.BYTES * 3 + retired.size() * 16;
            for (int index = 0; index < snapshot.size(); index++) {
                encodedTokens[index] = snapshot.get(index).encode();
                length += Integer.BYTES + encodedTokens[index].length;
            }
            final ByteBuffer buffer = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN)
                    .putInt(STATE_VERSION).putInt(snapshot.size());
            for (final byte[] token : encodedTokens) {
                buffer.putInt(token.length).put(token);
            }
            buffer.putInt(retired.size());
            for (final UUID readerId : retired) {
                buffer.putLong(readerId.getMostSignificantBits()).putLong(readerId.getLeastSignificantBits());
            }
            buffer.flip();
            AtomicFileWriter.write(this.statePath, channel ->
            {
                final ByteBuffer source = buffer.duplicate();
                while (source.hasRemaining()) {
                    if (channel.write(source) == 0) throw new IOException("Archive retention state write made no progress");
                }
            });
            this.persistedBoundary = this.completeBoundary(this.quorum);
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot persist Aeron retention state %s".formatted(this.statePath), failure);
        }
    }

    private AeronReaderWatermark completeBoundary(
            final AeronReaderWatermark.Quorum value) {
        return value.isComplete() ? value.aggregate() : null;
    }

    private boolean advancesPersistedBoundary(final AeronReaderWatermark current) {
        if (current == null || this.persistedBoundary == null) return current != null;
        return current.writerEpoch() != this.persistedBoundary.writerEpoch() ||
               current.recordingId() != this.persistedBoundary.recordingId() ||
               current.sequence() > this.persistedBoundary.sequence() ||
               current.sequence() == this.persistedBoundary.sequence() &&
               current.position() > this.persistedBoundary.position();
    }

        /// Minimal Archive position view required by retention decisions.
    record RecordingPositions(LongUnaryOperator startPosition, LongUnaryOperator stopPosition,
                              LongUnaryOperator recordingPosition) {
        RecordingPositions {
            Objects.requireNonNull(startPosition, "startPosition");
            Objects.requireNonNull(stopPosition, "stopPosition");
            Objects.requireNonNull(recordingPosition, "recordingPosition");
        }
    }
}
