package peruncs.datagrid.cluster.storage.aeron.writer;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.types.ReplicationDurabilityMode;
import peruncs.datagrid.cluster.storage.types.ReplicationRetry;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;

/**
 * Publishes the replication stream and waits for the Archive to record it.
 *
 * <p>The commit marker is the visibility boundary for a transaction. A commit
 * is reported only after the Archive's recorded position reaches that marker.
 * The Archive and Aeron client are borrowed from the transport; this class
 * closes only the publication and its recording.</p>
 */
public final class AeronArchiveReplicationPublisher implements AutoCloseable {
    private final AeronArchive archive;
    private final ExclusivePublication publication;
    private final AeronReplicationPublisher publisher;
    private final long recordingIdHint;
    private final AeronReplicationConfiguration configuration;
    private final SourceLocation sourceLocation;
    private final AtomicInteger recordingCounterId = new AtomicInteger(-1);
    /** Set only after the Archive confirms the recording has a stop position. */
    private boolean recordingStopped;
    private boolean closed;
    private AeronArchiveReplicationPublisher(
            final AeronArchive archive,
            final ExclusivePublication publication,
            final AeronReplicationPublisher publisher,
            final long recordingIdHint,
            final AeronReplicationConfiguration configuration,
            final SourceLocation sourceLocation
    ) {
        this.archive = archive;
        this.publication = publication;
        this.publisher = publisher;
        this.recordingIdHint = recordingIdHint;
        this.configuration = configuration;
        this.sourceLocation = sourceLocation;
    }

    /**
     * Creates a local Archive recording and its writer publication.
     *
     * @param archive         Archive client that owns the recording
     * @param channel         publication channel
     * @param streamId        publication stream
     * @param configuration   shared framing and timeout limits
     * @param clusterId       replication cluster identity
     * @param epoch           writer epoch
     * @param initialSequence first sequence to publish
     * @return a publisher that owns the publication and recording
     */
    public static AeronArchiveReplicationPublisher New(
            final AeronArchive archive,
            final String channel,
            final int streamId,
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence
    ) {
        return New(archive, channel, streamId, configuration, clusterId, epoch, initialSequence,
                SourceLocation.LOCAL);
    }

    /**
     * Creates a recording for a publication recorded by a separate Archive.
     * The caller still supplies the connected Archive client used for the
     * recording commands.
     *
     * @param archive         Archive client that owns the recording
     * @param channel         publication channel
     * @param streamId        publication stream
     * @param configuration   shared framing and timeout limits
     * @param clusterId       replication cluster identity
     * @param epoch           writer epoch
     * @param initialSequence first sequence to publish
     * @return a publisher that owns the publication and recording
     */
    public static AeronArchiveReplicationPublisher NewRemote(
            final AeronArchive archive,
            final String channel,
            final int streamId,
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence
    ) {
        return New(archive, channel, streamId, configuration, clusterId, epoch, initialSequence,
                SourceLocation.REMOTE);
    }

    private static AeronArchiveReplicationPublisher New(
            final AeronArchive archive,
            final String channel,
            final int streamId,
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence,
            final SourceLocation sourceLocation
    ) {
        ExclusivePublication publication = null;
        try {
            synchronized (archive) {
                if (sourceLocation == SourceLocation.LOCAL) {
                    publication = archive.addRecordedExclusivePublication(channel, streamId);
                } else {
                    publication = archive.context().aeron().addExclusivePublication(channel, streamId);
                    archive.startRecording(ChannelUri.addSessionId(channel, publication.sessionId()), streamId, sourceLocation);
                }
            }
            final long recordingId = awaitRecordingStarted(archive, publication, Aeron.NULL_VALUE, configuration);
            final ExclusivePublication ownedPublication = publication;
            return new AeronArchiveReplicationPublisher(
                    archive,
                    ownedPublication,
                    new AeronReplicationPublisher(
                            ownedPublication, configuration, clusterId, epoch, initialSequence,
                            position -> awaitRecorded(archive, ownedPublication, recordingId, configuration, position)
                    ),
                    recordingId,
                    configuration,
                    sourceLocation
            );
        } catch (final RuntimeException | Error failure) {
            if (publication != null) {
                final Throwable stopFailure = tryStopRecording(
                        archive, publication, Aeron.NULL_VALUE, configuration);
                if (stopFailure != null) failure.addSuppressed(stopFailure);
                try {
                    publication.close();
                } catch (final RuntimeException | Error closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
    }

    /**
     * Extends a stopped recording without changing its framing.
     * The recording must belong to {@code streamId}; the same initial-position
     * URI is used for the new publication and the Archive extension.
     *
     * @param archive         Archive client that owns the recording
     * @param recordingId     stopped recording to extend
     * @param streamId        publication stream
     * @param configuration   shared framing and timeout limits
     * @param clusterId       replication cluster identity
     * @param epoch           writer epoch
     * @param initialSequence first sequence to publish
     * @return a publisher that owns the extended publication and recording
     * @throws IllegalArgumentException if the recording is unknown, uses another
     *                                  stream, or has different framing
     * @throws IllegalStateException    if the recording is still active
     */
    public static AeronArchiveReplicationPublisher Extend(
            final AeronArchive archive,
            final long recordingId,
            final int streamId,
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence
    ) {
        return Extend(archive, recordingId, streamId, configuration, clusterId, epoch, initialSequence,
                SourceLocation.LOCAL);
    }

    /**
     * Reopens a stopped recording whose source publication uses another driver.
     *
     * @param archive         Archive client that owns the recording
     * @param recordingId     stopped recording to extend
     * @param streamId        publication stream
     * @param configuration   shared framing and timeout limits
     * @param clusterId       replication cluster identity
     * @param epoch           writer epoch
     * @param initialSequence first sequence to publish
     * @return a publisher that owns the extended publication and recording
     */
    public static AeronArchiveReplicationPublisher ExtendRemote(
            final AeronArchive archive,
            final long recordingId,
            final int streamId,
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence
    ) {
        return Extend(archive, recordingId, streamId, configuration, clusterId, epoch, initialSequence,
                SourceLocation.REMOTE);
    }

    private static AeronArchiveReplicationPublisher Extend(
            final AeronArchive archive,
            final long recordingId,
            final int streamId,
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence,
            final SourceLocation sourceLocation
    ) {
        final String[] channel = new String[1];
        final long[] position = new long[1];
        final int[] initialTermId = new int[1];
        final int[] termLength = new int[1];
        final int[] mtuLength = new int[1];
        final int[] recordedStream = new int[1];
        synchronized (archive) {
            if (archive.listRecording(recordingId, (a, b, id, c, d, start, stop, termId, segment, term, mtu, session,
                                                    stream, stripped, original, source) ->
            {
                channel[0] = stripped;
                position[0] = stop;
                initialTermId[0] = termId;
                termLength[0] = term;
                mtuLength[0] = mtu;
                recordedStream[0] = stream;
            }) == 0 || channel[0] == null) {
                throw new IllegalArgumentException("Unknown Aeron recording: " + recordingId);
            }
        }
        if (position[0] < 0) {
            throw new IllegalStateException("Aeron recording is still active: " + recordingId);
        }
        if (recordedStream[0] != streamId) {
            throw new IllegalArgumentException("Aeron recording stream does not match replication configuration");
        }
        if (termLength[0] != configuration.termLength() || mtuLength[0] != configuration.mtuLength()) {
            throw new IllegalArgumentException("Aeron recording framing does not match replication configuration");
        }
        final String baseChannel = new ChannelUriStringBuilder(channel[0]).sessionId((Integer) null).build();
        final String extendedChannel = new ChannelUriStringBuilder(baseChannel)
                .initialPosition(position[0], initialTermId[0], termLength[0]).build();
        final ExclusivePublication publication = archive.context().aeron().addExclusivePublication(extendedChannel, streamId);
        try {
            synchronized (archive) {
                archive.extendRecording(recordingId, extendedChannel, streamId, sourceLocation);
            }
            awaitRecordingStarted(archive, publication, recordingId, configuration);
            return new AeronArchiveReplicationPublisher(archive, publication,
                    new AeronReplicationPublisher(publication, configuration, clusterId, epoch, initialSequence,
                            positionValue -> awaitRecorded(archive, publication, recordingId, configuration, positionValue)), recordingId,
                    configuration, sourceLocation);
        } catch (final RuntimeException | Error failure) {
            final Throwable stopFailure = tryStopRecording(
                    archive, publication, recordingId, configuration);
            if (stopFailure != null) failure.addSuppressed(stopFailure);
            try {
                publication.close();
            } catch (final RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static long findRecordingId(final AeronArchive archive, final ExclusivePublication publication) {
        final CountersReader counters = archive.context().aeron().countersReader();
        final int counterId = RecordingPos.findCounterIdBySession(
                counters, publication.sessionId(), archive.archiveId());
        if (counterId >= 0) return RecordingPos.getRecordingId(counters, counterId);

        final long[] recordingId = {Aeron.NULL_VALUE};
        long from = 0L;
        final int pageSize = 128;
        while (recordingId[0] < 0) {
            final long[] last = {from - 1L};
            final int count;
            synchronized (archive) {
                count = archive.listRecordings(from, pageSize, (controlSessionId, correlationId, id,
                                                                startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId,
                                                                segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                                                                strippedChannel, originalChannel, sourceIdentity) ->
                {
                    last[0] = id;
                    if (sessionId == publication.sessionId() && streamId == publication.streamId()) {
                        recordingId[0] = id;
                    }
                });
            }
            if (count < pageSize || last[0] < from || last[0] == Long.MAX_VALUE) break;
            from = last[0] + 1L;
        }
        return recordingId[0];
    }

    private static long awaitRecorded(
            final AeronArchive archive,
            final ExclusivePublication publication,
            final long recordingIdHint,
            final AeronReplicationConfiguration configuration,
            final long commitPosition
    ) {
        final CountersReader counters = archive.context().aeron().countersReader();
        final long deadline = ReplicationRetry.deadlineNanos(configuration.recordedPositionTimeoutNanos());
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        long lastRecordedPosition = Aeron.NULL_VALUE;
        boolean lastActive = false;
        int counterId = -1;
        long nextArchiveProbe = 0L;
        while (true) {
            checkInterrupted("waiting for Aeron Archive recording");
            if (counterId < 0 || counterId > counters.maxCounterId() ||
                (recordingIdHint >= 0 && RecordingPos.getRecordingId(counters, counterId) != recordingIdHint)) {
                counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), archive.archiveId());
                if (counterId < 0 && recordingIdHint >= 0) {
                    counterId = RecordingPos.findCounterIdByRecording(counters, recordingIdHint, archive.archiveId());
                }
            }
            if (counterId >= 0) {
                final long recordingId = RecordingPos.getRecordingId(counters, counterId);
                final long recordedPosition = counters.getCounterValue(counterId);
                lastRecordedPosition = recordedPosition;
                lastActive = RecordingPos.isActive(counters, counterId, recordingId);
                if (lastActive && recordedPosition >= commitPosition) {
                    return recordedPosition;
                }
                if (!lastActive) {
                    final long stopPosition = getStopPosition(archive, recordingId);
                    if (stopPosition >= commitPosition) return stopPosition;
                    throw new IllegalStateException("Aeron archive recording stopped before commit position " + commitPosition);
                }
            }
            if (counterId < 0 && recordingIdHint >= 0 && System.nanoTime() >= nextArchiveProbe) {
                /* A local RecordingPos counter is authoritative. Only ask the Archive
                 * control session when the counter is not visible (the remote/fallback
                 * path); otherwise every back-pressured offer would issue a synchronous
                 * control round-trip. */
                final long archivePosition = getRecordingPosition(archive, recordingIdHint);
                if (archivePosition >= commitPosition) {
                    return archivePosition;
                }
                nextArchiveProbe = System.nanoTime() + 10_000_000L;
            }
            /* The local counter is checked first, so an ordinary successful commit
             * returns without touching the control subscription. While the recording
             * is behind, however, the counter can remain allocated after an external
             * Archive dies. Poll the control session on every such duty cycle so a
             * disconnected Archive fails promptly instead of masquerading as a slow
             * recording until the complete commit deadline expires. */
            final String archiveError = pollForErrorResponse(archive);
            if (archiveError != null) throw new IllegalStateException("Aeron archive error: " + archiveError);
            if (ReplicationRetry.expired(deadline)) {
                throw new IllegalStateException("Aeron archive did not record commit position " + commitPosition +
                                                " (publicationPosition=" + publication.position() + ", recordedPosition=" + lastRecordedPosition +
                                                ", active=" + lastActive + ", recordingId=" + recordingIdHint + ")");
            }
            idle.idle(0);
        }
    }

    private static long awaitRecordingStarted(
            final AeronArchive archive,
            final ExclusivePublication publication,
            final long recordingIdHint,
            final AeronReplicationConfiguration configuration
    ) {
        final CountersReader counters = archive.context().aeron().countersReader();
        final long deadline = ReplicationRetry.deadlineNanos(configuration.recordingStartTimeoutNanos());
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        long nextCatalogProbe = 0L;
        long catalogProbeDelayNanos = 1_000_000L;
        while (true) {
            checkInterrupted("waiting for Aeron Archive recording start");
            final String archiveError = pollForErrorResponse(archive);
            if (archiveError != null) throw new IllegalStateException("Aeron archive error: " + archiveError);
            int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), archive.archiveId());
            if (counterId < 0 && recordingIdHint >= 0) {
                counterId = RecordingPos.findCounterIdByRecording(counters, recordingIdHint, archive.archiveId());
            }
            if (counterId >= 0) {
                final long recordingId = RecordingPos.getRecordingId(counters, counterId);
                if (RecordingPos.isActive(counters, counterId, recordingId)) return recordingId;
            } else if (recordingIdHint >= 0 || System.nanoTime() >= nextCatalogProbe) {
                final long discovered = recordingIdHint >= 0 ? recordingIdHint : findRecordingId(archive, publication);
                if (discovered >= 0) {
                    /* A catalog entry is only a hint. A stopped entry can be found while
                     * the new publication is still being wired, so accept it only while
                     * the Archive reports the recording as active. */
                    try {
                        /* A catalog entry is active only when it has no stop position
                         * and the Archive can report its current recording position. */
                        if (getStopPosition(archive, discovered) < 0 &&
                            getRecordingPosition(archive, discovered) >= 0) return discovered;
                    } catch (final ArchiveException failure) {
                        if (failure.errorCode() != ArchiveException.UNKNOWN_RECORDING) {
                            throw failure;
                        }
                        /* The catalog may lag the publication. Retry only the explicit
                         * unknown-recording race; authentication, storage, and protocol
                         * failures must surface immediately. */
                    }
                }
                if (recordingIdHint < 0) {
                    nextCatalogProbe = System.nanoTime() + catalogProbeDelayNanos;
                    catalogProbeDelayNanos = Math.min(100_000_000L, catalogProbeDelayNanos * 2L);
                }
            }
            if (ReplicationRetry.expired(deadline)) {
                throw new IllegalStateException("Aeron archive recording did not start before timeout (recordingId=" +
                                                recordingIdHint + ", session=" + publication.sessionId() + ", channel=" + publication.channel() + ")");
            }
            idle.idle(0);
        }
    }

    private static Throwable tryStopRecording(
            final AeronArchive archive,
            final ExclusivePublication publication,
            final long recordingIdHint,
            final AeronReplicationConfiguration configuration
    ) {
        Throwable failure = null;
        long recordingId = recordingIdHint;
        try {
            if (recordingId < 0) {
                final CountersReader counters = archive.context().aeron().countersReader();
                final int counterId = RecordingPos.findCounterIdBySession(
                        counters, publication.sessionId(), archive.archiveId());
                if (counterId >= 0) {
                    recordingId = RecordingPos.getRecordingId(counters, counterId);
                }
                if (recordingId < 0) {
                    /* A counter may not be visible until the Archive catalog has indexed
                     * the publication.  Discovering by session/stream is the last safe
                     * fallback before abandoning construction cleanup. */
                    recordingId = findRecordingId(archive, publication);
                }
            }
            if (recordingId >= 0) {
                try {
                    tryStopRecordingByIdentity(archive, recordingId);
                } catch (final RuntimeException | Error stopFailure) {
                    failure = stopFailure;
                }
                try {
                    /* Stopping is asynchronous. Construction must not leak a recording
                     * when a later setup step fails, otherwise the next startup sees an
                     * apparently active recording and refuses to extend it. */
                    awaitStopped(archive, recordingId, configuration);
                } catch (final RuntimeException | Error awaitFailure) {
                    if (failure == null) failure = awaitFailure;
                    else if (failure != awaitFailure) failure.addSuppressed(awaitFailure);
                }
            }
        } catch (final RuntimeException | Error cleanupFailure) {
            if (failure == null) failure = cleanupFailure;
            else if (failure != cleanupFailure) failure.addSuppressed(cleanupFailure);
        }
        return failure;
    }

    /* AeronArchive's control client is not a concurrent API.  Keep each control
     * request atomic with respect to close/retention/replay probes without holding
     * the lock across the retry loops above. */
    private static long getStopPosition(final AeronArchive archive, final long recordingId) {
        synchronized (archive) {
            return archive.getStopPosition(recordingId);
        }
    }

    private static long getRecordingPosition(final AeronArchive archive, final long recordingId) {
        synchronized (archive) {
            return archive.getRecordingPosition(recordingId);
        }
    }

    private static String pollForErrorResponse(final AeronArchive archive) {
        synchronized (archive) {
            return archive.pollForErrorResponse();
        }
    }

    private static void tryStopRecordingByIdentity(final AeronArchive archive, final long recordingId) {
        synchronized (archive) {
            archive.tryStopRecordingByIdentity(recordingId);
        }
    }

    private static long purgeSegments(
            final AeronArchive archive,
            final long recordingId,
            final long newStartPosition
    ) {
        synchronized (archive) {
            return archive.purgeSegments(recordingId, newStartPosition);
        }
    }

    private static void extendRecording(
            final AeronArchive archive,
            final long recordingId,
            final String channel,
            final int streamId,
            final SourceLocation sourceLocation
    ) {
        synchronized (archive) {
            archive.extendRecording(recordingId, channel, streamId, sourceLocation);
        }
    }

    private static void awaitStopped(
            final AeronArchive archive,
            final long recordingId,
            final AeronReplicationConfiguration configuration
    ) {
        final long deadline = ReplicationRetry.deadlineNanos(configuration.recordingStopTimeoutNanos());
        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        long nextStopProbe = 0L;
        while (true) {
            checkInterrupted("waiting for Aeron Archive recording stop");
            final long now = System.nanoTime();
            if (now >= nextStopProbe) {
                if (getStopPosition(archive, recordingId) >= 0) return;
                nextStopProbe = now + 10_000_000L;
            }
            final String archiveError = pollForErrorResponse(archive);
            if (archiveError != null) throw new IllegalStateException("Aeron archive error: " + archiveError);
            if (ReplicationRetry.expired(deadline)) {
                throw new IllegalStateException("Aeron archive recording did not stop before timeout");
            }
            idle.idle(0);
        }
    }

    private static void checkInterrupted(final String operation) {
        if (Thread.currentThread().isInterrupted()) {
            final InterruptedException interrupted = new InterruptedException(operation);
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while " + operation, interrupted);
        }
    }

    /**
     * Publishes a raw transaction for package-local integration tests and
     * low-level Archive fixtures. Production Store writes must go through the
     * coordinator-backed persistence target so the local acceptance fence is
     * recorded as well.
     *
     * @param dictionary optional type dictionary bytes
     * @param data       Store binary buffers; their positions are read but not changed
     */
    synchronized void publishTransaction(final byte[] dictionary, final java.nio.ByteBuffer[] data) {
        if (this.closed) throw new IllegalStateException("Aeron archive publisher is closed");
        this.publisher.publishTransaction(dictionary, data);
    }

    /**
     * Returns the recording identity, discovering it from Aeron or the Archive
     * catalog when the local counter is unavailable.
     *
     * @return recording identity, or {@link Aeron#NULL_VALUE} when none is known
     */
    public synchronized long recordingId() {
        final CountersReader counters = this.archive.context().aeron().countersReader();
        final int counterId = this.recordingCounterId(counters);
        if (counterId >= 0) {
            return RecordingPos.getRecordingId(counters, counterId);
        }
        if (this.recordingIdHint >= 0) {
            return this.recordingIdHint;
        }
        /* A counter may disappear during shutdown before the Archive catalog has
         * forgotten the recording. Discover it from the catalog so close() can still
         * stop the recording instead of leaking an active entry. */
        return findRecordingId(this.archive, this.publication);
    }

    private int recordingCounterId(final CountersReader counters) {
        final int cached = this.recordingCounterId.get();
        if (cached >= 0 && cached <= counters.maxCounterId()) {
            final long cachedRecordingId = RecordingPos.getRecordingId(counters, cached);
            if (cachedRecordingId >= 0 && (this.recordingIdHint < 0 || cachedRecordingId == this.recordingIdHint)) {
                return cached;
            }
        }
        int counterId = RecordingPos.findCounterIdBySession(counters, this.publication.sessionId(), this.archive.archiveId());
        if (counterId < 0 && this.recordingIdHint >= 0) {
            counterId = RecordingPos.findCounterIdByRecording(counters, this.recordingIdHint, this.archive.archiveId());
        }
        this.recordingCounterId.set(counterId);
        return counterId;
    }

    /**
     * Returns whether publication or durability failure made this writer fail closed.
     *
     * @return {@code true} when the writer must reject further transactions
     */
    public boolean isFailed() {
        return this.publisher.isFailed();
    }

    /**
     * Suspends recording while complete historical segments are purged, then
     * extends the same recording at the exact stop position before writes resume.
     * The caller must hold the write coordinator monitor for the complete call;
     * otherwise a publication offer could land in the stop/extend gap.
     *
     * @param newStartPosition first retained Archive position
     * @return number of segment files deleted
     */
    public synchronized long purgeSegmentsWhileWritesPaused(final long newStartPosition) {
        if (this.closed) throw new IllegalStateException("Aeron archive publisher is closed");
        if (this.publisher.hasPendingTransaction()) {
            throw new IllegalStateException("cannot suspend Aeron recording with a prepared transaction");
        }
        final long recordingId = this.recordingId();
        if (recordingId < 0) throw new IllegalStateException("cannot determine active Aeron recording identity");
        boolean stopped = getStopPosition(this.archive, recordingId) >= 0;
        Throwable operationFailure = null;
        long deleted = 0L;
        try {
            if (!stopped) {
                tryStopRecordingByIdentity(this.archive, recordingId);
                awaitStopped(this.archive, recordingId, this.configuration);
                stopped = true;
            }
            deleted = purgeSegments(this.archive, recordingId, newStartPosition);
        } catch (final RuntimeException | Error failure) {
            operationFailure = failure;
        } finally {
            if (!stopped) {
                try {
                    stopped = getStopPosition(this.archive, recordingId) >= 0;
                } catch (final RuntimeException inspectionFailure) {
                    /* Reached only when the try block above already failed, so a
                     * primary failure is always present to suppress into. */
                    if (operationFailure != inspectionFailure) {
                        operationFailure.addSuppressed(inspectionFailure);
                    }
                }
            }
            if (stopped) {
                try {
                    final long stopPosition = getStopPosition(this.archive, recordingId);
                    if (stopPosition < 0) {
                        throw new IllegalStateException("Aeron recording has no stop position after maintenance");
                    }
                    final String extensionChannel = new ChannelUriStringBuilder(this.publication.channel())
                            .sessionId(this.publication.sessionId())
                            .initialPosition(stopPosition, this.publication.initialTermId(),
                                    this.publication.termBufferLength())
                            .build();
                    extendRecording(this.archive, recordingId, extensionChannel,
                            this.publication.streamId(), this.sourceLocation);
                    this.recordingCounterId.set(-1);
                    awaitRecordingStarted(this.archive, this.publication, recordingId, this.configuration);
                } catch (final RuntimeException | Error resumeFailure) {
                    this.publisher.failClosed();
                    if (operationFailure == null) operationFailure = resumeFailure;
                    else operationFailure.addSuppressed(resumeFailure);
                }
            } else {
                /* A failed stop request may have reached the Archive even when its
                 * response was lost. If its state cannot be established, admitting another
                 * offer could create an unrecorded gap. */
                this.publisher.failClosed();
            }
        }
        if (operationFailure instanceof Error fatal) throw fatal;
        if (operationFailure instanceof RuntimeException failure) throw failure;
        return deleted;
    }

    /**
     * Returns whether this publisher still owns an active Archive recording.
     *
     * <p>The retention controller uses this boundary to refuse segment deletion
     * while publication can still append to the recording.</p>
     */
    synchronized boolean recordingIsActive() {
        final CountersReader counters = this.archive.context().aeron().countersReader();
        final int counterId = this.recordingCounterId(counters);
        if (counterId >= 0) {
            return RecordingPos.isActive(counters, counterId, RecordingPos.getRecordingId(counters, counterId));
        }
        /* The local counter can disappear during driver shutdown while the Archive
         * catalog still knows the recording.  Treat an unknown stop position as
         * active; retention must fail closed rather than purge a live recording. */
        final long id = this.recordingId();
        if (id < 0) return true;
        try {
            return getStopPosition(this.archive, id) < 0;
        } catch (final RuntimeException failure) {
            return true;
        }
    }

    ExclusivePublication publication() {
        return this.publication;
    }

    /**
     * Creates a coordinator with a local write-admission predicate.
     *
     * @param durabilityMode ordering between local acceptance and Archive
     * @param writer         receiver for checkpoint transitions
     * @param writeAdmission predicate receiving payload plus dictionary bytes before local acceptance
     * @return a coordinator backed by this publisher
     */
    public AeronReplicationWriteCoordinator newWriteCoordinator(
            final ReplicationDurabilityMode durabilityMode,
            final CheckpointWriter writer,
            final LongPredicate writeAdmission) {
        if (writer == null) throw new NullPointerException("writer");
        return new AeronReplicationWriteCoordinator(this.publisher, durabilityMode, writer,
                writeAdmission);
    }

    /**
     * Aligns the next transaction with a sequence recovered from the Store.
     *
     * @param nextSequence next sequence that may be published
     */
    public void synchronizeNextSequence(final long nextSequence) {
        this.publisher.synchronizeNextSequence(nextSequence);
    }

    /** Stops the recording, aborts any pending transaction, and closes the publication. */
    @Override
    public synchronized void close() {
        if (this.closed) return;
        RuntimeException failure = null;
        Error fatalFailure = null;
        long recordingId = Aeron.NULL_VALUE;
        try {
            recordingId = this.recordingId();
            if (recordingId < 0 && this.recordingIdHint >= 0) {
                recordingId = this.recordingIdHint;
            }
        } catch (final RuntimeException recordingFailure) {
            failure = recordingFailure;
        } catch (final Error recordingFailure) {
            fatalFailure = recordingFailure;
        }
        if (!this.publisher.isClosed()) {
            try {
                /* Abort pending data before stopping the recording, otherwise the abort
                 * marker can be offered to an already-stopped recording. */
                this.publisher.close();
            } catch (final RuntimeException closeFailure) {
                if (failure == null) failure = closeFailure;
                else failure.addSuppressed(closeFailure);
            } catch (final Error closeFailure) {
                fatalFailure = closeFailure;
            }
        }
        /* If the pending abort could not be offered, keep the recording alive so a
         * retry can terminate the same sequence instead of making recovery harder. */
        if (this.publisher.hasPendingTransaction()) {
            final IllegalStateException pendingFailure = new IllegalStateException(
                    "cannot stop Aeron recording while pending abort is unresolved", failure);
            if (fatalFailure != null) {
                fatalFailure.addSuppressed(pendingFailure);
                throw fatalFailure;
            }
            throw pendingFailure;
        }
        /* RecordingPos can disappear while the publication is being closed.  Retry
         * discovery before stopping the Archive; otherwise a successful publisher
         * close would leak an active recording that no later code can identify. */
        if (recordingId < 0) {
            try {
                recordingId = this.recordingId();
            } catch (final RuntimeException rediscoveryFailure) {
                if (failure == null) failure = rediscoveryFailure;
                else failure.addSuppressed(rediscoveryFailure);
            } catch (final Error rediscoveryFailure) {
                if (fatalFailure == null) fatalFailure = rediscoveryFailure;
                else fatalFailure.addSuppressed(rediscoveryFailure);
            }
            if (recordingId < 0 && this.recordingIdHint >= 0) recordingId = this.recordingIdHint;
        }
        if (recordingId < 0) {
            final IllegalStateException identityFailure = new IllegalStateException(
                    "cannot determine Aeron Archive recording identity; refusing to close as successful");
            if (failure == null) failure = identityFailure;
            else failure.addSuppressed(identityFailure);
        }
        if (recordingId >= 0 && !this.recordingStopped) {
            try {
                tryStopRecordingByIdentity(this.archive, recordingId);
            } catch (final RuntimeException stopFailure) {
                if (failure == null) failure = stopFailure;
                else failure.addSuppressed(stopFailure);
            } catch (final Error stopFailure) {
                if (fatalFailure == null) fatalFailure = stopFailure;
                else fatalFailure.addSuppressed(stopFailure);
            }
        }
        if (recordingId >= 0) {
            try {
                /* A false response means the recording was already inactive, not that
                 * its terminal position is safely observable. Await the same postcondition
                 * after both outcomes. */
                awaitStopped(this.archive, recordingId, this.configuration);
                this.recordingStopped = true;
            } catch (final RuntimeException stopFailure) {
                if (failure == null) failure = stopFailure;
                else failure.addSuppressed(stopFailure);
            } catch (final Error stopFailure) {
                if (fatalFailure == null) fatalFailure = stopFailure;
                else fatalFailure.addSuppressed(stopFailure);
            }
        }
        /* A successful stop is the durable ownership boundary. If stopping failed,
         * retain this wrapper as open even when the local publication was already
         * closed: the provider must retain the writer so a later close can retry the
         * Archive operation instead of abandoning an active recording. */
        if (fatalFailure != null) {
            if (failure != null) fatalFailure.addSuppressed(failure);
            throw fatalFailure;
        }
        if (failure != null) {
            throw new IllegalStateException("failed to close Aeron archive publisher", failure);
        }
        /* Reaching here means the stop postcondition above succeeded (or no
         * recording identity existed, which already threw), so recordingStopped
         * is true and only the publication state remains. */
        this.closed = this.publisher.isClosed();
    }

    /**
     * Returns whether this wrapper has released its local publication and stopped
     * its Archive recording.
     *
     * <p>A close operation can release the local publication yet remain open when
     * the Archive stop cannot be confirmed. Callers must retain the wrapper and
     * retry {@link #close()} in that state.</p>
     *
     * @return {@code true} only when the publication is closed and the recording
     * has a confirmed stop position
     */
    public synchronized boolean isClosed() {
        return this.closed;
    }

    /** Persists the writer checkpoint after an Archive position is known. */
    @FunctionalInterface
    public interface CheckpointWriter {
        /**
         * Receives a writer transition and the metadata needed for restart.
         * Non-terminal states are deliberately useful for diagnosis only; restart
         * must not treat them as committed data.
         *
         * @param state          state reached by the writer
         * @param sequence       transaction sequence
         * @param dataLength     Store binary length
         * @param dataChunkCount Store binary chunk count
         * @param dataCrc32c     Store binary checksum
         * @param position       Archive position of the terminal marker, or {@code -1}
         */
        void onState(AeronReplicationCheckpoint.State state, long sequence, int dataLength,
                     int dataChunkCount, int dataCrc32c, long position);

        /** Removes a local acceptance fence for a Store write that was rejected. */
        default void clearEnqueueFence() {
        }
    }

}
