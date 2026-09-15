package peruncs.datagrid.cluster.node.aeron;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveEvent;
import io.aeron.archive.client.ArchiveException;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.TimeoutException;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import peruncs.datagrid.cluster.node.node.NodelibraryPropertiesProvider;
import peruncs.datagrid.cluster.node.replication.*;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronAuthenticatedWatermark;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpointStore;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCursor;
import peruncs.datagrid.cluster.storage.aeron.reader.CursorSnapshot;
import peruncs.datagrid.cluster.storage.aeron.reader.ReaderDeliveryListener;
import peruncs.datagrid.cluster.storage.aeron.reader.StorageBinaryDataClientAeronArchive;
import peruncs.datagrid.cluster.storage.aeron.writer.AeronArchiveReplicationPublisher;
import peruncs.datagrid.cluster.storage.aeron.writer.AeronReplicationWriteCoordinator;
import peruncs.datagrid.cluster.storage.aeron.writer.AeronStorageBinaryTargetDistributing;
import peruncs.datagrid.cluster.storage.types.AtomicFileStore;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataDistributor;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataReceiver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

import static java.lang.System.Logger.Level.WARNING;

/// Creates the Aeron cluster replication transport.
///
/// The returned transport creates its driver and Archive lazily. The
/// transport owns those shared resources and releases them from
/// [ClusterReplicationTransport#close()]; individual readers and
/// distributors do not close the runtime.
public final class AeronClusterReplicationTransportProvider {
    /* Test-only, thread-confined seam used by the forked crash harness. */
    private static final System.Logger LOGGER =
            System.getLogger(AeronClusterReplicationTransportProvider.class.getName());
    private static final ThreadLocal<BiConsumer<String, Long>> CRASH_HOOK = new ThreadLocal<>();
    private static final ThreadLocal<Long> CHECKPOINT_SEQUENCE = new ThreadLocal<>();
        /// Creates a provider that reads Aeron settings when a transport is created.
    public AeronClusterReplicationTransportProvider() {
    }

    static void setCrashHook(final BiConsumer<String, Long> hook) {
        if (hook == null) CRASH_HOOK.remove();
        else CRASH_HOOK.set(hook);
    }

    static void clearCrashHook() {
        CRASH_HOOK.remove();
    }

    private static void crashPoint(final String name, final long sequence) {
        final BiConsumer<String, Long> hook = CRASH_HOOK.get();
        if (hook != null) hook.accept(name, sequence);
    }

    static long currentCheckpointSequence() {
        final Long sequence = CHECKPOINT_SEQUENCE.get();
        return sequence == null ? -1L : sequence;
    }

    /* Package-private forked-test seam. Keeping the private Transport type hidden
     * avoids adding a production lifecycle interface solely for fault injection. */
    static void stopDriverForTest(final ClusterReplicationTransport transport) {
        if (!(transport instanceof Transport aeronTransport))
            throw new IllegalArgumentException("transport was not created by the Aeron provider");
        aeronTransport.stopDriver();
    }

        /// Creates a transport with settings read from the node properties.
    ///
    /// @param properties node configuration
    /// @return Aeron replication transport
    public ClusterReplicationTransport create(final NodelibraryPropertiesProvider properties) {
        final Transport transport = new Transport(AeronSettings.fromEnvironment(properties));
        /* Probe metadata durability before any synchronized runtime startup path. */
        transport.verifyMetadataStorage();
        return transport;
    }

        /// Owns the Aeron resources for one node and one transport lifecycle.
    private static final class Transport implements ClusterReplicationTransport {
        private final AeronSettings settings;
        private final AeronArchiveCapacity archiveCapacity;
        private final AtomicLong nextSequence = new AtomicLong();
        private final AtomicLong readerRecordingId = new AtomicLong(Aeron.NULL_VALUE);
                /// Recording identity selected at writer startup; retained when RecordingPos is briefly unavailable.
        private final AtomicLong writerRecordingId = new AtomicLong();
                /// First asynchronous MediaDriver failure; health must not hide it.
        private final AtomicReference<RuntimeException> driverFailure = new AtomicReference<>();
        private final AtomicLong rejectedWatermarks = new AtomicLong();
        /* A reader can publish its last durable cursor while the writer is still
         * recovering its Archive recording. Keep one authenticated value per reader
         * until the writer boundary exists instead of dropping that acknowledgement. */
        private final java.util.concurrent.ConcurrentHashMap<UUID, AeronAuthenticatedWatermark>
                deferredWatermarks = new java.util.concurrent.ConcurrentHashMap<>();
        private final ThreadLocal<Boolean> deliveryCallback = new ThreadLocal<>();
                /// One transport-owned copy; avoids cloning the configured HMAC key for every cursor.
        private byte[] retentionSecret;
        private volatile AeronRuntime runtime;
        private volatile AeronArchiveReplicationPublisher writer;
        private volatile AeronReplicationWriteCoordinator coordinator;
        private volatile StorageBinaryDataClientAeronArchive reader;
        /* Published together after the terminal checkpoint is durable.  Readers of
         * positionProvider() must never combine fields from two transactions. */
        private volatile AeronWriterBoundary writerBoundary = new AeronWriterBoundary(-1, -1, -1);
        private volatile boolean writerRecoveryInProgress;
        private volatile ReplicationHealth.State writerRecoveryState;
        private ClusterStorageBinaryDataDistributor distributor;
        private String distributorStream;
        private ReplicationPositionProvider positionProvider;
        private ReplicationLogRetention retention;
        private AeronWatermarkChannel watermarkChannel;
        private AeronHealth health;
        private volatile boolean closed;
        private volatile boolean closing;

        private Transport(final AeronSettings settings) {
            this.settings = settings;
            this.archiveCapacity = new AeronArchiveCapacity(settings);
            this.retentionSecret = settings.retentionSecretUnsafe();
            this.writerRecordingId.set(settings.recordingId());
        }

        private static ReseedRequiredException reseedRequired(final String message,
                                                              final Throwable cause) {
            return cause == null ? new ReseedRequiredException(message) :
                    new ReseedRequiredException(message, cause);
        }

        private static boolean isTerminalArchiveWarning(final Throwable failure) {
            return failure instanceof ArchiveEvent && failure.getMessage() != null &&
                   failure.getMessage().contains("control response publication is not connected");
        }

        private static RuntimeException appendFailure(final RuntimeException current,
                                                      final Throwable additional) {
            if (additional == null) return current;
            final RuntimeException normalized = additional instanceof RuntimeException runtime
                    ? runtime : new IllegalStateException("Aeron transport resource close failed", additional);
            if (current == null) return normalized;
            if (current != normalized) current.addSuppressed(normalized);
            return current;
        }

        private static Throwable appendCloseFailure(final Throwable current, final Throwable additional) {
            if (additional == null) return current;
            if (current == null) return additional;
            if (current != additional) current.addSuppressed(additional);
            return current;
        }

        private static boolean isArchiveUnavailable(final Throwable failure) {
            for (Throwable current = failure; current != null; current = current.getCause()) {
                if (current instanceof ArchiveException || current instanceof TimeoutException) {
                    return true;
                }
                final String message = current.getMessage();
                if (message != null && (message.contains("connection to the archive is no longer available") ||
                                        message.contains("awaiting response"))) {
                    return true;
                }
            }
            return false;
        }

        private void verifyMetadataStorage() {
            final Path parent = this.settings.checkpointPath().toAbsolutePath().getParent();
            if (parent == null) throw new IllegalArgumentException("Aeron checkpoint path must have a parent directory");
            if (Files.isSymbolicLink(this.settings.checkpointPath())) {
                throw new IllegalArgumentException(
                        "Aeron checkpoint path must not be a symbolic link: %s".formatted(this.settings.checkpointPath()));
            }
            /* Apply the same production permission policy before the capability probe
             * creates any temporary metadata files.  Otherwise a non-POSIX filesystem
             * would pass this early probe and fail only later during runtime startup. */
            AeronRuntime.ensurePrivateDirectory(parent, this.settings.productionMode());
            try {
                AtomicFileStore.verify(this.settings.checkpointPath());
            } catch (final IOException failure) {
                throw new IllegalStateException(
                        "Aeron replication metadata storage does not support atomic replacement", failure);
            }
        }

                /// Claims the provider's single configured replication stream.
        private void claimStream(final String streamName) {
            if (streamName == null || streamName.isBlank()) {
                throw new IllegalArgumentException("Aeron replication stream name must not be blank");
            }
            if (this.distributorStream == null) {
                this.distributorStream = streamName;
            } else if (!this.distributorStream.equals(streamName)) {
                throw new IllegalArgumentException(
                        "Aeron transport is configured for stream %s, not %s".formatted(this.distributorStream, streamName));
            }
        }

        @Override
        public String id() {
            return "aeron";
        }

        /// Returns the distributor for the provider's single replication stream.
        ///
        /// The returned handle carries dictionaries and lifecycle control
        /// only: data publication is deliberately unavailable through it and
        /// flows exclusively through the persistence-target factory, where
        /// local Store acceptance and checkpoint fencing are one serialized
        /// operation. Asynchronous distribution is rejected, and a second
        /// stream name is rejected — one provider owns exactly one stream.
        ///
        /// @param streamName logical stream name, claimed on first use
        /// @param asynchronous must be `false`
        /// @return binary distributor
        @Override
        public synchronized ClusterStorageBinaryDataDistributor distributor(
                final String streamName,
                final boolean asynchronous
        ) {
            /* Aeron data publication is intentionally available only through the
             * persistence target below, where local acceptance and checkpoint fencing
             * are one serialized operation. The distributor remains the dictionary and
             * lifecycle control object required by the neutral Store integration. */
            this.ensureOpen();
            this.claimStream(streamName);
            if (asynchronous) {
                throw new IllegalArgumentException(
                        "Aeron replication does not support asynchronous distribution; use the bounded Store write path");
            }
            if (this.distributor != null && java.util.Objects.equals(this.distributorStream, streamName)) {
                return this.distributor;
            }
            if (this.distributor != null) {
                throw new IllegalStateException("Aeron transport supports one replication stream per provider");
            }
            this.distributor = new AeronDistributor(
                    () -> "writer".equals(this.settings.role()),
                    value ->
                    {
                        this.nextSequence.accumulateAndGet(value, Math::max);
                        if (this.writer != null) this.writer.synchronizeNextSequence(value);
                    });
            return this.distributor;
        }

        @Override
        public synchronized UnaryOperator<PersistenceTarget<Binary>> persistenceTargetFactory(
                final String streamName, final StorageBinaryDataDistributor distributor) {
            this.ensureOpen();
            this.claimStream(streamName);
            if (!"writer".equals(this.settings.role())) {
                return UnaryOperator.identity();
            }
            final AeronReplicationWriteCoordinator coordinator = this.ensureCoordinator();
            return delegate -> new AeronStorageBinaryTargetDistributing(
                    delegate,
                    coordinator,
                    distributor,
                    sequence ->
                    {
                        if (distributor instanceof ClusterStorageBinaryDataDistributor cluster) {
                            cluster.messageIndex(sequence);
                        }
                    },
                    () -> !(distributor instanceof ClusterStorageBinaryDataDistributor cluster) || !cluster.ignoreDistribution()
            );
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
        /// @param packetAcceptor destination for received packets
        /// @param streamName logical stream name
        /// @param cursorListener callback after data is applied
        /// @param startingCursor durable starting cursor
        /// @param commitPosition whether reader positions are committed
        /// @return binary data client
        @Override
        public synchronized ClusterStorageBinaryDataClient client(
                final ClusterStorageBinaryDataPacketAcceptor packetAcceptor,
                final String streamName,
                final AfterDataMessageConsumedListener cursorListener,
                final ReplicationCursor startingCursor,
                final boolean commitPosition
        ) {
            this.ensureOpen();
            this.claimStream(streamName);
            /* A writer owns publication only. Even with an external Archive it must not
             * create a second subscription against its own recording; that would violate
             * the one-writer/N-reader topology and expose a partially initialized reader
             * through the writer's health path. Configure a separate reader/backup-reader
             * node when replay is required. */
            if ("writer".equals(this.settings.role())) {
                return ClusterStorageBinaryDataClient.NoOp(startingCursor);
            }
            if (packetAcceptor == null) throw new NullPointerException("packetAcceptor");
            if (this.reader != null) {
                final StorageBinaryDataClientAeronArchive previous = this.reader;
                previous.dispose();
                this.reader = null;
            }
            this.ensureRuntime();
            final long recordingId = this.settings.recordingId() >= 0
                    ? this.settings.recordingId() : this.discoverReaderRecordingId();
            this.readerRecordingId.set(recordingId);
            this.rejectUncertainReaderImport(recordingId);
            final ReplicationCursor cursor = startingCursor == null
                    ? new ReplicationCursor("aeron", null, -1, new byte[0]) : startingCursor;
            final boolean aeronCursor = "aeron".equalsIgnoreCase(cursor.transport());
            if (!aeronCursor && !"none".equalsIgnoreCase(cursor.transport())) {
                throw new IllegalArgumentException("cursor belongs to transport %s".formatted(cursor.transport()));
            }
            if (!aeronCursor && cursor.providerPosition().length != 0) {
                throw new IllegalArgumentException("an uninitialized cursor cannot carry Aeron provider state");
            }
            if (aeronCursor && cursor.storeGeneration() != null &&
                !this.settings.storeGeneration().equals(cursor.storeGeneration())) {
                throw new IllegalArgumentException("cursor store generation does not match Aeron configuration");
            }
            if (aeronCursor && cursor.logicalSequence() >= 0 && cursor.storeGeneration() == null) {
                throw new IllegalArgumentException("resolved Aeron cursor must identify its Store generation");
            }
            final AtomicReference<StorageBinaryDataClientAeronArchive> readerRef = new AtomicReference<>();
            final long cursorPosition = this.cursorPosition(cursor, recordingId);
            if (aeronCursor && cursor.logicalSequence() >= 0 && cursorPosition < 0) {
                throw new IllegalArgumentException("Aeron cursor has a sequence but no recording position");
            }
            final StorageBinaryDataClientAeronArchive replacement;
            try {
                replacement = StorageBinaryDataClientAeronArchive.New(
                        this.aeron(),
                        archiveContext(),
                        recordingId,
                        aeronCursor && cursorPosition >= 0
                                ? cursorPosition
                                : io.aeron.archive.client.PersistentSubscription.FROM_START,
                        this.settings.liveChannel(),
                        this.settings.streamId(),
                        this.settings.replayChannel(),
                        this.settings.streamId() + 1,
                        this.settings.replication(),
                        this.settings.clusterId(),
                        this.settings.epoch(),
                        aeronCursor ? cursor.logicalSequence() : -1,
                        new ReceiverAdapter(this, packetAcceptor),
                        () ->
                        {
                            final StorageBinaryDataClientAeronArchive current = readerRef.get();
                            if (current != null && current == this.reader) {
                                final CursorSnapshot snapshot = current.cursorSnapshot();
                                this.nextSequence.accumulateAndGet(snapshot.sequence() + 1, Math::max);
                                /* Aeron has no broker offset to commit. Its cursor is the
                                 * durability boundary for every reader, including ordinary readers;
                                 * the neutral commitPosition flag only controls broker transports. */
                                if (cursorListener != null) {
                                    /* Persist the local recovery cursor before advertising the same
                                     * boundary to the writer's retention controller. */
                                    final byte[] position = new AeronReplicationCursor(
                                            this.settings.clusterId(), this.settings.nodeId(), this.settings.storeGeneration(),
                                            this.settings.epoch(), recordingId, snapshot.position(), snapshot.sequence()).encode();
                                    this.enterDeliveryCallback();
                                    try {
                                        cursorListener.onApplied(new ReplicationCursor("aeron",
                                                this.settings.storeGeneration(), snapshot.sequence(), position));
                                    } finally {
                                        this.exitDeliveryCallback();
                                    }
                                }
                                /* Watermark delivery is independent from the optional neutral
                                 * cursor callback. A direct Aeron reader may not install a
                                 * persistence listener, but it must still advance the writer's
                                 * retention quorum after a durable import. */
                                this.publishReaderWatermark(snapshot, recordingId);
                            }
                        },
                        this.readerDeliveryListener(),
                        aeronCursor ? cursorPosition : -1
                );
            } catch (final RuntimeException | Error failure) {
                this.reader = null;
                throw failure;
            }
            this.reader = replacement;
            readerRef.set(replacement);
            /* Re-advertise the durable cursor when a reader restarts even if no new
             * transaction arrives. Otherwise a restarted writer cannot rebuild its
             * retention quorum until unrelated Store traffic happens. */
            if (aeronCursor && cursor.logicalSequence() >= 0) {
                this.publishReaderWatermark(
                        new CursorSnapshot(cursor.logicalSequence(), cursorPosition), recordingId);
            }
            return new ClientAdapter(replacement, recordingId, this.settings.clusterId(),
                    this.settings.nodeId(), this.settings.storeGeneration(), this.settings.epoch());
        }

                /// Decodes and validates the sole supported self-describing Aeron cursor.
        private long cursorPosition(final ReplicationCursor cursor, final long expectedRecordingId) {
            final byte[] positionBytes = cursor.providerPosition();
            if (positionBytes.length == 0) return -1L;
            try {
                final AeronReplicationCursor aeron = AeronReplicationCursor.decode(positionBytes);
                if (!aeron.clusterId().equals(this.settings.clusterId()) ||
                    !aeron.storeGeneration().equals(this.settings.storeGeneration()) ||
                    aeron.epoch() != this.settings.epoch() || aeron.recordingId() != expectedRecordingId ||
                    aeron.sequence() != cursor.logicalSequence()) {
                    throw new ReseedRequiredException(
                            "Aeron cursor identity does not match the configured reader or writer");
                }
                return aeron.recordingPosition();
            } catch (final IllegalArgumentException failure) {
                throw new ReseedRequiredException("invalid Aeron cursor", failure);
            }
        }

        private long discoverReaderRecordingId() {
            final io.aeron.ChannelUri live = io.aeron.ChannelUri.parse(this.settings.liveChannel());
            final String alias = live.get(io.aeron.CommonContext.ALIAS_PARAM_NAME);
            if (alias == null || alias.isBlank()) {
                throw new IllegalStateException(
                        "Aeron recording discovery requires alias= on ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL");
            }
            final AtomicLong discovered = new AtomicLong(Aeron.NULL_VALUE);
            final int count = this.listRecordingsForUri("alias=%s".formatted(alias), this.settings.streamId(),
                    (controlSessionId, correlationId, recordingId, startTimestamp,
                     stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength,
                     termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                     sourceIdentity) -> discovered.set(recordingId));
            if (count != 1 || discovered.get() < 0) {
                throw new IllegalStateException(
                        "Aeron recording discovery requires exactly one alias=%s recording for stream %s; found %s".formatted(alias, this.settings.streamId(), count));
            }
            return discovered.get();
        }

        private Path readerUncertaintyPath() {
            return this.settings.checkpointPath().resolveSibling(
                    "%s.reader-inflight".formatted(this.settings.checkpointPath().getFileName()));
        }

        private void rejectUncertainReaderImport(final long recordingId) {
            final Path path = this.readerUncertaintyPath();
            try {
                final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(path);
                if (checkpoint.recordType() != AeronReplicationCheckpoint.RecordType.READER_CURSOR ||
                    checkpoint.state() != AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN ||
                    !checkpoint.clusterId().equals(this.settings.clusterId()) ||
                    !checkpoint.nodeId().equals(this.settings.nodeId()) ||
                    !checkpoint.storeGeneration().equals(this.settings.storeGeneration()) ||
                    checkpoint.recordingId() != recordingId ||
                    checkpoint.writerEpoch() != this.settings.epoch()) {
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
            return new ReaderDeliveryListener() {
                @Override
                public void beforeStoreImport(final long sequence, final long position, final int dataLength,
                                              final int dataChunkCount, final int crc32c) {
                    enterDeliveryCallback();
                    try {
                        final AeronReplicationCheckpoint checkpoint = new AeronReplicationCheckpoint(
                                AeronReplicationCheckpoint.RecordType.READER_CURSOR,
                                AeronReplicationCheckpoint.DurabilityMode.ARCHIVE_FIRST,
                                AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN,
                                settings.clusterId(), settings.nodeId(), settings.storeGeneration(), readerRecordingId.get(),
                                settings.epoch(), sequence, position, dataLength, dataChunkCount, crc32c);
                        AeronReplicationCheckpointStore.write(path, checkpoint);
                    } catch (final IOException failure) {
                        throw reseedRequired("cannot persist uncertain reader import marker: %s".formatted(path), failure);
                    } finally {
                        exitDeliveryCallback();
                    }
                }

                @Override
                public void afterStoreImport() {
                    enterDeliveryCallback();
                    try {
                        /* The cursor has already been forced successfully. A directory force
                         * here only makes marker removal durable; omitting it is safe because
                         * a stale marker fails closed and requests a reseed after a crash. */
                        AtomicFileStore.delete(path, false);
                    } catch (final IOException failure) {
                        throw reseedRequired("cannot clear uncertain reader import marker: %s".formatted(path), failure);
                    } finally {
                        exitDeliveryCallback();
                    }
                }
            };
        }

        @Override
        public synchronized ReplicationPositionProvider positionProvider(final String streamName) {
            this.ensureOpen();
            this.claimStream(streamName);
            if (this.positionProvider == null) {
                this.positionProvider = new peruncs.datagrid.cluster.node.aeron.AeronPositionProvider(
                        () -> "writer".equals(this.settings.role()),
                        () -> this.writer != null && !this.writerRecoveryInProgress && !this.closed,
                        this::ensureWriter,
                        () -> this.writerBoundary,
                        this.settings::clusterId,
                        this.settings::nodeId,
                        this.settings::storeGeneration,
                        this.settings::epoch);
            }
            return this.positionProvider;
        }

        /// Returns the authenticated Archive retention controller.
        ///
        /// Without a shared retention secret and an embedded Archive there is
        /// nothing safe to delete, so retention reports unsupported and history
        /// is preserved rather than risking an unauthenticated purge.
        ///
        /// @return retention controller
        @Override
        public synchronized ReplicationLogRetention retention() {
            this.ensureOpen();
            if (this.retention != null) return this.retention;
            final byte[] retentionSecret = this.retentionSecret;
            if (!this.retentionSupported()) {
                /* Retention without a shared authentication key and an embedded Archive
                 * cannot prove that every reader has crossed the requested boundary. */
                this.retention = new ReplicationLogRetention() {
                    @Override
                    public boolean isSupported() {
                        return false;
                    }

                    @Override
                    public MaintenanceResult deleteThrough(final ReplicationCursor cursor) {
                        throw new UnsupportedOperationException(
                                "Aeron Archive retention requires an embedded writer and an authenticated watermark secret");
                    }

                    @Override
                    public void close() {
                    }
                };
                return this.retention;
            }
            final AeronArchiveRetention created = this.newRetentionController(retentionSecret);
            /* Publish the controller before runtime startup because the writer-side
             * watermark setup consults this field.  If startup fails, however, do not
             * leave a closed/broken controller cached for the next retention() call. */
            this.retention = created;
            try {
                this.ensureRuntime();
                return created;
            } catch (final RuntimeException | Error failure) {
                this.retention = null;
                try {
                    created.close();
                } catch (final RuntimeException | Error closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }

        private AeronArchiveRetention newRetentionController(final byte[] retentionSecret) {
            return new AeronArchiveRetention(retentionSecret, this.settings.retentionReaders(),
                    () ->
                    {
                        if (!this.writerReady())
                            throw new IllegalStateException("Aeron writer must be running before retention maintenance");
                    }, new AeronArchiveRetention.RecordingPositions(
                    this::getStartPosition,
                    this::getStopPosition,
                    this::getRecordingPosition),
                    this.writerRecordingId::get,
                    () -> this.writerBoundary, boundary ->
            {
                final AeronReplicationWriteCoordinator currentCoordinator = this.coordinator;
                final AeronArchiveReplicationPublisher currentWriter = this.writer;
                if (currentCoordinator == null || currentWriter == null) {
                    throw new IllegalStateException(
                            "Aeron retention requires the coordinator-backed Store writer");
                }
                return currentCoordinator.withWritesPaused(
                        () -> currentWriter.purgeSegmentsWhileWritesPaused(boundary));
            }, this.settings.clusterId(), this.settings.storeGeneration(),
                    this.settings.epoch(), this.settings.replication()::termLength,
                    this.settings::archiveSegmentFileLength,
                    () -> this.watermarkChannel != null && this.watermarkChannel.available(),
                    this.settings.checkpointPath().resolveSibling(
                            "%s.retention".formatted(this.settings.checkpointPath().getFileName())));
        }

        private void publishReaderWatermark(final CursorSnapshot snapshot, final long recordingId) {
            final AeronWatermarkChannel channel = this.watermarkChannel;
            final byte[] secret = this.retentionSecret;
            if (channel == null || secret == null) return;
            channel.publishEncoded(this.settings.nodeId(), this.settings.clusterId(), this.settings.storeGeneration(),
                    this.settings.epoch(), recordingId, snapshot.sequence(), snapshot.position(), secret);
        }

        private void ensureWatermarkChannel() {
            if (this.watermarkChannel != null || this.retentionSecret == null) return;
            if ("writer".equals(this.settings.role())) {
                if (!this.retentionSupported()) return;
                if (this.retention == null) {
                    this.retention = this.newRetentionController(this.retentionSecret);
                }
                final AeronArchiveRetention controller = (AeronArchiveRetention) this.retention;
                this.watermarkChannel = AeronWatermarkChannel.writer(this.aeron(),
                        this.settings.watermarkChannel(), this.settings.watermarkStreamId(), (encoded, offset, length) ->
                        {
                            try {
                                final AeronAuthenticatedWatermark watermark =
                                        AeronAuthenticatedWatermark.decode(encoded, offset, length);
                                /* A reader may publish its cursor while the writer is still
                                 * recovering its recording. Keep one authenticated value per
                                 * configured reader instead of losing the only acknowledgement. */
                                if (!this.writerReady()) {
                                    if (this.settings.retentionReaders().contains(watermark.readerId()) &&
                                        watermark.verify(this.retentionSecret) &&
                                        watermark.clusterId().equals(this.settings.clusterId()) &&
                                        watermark.storeGeneration().equals(this.settings.storeGeneration()) &&
                                        watermark.writerEpoch() == this.settings.epoch()) {
                                        this.deferredWatermarks.put(watermark.readerId(), watermark);
                                    }
                                    return;
                                }
                                controller.recordReaderWatermark(watermark);
                            } catch (final RuntimeException rejected) {
                                /* Reject one malformed, unauthenticated, stale, or future
                                 * watermark without killing delivery of later valid progress. */
                                final long count = this.rejectedWatermarks.incrementAndGet();
                                if ((count & (count - 1)) == 0) {
                                    LOGGER.log(WARNING,
                                            "Rejected Aeron reader watermark count=%s".formatted(count), rejected);
                                }
                            }
                        }, this.settings.replication().offerTimeoutNanos());
            } else {
                this.watermarkChannel = AeronWatermarkChannel.reader(this.aeron(),
                        this.settings.watermarkChannel(), this.settings.watermarkStreamId(),
                        this.settings.replication().offerTimeoutNanos());
            }
        }

                /// Replays authenticated reader progress received during writer recovery.
        private void drainDeferredWatermarks() {
            if (this.deferredWatermarks.isEmpty() || !this.writerReady() || this.retention == null) {
                return;
            }
            final AeronArchiveRetention controller = (AeronArchiveRetention) this.retention;
            for (final var entry : this.deferredWatermarks.entrySet()) {
                try {
                    controller.recordReaderWatermark(entry.getValue());
                    this.deferredWatermarks.remove(entry.getKey(), entry.getValue());
                } catch (final RuntimeException failure) {
                    /* The writer boundary is now available, so a rejected token is
                     * stale or invalid; remove it and wait for the reader's next durable
                     * cursor rather than retrying a bad value forever. */
                    this.deferredWatermarks.remove(entry.getKey(), entry.getValue());
                    final long count = this.rejectedWatermarks.incrementAndGet();
                    if ((count & (count - 1)) == 0) {
                        LOGGER.log(WARNING, "Rejected deferred Aeron reader watermark count=%s".formatted(count), failure);
                    }
                }
            }
        }

                /// Stops the writer-side watermark worker before any writer/retention resource
        /// is closed. The worker invokes retention callbacks and must never race a
        /// transport shutdown while the writer monitor is being dismantled.
        private RuntimeException closeWatermarkChannel() {
            if (this.watermarkChannel == null) return null;
            try {
                this.watermarkChannel.close();
                this.watermarkChannel = null;
                return null;
            } catch (final RuntimeException failure) {
                if (this.watermarkChannel.isClosed()) this.watermarkChannel = null;
                return failure;
            }
        }

        private boolean retentionSupported() {
            return this.retentionSecret != null && !this.settings.retentionReaders().isEmpty() &&
                   "writer".equals(this.settings.role()) && !this.settings.externalArchive();
        }

        @Override
        public synchronized ReplicationHealth health(
                final StorageControllerAdapter storage,
                final ClusterStorageBinaryDataClient client
        ) {
            this.ensureOpen();
            if (this.health == null || !this.health.matches(storage, client)) {
                if (this.health != null) this.health.close();
                this.health = new peruncs.datagrid.cluster.node.aeron.AeronHealth(
                        storage,
                        client,
                        () -> this.closed,
                        () -> this.driverFailure.get() != null,
                        () -> this.archiveCapacity.available(this.settings.replication().maxTransactionBytes()),
                        this::writerReady,
                        () -> "writer".equals(this.settings.role()),
                        this::writerCheckpointState,
                        this.archiveCapacity::usableSpaceBytes,
                        () -> this.writerBoundary.position(),
                        () -> this.writerBoundary.sequence(),
                        () -> this.reader == null ? -1L : this.reader.lastAppliedSequence(),
                        () -> this.watermarkChannel != null && this.watermarkChannel.failure() != null);
            }
            return this.health;
        }

        private boolean writerReady() {
            return "writer".equals(this.settings.role()) && !this.writerRecoveryInProgress &&
                   this.writerRecoveryState == null && this.writer != null && !this.writer.isFailed();
        }

                /// Reports current writer readiness without starting the runtime. Lifecycle
        /// startup belongs to an explicit client or write operation, not a health
        /// probe.

        private synchronized ReplicationHealth.State writerCheckpointState() {
            if (this.driverFailure.get() != null) {
                return ReplicationHealth.State.FAILED;
            }
            if (!"writer".equals(this.settings.role())) {
                return null;
            }
            /* Preserve a failed startup result even though the writer object was never
             * installed. Returning STARTING for a null writer would hide a permanent
             * RESEED_REQUIRED/FAILED state from health probes. */
            if (this.writerRecoveryState != null) return this.writerRecoveryState;
            if (this.writerRecoveryInProgress || this.writer == null) {
                return ReplicationHealth.State.STARTING;
            }
            if (this.writer != null) {
                /* A live writer can fail closed after an offer, await, or checkpoint
                 * error without being discarded immediately.  Do not advertise LIVE
                 * while that publisher can no longer accept a durable transaction. */
                return this.writer.isFailed() ? ReplicationHealth.State.FAILED : null;
            }
            /* A writer with no checkpoint state is healthy once its publisher exists. */
            return null;
        }

        private synchronized AeronArchiveReplicationPublisher ensureWriter() {
            if (this.closing || this.closed) {
                throw new IllegalStateException("Aeron writer cannot start while transport is closing");
            }
            if (this.writer != null) return this.writer;
            this.writerRecoveryInProgress = true;
            try {
                this.ensureRuntime();
                final AeronReplicationCheckpoint checkpoint = this.loadWriterCheckpoint();
                crashPoint("AFTER_RECOVERY_CHECKPOINT_READ",
                        checkpoint == null ? -1L : checkpoint.transactionSequence());
                final long initialSequence = checkpoint == null ? this.nextSequence.get() :
                        checkpoint.transactionSequence() + 1;
                this.nextSequence.set(initialSequence);
                final long recordingId = checkpoint != null ? checkpoint.recordingId() : this.settings.recordingId();
                if (recordingId >= 0) {
                    this.validateRecordingBoundary(recordingId, checkpoint);
                    try {
                        this.writer = (this.settings.externalArchive()
                                ? AeronArchiveReplicationPublisher.ExtendRemote(this.archive(), recordingId,
                                this.settings.streamId(), this.settings.replication(), this.settings.clusterId(),
                                this.settings.epoch(), initialSequence)
                                : AeronArchiveReplicationPublisher.Extend(this.archive(), recordingId,
                                this.settings.streamId(), this.settings.replication(), this.settings.clusterId(),
                                this.settings.epoch(), initialSequence));
                    } catch (final RuntimeException failure) {
                        throw reseedRequired("cannot extend configured recording %s after restart; the Archive recording is not safely reusable".formatted(recordingId), failure);
                    }
                } else {
                    if (checkpoint != null && checkpoint.transactionSequence() >= 0) {
                        throw reseedRequired("writer checkpoint has no recording identity", null);
                    }
                    this.writer = (this.settings.externalArchive()
                            ? AeronArchiveReplicationPublisher.NewRemote(this.archive(), this.settings.liveChannel(),
                            this.settings.streamId(), this.settings.replication(), this.settings.clusterId(),
                            this.settings.epoch(), initialSequence)
                            : AeronArchiveReplicationPublisher.New(this.archive(), this.settings.liveChannel(),
                            this.settings.streamId(), this.settings.replication(), this.settings.clusterId(),
                            this.settings.epoch(), initialSequence));
                }
                final long discoveredRecordingId = this.writer.recordingId();
                this.writerRecordingId.set(discoveredRecordingId >= 0 ? discoveredRecordingId : recordingId);
                if (checkpoint == null && this.writerRecordingId.get() >= 0) {
                    long startPosition = -1L;
                    try {
                        startPosition = this.runtime == null ? -1L :
                                this.getStartPosition(this.writerRecordingId.get());
                    } catch (final ArchiveException failure) {
                        if (failure.errorCode() != ArchiveException.UNKNOWN_RECORDING) {
                            throw reseedRequired("cannot inspect new Aeron recording %s".formatted(this.writerRecordingId.get()), failure);
                        }
                        /* A fresh recording may not expose its catalog position until the
                         * first image is connected. Keep the boundary sequence explicit and
                         * leave the position unknown rather than inventing a byte offset. */
                    }
                    this.writerBoundary = new AeronWriterBoundary(initialSequence - 1,
                            this.writerRecordingId.get(), startPosition);
                }
                this.writerRecoveryState = null;
                /* writerRecoveryInProgress remains true until the finally block below;
                 * publish the recovered boundary before draining deferred reader
                 * watermarks, otherwise writerReady() would reject every queued ACK and
                 * they would remain stranded indefinitely. */
                this.writerRecoveryInProgress = false;
                this.drainDeferredWatermarks();
                return this.writer;
            } catch (final RuntimeException failure) {
                this.writerRecoveryState = failure instanceof ReseedRequiredException
                        ? ReplicationHealth.State.RESEED_REQUIRED : ReplicationHealth.State.FAILED;
                throw failure;
            } catch (final Error failure) {
                this.writerRecoveryState = ReplicationHealth.State.FAILED;
                throw failure;
            } finally {
                this.writerRecoveryInProgress = false;
            }
        }

        private synchronized AeronReplicationWriteCoordinator ensureCoordinator() {
            if (this.coordinator == null) {
                final AeronArchiveReplicationPublisher.CheckpointWriter checkpointWriter =
                        new AeronArchiveReplicationPublisher.CheckpointWriter() {
                            @Override
                            public void onState(final AeronReplicationCheckpoint.State state, final long sequence,
                                                final int dataLength, final int dataChunkCount, final int dataCrc32c, final long position) {
                                persistWriterCheckpoint(state, sequence, dataLength, dataChunkCount, dataCrc32c, position);
                            }

                            @Override
                            public void clearEnqueueFence() {
                                try {
                                    AtomicFileStore.delete(inFlightCheckpointPath());
                                } catch (final IOException failure) {
                                    throw new IllegalStateException("cannot clear local enqueue fence", failure);
                                }
                            }
                        };
                this.coordinator = this.ensureWriter().newWriteCoordinator(
                        this.settings.replication().durabilityMode(), checkpointWriter,
                        this.archiveCapacity::available);
            }
            return this.coordinator;
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
                final AeronReplicationCheckpoint terminal = this.readOptionalCheckpoint(this.settings.checkpointPath());
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
                        AtomicFileStore.delete(inFlightPath);
                    } catch (final IOException failure) {
                        throw reseedRequired("cannot clear covered in-flight writer checkpoint", failure);
                    }
                } else {
                    throw reseedRequired("in-flight writer transaction is not covered by a terminal checkpoint %s".formatted(inFlightPath), null);
                }
            }
            final AeronReplicationCheckpoint checkpoint = this.readOptionalCheckpoint(this.settings.checkpointPath());
            if (checkpoint == null) return null;
            this.validateWriterCheckpointIdentity(checkpoint);
            if (checkpoint.state() != AeronReplicationCheckpoint.State.COMMITTED &&
                checkpoint.state() != AeronReplicationCheckpoint.State.REJECTED) {
                throw new ReseedRequiredException(
                        "writer checkpoint is not restartable (state=%s, sequence=%s, path=%s)".formatted(checkpoint.state(), checkpoint.transactionSequence(), this.settings.checkpointPath()));
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
            final AeronReplicationCheckpoint.DurabilityMode expectedMode = switch (
                    this.settings.replication().durabilityMode()) {
                case ARCHIVE_FIRST -> AeronReplicationCheckpoint.DurabilityMode.ARCHIVE_FIRST;
                case ENQUEUE_THEN_ARCHIVE -> AeronReplicationCheckpoint.DurabilityMode.ENQUEUE_THEN_ARCHIVE;
            };
            if (checkpoint.recordType() != AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT ||
                checkpoint.durabilityMode() != expectedMode ||
                !checkpoint.clusterId().equals(this.settings.clusterId()) ||
                !checkpoint.nodeId().equals(this.settings.nodeId()) ||
                !checkpoint.storeGeneration().equals(this.settings.storeGeneration()) ||
                checkpoint.writerEpoch() != this.settings.epoch()) {
                throw reseedRequired("writer checkpoint identity does not match Aeron configuration", null);
            }
            if (this.settings.recordingId() >= 0 && checkpoint.recordingId() >= 0 &&
                this.settings.recordingId() != checkpoint.recordingId()) {
                throw reseedRequired("configured recording does not match writer checkpoint", null);
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
                stopPosition = this.getStopPosition(recordingId);
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
                final long start = this.getStartPosition(recordingId);
                final long stop = this.getStopPosition(recordingId);
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
                state == AeronReplicationCheckpoint.State.ENQUEUED ||
                state == AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN) {
                this.writeCheckpoint(this.inFlightCheckpointPath(), state, sequence, dataLength,
                        dataChunkCount, dataCrc32c, position);
                return;
            }
            if (state != AeronReplicationCheckpoint.State.COMMITTED &&
                state != AeronReplicationCheckpoint.State.REJECTED) {
                return;
            }
            this.writeCheckpoint(this.settings.checkpointPath(), state, sequence, dataLength,
                    dataChunkCount, dataCrc32c, position);
            crashPoint("AFTER_CHECKPOINT_WRITE_BEFORE_COMMITTED_SEQUENCE_UPDATE", sequence);
            /* The terminal checkpoint is the durable boundary. Publish it to
             * readers before best-effort cleanup of the diagnostic fence so a
             * cleanup failure cannot make a durable commit look unavailable. */
            this.writerBoundary = new AeronWriterBoundary(sequence, this.writerRecordingId.get(), position);
            try {
                AtomicFileStore.delete(this.inFlightCheckpointPath());
            } catch (final IOException failure) {
                throw new IllegalStateException("cannot clear in-flight writer checkpoint", failure);
            }
        }

        private Path inFlightCheckpointPath() {
            final Path path = this.settings.checkpointPath();
            return path.resolveSibling("%s.inflight".formatted(path.getFileName()));
        }

        private void writeCheckpoint(final Path path, final AeronReplicationCheckpoint.State state,
                                     final long sequence, final int dataLength, final int dataChunkCount,
                                     final int dataCrc32c, final long position) {
            this.writeCheckpoint(path, this.newWriterCheckpoint(
                    state, sequence, dataLength, dataChunkCount, dataCrc32c, position));
        }

        private void writeCheckpoint(final Path path, final AeronReplicationCheckpoint checkpoint) {
            CHECKPOINT_SEQUENCE.set(checkpoint.transactionSequence());
            try {
                AeronReplicationCheckpointStore.write(path, checkpoint);
            } catch (final IOException failure) {
                throw new IllegalStateException("cannot persist writer checkpoint", failure);
            } finally {
                CHECKPOINT_SEQUENCE.remove();
            }
        }

        private AeronReplicationCheckpoint newWriterCheckpoint(
                final AeronReplicationCheckpoint.State state, final long sequence,
                final int dataLength, final int dataChunkCount, final int dataCrc32c,
                final long position) {
            final long discoveredRecordingId = this.writer == null ? Aeron.NULL_VALUE : this.writer.recordingId();
            if (discoveredRecordingId >= 0) this.writerRecordingId.set(discoveredRecordingId);
            final long writerRecordingId = this.writerRecordingId.get();
            final long recordingId = writerRecordingId >= 0 ? writerRecordingId : this.settings.recordingId();
            final AeronReplicationCheckpoint.DurabilityMode checkpointMode = switch (this.settings.replication().durabilityMode()) {
                case ARCHIVE_FIRST -> AeronReplicationCheckpoint.DurabilityMode.ARCHIVE_FIRST;
                case ENQUEUE_THEN_ARCHIVE -> AeronReplicationCheckpoint.DurabilityMode.ENQUEUE_THEN_ARCHIVE;
            };
            return new AeronReplicationCheckpoint(
                    AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT,
                    checkpointMode,
                    state, this.settings.clusterId(), this.settings.nodeId(), this.settings.storeGeneration(),
                    recordingId, this.settings.epoch(), sequence, position, dataLength, dataChunkCount, dataCrc32c);
        }

        private synchronized void ensureRuntime() {
            this.ensureOpen();
            if (this.driverFailure.get() != null) {
                throw new IllegalStateException("Aeron runtime has failed; create a new transport", this.driverFailure.get());
            }
            if (this.runtime != null) return;
            if ("writer".equals(this.settings.role())) this.archiveCapacity.invalidate();
            this.runtime = AeronRuntime.start(this.settings, this::recordDriverFailure,
                    () -> crashPoint("BEFORE_PUBLICATION_CONNECTED", -1L));
            try {
                this.ensureWatermarkChannel();
            } catch (final RuntimeException | Error failure) {
                final RuntimeException cleanupFailure = this.closeRuntimeQuietly();
                if (cleanupFailure != null) failure.addSuppressed(cleanupFailure);
                throw failure;
            }
        }

        private void recordDriverFailure(final Throwable failure) {
            if (failure instanceof AeronException aeronFailure &&
                aeronFailure.category() == AeronException.Category.WARN) {
                /* Aeron uses the configured handler for non-terminal operational events
                 * too. A control-response disconnect is different: it means the Archive
                 * session can no longer validate or publish durable replication state. It
                 * is terminal while the transport is live, but remains a normal shutdown
                 * diagnostic after close has begun. */
                if (!isTerminalArchiveWarning(failure) || this.closing || this.closed) {
                    LOGGER.log(WARNING, "Aeron transport warning", aeronFailure);
                    return;
                }
            }
            final RuntimeException normalized = failure instanceof RuntimeException runtimeException
                    ? runtimeException
                    : new IllegalStateException("Aeron MediaDriver failed", failure);
            if (!this.driverFailure.compareAndSet(null, normalized) && this.driverFailure.get() != normalized) {
                /* Health keeps the first terminal cause, but later callbacks still carry
                 * useful diagnostics (and must not disappear silently). */
                LOGGER.log(WARNING, "Additional Aeron transport failure", normalized);
            }
            final StorageBinaryDataClientAeronArchive current = this.reader;
            if (current != null) {
                current.fail(normalized);
            }
        }

        private synchronized void stopDriver() {
            this.ensureOpen();
            /* Stopping is idempotent: failure-injection and shutdown callers can race
             * without turning an already stopped runtime into a spurious failure. */
            if (this.runtime == null) return;
            this.runtime.stopDriver();
        }

        private RuntimeException closeRuntimeQuietly() {
            RuntimeException failure = null;
            /* Writer-side watermark delivery calls back into this Transport. Stop that
             * worker first so it cannot enter ensureWriter/retention while writer close
             * is in progress. Reader-side channels remain open until the reader exits so
             * its final cursor publication is not turned into a spurious failure. */
            if (this.reader == null && "writer".equals(this.settings.role()) && this.watermarkChannel != null) {
                failure = this.closeWatermarkChannel();
            }
            if (this.watermarkChannel != null) {
                try {
                    this.watermarkChannel.close();
                    this.watermarkChannel = null;
                } catch (final RuntimeException closeFailure) {
                    /* A terminal channel has already stopped its worker and closed both
                     * native endpoints. Do not retain that dead object merely because one
                     * endpoint reported a close error; otherwise the runtime below can never
                     * be released during initialization cleanup. */
                    if (this.watermarkChannel.isClosed()) this.watermarkChannel = null;
                    failure = appendFailure(failure, closeFailure);
                }
            }
            if (this.retention != null) {
                try {
                    this.retention.close();
                    this.retention = null;
                } catch (final RuntimeException retentionFailure) {
                    failure = appendFailure(failure, retentionFailure);
                }
            }
            if (this.watermarkChannel == null && this.retention == null && this.runtime != null) {
                try {
                    this.runtime.close();
                    this.runtime = null;
                } catch (final RuntimeException closeFailure) {
                    failure = appendFailure(failure, closeFailure);
                }
            }
            if (this.watermarkChannel == null && this.retention == null && this.runtime == null) {
                this.clearRetentionSecrets();
            }
            return failure;
        }

        private void clearRetentionSecrets() {
            this.deferredWatermarks.clear();
            this.settings.clearRetentionSecret();
            final byte[] secret = this.retentionSecret;
            this.retentionSecret = null;
            if (secret != null) Arrays.fill(secret, (byte) 0);
        }

        private Aeron aeron() {
            if (this.runtime == null) throw new IllegalStateException("Aeron runtime is not initialized");
            return this.runtime.aeron();
        }

        private AeronArchive archive() {
            if (this.runtime == null) throw new IllegalStateException("Aeron runtime is not initialized");
            return this.runtime.archive();
        }

        /* AeronArchive's synchronous control client is not thread-safe.  Retention,
         * writer recovery, and reader discovery can run on different worker threads,
         * so every provider-owned request is serialized on the client itself.  The
         * lock covers only the request; callers retain their own retry/deadline logic
         * outside this helper. */
        private long getStartPosition(final long recordingId) {
            final AeronArchive archive = this.archive();
            synchronized (archive) {
                return archive.getStartPosition(recordingId);
            }
        }

        private long getStopPosition(final long recordingId) {
            final AeronArchive archive = this.archive();
            synchronized (archive) {
                return archive.getStopPosition(recordingId);
            }
        }

        private long getRecordingPosition(final long recordingId) {
            final AeronArchive archive = this.archive();
            synchronized (archive) {
                return archive.getRecordingPosition(recordingId);
            }
        }

        private int listRecordingsForUri(final String channelFragment, final int streamId,
                                         final io.aeron.archive.client.RecordingDescriptorConsumer consumer) {
            final AeronArchive archive = this.archive();
            synchronized (archive) {
                return archive.listRecordingsForUri(0L, 2, channelFragment,
                        streamId, consumer);
            }
        }

        private AeronArchive.Context archiveContext() {
            if (this.runtime == null) throw new IllegalStateException("Aeron runtime is not initialized");
            return this.runtime.archiveContext();
        }

        private void ensureOpen() {
            this.ensureNotDeliveryCallback();
            if (this.closed || this.closing) {
                throw new IllegalStateException(this.closed ? "Aeron transport is closed" : "Aeron transport is closing");
            }
        }

        private void ensureNotDeliveryCallback() {
            if (Boolean.TRUE.equals(this.deliveryCallback.get())) {
                throw new IllegalStateException(
                        "Aeron transport cannot be re-entered from a reader delivery callback");
            }
        }

        private void enterDeliveryCallback() {
            if (Boolean.TRUE.equals(this.deliveryCallback.get())) {
                throw new IllegalStateException("nested Aeron reader delivery callback");
            }
            this.deliveryCallback.set(Boolean.TRUE);
        }

        private void exitDeliveryCallback() {
            this.deliveryCallback.remove();
        }

        /// Shuts the transport down in dependency order: reader, write
        /// coordinator, writer, watermark channel, retention, and finally the
        /// shared Aeron runtime.
        ///
        /// Each stage runs only after the previous one is fully gone, and a
        /// stage that fails leaves the transport retryable instead of torn
        /// down beneath live threads — closing the runtime early would turn a
        /// retryable abort into a use-after-close. All failures are aggregated
        /// and thrown together. Never call this from inside a reader delivery
        /// callback.
        @Override
        public synchronized void close() {
            this.ensureNotDeliveryCallback();
            synchronized (this) {
                if (this.closed) return;
                if (this.closing) {
                    throw new IllegalStateException("Aeron transport close is already in progress");
                }
                if (this.reader == null && this.writer == null && this.coordinator == null &&
                    this.runtime == null && this.retention == null && this.watermarkChannel == null) {
                    this.clearRetentionSecrets();
                    this.closed = true;
                    return;
                }
                this.closing = true;
            }

            Throwable failure = null;
            if (this.reader != null) {
                try {
                    this.reader.dispose();
                    this.reader = null;
                } catch (final Throwable readerFailure) {
                    /* Keep the shared Aeron resources open while the polling thread is
                     * still alive; a retry of close() can finish the shutdown safely. */
                    failure = appendCloseFailure(failure, readerFailure);
                }
            }
            /* The coordinator owns every pending transaction and must resolve or preserve
             * its fence before the Archive wrapper stops the recording. Closing the raw
             * writer first can publish an ABORT beneath a Store write that still owns the
             * coordinator. */
            if (this.reader == null && this.coordinator != null) {
                try {
                    this.coordinator.dispose();
                    this.coordinator = null;
                } catch (final Throwable coordinatorFailure) {
                    failure = appendCloseFailure(failure, coordinatorFailure);
                }
            }
            if (this.reader == null && this.coordinator == null && this.writer != null) {
                try {
                    this.writer.close();
                    this.writer = null;
                } catch (final Throwable writerFailure) {
                    /* A released publication is not proof that the Archive recording
                     * stopped. Keep the wrapper until isClosed() confirms the stop
                     * postcondition; otherwise a transient external-Archive failure would
                     * abandon an active recording and make a retry impossible. If the
                     * Archive connection is definitively gone, however, retaining the
                     * wrapper also retains the local driver forever and prevents the
                     * failed process from exiting. The checkpoint remains fail-closed, so
                     * release the local runtime in that terminal case. */
                    if (this.writer.isClosed() ||
                        (this.settings.externalArchive() && isArchiveUnavailable(writerFailure))) {
                        this.writer = null;
                    }
                    failure = appendCloseFailure(failure, writerFailure);
                }
            }
            /* A reader can publish its final durable cursor from the polling thread.
             * Stop that thread before flushing and closing the watermark publication.
             * Likewise, keep the writer-side receiver alive until publication shutdown
             * has completed. Closing the auxiliary channel first creates a spurious
             * reader failure during otherwise healthy transport shutdown. */
            if (this.reader == null && this.writer == null && this.coordinator == null &&
                this.watermarkChannel != null) {
                failure = appendCloseFailure(failure, this.closeWatermarkChannel());
            }
            if (this.reader == null && this.writer == null && this.coordinator == null &&
                this.watermarkChannel == null && this.retention != null) {
                try {
                    this.retention.close();
                    this.retention = null;
                } catch (final Throwable retentionFailure) {
                    failure = appendCloseFailure(failure, retentionFailure);
                }
            }
            /* Do not close the shared runtime while a writer or coordinator still owns
             * the publication. A failed abort/stop is retryable; closing Aeron here would
             * turn that retry into a use-after-close and leak the unresolved sequence. */
            if (this.reader == null && this.writer == null && this.coordinator == null && this.retention == null &&
                this.watermarkChannel == null && this.runtime != null) {
                try {
                    this.runtime.close();
                    this.runtime = null;
                } catch (final Throwable runtimeFailure) {
                    failure = appendCloseFailure(failure, runtimeFailure);
                }
            }
            if (failure != null) {
                this.closing = false;
                if (failure instanceof Error error) throw error;
                throw new IllegalStateException("failed to close Aeron transport", failure);
            }
            synchronized (this) {
                this.distributor = null;
                this.distributorStream = null;
                this.clearRetentionSecrets();
                this.closed = true;
                this.closing = false;
            }
        }
    }

        /// Adapts complete Aeron data to the neutral packet acceptor.
    private record ReceiverAdapter(Transport owner, ClusterStorageBinaryDataPacketAcceptor packetAcceptor)
            implements StorageBinaryDataReceiver {
        public void receiveTypeDictionary(final String value) {
            this.owner().enterDeliveryCallback();
            try {
                this.packetAcceptor().acceptTypeDictionary(value);
            } finally {
                this.owner().exitDeliveryCallback();
            }
        }

        public void receiveData(final org.eclipse.serializer.persistence.binary.types.Binary value) {
            this.owner().enterDeliveryCallback();
            try {
                this.packetAcceptor().acceptData(value);
                this.packetAcceptor().awaitApplied();
            } finally {
                this.owner().exitDeliveryCallback();
            }
        }

        @Override
        public boolean receiveDataOwned(final org.eclipse.serializer.persistence.binary.types.Binary value) {
            this.owner().enterDeliveryCallback();
            try {
                return this.packetAcceptor().acceptDataOwned(value);
            } finally {
                this.owner().exitDeliveryCallback();
            }
        }

        @Override
        public boolean canReceiveDataOwned() {
            return this.packetAcceptor().canAcceptDataOwned();
        }

        @Override
        public void awaitApplied() {
            this.owner().enterDeliveryCallback();
            try {
                this.packetAcceptor().awaitApplied();
            } finally {
                this.owner().exitDeliveryCallback();
            }
        }
    }

        /// Adds the neutral message-position view to the Aeron client.
    private record ClientAdapter(
            StorageBinaryDataClientAeronArchive delegate,
            long recordingId,
            UUID clusterId,
            UUID nodeId,
            UUID storeGeneration,
            long epoch) implements ClusterStorageBinaryDataClient {
        public void start() {
            this.delegate().start();
        }

        public void stopAtLatestMessage() {
            this.delegate().stopAtLatestMessage();
        }

        public ReplicationCursor cursor() {
            final CursorSnapshot snapshot = this.delegate().cursorSnapshot();
            return new ReplicationCursor("aeron", this.storeGeneration(), snapshot.sequence(),
                    new AeronReplicationCursor(this.clusterId(), this.nodeId(), this.storeGeneration(),
                            this.epoch(), this.recordingId(), snapshot.position(), snapshot.sequence()).encode());
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

        public StorageBinaryDataClient.StopOutcome stopOutcome() {
            return this.delegate().stopOutcome();
        }

        public StorageBinaryDataClient.StopResult stopResult() {
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
