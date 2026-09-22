package peruncs.datagrid.cluster.node.aeron;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.archive.client.*;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.TimeoutException;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.types.StorageConnection;
import peruncs.datagrid.cluster.node.CloseSequencer;
import peruncs.datagrid.cluster.node.NodeLibraryPropertiesProvider;
import peruncs.datagrid.cluster.node.backup.BackupMetadata;
import peruncs.datagrid.cluster.node.exceptions.ReplicationPositionUnavailableException;
import peruncs.datagrid.cluster.node.exceptions.WriterFencedException;
import peruncs.datagrid.cluster.node.replication.*;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpoint;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCheckpointStore;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCursor;
import peruncs.datagrid.cluster.storage.aeron.reader.AeronArchiveReader;
import peruncs.datagrid.cluster.storage.aeron.reader.CursorSnapshot;
import peruncs.datagrid.cluster.storage.aeron.reader.ReaderDeliveryListener;
import peruncs.datagrid.cluster.storage.aeron.writer.*;
import peruncs.datagrid.cluster.storage.types.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.lang.System.Logger.Level.WARNING;

/// Creates the Aeron cluster replication transport.
///
/// The returned transport creates its driver and Archive lazily. The
/// transport owns those shared resources and releases them from
/// [ClusterReplicationTransport#close()]; individual readers and
/// distributors do not close the runtime.
public final class AeronClusterReplicationTransportProvider {
    /* Test-only, dynamically scoped seams used by the forked crash harness. */
    private static final System.Logger LOGGER = System.getLogger(AeronClusterReplicationTransportProvider.class.getName());
    private static final ScopedValue<Long> CHECKPOINT_SEQUENCE = ScopedValue.newInstance();
    private static final ScopedValue<Boolean> DELIVERY_CALLBACK = ScopedValue.newInstance();
    /// Default heartbeat staleness bound for the writer fencing lease.
    private static final long DEFAULT_LEASE_STALENESS_MILLIS = 30_000L;
        /// Creates a provider that reads Aeron settings when a transport is created.
    public AeronClusterReplicationTransportProvider() {
    }

    /* Aeron 1.53 surfaces a control-response disconnect only as an
     * ArchiveEvent carrying this producer-owned text; ArchiveEvent exposes no
     * error code, so the probe is string-based and pinned by a test. Once the
     * response publication disconnects, the Archive control session can no
     * longer validate or publish durable replication state, so this warning is
     * terminal while the transport is live; during shutdown it is an expected
     * diagnostic. */
    private static final String CONTROL_RESPONSE_DISCONNECTED_MESSAGE =
            "control response publication is not connected";

    /// Reports whether one driver error handler callback is a terminal
    /// Archive control-response disconnect.
    ///
    /// @param failure callback failure
    /// @return `true` when the failure is a live control-response disconnect
    static boolean isTerminalArchiveWarning(final Throwable failure) {
        return failure instanceof ArchiveEvent && failure.getMessage() != null &&
               failure.getMessage().contains(CONTROL_RESPONSE_DISCONNECTED_MESSAGE);
    }

    static long currentCheckpointSequence() {
        return CHECKPOINT_SEQUENCE.orElse(-1L);
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
    public ClusterReplicationTransport create(final NodeLibraryPropertiesProvider properties) {
        final AeronSettings settings = AeronSettings.fromEnvironment(properties);
        final Path leaseDirectory = leaseDirectory(properties);
        validateLeaseDirectory(settings, leaseDirectory);
        final Transport transport = new Transport(settings, leaseDirectory,
                properties.writerLeaseStalenessMillis());
        /* Probe metadata durability before any synchronized runtime startup path. */
        transport.verifyMetadataStorage();
        return transport;
    }

        /// Rejects a lease directory the Aeron runtime recreates or overwrites.
    ///
    /// The MediaDriver recreates its directory on every start, so a lease
    /// stored inside the driver, archive, or checkpoint tree is deleted on the
    /// next startup and the writer would appear fenced. Failing at wiring time
    /// is clearer than a lost lease on the first write.
    ///
    /// @param settings       resolved Aeron settings
    /// @param leaseDirectory resolved lease directory, or `null` when unset
    private static void validateLeaseDirectory(final AeronSettings settings, final Path leaseDirectory) {
        if (leaseDirectory == null) {
            return;
        }
        if (overlaps(leaseDirectory, settings.directories().aeronDirectory()) ||
            overlaps(leaseDirectory, settings.directories().archiveDirectory()) ||
            overlaps(leaseDirectory, settings.directories().checkpointPath())) {
            throw new IllegalArgumentException(
                    "writer lease directory must not overlap the Aeron driver, archive, or checkpoint paths: lease=%s, driver=%s, archive=%s, checkpoint=%s".formatted(
                            leaseDirectory, settings.directories().aeronDirectory(), settings.directories().archiveDirectory(), settings.directories().checkpointPath()));
        }
    }

    private static boolean overlaps(final Path left, final Path right) {
        return left.startsWith(right) || right.startsWith(left);
    }

        /// Resolves the shared directory holding the writer fencing lease.
    ///
    /// The lease must live where every writer of one cluster/generation can
    /// see it, so it shares the backup volume: the one filesystem this
    /// topology already treats as shared across machines. A writer without a
    /// configured shared directory cannot fence, so the directory is required
    /// for the writer role and absent for readers.
    ///
    /// @param properties node configuration
    /// @return lease directory, or `null` when no shared backup volume is configured
    private static Path leaseDirectory(final NodeLibraryPropertiesProvider properties) {
        final String configured = properties.replicationProperty(
                NodeLibraryPropertiesProvider.Env.EnvKeys.BACKUP_PATH);
        return configured == null || configured.isBlank()
                ? null
                : Paths.get(configured).toAbsolutePath().normalize();
    }

        /// Owns the Aeron resources for one node and one transport lifecycle.
    private static final class Transport implements ClusterReplicationTransport {
        private final AeronSettings settings;
        private final Path leaseDirectory;
        /// Heartbeat staleness bound for the writer fencing lease.
        private final long leaseStalenessMillis;
        private volatile WriterFencingLease writerLease;
        /* Fencing token captured when the lease is acquired. Writer checkpoints
         * and the position supplier read this snapshot instead of performing a
         * live lease lookup that would throw a bare exception when unleased. */
        private volatile long writerFencingTokenSnapshot = -1L;
        private final AeronArchiveCapacity archiveCapacity;
        private final AtomicLong nextSequence = new AtomicLong();
        private final AtomicLong readerRecordingId = new AtomicLong(Aeron.NULL_VALUE);
                /// Recording identity selected at writer startup; retained when RecordingPos is briefly unavailable.
        private final AtomicLong writerRecordingId = new AtomicLong();
                /// First asynchronous MediaDriver failure; health must not hide it.
        private final AtomicReference<RuntimeException> driverFailure = new AtomicReference<>();
        private volatile AeronRuntime runtime;
        private volatile AeronArchiveReplicationPublisher writer;
        private volatile AeronReplicationWriteCoordinator coordinator;
        /* Reader replacement and disposal run under this slot's own lifecycle
         * lock, never under the transport monitor: a reader dispose can join
         * its polling thread for up to the reader-stop timeout, and health()/
         * failure callbacks must keep observing the transport while that join
         * is in flight. The slot publishes the replacement reference last. */
        private final AeronReaderSlot<AeronArchiveReader> readers = new AeronReaderSlot<>();
        /* Guards health-view replacement only, for the same reason. */
        private final Object healthLock = new Object();
        /* Published together after the terminal checkpoint is durable.  Readers of
         * positionProvider() must never combine fields from two transactions. */
        private volatile AeronWriterBoundary writerBoundary = new AeronWriterBoundary(-1, -1, -1);
        private volatile boolean writerRecoveryInProgress;
        private volatile ReplicationHealth.State writerRecoveryState;
        private StorageBinaryDataDistributor distributor;
        private String distributorStream;
        private ReplicationPositionProvider positionProvider;
        private ReplicationLogRetention retention;
        /* Reader-watermark channel and its fan-in state, owned by [WatermarkFanIn]. */
        private final WatermarkFanIn watermarks;
        private AeronHealth health;
        /// Ordered retryable close stages, built once; each readiness check reads live fields.
        private final CloseSequencer closeSequencer;
        private volatile boolean closed;
        private volatile boolean closing;

        private Transport(final AeronSettings settings, final Path leaseDirectory, final Long leaseStalenessMillis) {
            this.settings = settings;
            this.leaseDirectory = leaseDirectory;
            /* Heartbeat staleness bound for the fencing lease: configurable so
             * deployments with slow shared volumes can widen it, while the
             * default keeps takeover within half a minute. */
            this.leaseStalenessMillis = leaseStalenessMillis == null || leaseStalenessMillis <= 0L
                    ? DEFAULT_LEASE_STALENESS_MILLIS : leaseStalenessMillis;
            this.archiveCapacity = new AeronArchiveCapacity(settings);
            this.writerRecordingId.set(settings.recordingId());
            this.watermarks = new WatermarkFanIn(this::aeron, settings, this::retentionSupported,
                    () -> (AeronArchiveRetention) this.retention, this::writerReady);
            this.closeSequencer = new CloseSequencer(this.closeStages());
        }

        private static ReseedRequiredException reseedRequired(final String message,
                                                              final Throwable cause) {
            return cause == null ? new ReseedRequiredException(message) :
                    new ReseedRequiredException(message, cause);
        }

        private static Throwable appendFailure(final Throwable current,
                                             final Throwable additional) {
            if (additional == null) return current;
            /* Checked failures are normalized so callers always hold a
             * runtime-typed aggregate; the shared sequencer owns the
             * aggregation order and the Error-priority rule. */
            final Throwable normalized = additional instanceof RuntimeException runtime
                    ? runtime : new IllegalStateException("Aeron transport resource close failed", additional);
            return CloseSequencer.append(current, normalized);
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
            final Path parent = this.settings.directories().checkpointPath().toAbsolutePath().getParent();
            if (parent == null) throw new IllegalArgumentException("Aeron checkpoint path must have a parent directory");
            if (Files.isSymbolicLink(this.settings.directories().checkpointPath())) {
                throw new IllegalArgumentException(
                        "Aeron checkpoint path must not be a symbolic link: %s".formatted(this.settings.directories().checkpointPath()));
            }
            /* Apply the same production permission policy before the capability probe
             * creates any temporary metadata files.  Otherwise a non-POSIX filesystem
             * would pass this early probe and fail only later during runtime startup. */
            AeronRuntime.ensurePrivateDirectory(parent, this.settings.productionMode());
            try {
                AtomicFileWriter.verify(this.settings.directories().checkpointPath());
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

        @Override
        public BackupMetadata.Identity configuredBackupIdentity() {
            return new BackupMetadata.Identity(
                    this.settings.clusterId(), this.settings.identity().storeGeneration(),
                    this.settings.epoch(), this.settings.recordingId());
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
        public synchronized StorageBinaryDataDistributor distributor(
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
            if (this.distributor != null && Objects.equals(this.distributorStream, streamName)) {
                return this.distributor;
            }
            if (this.distributor != null) {
                throw new IllegalStateException("Aeron transport supports one replication stream per provider");
            }
            this.distributor = new AeronDistributionGate(
                    () -> this.settings.role().isWriter(),
                    value ->
                    {
                        this.nextSequence.accumulateAndGet(value, Math::max);
                        if (this.writer != null) this.writer.synchronizeNextSequence(value);
                    });
            return this.distributor;
        }

        @Override
        public UnaryOperator<PersistenceTarget<Binary>> persistenceTargetFactory(
                final String streamName, final StorageBinaryDataDistributor distributor,
                final Supplier<StorageConnection> writerStorage) {
            synchronized (this) {
                this.ensureOpen();
                this.claimStream(streamName);
                if (!this.settings.role().isWriter()) {
                    /* Reader roles must reproduce the writer's history through
                     * the replication import path, which bypasses this target.
                     * Any locally originated write is rejected instead of
                     * persisting an unreplicated divergence. */
                    return RejectingPersistenceTarget::New;
                }
            }
            final AeronReplicationWriteCoordinator coordinator = this.ensureCoordinator();
            return delegate -> new AeronStorageBinaryReplicationTarget(
                    delegate,
                    coordinator,
                    distributor,
                    sequence ->
                    {
                        if (distributor instanceof StorageBinaryDataDistributor cluster) {
                            cluster.messageIndex(sequence);
                        }
                    },
                    () -> !(distributor instanceof StorageBinaryDataDistributor cluster) || !cluster.ignoreDistribution(),
                    /* The reachable index topology can change without replacing
                     * a Store root, so every distributed write validates the
                     * live graph. The storage connection does not exist during
                     * wiring; a null connection during root creation skips it. */
                    () ->
                    {
                        final StorageConnection connection = writerStorage == null ? null : writerStorage.get();
                        if (connection != null) {
                            ClusterStoreIndexes.validateStorageRoots(connection);
                        }
                    }
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
        /// @param receiver destination for received binaries and dictionaries
        /// @param streamName logical stream name
        /// @param cursorListener callback after data is applied
        /// @param startingCursor durable starting cursor
        /// @param commitPosition whether reader positions are committed
        /// @return binary data client
        @Override
        public StorageBinaryDataClient client(
                final StorageBinaryDataReceiver receiver,
                final String streamName,
                final DataMessageAppliedListener cursorListener,
                final ReplicationCursor startingCursor,
                final boolean commitPosition
        ) {
            synchronized (this) {
                this.ensureOpen();
                this.claimStream(streamName);
                /* A writer owns publication only. Even with an external Archive it must not
                 * create a second subscription against its own recording; that would violate
                 * the one-writer/N-reader topology and expose a partially initialized reader
                 * through the writer's health path. Configure a separate reader/backup-reader
                 * node when replay is required. */
                if (this.settings.role().isWriter()) {
                    return StorageBinaryDataClient.NoOp(startingCursor);
                }
            }
            Objects.requireNonNull(receiver, "receiver");
            this.ensureRuntime();
            final long recordingId = this.settings.recordingId() >= 0
                    ? this.settings.recordingId() : this.discoverReaderRecordingId();
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
                !this.settings.identity().storeGeneration().equals(cursor.storeGeneration())) {
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
            this.ensureOpen();
            replacement = this.readers.replace(() -> {
                /* Re-check under the slot lock: close() may have started after
                 * the lock-free prologue above. */
                this.ensureOpen();
                final AeronArchiveReader created = AeronArchiveReader.New(
                        AeronArchiveReader.Configuration.builder()
                        .aeron(this.aeron())
                        .archiveContext(archiveContext())
                        .recordingId(recordingId)
                        .startPosition(aeronCursor && cursorPosition >= 0
                                ? cursorPosition : PersistentSubscription.FROM_START)
                        .liveChannel(this.settings.channels().live())
                        .liveStreamId(this.settings.streamId())
                        .replayChannel(this.settings.channels().replay())
                        .replayStreamId(this.settings.streamId() + 1)
                        .replicationConfiguration(this.settings.replication())
                        .clusterId(this.settings.clusterId())
                        .wireNonce(this.settings.wireNonce())
                        .epoch(this.settings.epoch())
                        .initialSequence(aeronCursor ? cursor.logicalSequence() : -1)
                        .initialPosition(aeronCursor ? cursorPosition : -1)
                        .receiver(new ReceiverAdapter(this, receiver))
                        .transactionResolved(() -> {
                            final AeronArchiveReader current = readerRef.get();
                            if (current != null && current == this.readers.current()) {
                                final CursorSnapshot snapshot = current.cursorSnapshot();
                                this.nextSequence.accumulateAndGet(snapshot.sequence() + 1, Math::max);
                                /* A retention watermark is proof of a durable recovery
                                 * cursor, not merely an applied Store update. Persist the
                                 * barrier tail first and publish exactly that boundary.
                                 * Readers without a cursor sink cannot join retention. */
                                if (cursorListener != null) {
                                    final byte[] position = new AeronReplicationCursor(
                                            this.settings.clusterId(), this.settings.identity().nodeId(), this.settings.identity().storeGeneration(),
                                            this.settings.epoch(), this.currentFencingToken(), recordingId,
                                            snapshot.position(), snapshot.sequence()).encode();
                                    this.runInDeliveryCallback(() -> cursorListener.onApplied(ReplicationCursor.of(
                                            "aeron", this.settings.identity().storeGeneration(), snapshot.sequence(), position)));
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
            return new ClientAdapter(replacement, recordingId, this.settings.clusterId(),
                    this.settings.identity().nodeId(), this.settings.identity().storeGeneration(), this.settings.epoch());
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
            if (!aeron.clusterId().equals(this.settings.clusterId()) ||
                !aeron.storeGeneration().equals(this.settings.identity().storeGeneration()) ||
                aeron.epoch() != this.settings.epoch() || aeron.recordingId() != expectedRecordingId ||
                aeron.sequence() != cursor.logicalSequence()) {
                throw reseedRequired("Aeron cursor identity does not match the configured reader or writer", null);
            }
            return aeron;
        }

        private long discoverReaderRecordingId() {
            final ChannelUri live = ChannelUri.parse(this.settings.channels().live());
            final String alias = live.get(CommonContext.ALIAS_PARAM_NAME);
            if (alias == null || alias.isBlank()) {
                throw new IllegalStateException("Aeron recording discovery requires alias= on ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL");
            }
            final AtomicLong discovered = new AtomicLong(Aeron.NULL_VALUE);
            final int count = this.listRecordingsForUri("alias=%s".formatted(alias), this.settings.streamId(),
                    (_, _, recordingId, _,
                     _, _, _, _, _,
                     _, _, _, _, _, _,
                     _) -> discovered.set(recordingId));
            if (count != 1 || discovered.get() < 0) {
                throw new IllegalStateException("Aeron recording discovery requires exactly one alias=%s recording for stream %s; found %s".formatted(alias, this.settings.streamId(), count));
            }
            return discovered.get();
        }

        private Path readerUncertaintyPath() {
            return this.settings.directories().checkpointPath().resolveSibling("%s.reader-inflight".formatted(this.settings.directories().checkpointPath().getFileName()));
        }

        private void rejectUncertainReaderImport(final long recordingId) {
            final Path path = this.readerUncertaintyPath();
            try {
                final AeronReplicationCheckpoint checkpoint = AeronReplicationCheckpointStore.read(path);
                if (checkpoint.recordType() != AeronReplicationCheckpoint.RecordType.READER_CURSOR ||
                    checkpoint.state() != AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN ||
                    !checkpoint.clusterId().equals(this.settings.clusterId()) ||
                    !checkpoint.nodeId().equals(this.settings.identity().nodeId()) ||
                    !checkpoint.storeGeneration().equals(this.settings.identity().storeGeneration()) ||
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
                    runInDeliveryCallback(() -> {
                        try {
                            final AeronReplicationCheckpoint checkpoint = new AeronReplicationCheckpoint(
                                    AeronReplicationCheckpoint.RecordType.READER_CURSOR,
                                    ReplicationDurabilityMode.ARCHIVE_FIRST,
                                    AeronReplicationCheckpoint.State.COMMITTING_UNCERTAIN,
                                    settings.clusterId(), settings.identity().nodeId(), settings.identity().storeGeneration(), readerRecordingId.get(),
                                    settings.epoch(), Transport.this.currentFencingToken(), sequence, position,
                                    dataLength, dataChunkCount, crc32c);
                            AeronReplicationCheckpointStore.write(path, checkpoint);
                        } catch (final IOException failure) {
                            throw reseedRequired("cannot persist uncertain reader import marker: %s".formatted(path), failure);
                        }
                    });
                }

                @Override
                public void afterStoreImport() {
                    runInDeliveryCallback(() -> {
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

        @Override
        public synchronized ReplicationPositionProvider positionProvider(final String streamName) {
            this.ensureOpen();
            this.claimStream(streamName);
            if (this.positionProvider == null) {
                this.positionProvider = new AeronPositionProvider(
                        () -> this.settings.role().isWriter(),
                        () -> this.writer != null && !this.writerRecoveryInProgress && !this.closed,
                        this::ensureWriter,
                        () -> this.writerBoundary,
                        this.settings::clusterId,
                        () -> this.settings.identity().nodeId(),
                        () -> this.settings.identity().storeGeneration(),
                        this.settings::epoch,
                        this::positionWriterFencingToken);
            }
            return this.positionProvider;
        }

        /// Reports whether the held writer lease is still current.
        ///
        /// Write admission deliberately bypasses the lease freshness cache:
        /// commit fencing must observe a takeover immediately, not after the
        /// background-check interval expires.
        private boolean writerFencingTokenValid() {
            final WriterFencingLease lease = this.writerLease;
            return lease != null && lease.isCurrentUncached();
        }

        private WriterLeaseGate writerLeaseGate() {
            return new WriterLeaseGate() {
                @Override
                public boolean isValid() {
                    return writerFencingTokenValid();
                }

                @Override
                public long offerUnderOwnership(final LongSupplier offer) {
                    final WriterFencingLease lease = writerLease;
                    if (lease == null) {
                        throw new WriterFencedException(
                                "writer fencing lease lost before commit; restart required");
                    }
                    return lease.executeUnderOwnership(ignored -> offer.getAsLong());
                }

                @Override
                public long offerUnderOwnership(final WriterLeaseGate.OwnedOffer offer) {
                    final WriterFencingLease lease = writerLease;
                    if (lease == null) {
                        throw new WriterFencedException(
                                "writer fencing lease lost before commit; restart required");
                    }
                    return lease.executeUnderOwnership(offer);
                }
            };
        }

        /// Returns the Archive retention controller.
        ///
        /// Without a configured reader set and an embedded Archive there is
        /// nothing safe to delete, so retention reports unsupported and history
        /// is preserved rather than risking an unacknowledged purge.
        ///
        /// @return retention controller
        @Override
        public synchronized ReplicationLogRetention retention() {
            this.ensureOpen();
            if (this.retention != null) return this.retention;
            if (!this.retentionSupported()) {
                /* Retention without a configured reader set and an embedded Archive
                 * cannot prove that every reader has crossed the requested boundary. */
                this.retention = new ReplicationLogRetention() {
                    @Override
                    public boolean isSupported() {
                        return false;
                    }

                    @Override
                    public MaintenanceResult deleteThrough(final ReplicationCursor cursor) {
                        throw new UnsupportedOperationException(
                                "Aeron Archive retention requires an embedded writer and configured retention readers");
                    }

                    @Override
                    public void close() {
                    }
                };
                return this.retention;
            }
            final AeronArchiveRetention created = this.newRetentionController();
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

        private AeronArchiveRetention newRetentionController() {
            return new AeronArchiveRetention(this.settings.retentionReaders(),
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
            }, this.settings.clusterId(), this.settings.identity().storeGeneration(),
                    this.settings.epoch(), this.settings.replication()::termLength,
                    this.settings::archiveSegmentFileLength,
                    this.watermarks::available,
                    this.settings.directories().checkpointPath().resolveSibling(
                            "%s.retention".formatted(this.settings.directories().checkpointPath().getFileName())),
                    AeronArchiveRetention.DEFAULT_OPERATION_TIMEOUT_MILLIS);
        }

        private void publishReaderWatermark(final CursorSnapshot snapshot, final long recordingId) {
            this.watermarks.publish(snapshot, recordingId);
        }

        private void ensureWatermarkChannel() {
            if (this.watermarks.hasChannel()) return;
            if (this.settings.role().isWriter() && this.retentionSupported() && this.retention == null) {
                this.retention = this.newRetentionController();
            }
            this.watermarks.ensure();
        }

                /// Replays reader progress received during writer recovery.
        private void drainDeferredWatermarks() {
            this.watermarks.drainDeferred();
        }

                /// Stops the writer-side watermark worker before any writer/retention resource
        /// is closed. The worker invokes retention callbacks and must never race a
        /// transport shutdown while the writer monitor is being dismantled.
        private RuntimeException closeWatermarkChannel() {
            return this.watermarks.closeChannel();
        }

        private boolean retentionSupported() {
            return !this.settings.retentionReaders().isEmpty() &&
                   this.settings.role().isWriter() && !this.settings.externalArchive();
        }

        @Override
        public ReplicationHealth health(
                final StorageControllerAdapter storage,
                final StorageBinaryDataClient client
        ) {
            this.ensureOpen();
            /* Health-view replacement uses its own lock, so a health probe never
             * queues behind reader replacement/disposal on the reader lock or
             * behind writer startup on the transport monitor. */
            synchronized (this.healthLock) {
                if (this.health == null || !this.health.matches(storage, client)) {
                    if (this.health != null) this.health.close();
                    this.health = new AeronHealth(
                            storage,
                            client,
                            () -> this.closed,
                            () -> this.driverFailure.get() != null,
                            () -> this.archiveCapacity.available(this.settings.replication().maxTransactionBytes()),
                            this::writerReady,
                            () -> this.settings.role().isWriter(),
                            this::writerCheckpointState,
                            this.archiveCapacity::usableSpaceBytes,
                            () -> this.writerBoundary.position(),
                            () -> this.writerBoundary.sequence(),
                            () -> {
                                /* Capture once: a concurrent replace/dispose can clear the
                                 * slot between two separate current() reads and turn this
                                 * supplier into an NPE. */
                                final AeronArchiveReader current = this.readers.current();
                                return current == null ? -1L : current.lastAppliedSequence();
                            },
                            () -> this.watermarks.channelFailure() != null);
                }
                return this.health;
            }
        }

        private boolean writerReady() {
            return this.settings.role().isWriter() && !this.writerRecoveryInProgress &&
                   this.writerRecoveryState == null && this.writer != null && !this.writer.isFailed();
        }

                /// Reports current writer readiness without starting the runtime. Lifecycle
        /// startup belongs to an explicit client or write operation, not a health
        /// probe.

        private ReplicationHealth.State writerCheckpointState() {
            if (this.driverFailure.get() != null) {
                return ReplicationHealth.State.FAILED;
            }
            if (!this.settings.role().isWriter()) {
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
                return ReplicationHealth.State.STARTING;
            }
            /* A live writer can fail closed after an offer, await, or checkpoint
             * error without being discarded immediately.  Do not advertise LIVE
             * while that publisher can no longer accept a durable transaction. */
            return current.isFailed() ? ReplicationHealth.State.FAILED : null;
        }

        private void ensureWriter() {
            synchronized (this) {
                this.ensureWriterLocked();
            }
            /* Retention callbacks acquire their own monitor. Do not invoke them
             * while the transport monitor is held, or retention->transport and
             * transport->retention paths can deadlock during writer recovery. */
            this.drainDeferredWatermarks();
        }

        private AeronArchiveReplicationPublisher ensureWriterLocked() {
            if (this.closing || this.closed) {
                throw new IllegalStateException("Aeron writer cannot start while transport is closing");
            }
            if (this.writer != null) return this.writer;
            /* Fence before any publication path exists: a second writer for
             * the same cluster/generation fails here while the first writer's
             * heartbeat is fresh, instead of publishing concurrently. */
            this.ensureWriterLease();
            this.writerRecoveryInProgress = true;
            try {
                this.ensureRuntime();
                final AeronReplicationCheckpoint checkpoint = this.loadWriterCheckpoint();
                CrashHook.invoke("AFTER_RECOVERY_CHECKPOINT_READ",
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
                                this.settings.epoch(), initialSequence, this.settings.wireNonce())
                                : AeronArchiveReplicationPublisher.Extend(this.archive(), recordingId,
                                this.settings.streamId(), this.settings.replication(), this.settings.clusterId(),
                                this.settings.epoch(), initialSequence, this.settings.wireNonce()));
                    } catch (final RuntimeException failure) {
                        throw reseedRequired("cannot extend configured recording %s after restart; the Archive recording is not safely reusable".formatted(recordingId), failure);
                    }
                } else {
                    if (checkpoint != null && checkpoint.transactionSequence() >= 0) {
                        throw reseedRequired("writer checkpoint has no recording identity", null);
                    }
                    this.writer = (this.settings.externalArchive()
                            ? AeronArchiveReplicationPublisher.NewRemote(this.archive(), this.settings.channels().live(),
                            this.settings.streamId(), this.settings.replication(), this.settings.clusterId(),
                            this.settings.epoch(), initialSequence, this.settings.wireNonce())
                            : AeronArchiveReplicationPublisher.New(this.archive(), this.settings.channels().live(),
                            this.settings.streamId(), this.settings.replication(), this.settings.clusterId(),
                            this.settings.epoch(), initialSequence, this.settings.wireNonce()));
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
                /* Publish the recovered boundary before the outer method drains
                 * deferred reader watermarks. */
                this.writerRecoveryInProgress = false;
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

                /// Acquires the writer fencing lease, failing when another writer holds it.
        ///
        /// The lease carries the monotonically increasing token every envelope,
        /// checkpoint, and cursor publishes; readers reject stale tokens so a
        /// deposed writer cannot interleave history. Acquisition happens before
        /// the writer publication path exists, so a second writer for the same
        /// cluster/generation fails here instead of publishing concurrently.
        private void ensureWriterLease() {
            if (this.writerLease == null) {
                if (this.leaseDirectory == null) {
                    throw new IllegalStateException(
                            "a writer requires a shared lease directory (%s) so it can fence against another writer for the same cluster/generation".formatted(
                                    NodeLibraryPropertiesProvider.Env.EnvKeys.BACKUP_PATH));
                }
                final Duration staleness = Duration.ofMillis(this.leaseStalenessMillis);
                final WriterFencingLease acquired = WriterFencingLease.acquire(
                        this.leaseDirectory, this.settings.clusterId(), this.settings.identity().storeGeneration(),
                        this.settings.identity().nodeId(), staleness,
                        Duration.ofMillis(this.settings.leaseAcquireLockTimeoutMillis()));
                this.writerLease = acquired;
                /* Capture the token once, while the lease is known held. Every
                 * later checkpoint and cursor reuses this snapshot instead of a
                 * live lookup that would fail once the lease is gone. */
                this.writerFencingTokenSnapshot = acquired.fencingToken();
            }
        }

                /// Returns the fencing token captured at lease acquisition.
        ///
        /// Write paths always run after [ensureWriterLease], so the snapshot is
        /// set; a missing snapshot is a defensive fail-closed, never a live-path
        /// lookup that could observe the lease disappearing mid-write.
        private long heldWriterFencingToken() {
            final long token = this.writerFencingTokenSnapshot;
            if (token <= 0) {
                throw new IllegalStateException("writer publishes without a fencing lease; a restart is required");
            }
            return token;
        }

                /// Returns the captured fencing token for the writer position supplier.
        ///
        /// The position path must report a typed unavailability instead of the
        /// bare lease exception used on internal write paths.
        private long positionWriterFencingToken() {
            final long token = this.writerFencingTokenSnapshot;
            if (token <= 0) {
                throw new ReplicationPositionUnavailableException(
                        "Aeron writer fencing lease is not held; no writer position can be established");
            }
            return token;
        }

                /// Returns the newest writer fencing token this reader has accepted.
        private long currentFencingToken() {
            final AeronArchiveReader current = this.readers.current();
            return current == null ? 0L : current.fencingToken();
        }

        private AeronReplicationWriteCoordinator ensureCoordinator() {
            final AeronReplicationWriteCoordinator coordinator;
            synchronized (this) {
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
                                        AtomicFileWriter.delete(inFlightCheckpointPath());
                                    } catch (final IOException failure) {
                                        throw new IllegalStateException("cannot clear local enqueue fence", failure);
                                    }
                                }
                            };
                    final AeronArchiveReplicationPublisher writer = this.ensureWriterLocked();
                    /* Claim the lease token before the coordinator admits its
                     * first transaction, and suspend admission once the lease
                     * is lost: a writer that lost a steal race must stop, not
                     * keep publishing with a stale token readers will reject.
                     * Lease validity is checked before Archive capacity so a
                     * fenced writer fails with a distinct lease-lost error,
                     * never a misleading capacity message. */
                    writer.claimFencingToken(this.heldWriterFencingToken());
                    this.coordinator = writer.newWriteCoordinator(
                            this.settings.replication().durabilityMode(), checkpointWriter,
                            this.archiveCapacity::available,
                            this.writerLeaseGate());
                }
                coordinator = this.coordinator;
            }
            this.drainDeferredWatermarks();
            return coordinator;
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
                final AeronReplicationCheckpoint terminal = this.readOptionalCheckpoint(this.settings.directories().checkpointPath());
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
                        AtomicFileWriter.delete(inFlightPath);
                    } catch (final IOException failure) {
                        throw reseedRequired("cannot clear covered in-flight writer checkpoint", failure);
                    }
                } else {
                    throw reseedRequired("in-flight writer transaction is not covered by a terminal checkpoint %s".formatted(inFlightPath), null);
                }
            }
            final AeronReplicationCheckpoint checkpoint = this.readOptionalCheckpoint(this.settings.directories().checkpointPath());
            if (checkpoint == null) return null;
            this.validateWriterCheckpointIdentity(checkpoint);
            if (checkpoint.state() != AeronReplicationCheckpoint.State.COMMITTED &&
                checkpoint.state() != AeronReplicationCheckpoint.State.REJECTED) {
                throw new ReseedRequiredException(
                        "writer checkpoint is not restartable (state=%s, sequence=%s, path=%s)".formatted(checkpoint.state(), checkpoint.transactionSequence(), this.settings.directories().checkpointPath()));
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
            if (checkpoint.recordType() != AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT ||
                checkpoint.durabilityMode() != this.settings.replication().durabilityMode() ||
                !checkpoint.clusterId().equals(this.settings.clusterId()) ||
                !checkpoint.nodeId().equals(this.settings.identity().nodeId()) ||
                !checkpoint.storeGeneration().equals(this.settings.identity().storeGeneration()) ||
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
            this.writeCheckpoint(this.settings.directories().checkpointPath(), state, sequence, dataLength,
                    dataChunkCount, dataCrc32c, position);
            CrashHook.invoke("AFTER_CHECKPOINT_WRITE_BEFORE_COMMITTED_SEQUENCE_UPDATE", sequence);
            /* The terminal checkpoint is the durable boundary. Publish it to
             * readers before best-effort cleanup of the diagnostic fence so a
             * cleanup failure cannot make a durable commit look unavailable. */
            this.writerBoundary = new AeronWriterBoundary(sequence, this.writerRecordingId.get(), position);
            try {
                AtomicFileWriter.delete(this.inFlightCheckpointPath());
            } catch (final IOException failure) {
                throw new IllegalStateException("cannot clear in-flight writer checkpoint", failure);
            }
        }

        private Path inFlightCheckpointPath() {
            final Path path = this.settings.directories().checkpointPath();
            return path.resolveSibling("%s.inflight".formatted(path.getFileName()));
        }

        private void writeCheckpoint(final Path path, final AeronReplicationCheckpoint.State state,
                                     final long sequence, final int dataLength, final int dataChunkCount,
                                     final int dataCrc32c, final long position) {
            this.writeCheckpoint(path, this.newWriterCheckpoint(
                    state, sequence, dataLength, dataChunkCount, dataCrc32c, position));
        }

        private void writeCheckpoint(final Path path, final AeronReplicationCheckpoint checkpoint) {
            ScopedValue.where(CHECKPOINT_SEQUENCE, checkpoint.transactionSequence()).run(() ->
            {
                try {
                    AeronReplicationCheckpointStore.write(path, checkpoint);
                } catch (final IOException failure) {
                    throw new IllegalStateException("cannot persist writer checkpoint", failure);
                }
            });
        }

        private AeronReplicationCheckpoint newWriterCheckpoint(
                final AeronReplicationCheckpoint.State state, final long sequence,
                final int dataLength, final int dataChunkCount, final int dataCrc32c,
                final long position) {
            final AeronArchiveReplicationPublisher current = this.writer;
            final long discoveredRecordingId = current == null ? Aeron.NULL_VALUE : current.recordingId();
            if (discoveredRecordingId >= 0) this.writerRecordingId.set(discoveredRecordingId);
            final long writerRecordingId = this.writerRecordingId.get();
            final long recordingId = writerRecordingId >= 0 ? writerRecordingId : this.settings.recordingId();
            return new AeronReplicationCheckpoint(
                    AeronReplicationCheckpoint.RecordType.WRITER_CHECKPOINT,
                    this.settings.replication().durabilityMode(),
                    state, this.settings.clusterId(), this.settings.identity().nodeId(), this.settings.identity().storeGeneration(),
                    recordingId, this.settings.epoch(), this.heldWriterFencingToken(), sequence, position,
                    dataLength, dataChunkCount, dataCrc32c);
        }

        private synchronized void ensureRuntime() {
            this.ensureOpen();
            if (this.driverFailure.get() != null) {
                throw new IllegalStateException("Aeron runtime has failed; create a new transport", this.driverFailure.get());
            }
            if (this.runtime != null) return;
            if (this.settings.role().isWriter()) this.archiveCapacity.invalidate();
            this.runtime = AeronRuntime.start(this.settings, this::recordDriverFailure,
                    this::recordSubscriberFailure,
                    () -> CrashHook.invoke("BEFORE_PUBLICATION_CONNECTED", -1L));
            try {
                this.ensureWatermarkChannel();
            } catch (final RuntimeException | Error failure) {
                final Throwable cleanupFailure = this.closeRuntimeQuietly();
                if (cleanupFailure != null && cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
                throw failure;
            }
        }

        /// Records an asynchronous transport failure from an Aeron callback thread.
        ///
        /// This runs on the MediaDriver conductor thread, so it must never
        /// acquire a Transport monitor: reader disposal, writer startup, and
        /// close all take those monitors, and blocking the conductor stalls
        /// every Aeron client sharing the driver. The method only writes
        /// volatile state and calls the reader's lock-free
        /// [AeronArchiveReader#fail], which is safe to invoke from the error
        /// handler without touching transport lifecycle locks.
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
            final AeronArchiveReader current = this.readers.current();
            if (current != null) {
                current.fail(normalized);
            }
        }

        private void recordSubscriberFailure(final Throwable failure) {
            /* Fragment-handler failures are application-level: the polling reader
             * already records them on its own assembler before rethrowing, and Aeron
             * forwards the same instance here. A failed Store import, CRC mismatch,
             * or dispose-time interrupt must never poison the shared
             * MediaDriver/Archive runtime used by subsequent readers, and must not
             * fail a replacement reader installed after the failing poll. Logging
             * here keeps the diagnostic; reader state stays with its own instance. */
            if (this.closing || this.closed) {
                LOGGER.log(System.Logger.Level.DEBUG, "Aeron subscriber failure during shutdown", failure);
            } else {
                LOGGER.log(WARNING, "Aeron subscriber failure", failure);
            }
        }

        private synchronized void stopDriver() {
            this.ensureOpen();
            /* Stopping is idempotent: failure-injection and shutdown callers can race
             * without turning an already stopped runtime into a spurious failure. */
            if (this.runtime == null) return;
            this.runtime.stopDriver();
        }

        private Throwable closeRuntimeQuietly() {
            Throwable failure = null;
            /* Writer-side watermark delivery calls back into this Transport. Stop that
             * worker first so it cannot enter ensureWriter/retention while writer close
             * is in progress. Reader-side channels remain open until the reader exits so
             * its final cursor publication is not turned into a spurious failure. */
            if (this.readers.current() == null && this.settings.role().isWriter() && this.watermarks.hasChannel()) {
                failure = this.closeWatermarkChannel();
            }
            failure = appendFailure(failure, this.closeWatermarkChannel());
            if (this.retention != null) {
                try {
                    this.retention.close();
                    this.retention = null;
                } catch (final RuntimeException retentionFailure) {
                    failure = appendFailure(failure, retentionFailure);
                }
            }
            if (!this.watermarks.hasChannel() && this.retention == null && this.runtime != null) {
                try {
                    this.runtime.close();
                    this.runtime = null;
                } catch (final RuntimeException closeFailure) {
                    failure = appendFailure(failure, closeFailure);
                }
            }
            if (!this.watermarks.hasChannel() && this.retention == null && this.runtime == null) {
                this.watermarks.discardDeferred();
            }
            return failure;
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
                                         final RecordingDescriptorConsumer consumer) {
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
            if (DELIVERY_CALLBACK.isBound()) {
                throw new IllegalStateException(
                        "Aeron transport cannot be re-entered from a reader delivery callback");
            }
        }

        private void runInDeliveryCallback(final Runnable action) {
            if (DELIVERY_CALLBACK.isBound()) {
                throw new IllegalStateException("nested Aeron reader delivery callback");
            }
            ScopedValue.where(DELIVERY_CALLBACK, Boolean.TRUE).run(action);
        }

        private <T> T callInDeliveryCallback(final Supplier<T> action) {
            if (DELIVERY_CALLBACK.isBound()) {
                throw new IllegalStateException("nested Aeron reader delivery callback");
            }
            return ScopedValue.where(DELIVERY_CALLBACK, Boolean.TRUE).call(action::get);
        }

        /// Shuts the transport down in dependency order: reader, write
        /// coordinator, writer, watermark channel, retention, the shared Aeron
        /// runtime, and finally the writer fencing lease.
        ///
        /// The lease is released only after the publication path is fully
        /// gone, so no successor can steal the token while this writer can
        /// still offer. Each stage runs only after the previous one is fully
        /// gone, and a stage that fails leaves the transport retryable instead
        /// of torn down beneath live threads — closing the runtime early would
        /// turn a retryable abort into a use-after-close. All failures are
        /// aggregated and thrown together. Never call this from inside a reader
        /// delivery callback.
        @Override
        public void close() {
            this.ensureNotDeliveryCallback();
            synchronized (this) {
                if (this.closed) return;
                if (this.closing) {
                    throw new IllegalStateException("Aeron transport close is already in progress");
                }
                /* A lease held without any other resource means writer startup
                 * failed after acquisition; it still shuts down on the full
                 * path below, never leaking its file and heartbeat thread. */
                if (this.readers.current() == null && this.writer == null && this.coordinator == null &&
                    this.runtime == null && this.retention == null && !this.watermarks.hasChannel() &&
                    this.writerLease == null) {
                    this.watermarks.discardDeferred();
                    this.closed = true;
                    return;
                }
                this.closing = true;
            }

            final Throwable failure = this.closeSequencer.close();
            if (failure != null) {
                this.closing = false;
                if (failure instanceof Error error) throw error;
                if (failure instanceof RuntimeException r)
                    throw r;
                throw new IllegalStateException("failed to close Aeron transport", failure);
            }
            synchronized (this) {
                this.distributor = null;
                this.distributorStream = null;
                this.watermarks.discardDeferred();
                this.closed = true;
                this.closing = false;
            }
        }

        /// Declares the ordered release stages exactly as the dependency graph
        /// requires them: reader, coordinator, writer, watermark channel,
        /// retention, shared runtime, and finally the writer fencing lease.
        ///
        /// A stage is skipped when its resource is already gone, so a retry
        /// after a failed stage resumes without repeating completed work. The
        /// lease is released only after the publication path is fully stopped,
        /// so no successor can steal the token while this writer can still
        /// offer.
        private List<CloseSequencer.Stage> closeStages() {
            return List.of(
                    CloseSequencer.stage("reader",
                            () -> this.readers.current() != null,
                            this.readers::dispose),
                    /* The coordinator owns every pending transaction and must resolve or
                     * preserve its fence before the Archive wrapper stops the recording.
                     * Closing the raw writer first can publish an ABORT beneath a Store
                     * write that still owns the coordinator. */
                    CloseSequencer.stage("coordinator",
                            () -> this.readers.current() == null && this.coordinator != null,
                            () -> {
                                this.coordinator.dispose();
                                this.coordinator = null;
                            }),
                    CloseSequencer.stage("writer",
                            () -> this.readers.current() == null && this.coordinator == null && this.writer != null,
                            () -> {
                                try {
                                    this.writer.close();
                                    this.writer = null;
                                } catch (final Throwable writerFailure) {
                                    /* A released publication is not proof that the Archive
                                     * recording stopped. Keep the wrapper until isClosed()
                                     * confirms the stop postcondition; otherwise a transient
                                     * external-Archive failure would abandon an active
                                     * recording and make a retry impossible. If the Archive
                                     * connection is definitively gone, however, retaining the
                                     * wrapper also retains the local driver forever and
                                     * prevents the failed process from exiting. The
                                     * checkpoint remains fail-closed, so release the local
                                     * runtime in that terminal case. */
                                    if (this.writer.isClosed() || this.driverFailure.get() != null ||
                                        (this.settings.externalArchive() && isArchiveUnavailable(writerFailure))) {
                                        this.writer = null;
                                    }
                                    throw writerFailure;
                                }
                            }),
                    /* A reader can publish its final durable cursor from the polling
                     * thread. Stop that thread before flushing and closing the watermark
                     * publication. Likewise, keep the writer-side receiver alive until
                     * publication shutdown has completed. */
                    CloseSequencer.stage("watermark",
                            () -> this.readers.current() == null && this.writer == null && this.coordinator == null &&
                                  this.watermarks.hasChannel(),
                            () -> {
                                final RuntimeException watermarkFailure = this.closeWatermarkChannel();
                                if (watermarkFailure != null) throw watermarkFailure;
                            }),
                    CloseSequencer.stage("retention",
                            () -> this.readers.current() == null && this.writer == null && this.coordinator == null &&
                                  !this.watermarks.hasChannel() && this.retention != null,
                            () -> {
                                this.retention.close();
                                this.retention = null;
                            }),
                    /* Do not close the shared runtime while a writer or coordinator still
                     * owns the publication. A failed abort/stop is retryable; closing
                     * Aeron here would turn that retry into a use-after-close and leak
                     * the unresolved sequence. */
                    CloseSequencer.stage("runtime",
                            () -> this.readers.current() == null && this.writer == null && this.coordinator == null &&
                                  this.retention == null && !this.watermarks.hasChannel() && this.runtime != null,
                            () -> {
                                this.runtime.close();
                                this.runtime = null;
                            }),
                    CloseSequencer.stage("writer-lease",
                            () -> this.readers.current() == null && this.writer == null && this.coordinator == null &&
                                  this.runtime == null,
                            () -> {
                                final WriterFencingLease lease;
                                synchronized (this) {
                                    lease = this.writerLease;
                                    this.writerLease = null;
                                }
                                if (lease != null) lease.close();
                            })
            );
        }
    }

        /// Adapts complete Aeron data to the neutral binary receiver.
    private record ReceiverAdapter(Transport owner, StorageBinaryDataReceiver receiver)
            implements StorageBinaryDataReceiver {
        public void receiveTypeDictionary(final String value) {
            this.owner().runInDeliveryCallback(() -> this.receiver().receiveTypeDictionary(value));
        }

        public void receiveData(final Binary value) {
            this.owner().runInDeliveryCallback(() -> {
                this.receiver().receiveData(value);
                this.receiver().awaitApplied();
            });
        }

        @Override
        public boolean receiveDataOwned(final Binary value) {
            return this.owner().callInDeliveryCallback(() -> this.receiver().receiveDataOwned(value));
        }

        @Override
        public boolean canReceiveDataOwned() {
            return this.receiver().canReceiveDataOwned();
        }

        @Override
        public void awaitApplied() {
            this.owner().runInDeliveryCallback(this.receiver()::awaitApplied);
        }
    }

        /// Adds the neutral message-position view to the Aeron client.
    private record ClientAdapter(
            AeronArchiveReader delegate,
            long recordingId,
            UUID clusterId,
            UUID nodeId,
            UUID storeGeneration,
            long epoch) implements StorageBinaryDataClient {
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
