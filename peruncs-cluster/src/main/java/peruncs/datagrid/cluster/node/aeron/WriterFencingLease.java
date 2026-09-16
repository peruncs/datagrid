package peruncs.datagrid.cluster.node.aeron;

import peruncs.datagrid.cluster.storage.types.Crc32c;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.*;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/// Renewable external writer lease carrying a monotonically increasing fencing token.
///
/// Only the writer holding the lease for a cluster/generation may publish.
/// The lease lives as one CRC-protected file in the shared backup volume, so
/// two writers configured for the same cluster/generation on different
/// machines fence each other as long as they share that volume:
///
/// - acquisition creates the file atomically and fails while another
///   writer's heartbeat is fresh;
/// - a stale lease is stolen with the token incremented, so every new
///   holder publishes a strictly greater token than any predecessor;
/// - the holder renews its heartbeat in the background and the write path
///   refuses admission once the lease is lost or stale.
///
/// Readers track the greatest token observed and fail closed on a lower one,
/// which bounds the damage when writers do not share a volume: a deposed
/// writer's frames are rejected instead of interleaved.
///
/// The lease directory must be shared by all writers of one cluster. Sharing
/// only the Archive is not enough: separate Archives cannot see each other's
/// lease without the shared volume.
final class WriterFencingLease implements AutoCloseable {
    private static final int MAGIC = 0x4447574c; // DGWL
    private static final short VERSION = 2;
    private static final int ENCODED_BYTES = Integer.BYTES + Short.BYTES + Long.BYTES + Long.BYTES * 2
            + Long.BYTES * 2 + Long.BYTES + Integer.BYTES;
    /// Heartbeat freshness bound used when the caller does not supply one.
    private static final Duration DEFAULT_MAX_STALENESS = Duration.ofSeconds(30);
    /* Serializes acquisition inside one JVM. The file lock below serializes
     * across processes; without this mutex two threads of one process would
     * fail with OverlappingFileLockException instead of acquiring in turn. */
    private static final Object ACQUIRE_LOCK = new Object();
    private static final UUID PROCESS_HOLDER_ID = UUID.randomUUID();
    private static final ConcurrentHashMap<Path, WriterFencingLease> ACTIVE = new ConcurrentHashMap<>();
    /// Bounded wait for an in-flight heartbeat before the lease file is deleted.
    private static final long CLOSE_AWAIT_MILLIS = 5_000L;

        /// Creates and acquires a lease, starting heartbeat renewal.
    ///
    /// Acquisition holds an exclusive lock on a lock file in the lease
    /// directory across the read-decide-write sequence, so two writers racing
    /// on the same shared volume cannot both read an absent lease and mint
    /// duplicate starting tokens. A lease file survives release, so every new
    /// acquisition mints a strictly greater token, including a clean restart
    /// of the same writer. A successor — same node after staleness or a
    /// different node — therefore fences every prior publisher instance.
    /// A missing lease file starts the token series at `1`; readers persist the
    /// greatest token they saw, so a missing file on an established cluster
    /// requires reseeding every reader. A present but corrupt lease (bad magic,
    /// version, or checksum) fails closed instead of resetting the series.
    ///
    /// @param volumeDirectory shared backup volume directory
    /// @param clusterId       replication cluster identity
    /// @param storeGeneration Store generation identity
    /// @param nodeId          acquiring node identity
    /// @param maxStaleness    heartbeat freshness bound
    /// @return held lease
    /// @throws IllegalStateException if another writer holds a fresh lease, the lease file is corrupt,
    ///                               or the token series is exhausted
    public static WriterFencingLease acquire(
            final Path volumeDirectory,
            final UUID clusterId,
            final UUID storeGeneration,
            final UUID nodeId,
            final Duration maxStaleness
    ) {
        Objects.requireNonNull(volumeDirectory, "volumeDirectory");
        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(storeGeneration, "storeGeneration");
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(maxStaleness, "maxStaleness");
        if (maxStaleness.isZero() || maxStaleness.isNegative()) {
            throw new IllegalArgumentException("lease staleness bound must be positive");
        }
        try {
            Files.createDirectories(volumeDirectory);
            if (Files.isSymbolicLink(volumeDirectory)) {
                throw new IOException("writer lease directory must not be a symbolic link");
            }
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot create writer lease directory %s".formatted(volumeDirectory), failure);
        }
        synchronized (ACQUIRE_LOCK) {
            final WriterFencingLease active = ACTIVE.get(volumeDirectory.resolve(
                    "writer-lease-%s-%s.lease".formatted(clusterId, storeGeneration)));
            if (active != null && active.isCurrent()) {
                throw new IllegalStateException("a writer lease is already held in this JVM");
            }
            return acquireLocked(volumeDirectory, clusterId, storeGeneration, nodeId, maxStaleness);
        }
    }

        /// Creates and acquires a lease with the default heartbeat freshness bound.
    ///
    /// @param volumeDirectory shared backup volume directory
    /// @param clusterId       replication cluster identity
    /// @param storeGeneration Store generation identity
    /// @param nodeId          acquiring node identity
    /// @return held lease
    /// @throws IllegalStateException if another writer holds a fresh lease, the lease file is corrupt,
    ///                               or the token series is exhausted
    public static WriterFencingLease acquire(
            final Path volumeDirectory,
            final UUID clusterId,
            final UUID storeGeneration,
            final UUID nodeId
    ) {
        return acquire(volumeDirectory, clusterId, storeGeneration, nodeId, DEFAULT_MAX_STALENESS);
    }

    private static WriterFencingLease acquireLocked(
            final Path volumeDirectory,
            final UUID clusterId,
            final UUID storeGeneration,
            final UUID nodeId,
            final Duration maxStaleness
    ) {
        final Path path = leasePath(volumeDirectory, clusterId, storeGeneration);
        final Path lockPath = volumeDirectory.resolve("writer-lease.lock");
        final long now = System.currentTimeMillis();
        final long token;
        try (final FileChannel lockChannel = FileChannel.open(
                rejectSymbolicLink(lockPath), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             final FileLock ignored = lockChannel.lock()) {
            final LeaseFile existing = readExistingOrNull(path, clusterId, storeGeneration);
            if (existing == null) {
                token = 1L;
                writeAtomically(path, new LeaseFile(token, nodeId, PROCESS_HOLDER_ID, now));
            } else if (now >= existing.heartbeatMillis() &&
                    now - existing.heartbeatMillis() > maxStaleness.toMillis()) {
                try {
                    token = Math.addExact(existing.token(), 1L);
                } catch (final ArithmeticException overflow) {
                    throw new IllegalStateException(
                            "writer fencing token space is exhausted for cluster %s generation %s; manual intervention is required".formatted(
                                    clusterId, storeGeneration),
                            overflow);
                }
                writeAtomically(path, new LeaseFile(token, nodeId, PROCESS_HOLDER_ID, now));
            } else if (existing.nodeId().equals(nodeId)) {
                /* The same writer restarting — cleanly or after a crash, in
                 * this JVM or a new process — mints the next token without
                 * waiting out its own heartbeat. Node identity is the fencing
                 * principal (settings require one stable id per writer), so a
                 * restarted writer is the same writer. Reusing its token would
                 * make in-flight frames from the old instance
                 * indistinguishable from the new holder. A live clone with a
                 * copied node id is deposed instead: the bump makes its frames
                 * stale, and its next renewal fails the holder check. */
                try {
                    token = Math.addExact(existing.token(), 1L);
                } catch (final ArithmeticException overflow) {
                    throw new IllegalStateException(
                            "writer fencing token space is exhausted for cluster %s generation %s; manual intervention is required".formatted(
                                    clusterId, storeGeneration),
                            overflow);
                }
                writeAtomically(path, new LeaseFile(token, nodeId, PROCESS_HOLDER_ID, now));
            } else {
                throw new IllegalStateException(
                        "writer lease for cluster %s generation %s is held by node %s (token %s)".formatted(
                                clusterId, storeGeneration, existing.nodeId(), existing.token()));
            }
        } catch (final OverlappingFileLockException contention) {
            throw new IllegalStateException("concurrent writer lease acquisition is already in progress", contention);
        } catch (final IOException failure) {
            throw new IllegalStateException(
                    "cannot serialize writer lease acquisition for cluster %s generation %s".formatted(
                            clusterId, storeGeneration),
                    failure);
        }
        final WriterFencingLease lease = new WriterFencingLease(path, token, nodeId, PROCESS_HOLDER_ID, maxStaleness);
        ACTIVE.put(path, lease);
        lease.startHeartbeat();
        return lease;
    }

        /// Returns the lease file for one cluster/generation.
    ///
    /// @param volumeDirectory shared backup volume directory
    /// @param clusterId       replication cluster identity
    /// @param storeGeneration Store generation identity
    /// @return lease path
    public static Path leasePath(final Path volumeDirectory, final UUID clusterId, final UUID storeGeneration) {
        return volumeDirectory.resolve("writer-lease-%s-%s.lease".formatted(clusterId, storeGeneration));
    }

    private final Path path;
    private final long token;
    private final UUID nodeId;
    private final UUID holderId;
    private final long maxStalenessMillis;
    private final long checkIntervalMillis;
    private final ScheduledExecutorService heartbeat;
    /* Guards renewal against release: a renew() that entered before close()
     * must finish before the file is deleted, never rewrite after it. The
     * same monitor guards the cached freshness below. */
    private final Object stateLock = new Object();
    private boolean closed;
    private boolean hasCheck;
    private long lastCheckMillis;
    private boolean lastCheckResult;

    private WriterFencingLease(
            final Path path, final long token, final UUID nodeId, final UUID holderId, final Duration maxStaleness) {
        this.path = path;
        this.token = token;
        this.nodeId = nodeId;
        this.holderId = holderId;
        this.maxStalenessMillis = maxStaleness.toMillis();
        this.checkIntervalMillis = Math.max(1L, maxStaleness.toMillis() / 3L);
        this.heartbeat = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().name("dg-writer-lease-heartbeat").factory());
    }

    private void startHeartbeat() {
        final long period = this.checkIntervalMillis;
        this.heartbeat.scheduleAtFixedRate(() -> {
            try {
                this.renew();
            } catch (final RuntimeException failure) {
                System.getLogger(WriterFencingLease.class.getName()).log(System.Logger.Level.WARNING,
                        "writer lease heartbeat failed; write admission is suspended until renewal succeeds",
                        failure);
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

        /// Returns the fencing token this holder publishes with every envelope.
    ///
    /// @return fencing token, always positive
    public long fencingToken() {
        return this.token;
    }

        /// Stops heartbeat renewal while keeping the lease file.
    ///
    /// The lease file survives release on purpose: a clean restart of the same
    /// writer mints the next token, and any successor — same node after
    /// staleness or a different node — mints a strictly greater token. Deleting
    /// the file would reset the series to `1`, and readers persist the greatest
    /// token they ever saw, so a rolling writer restart would brick every
    /// reader until reseed. Only a manually deleted lease file starts a new
    /// series, which then requires reseeding every reader.
    void suspendHeartbeatForTest() {
        this.heartbeat.shutdownNow();
    }

        /// Reports whether this holder still owns a fresh lease.
    ///
    /// The lease file is re-read at most once per third of the staleness
    /// bound; calls inside that window return the cached result instead of
    /// performing file I/O on the write path. A stolen, deleted, or corrupt
    /// lease reports stale on the next re-read and suspends write admission.
    /// A closed lease is never current.
    ///
    /// @return `true` while the lease file still names this holder with a fresh heartbeat
    public boolean isCurrent() {
        final long now = System.currentTimeMillis();
        synchronized (this.stateLock) {
            if (this.closed) {
                return false;
            }
            if (this.hasCheck && now - this.lastCheckMillis < this.checkIntervalMillis) {
                return this.lastCheckResult;
            }
        }
        return this.isCurrentUncached();
    }

        /// Re-reads the lease file without the freshness cache.
    ///
    /// Commit admission uses this path so a stolen lease cannot remain usable
    /// for the normal cached-check interval. The result is also published to
    /// the cache for callers that only need the cheaper [#isCurrent()] guard.
    ///
    /// @return `true` while this holder owns a fresh lease
    boolean isCurrentUncached() {
        synchronized (this.stateLock) {
            if (this.closed) return false;
        }
        final long checkedAt = System.currentTimeMillis();
        final LeaseFile current = readQuietly(this.path);
        final boolean fresh = current != null &&
                (checkedAt < current.heartbeatMillis() ||
                        checkedAt - current.heartbeatMillis() <= this.maxStalenessMillis);
        final boolean result = current != null && current.token() == this.token &&
                current.nodeId().equals(this.nodeId) && current.holderId().equals(this.holderId) && fresh;
        synchronized (this.stateLock) {
            if (this.closed) return false;
            this.hasCheck = true;
            this.lastCheckMillis = checkedAt;
            this.lastCheckResult = result;
            return result;
        }
    }

    private void renew() {
        synchronized (ACQUIRE_LOCK) {
            synchronized (this.stateLock) {
                if (this.closed) {
                    return;
                }
            }
            try {
                final Path lockPath = this.path.getParent().resolve("writer-lease.lock");
                try (final FileChannel lockChannel = FileChannel.open(
                        rejectSymbolicLink(lockPath), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                      final FileLock ignored = lockChannel.lock()) {
                    synchronized (this.stateLock) {
                        if (this.closed) {
                            return;
                        }
                    }
                    final LeaseFile current = readForAcquire(this.path);
                    if (current.token() != this.token || !current.nodeId().equals(this.nodeId) ||
                        !current.holderId().equals(this.holderId)) {
                        throw new IllegalStateException(
                                "writer lease for token %s was stolen or removed; this writer is fenced".formatted(this.token));
                    }
                    writeAtomically(this.path, new LeaseFile(this.token, this.nodeId, this.holderId,
                            System.currentTimeMillis()));
                }
            } catch (final IOException | RuntimeException failure) {
                synchronized (this.stateLock) {
                    this.hasCheck = true;
                    this.lastCheckMillis = System.currentTimeMillis();
                    this.lastCheckResult = false;
                }
                throw new IllegalStateException("writer lease heartbeat renewal failed", failure);
            }
        }
    }

        /// Offers one terminal marker while holding the interprocess lease lock.
    ///
    /// The caller supplies the bounded Aeron offer (back-pressure retries only,
    /// never the slow Archive acknowledgement wait). Ownership is verified under
    /// the same lock file used for acquisition, the offer runs, and the
    /// heartbeat is refreshed before the lock is released — so a successor
    /// racing in another process either blocks until the marker is offered with
    /// a still-current heartbeat, or observes the refreshed heartbeat and fails
    /// its steal. A deposed writer fails here instead of offering a stale marker.
    ///
    /// @param offer bounded marker offer returning the Aeron position
    /// @return Aeron position returned by the offer
    /// @throws IllegalStateException when this holder no longer owns a fresh lease
    public long executeUnderOwnership(final java.util.function.LongSupplier offer) {
        Objects.requireNonNull(offer, "offer");
        synchronized (ACQUIRE_LOCK) {
            synchronized (this.stateLock) {
                if (this.closed) {
                    throw new IllegalStateException("writer fencing lease is closed");
                }
            }
            final Path lockPath = this.path.getParent().resolve("writer-lease.lock");
            try (final FileChannel lockChannel = FileChannel.open(
                    rejectSymbolicLink(lockPath), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                 final FileLock ignored = lockChannel.lock()) {
                final LeaseFile current = readForAcquire(this.path);
                final long now = System.currentTimeMillis();
                final boolean fresh = now < current.heartbeatMillis() ||
                        now - current.heartbeatMillis() <= this.maxStalenessMillis;
                if (current.token() != this.token || !current.nodeId().equals(this.nodeId) ||
                    !current.holderId().equals(this.holderId) || !fresh) {
                    synchronized (this.stateLock) {
                        this.hasCheck = true;
                        this.lastCheckMillis = now;
                        this.lastCheckResult = false;
                    }
                    throw new IllegalStateException(
                            "writer fencing lease lost before commit; this writer is fenced");
                }
                final long position = offer.getAsLong();
                writeAtomically(this.path, new LeaseFile(this.token, this.nodeId, this.holderId,
                        System.currentTimeMillis()));
                return position;
            } catch (final IOException failure) {
                synchronized (this.stateLock) {
                    this.hasCheck = true;
                    this.lastCheckMillis = System.currentTimeMillis();
                    this.lastCheckResult = false;
                }
                throw new IllegalStateException("writer lease commit offer failed", failure);
            }
        }
    }

        /// Stops renewal, leaving the lease file in place for the next holder.
    ///
    /// An in-flight heartbeat is awaited (bounded) before close returns, so a
    /// renewal that entered before close can never rewrite the file after this
    /// holder stopped. The file's heartbeat then ages out: a same-node restart
    /// re-acquires with the next token, and any successor steals the lease with
    /// a strictly greater token once the staleness bound passes. The fencing
    /// token series therefore survives clean restarts, crashes, and takeovers.
    @Override
    public void close() {
        synchronized (this.stateLock) {
            if (this.closed) {
                return;
            }
            this.closed = true;
        }
        this.heartbeat.shutdownNow();
        try {
            if (!this.heartbeat.awaitTermination(CLOSE_AWAIT_MILLIS, TimeUnit.MILLISECONDS)) {
                System.getLogger(WriterFencingLease.class.getName()).log(System.Logger.Level.WARNING,
                        "writer lease heartbeat did not stop before release; a late renewal may have refreshed the heartbeat");
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        ACTIVE.remove(this.path, this);
    }

    private record LeaseFile(long token, UUID nodeId, UUID holderId, long heartbeatMillis) {
    }

    private static LeaseFile readQuietly(final Path path) {
        try {
            return readForAcquire(path);
        } catch (final IOException | RuntimeException failure) {
            return null;
        }
    }

        /// Reads the lease file for acquisition, mapping absence to `null`.
    ///
    /// @param path            lease file
    /// @param clusterId       replication cluster identity, for diagnostics
    /// @param storeGeneration Store generation identity, for diagnostics
    /// @return decoded lease, or `null` when no lease file exists
    /// @throws IllegalStateException when the file is present but corrupt or unreadable
    private static LeaseFile readExistingOrNull(final Path path, final UUID clusterId, final UUID storeGeneration) {
        try {
            return readForAcquire(path);
        } catch (final NoSuchFileException absent) {
            return null;
        } catch (final IOException failure) {
            throw new IllegalStateException(
                    "cannot read writer lease for cluster %s generation %s".formatted(clusterId, storeGeneration),
                    failure);
        }
    }

        /// Reads the lease file for acquisition, distinguishing absence from corruption.
    ///
    /// @param path lease file
    /// @return decoded lease
    /// @throws NoSuchFileException  when no lease file exists; the caller starts a new token series
    /// @throws IOException          when the file cannot be read for a non-corruption reason
    /// @throws IllegalStateException when the file is present but corrupt; acquisition fails closed
    ///                              instead of resetting the token series
    private static LeaseFile readForAcquire(final Path path) throws NoSuchFileException, IOException {
        if (Files.isSymbolicLink(path)) {
            throw new IllegalStateException(
                    "writer lease path %s must not be a symbolic link".formatted(path));
        }
        final byte[] bytes;
        try (final FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            if (channel.size() != ENCODED_BYTES) {
                throw new IllegalStateException(
                        "writer lease file at %s has an unexpected length; refusing to reset the fencing token".formatted(path));
            }
            bytes = new byte[ENCODED_BYTES];
            final ByteBuffer view = ByteBuffer.wrap(bytes);
            while (view.hasRemaining() && channel.read(view) >= 0) {
            }
            if (view.hasRemaining()) {
                throw new IllegalStateException(
                        "writer lease file at %s is truncated; refusing to reset the fencing token".formatted(path));
            }
        } catch (final NoSuchFileException absent) {
            throw absent;
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot read writer lease file at %s".formatted(path), failure);
        }
        final ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (buffer.getInt() != MAGIC || buffer.getShort() != VERSION) {
            throw new IllegalStateException(
                    "writer lease file at %s has an unknown format; refusing to reset the fencing token".formatted(path));
        }
        final long token = buffer.getLong();
        final UUID nodeId = new UUID(buffer.getLong(), buffer.getLong());
        final UUID holderId = new UUID(buffer.getLong(), buffer.getLong());
        final long heartbeat = buffer.getLong();
        if (token <= 0 || heartbeat <= 0 ||
            buffer.getInt() != Crc32c.compute(bytes, 0, ENCODED_BYTES - Integer.BYTES)) {
            throw new IllegalStateException(
                    "writer lease file at %s failed validation; refusing to reset the fencing token".formatted(path));
        }
        return new LeaseFile(token, nodeId, holderId, heartbeat);
    }

    private static Path rejectSymbolicLink(final Path path) throws IOException {
        if (Files.isSymbolicLink(path)) {
            throw new IOException("writer lease lock path must not be a symbolic link: %s".formatted(path));
        }
        return path;
    }

    private static void writeAtomically(final Path path, final LeaseFile lease) {
        final byte[] bytes = new byte[ENCODED_BYTES];
        final ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(MAGIC).putShort(VERSION).putLong(lease.token())
                .putLong(lease.nodeId().getMostSignificantBits()).putLong(lease.nodeId().getLeastSignificantBits())
                .putLong(lease.holderId().getMostSignificantBits()).putLong(lease.holderId().getLeastSignificantBits())
                .putLong(lease.heartbeatMillis());
        buffer.putInt(Crc32c.compute(bytes, 0, ENCODED_BYTES - Integer.BYTES));
        try {
            final Path temporary = Files.createTempFile(path.toAbsolutePath().getParent(), "lease-", ".tmp");
            try {
                /* Force the temporary file to disk before the move so a crash
                 * cannot leave the lease moved but its content unwritten: a
                 * corrupt lease fails closed, which would suspend write
                 * admission on every node sharing the volume. */
                try (final FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                    final ByteBuffer source = ByteBuffer.wrap(bytes);
                    while (source.hasRemaining()) {
                        if (channel.write(source) == 0) {
                            throw new IOException("writer lease write made no progress");
                        }
                    }
                    channel.force(true);
                }
                try {
                    Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (final AtomicMoveNotSupportedException fallback) {
                    /* Best effort without rename atomicity: a torn file is
                     * rejected by validation, so the lease fails closed rather
                     * than resetting the token series. */
                    Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING);
                }
            } finally {
                Files.deleteIfExists(temporary);
            }
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot persist writer lease at %s".formatted(path), failure);
        }
    }
}
