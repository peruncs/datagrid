package peruncs.datagrid.cluster.node.aeron;

import peruncs.datagrid.cluster.errors.WriterFencedException;
import peruncs.datagrid.cluster.storage.Crc32C;
import peruncs.datagrid.cluster.storage.ReplicationRetry;
import peruncs.datagrid.cluster.storage.aeron.writer.CrashHook;
import peruncs.datagrid.cluster.storage.aeron.writer.WriterLeaseGate;
import peruncs.datagrid.cluster.storage.io.AtomicFileWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.lang.System.Logger.Level.WARNING;

/// Chooses the only writer allowed to publish for this Store generation.
///
/// The lease is renewable and carries a monotonically increasing fencing
/// token.
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
///
/// ## Clock requirements
///
/// Freshness is ultimately a wall-clock comparison across machines, so every
/// writer sharing the volume must run a synchronized wall clock (NTP, chrony,
/// or equivalent hypervisor time sync) with drift well below the configured
/// staleness bound. The holder additionally pairs its heartbeat with its own
/// monotonic clock, so a backward wall-clock step does not immediately stale
/// the holder's own view while the monotonic age stays within bound. Foreign
/// readers cannot pair clocks across processes: a peer whose clock is ahead
/// by more than half the staleness bound fails acquisition closed instead of
/// stealing, and an operator must fix time sync before the lease can move.
final class WriterFencingLease implements AutoCloseable {
    private static final int MAGIC = 0x4447574c; // DGWL
    private static final short VERSION = 2;
    private static final int ENCODED_BYTES = Integer.BYTES + Short.BYTES + Long.BYTES + Long.BYTES * 2
            + Long.BYTES * 2 + Long.BYTES + Integer.BYTES;
    /* Serializes acquisition, renewal, terminal offers, and release for one
     * lease path inside this JVM. The interprocess file lock below serializes
     * across processes; without a per-path mutex two threads of one process
     * would fail with OverlappingFileLockException instead of serializing.
     * The map is keyed by absolute lease path and never pruned: one entry per
     * cluster/generation is negligible and pruning would race a concurrent
     * acquisition. */
    private static final ConcurrentHashMap<Path, Object> PATH_MUTEXES = new ConcurrentHashMap<>();
    private static final UUID PROCESS_HOLDER_ID = UUID.randomUUID();
    private static final ConcurrentHashMap<Path, WriterFencingLease> ACTIVE = new ConcurrentHashMap<>();
    /// Bounded wait for an in-flight heartbeat before the lease is released.
    private static final long CLOSE_AWAIT_MILLIS = 5_000L;
    /// Default bounded wait for the interprocess lease lock.
    private static final Duration DEFAULT_LOCK_TIMEOUT = Duration.ofSeconds(5);
    private static final long LOCK_RETRY_PARK_NANOS = 1_000_000L;

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
    ///                               the interprocess lock cannot be acquired within the default
    ///                               bounded wait, or the token series is exhausted
    static WriterFencingLease acquire(
            final Path volumeDirectory,
            final UUID clusterId,
            final UUID storeGeneration,
            final UUID nodeId,
            final Duration maxStaleness
    ) {
        return acquire(volumeDirectory, clusterId, storeGeneration, nodeId, maxStaleness, DEFAULT_LOCK_TIMEOUT);
    }

        /// Creates and acquires a lease with an explicit interprocess lock wait.
    ///
    /// @param volumeDirectory shared backup volume directory
    /// @param clusterId       replication cluster identity
    /// @param storeGeneration Store generation identity
    /// @param nodeId          acquiring node identity
    /// @param maxStaleness    heartbeat freshness bound
    /// @param lockTimeout     bounded wait for the interprocess lease lock
    /// @return held lease
    /// @throws IllegalStateException when the lease cannot be acquired
    static WriterFencingLease acquire(
            final Path volumeDirectory,
            final UUID clusterId,
            final UUID storeGeneration,
            final UUID nodeId,
            final Duration maxStaleness,
            final Duration lockTimeout
    ) {
        Objects.requireNonNull(volumeDirectory, "volumeDirectory");
        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(storeGeneration, "storeGeneration");
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(maxStaleness, "maxStaleness");
        Objects.requireNonNull(lockTimeout, "lockTimeout");
        if (maxStaleness.isZero() || maxStaleness.isNegative()) {
            throw new IllegalArgumentException("lease staleness bound must be positive");
        }
        if (lockTimeout.isZero() || lockTimeout.isNegative()) {
            throw new IllegalArgumentException("lease lock wait must be positive");
        }
        Path canonicalVolume = volumeDirectory.toAbsolutePath().normalize();
        try {
            Files.createDirectories(canonicalVolume);
            AtomicFileWriter.ensureNoSymbolicLinks(canonicalVolume);
            /* One canonical path per physical directory: `/tmp` and
             * `/private/tmp` are the same volume on macOS, and without
             * toRealPath they would mint two PATH_MUTEXES/ACTIVE keys so an
             * orphan recorded under one alias would not block the other. */
            canonicalVolume = canonicalVolume.toRealPath();
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot create writer lease directory %s".formatted(canonicalVolume), failure);
        }
        final Path path = leasePath(canonicalVolume, clusterId, storeGeneration);
        synchronized (mutexFor(path)) {
            final WriterFencingLease active = ACTIVE.get(path);
            if (active != null && (active.releaseUnproven || active.isCurrent())) {
                throw new IllegalStateException("a writer lease is already held or its release is unresolved in this JVM");
            }
            return acquireLocked(canonicalVolume, clusterId, storeGeneration, nodeId, maxStaleness, lockTimeout);
        }
    }

    private static Object mutexFor(final Path path) {
        return PATH_MUTEXES.computeIfAbsent(path.toAbsolutePath().normalize(), ignored -> new Object());
    }

    private static WriterFencingLease acquireLocked(
            final Path volumeDirectory,
            final UUID clusterId,
            final UUID storeGeneration,
            final UUID nodeId,
            final Duration maxStaleness,
            final Duration lockTimeout
    ) {
        final Path path = leasePath(volumeDirectory, clusterId, storeGeneration);
        final Path lockPath = volumeDirectory.resolve("writer-lease.lock");
        final long token;
        final long writeNanos;
        final long now;
        try (final FileChannel lockChannel = openLockChannel(lockPath);
             final FileLock ignored = lockFile(lockChannel, lockTimeout)) {
            /* Sample the wall clock only once the interprocess lock is held: a
             * long wait for another writer must not make the freshly written
             * heartbeat look older than it is, and the acquire decision must be
             * based on the clock at decision time, not at open time. */
            now = System.currentTimeMillis();
            final LeaseFile existing = readExistingOrNull(path, clusterId, storeGeneration);
            if (existing == null) {
                token = 1L;
                writeAtomically(path, new LeaseFile(token, nodeId, PROCESS_HOLDER_ID, now));
            } else if (futureSkewed(now, existing.heartbeatMillis(), maxStaleness.toMillis() / 2L)) {
                throw new IllegalStateException(
                        "writer lease heartbeat is in the future beyond the configured clock-skew bound; synchronize writer clocks");
            } else if (now - existing.heartbeatMillis() > maxStaleness.toMillis()) {
                token = nextToken(existing.token(), clusterId, storeGeneration);
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
                token = nextToken(existing.token(), clusterId, storeGeneration);
                writeAtomically(path, new LeaseFile(token, nodeId, PROCESS_HOLDER_ID, now));
            } else {
                throw new IllegalStateException(
                        "writer lease for cluster %s generation %s is held by node %s (token %s)".formatted(
                                clusterId, storeGeneration, existing.nodeId(), existing.token()));
            }
            writeNanos = System.nanoTime();
        } catch (final OverlappingFileLockException contention) {
            throw new IllegalStateException("concurrent writer lease acquisition is already in progress", contention);
        } catch (final IOException failure) {
            throw new IllegalStateException(
                    "cannot serialize writer lease acquisition for cluster %s generation %s".formatted(
                            clusterId, storeGeneration),
                    failure);
        }
        final WriterFencingLease lease = new WriterFencingLease(path, token, nodeId, maxStaleness, lockTimeout, writeNanos);
        ACTIVE.put(path, lease);
        lease.startHeartbeat();
        return lease;
    }

    private static long nextToken(final long current, final UUID clusterId, final UUID storeGeneration) {
        try {
            return Math.addExact(current, 1L);
        } catch (final ArithmeticException overflow) {
            throw new IllegalStateException(
                    "writer fencing token space is exhausted for cluster %s generation %s; manual intervention is required".formatted(
                            clusterId, storeGeneration),
                    overflow);
        }
    }

        /// Acquires the interprocess lock with a bounded wait.
    ///
    /// An unbounded [FileChannel#lock()] can park a writer (and, through the
    /// shared path mutex, every other lease operation in this JVM) for as long
    /// as another process holds the lock, including a wedged peer. The
    /// bounded retry fails closed instead. [OverlappingFileLockException] is
    /// ignored here because the per-path JVM mutex already serializes threads
    /// of one process; a stray overlap means another path aliased the lock and
    /// the bounded wait then expires.
    ///
    /// @param channel        channel for the lease lock file
    /// @param lockTimeout    bounded wait
    /// @return held file lock, released by the caller
    /// @throws IOException when the lock cannot be acquired within the bound
    private static FileLock lockFile(final FileChannel channel, final Duration lockTimeout) throws IOException {
        final long deadline = ReplicationRetry.deadlineNanos(lockTimeout.toNanos());
        for (; ; ) {
            FileLock lock;
            try {
                lock = channel.tryLock();
            } catch (final OverlappingFileLockException overlap) {
                /* Serialized by the per-path mutex; a genuine overlap here means
                 * the same file is reachable through a different path and must
                 * fail closed rather than spin forever. */
                throw new IOException("writer lease lock is already held by this process through another path", overlap);
            }
            if (lock != null) return lock;
            if (ReplicationRetry.expired(deadline)) {
                throw new IOException(
                        "timed out after %s ms waiting for the writer lease lock".formatted(lockTimeout.toMillis()));
            }
            LockSupport.parkNanos(LOCK_RETRY_PARK_NANOS);
        }
    }

        /// Returns the lease file for one cluster/generation.
    ///
    /// @param volumeDirectory shared backup volume directory
    /// @param clusterId       replication cluster identity
    /// @param storeGeneration Store generation identity
    /// @return lease path
    static Path leasePath(final Path volumeDirectory, final UUID clusterId, final UUID storeGeneration) {
        return volumeDirectory.toAbsolutePath().normalize()
                .resolve("writer-lease-%s-%s.lease".formatted(clusterId, storeGeneration));
    }

    private final Path path;
    private final long token;
    private final UUID nodeId;
    private final UUID holderId;
    private final long maxStalenessNanos;
    private final long checkIntervalNanos;
    /* Bounded wait for every interprocess lease lock (acquisition, renewal,
     * commit-offer proof, and release). Configured at acquisition so a
     * deployment with a slow shared volume widens every lock wait, not only
     * the initial one. */
    private final Duration lockTimeout;
    private final ScheduledExecutorService heartbeat;
    /* Guards renewal against release and the cached freshness below. A renew()
     * that entered before close() completes is additionally kept from writing
     * by the closed check performed while holding the interprocess file lock,
     * so a heartbeat can never be written after close() returns. */
    private final Object stateLock = new Object();
    private boolean closed;
    private volatile boolean releaseUnproven;
    private boolean hasCheck;
    private long lastCheckNanos;
    private boolean lastCheckResult;
    /* Monotonic timestamp of the heartbeat this holder last wrote. It lets the
     * holder treat a backward or forward wall-clock step as fresh while its
     * own monotonic age stays within the staleness bound; a foreign reader
     * cannot pair clocks across processes and must rely on wall time alone
     * (see the class javadoc). */
    private volatile long lastWriteNanos;
    /* A heartbeat task that dies on an Error silently cancels a
     * ScheduledExecutorService periodic task: the lease must fail closed
     * with that cause recorded until an operator restarts it, instead of
     * merely going stale. */
    private final java.util.concurrent.atomic.AtomicReference<RuntimeException> heartbeatFailure =
            new java.util.concurrent.atomic.AtomicReference<>();

    private WriterFencingLease(
            final Path path, final long token, final UUID nodeId, final Duration maxStaleness,
            final Duration lockTimeout, final long lastWriteNanos) {
        this.path = path;
        this.token = token;
        this.nodeId = nodeId;
        this.holderId = PROCESS_HOLDER_ID;
        this.maxStalenessNanos = maxStaleness.toNanos();
        this.checkIntervalNanos = Math.max(1L, this.maxStalenessNanos / 3L);
        this.lockTimeout = lockTimeout;
        this.lastWriteNanos = lastWriteNanos;
        this.heartbeat = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().name("dg-writer-lease-heartbeat").factory());
    }

    private void startHeartbeat() {
        final long period = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(this.checkIntervalNanos));
        /* The heartbeat runs on its own virtual thread, outside the caller's
         * scoped bindings. Inherit the crash hook explicitly so forked crash
         * children can park inside a renewal; unbound in production. */
        this.heartbeat.scheduleAtFixedRate(CrashHook.inheritCurrent(() -> {
            try {
                this.renew();
            } catch (final RuntimeException failure) {
                System.getLogger(WriterFencingLease.class.getName()).log(WARNING,
                        "writer lease heartbeat failed; write admission is suspended until renewal succeeds",
                        failure);
            } catch (final Error failure) {
                /* An Error escaping a periodic task cancels it silently on the
                 * executor: record the cause so write admission fails closed
                 * with a named terminal failure, then let the throw end the
                 * heartbeat. A permanent fix is a restart, not supression. */
                this.heartbeatFailure.compareAndSet(null,
                        new IllegalStateException("writer lease heartbeat worker died; this writer is fenced", failure));
                throw failure;
            }
        }), period, period, TimeUnit.MILLISECONDS);
    }

        /// Returns the fencing token this holder publishes with every envelope.
    ///
    /// @return fencing token, always positive
    long fencingToken() {
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

    /// Re-reads the lease file with a rate-limited ownership proof.
    ///
    /// The physical re-read runs at most once per third of the staleness
    /// bound; calls inside that window reuse the cached result, so a busy
    /// write path cannot turn every admission check into file I/O. Use
    /// [#executeUnderOwnership] for the authoritative per-commit proof: it
    /// verifies ownership while holding the interprocess lock that serializes
    /// acquisition and takeover.
    ///
    /// @return `true` while this holder owns a fresh lease
    boolean isCurrent() {
        final long nowNanos = System.nanoTime();
        synchronized (this.stateLock) {
            if (this.closed || this.releaseUnproven || this.heartbeatFailure.get() != null) return false;
            if (this.hasCheck && nowNanos - this.lastCheckNanos < this.checkIntervalNanos) {
                return this.lastCheckResult;
            }
        }
        final LeaseFile current = readQuietly(this.path);
        final boolean result = current != null && this.matchesHolder(current) && this.ownLeaseFresh(nowNanos);
        synchronized (this.stateLock) {
            if (this.closed || this.releaseUnproven || this.heartbeatFailure.get() != null) return false;
            this.hasCheck = true;
            this.lastCheckNanos = nowNanos;
            this.lastCheckResult = result;
            return result;
        }
    }

    private boolean matchesHolder(final LeaseFile current) {
        return current.token() == this.token && current.nodeId().equals(this.nodeId) &&
               current.holderId().equals(this.holderId);
    }

        /// Reports whether the heartbeat this holder last wrote is still within
    /// the staleness bound on the holder's monotonic clock.
    ///
    /// The wall-clock heartbeat written by the holder may look stale after a
    /// forward step or fresh after a backward step; the monotonic age is the
    /// authority for the holder's own view. Renewal keeps it below the bound,
    /// so an age beyond the bound means renewal has stalled and admission must
    /// suspend regardless of the wall reading.
    private boolean ownLeaseFresh(final long nowNanos) {
        final long writeNanos = this.lastWriteNanos;
        final long age = nowNanos - writeNanos;
        return writeNanos != 0L && age >= 0L && age <= this.maxStalenessNanos;
    }

    private static boolean futureSkewed(final long now, final long heartbeat, final long boundMillis) {
        return heartbeat > now && heartbeat - now > boundMillis;
    }

    private void renew() {
        final Path lockPath = this.path.getParent().resolve("writer-lease.lock");
        synchronized (mutexFor(this.path)) {
            synchronized (this.stateLock) {
                if (this.closed || this.releaseUnproven || this.heartbeatFailure.get() != null) return;
            }
            try (final FileChannel lockChannel = openLockChannel(lockPath);
                 final FileLock ignored = lockFile(lockChannel, this.lockTimeout)) {
                final LeaseFile current = readForAcquire(this.path);
                if (!this.matchesHolder(current)) {
                    throw new WriterFencedException(
                            "writer lease for token %s was stolen or removed; this writer is fenced".formatted(this.token));
                }
                /* Renewal window: the interprocess lock is held and the holder
                 * was verified, but the new heartbeat is not durable yet. */
                CrashHook.invoke("BEFORE_LEASE_RENEWAL_HEARTBEAT", this.token);
                this.refreshHeartbeatLocked();
            } catch (final IOException | RuntimeException failure) {
                synchronized (this.stateLock) {
                    this.hasCheck = true;
                    this.lastCheckNanos = System.nanoTime();
                    this.lastCheckResult = false;
                }
                if (failure instanceof WriterFencedException fenced) {
                    /* Fencing loss must surface with its own type: wrapping it in a
                     * generic renewal failure would misdirect recovery diagnostics. */
                    throw fenced;
                }
                throw new IllegalStateException("writer lease heartbeat renewal failed", failure);
            }
        }
    }

        /// Writes a fresh heartbeat while the caller holds the interprocess lock.
    ///
    /// A closed lease is never rewritten: the check runs under the state lock,
    /// so a concurrent [close()] cannot return while this method is about to
    /// write. The wall heartbeat and the writing process's monotonic timestamp
    /// are stored together for [#ownLeaseFresh].
    ///
    private void refreshHeartbeatLocked() {
        final long writeMillis = System.currentTimeMillis();
        final long writeNanos = System.nanoTime();
        synchronized (this.stateLock) {
            if (this.closed) return;
        }
        writeAtomically(this.path, new LeaseFile(this.token, this.nodeId, this.holderId, writeMillis));
        this.lastWriteNanos = writeNanos;
        synchronized (this.stateLock) {
            if (this.closed) return;
            this.hasCheck = true;
            this.lastCheckNanos = writeNanos;
            this.lastCheckResult = true;
        }
    }

    /// Returns the time budget for one terminal-offer attempt.
    ///
    /// The budget is a third of the staleness bound, floored at one nanosecond,
    /// so a deposed writer still has room to renew before its lease can go
    /// stale. The lock acquisition in [#executeUnderOwnership] is capped by the
    /// same value.
    ///
    /// @return per-attempt offer budget in nanos, always positive
    long terminalOfferBudgetNanos() {
        return Math.max(1L, this.maxStalenessNanos / 3L);
    }

    /// Makes one publication attempt while holding the interprocess lease lock.
    ///
    /// The caller supplies one non-blocking Aeron offer attempt. Ownership is
    /// verified under the same lock file used for acquisition, the offer runs,
    /// and the heartbeat is refreshed before the lock is released — so a
    /// successor racing in another process either blocks until the marker is
    /// offered with a still-current heartbeat, or observes the refreshed
    /// heartbeat and fails its steal. A deposed writer fails here instead of
    /// offering a stale marker.
    ///
    /// The heartbeat is rewritten only when it has aged past a third of the
    /// staleness bound, and is also refreshed in the offer's failure path while
    /// the interprocess lock is still held, so a throwing offer cannot leave a
    /// stale-but-present lease for a successor to steal. This keeps the shared
    /// volume's per-commit fsync cost proportional to renewal need rather than
    /// to commit rate.
    ///
    /// @param offer offer operation receiving the per-attempt ownership check
    /// @return Aeron position returned by the offer
    /// @throws WriterFencedException when this holder no longer owns a fresh lease
    long executeUnderOwnership(final WriterLeaseGate.OwnedOffer offer) {
        Objects.requireNonNull(offer, "offer");
        synchronized (mutexFor(this.path)) {
            synchronized (this.stateLock) {
                if (this.closed) {
                    throw new WriterFencedException("writer fencing lease is closed");
                }
                final RuntimeException heartbeatDeath = this.heartbeatFailure.get();
                if (heartbeatDeath != null) {
                    throw new WriterFencedException(
                            "writer fencing lease heartbeat died; this writer is fenced, restart required",
                            heartbeatDeath);
                }
                if (this.releaseUnproven) {
                    throw new WriterFencedException(
                            "writer fencing lease release was unresolved; this writer is fenced");
                }
            }
            final Path lockPath = this.path.getParent().resolve("writer-lease.lock");
            try (final FileChannel lockChannel = openLockChannel(lockPath);
                 final FileLock ignored = lockFile(lockChannel,
                         Duration.ofNanos(Math.min(this.lockTimeout.toNanos(), this.terminalOfferBudgetNanos())))) {
                final LeaseFile current = readForAcquire(this.path);
                final long nowNanos = System.nanoTime();
                final boolean fresh = this.matchesHolder(current) && this.ownLeaseFresh(nowNanos);
                if (!fresh) {
                    this.publishFailedCheck(nowNanos);
                    throw new WriterFencedException(
                            "writer fencing lease lost before commit; this writer is fenced");
                }
                this.refreshHeartbeatIfDueLocked();
                try {
                    final long position = offer.offer(this::isCurrent);
                    /* Post-offer window: the terminal marker is offered while
                     * the interprocess lock is still held, but the protecting
                     * heartbeat refresh has not run yet. */
                    CrashHook.invoke("AFTER_OWNED_OFFER_BEFORE_HEARTBEAT", this.token);
                    this.refreshHeartbeatIfDueLocked();
                    return position;
                } catch (final RuntimeException | Error failure) {
                    /* The offer held the interprocess lock for an unknown time and
                     * may now be older than the renewal interval. Refresh before
                     * releasing the lock so a successor cannot observe a
                     * stale-but-present lease caused by this failure; never let a
                     * refresh failure mask the original offer failure. */
                    try {
                        this.refreshHeartbeatLocked();
                    } catch (final RuntimeException refreshFailure) {
                        failure.addSuppressed(refreshFailure);
                    }
                    throw failure;
                }
            } catch (final IOException failure) {
                this.publishFailedCheck(System.nanoTime());
                throw new IllegalStateException("writer lease commit offer failed", failure);
            }
        }
    }

    private void refreshHeartbeatIfDueLocked() {
        if (System.nanoTime() - this.lastWriteNanos <= this.checkIntervalNanos) return;
        this.refreshHeartbeatLocked();
    }

    private void publishFailedCheck(final long nowNanos) {
        synchronized (this.stateLock) {
            this.hasCheck = true;
            this.lastCheckNanos = nowNanos;
            this.lastCheckResult = false;
        }
    }

    /// Stops renewal, leaving the lease file in place for the next holder.
    ///
    /// An in-flight heartbeat is awaited (bounded) and the interprocess lock
    /// is acquired (bounded) before close returns. A renewal that entered
    /// before close observes `closed` under the state lock while holding the
    /// interprocess lock and never rewrites the file, so no heartbeat write
    /// can happen after close completes. The file's heartbeat then ages out:
    /// a same-node restart re-acquires with the next token, and any successor
    /// steals the lease with a strictly greater token once the staleness bound
    /// passes. The fencing token series therefore survives clean restarts,
    /// crashes, and takeovers.
    @Override
    public synchronized void close() {
        synchronized (this.stateLock) {
            if (this.closed) return;
        }
        this.heartbeat.shutdownNow();
        try {
            if (!this.heartbeat.awaitTermination(CLOSE_AWAIT_MILLIS, TimeUnit.MILLISECONDS)) {
                System.getLogger(WriterFencingLease.class.getName()).log(WARNING,
                        "writer lease heartbeat did not stop before release; a late renewal may have refreshed the heartbeat");
            }
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        final Path lockPath = this.path.getParent().resolve("writer-lease.lock");
        boolean releaseProven = false;
        try (final FileChannel lockChannel = openLockChannel(lockPath);
             final FileLock ignored = lockFile(lockChannel, this.lockTimeout)) {
            /* Nothing to write: the file intentionally survives release.
             * Holding the interprocess lock here proves that a renewal or
             * terminal offer which passed the closed check has finished. A
             * bounded failure is logged rather than thrown: `closed` already
             * prevents every future renewal write, and close() must stay
             * callable while an unrelated offer is in flight. */
            if (!ignored.isValid()) {
                throw new IOException("writer lease lock became invalid during release");
            }
            releaseProven = true;
        } catch (final IOException | RuntimeException releaseFailure) {
            this.releaseUnproven = true;
            System.getLogger(WriterFencingLease.class.getName()).log(WARNING,
                    "writer lease lock was not available during release; a concurrent heartbeat or offer may still be finishing",
                    releaseFailure);
        }
        if (releaseProven) {
            synchronized (this.stateLock) {
                this.closed = true;
            }
            ACTIVE.remove(this.path, this);
        }
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
            while (view.hasRemaining()) {
                if (channel.read(view) < 0) break;
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
            buffer.getInt() != Crc32C.compute(bytes, 0, ENCODED_BYTES - Integer.BYTES)) {
            throw new IllegalStateException(
                    "writer lease file at %s failed validation; refusing to reset the fencing token".formatted(path));
        }
        return new LeaseFile(token, nodeId, holderId, heartbeat);
    }

    private static final FileAttribute<Set<PosixFilePermission>> LOCK_FILE_PERMISSIONS =
            PosixFilePermissions.asFileAttribute(Set.of(
                    PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));

    /// Opens the interprocess lock file with owner-only permissions, matching
    /// the lease file itself: a world-writable lock file would let any local
    /// account hold the writer's fencing lock.
    private static FileChannel openLockChannel(final Path lockPath) throws IOException {
        AtomicFileWriter.ensureNoSymbolicLinks(lockPath);
        return (FileChannel) Files.newByteChannel(lockPath,
                Set.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE), LOCK_FILE_PERMISSIONS);
    }

    private static void writeAtomically(final Path path, final LeaseFile lease) {
        final byte[] bytes = new byte[ENCODED_BYTES];
        final ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(MAGIC).putShort(VERSION).putLong(lease.token())
                .putLong(lease.nodeId().getMostSignificantBits()).putLong(lease.nodeId().getLeastSignificantBits())
                .putLong(lease.holderId().getMostSignificantBits()).putLong(lease.holderId().getLeastSignificantBits())
                .putLong(lease.heartbeatMillis());
        buffer.putInt(Crc32C.compute(bytes, 0, ENCODED_BYTES - Integer.BYTES));
        try {
            AtomicFileWriter.writeBytes(path, bytes);
        } catch (final IOException failure) {
            throw new IllegalStateException("cannot persist writer lease at %s".formatted(path), failure);
        }
    }
}
