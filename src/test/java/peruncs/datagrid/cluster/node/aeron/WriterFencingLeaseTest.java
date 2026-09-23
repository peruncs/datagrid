package peruncs.datagrid.cluster.node.aeron;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.datagrid.cluster.test.ChildJava;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the writer fencing lease: only one writer per cluster/generation.
class WriterFencingLeaseTest {
    private static final Duration STALENESS = Duration.ofMillis(300);

    /// Verifies a forked deposed writer reports fenced and never runs its offer after a successor takes over.
    @Test
    void forkedDeposedWriterCannotOfferAfterTakeover(@TempDir final Path volume) throws Exception {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final Process child = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--enable-preview", "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-cp", ChildJava.classpath(), WriterLeaseTakeoverChildMain.class.getName(),
                volume.toString(), cluster.toString(), generation.toString())
                .redirectErrorStream(true).start();
        try {
            final long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (!Files.exists(volume.resolve("child-ready")) && System.nanoTime() < deadline) {
                if (!child.isAlive()) fail("lease child exited: " + new String(child.getInputStream().readAllBytes()));
                Thread.sleep(5);
            }
            assertTrue(Files.exists(volume.resolve("child-ready")), "lease child did not reach offer boundary");
            Thread.sleep(STALENESS.toMillis() * 2L);
            try (WriterFencingLease successor = WriterFencingLease.acquire(
                    volume, cluster, generation, UUID.randomUUID(), STALENESS)) {
                assertEquals(2L, successor.fencingToken());
                Files.writeString(volume.resolve("child-release"), "release");
                assertTrue(child.waitFor(10, TimeUnit.SECONDS), "deposed child did not exit");
                assertEquals(0, child.exitValue(), new String(child.getInputStream().readAllBytes()));
                assertEquals("FENCED", Files.readString(volume.resolve("child-result")));
                assertFalse(Files.exists(volume.resolve("child-offered")),
                        "a deposed writer must not run a terminal offer");
            }
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                child.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    /// Verifies a killed writer process leaves a stale lease that a successor can take over.
    @Test
    void killedForkedWriterCanBeTakenOver(@TempDir final Path volume) throws Exception {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final Process child = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--enable-preview", "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-Ddg.lease.suspendHeartbeat=false",
                "-cp", ChildJava.classpath(), WriterLeaseTakeoverChildMain.class.getName(),
                volume.toString(), cluster.toString(), generation.toString())
                .redirectErrorStream(true).start();
        try {
            final long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (!Files.exists(volume.resolve("child-ready")) && System.nanoTime() < deadline) {
                if (!child.isAlive()) fail("lease child exited: " + new String(child.getInputStream().readAllBytes()));
                Thread.sleep(5);
            }
            assertTrue(Files.exists(volume.resolve("child-ready")), "lease child did not acquire the lease");
            child.destroyForcibly();
            assertTrue(child.waitFor(10, TimeUnit.SECONDS), "killed lease child did not exit");
            Thread.sleep(STALENESS.toMillis() * 2L);
            try (WriterFencingLease successor = WriterFencingLease.acquire(
                    volume, cluster, generation, UUID.randomUUID(), STALENESS)) {
                assertEquals(2L, successor.fencingToken(),
                        "a successor after process death must mint a higher fencing token");
                assertTrue(successor.isCurrent());
            }
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                child.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    /// Verifies a second writer fails to acquire while the first holder stays current.
    @Test
    void secondWriterFailsWhileFirstHoldsTheLease(@TempDir final Path volume) {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        try (final WriterFencingLease first =
                     WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS)) {
            assertEquals(1L, first.fencingToken());
            assertTrue(first.isCurrent());
            assertThrows(IllegalStateException.class, () ->
                    WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS));
        }
    }

    /// Verifies a clean restart mints the next fencing token immediately without waiting for staleness.
    @Test
    void cleanRestartKeepsTheFencingTokenSeries(@TempDir final Path volume) {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final UUID nodeId = UUID.randomUUID();
        long previousToken;
        try (final WriterFencingLease first =
                     WriterFencingLease.acquire(volume, cluster, generation, nodeId, STALENESS)) {
            previousToken = first.fencingToken();
            assertTrue(previousToken >= 1L);
        }
        /* A clean restart fences the previous publisher instance immediately
         * by minting the next token, without waiting for the staleness bound. */
        try (final WriterFencingLease restarted =
                     WriterFencingLease.acquire(volume, cluster, generation, nodeId, STALENESS)) {
            assertEquals(previousToken + 1L, restarted.fencingToken(),
                    "a clean restart must fence the previous publisher with the next token");
            assertTrue(restarted.isCurrent());
        }
    }

    /// Verifies a released lease admits takeover with a monotonic token only after the staleness bound.
    @Test
    void releasedLeaseIsTakenOverWithAMonotonicTokenAfterStaleness(@TempDir final Path volume) throws Exception {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final WriterFencingLease first =
                WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS);
        first.close();
        assertFalse(first.isCurrent(), "closed lease must not report current");
        /* The released file still carries a fresh heartbeat, so a different
         * node must wait out the staleness bound before taking over. */
        final long releasedToken = tokenAt(volume, cluster, generation);
        assertThrows(IllegalStateException.class, () ->
                WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS));
        Thread.sleep(STALENESS.toMillis() * 2L);
        try (final WriterFencingLease successor =
                     WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS)) {
            assertEquals(releasedToken + 1L, successor.fencingToken(),
                    "takeover after release must mint a strictly greater token, never a lower one");
        }
    }

    /// Verifies a lease whose heartbeat stopped is stolen with a strictly greater token.
    @Test
    void staleLeaseIsStolenWithAMonotonicToken(@TempDir final Path volume) throws Exception {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final WriterFencingLease holder =
                WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS);
        assertEquals(1L, holder.fencingToken());
        /* A dead writer stops its heartbeat; the file goes stale and a
         * successor steals the lease with a strictly greater token. The
         * zombie's frames then fail the reader's stale-token floor. */
        holder.suspendHeartbeatForTest();
        Thread.sleep(STALENESS.toMillis() * 2L);
        try (holder;
             final WriterFencingLease successor =
                     WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS)) {
            assertEquals(2L, successor.fencingToken());
            assertTrue(successor.isCurrent());
        }
    }

    /// Verifies heartbeat renewal keeps a live holder current and blocks takeover across the staleness bound.
    @Test
    void heartbeatKeepsTheLeaseCurrent(@TempDir final Path volume) throws Exception {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        try (final WriterFencingLease holder =
                     WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS)) {
            Thread.sleep(STALENESS.toMillis() * 2L);
            assertTrue(holder.isCurrent(), "heartbeat renewal must keep a live holder current");
            assertThrows(IllegalStateException.class, () ->
                    WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS));
        }
    }

    /// Verifies leases for different clusters and generations are held independently.
    @Test
    void leasesAreScopedPerClusterAndGeneration(@TempDir final Path volume) {
        try (final WriterFencingLease first =
                     WriterFencingLease.acquire(volume, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), STALENESS);
             final WriterFencingLease second =
                     WriterFencingLease.acquire(volume, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), STALENESS)) {
            assertTrue(first.isCurrent());
            assertTrue(second.isCurrent());
        }
    }

    /// Verifies racing acquires serialize so exactly one winner mints the starting token and the rest fail.
    @Test
    void concurrentAcquiresSerializeToOneWinner(@TempDir final Path volume) throws Exception {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final int racers = 8;
        final AtomicInteger winners = new AtomicInteger();
        final AtomicInteger rejections = new AtomicInteger();
        final java.util.List<WriterFencingLease> held = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        try (final var pool = java.util.concurrent.Executors.newFixedThreadPool(racers)) {
            final var ready = new java.util.concurrent.CountDownLatch(racers);
            final var starting = new java.util.concurrent.CountDownLatch(1);
            final var done = new java.util.concurrent.CountDownLatch(racers);
            for (int index = 0; index < racers; index++) {
                pool.submit(() -> {
                    ready.countDown();
                    try {
                        assertTrue(starting.await(5, java.util.concurrent.TimeUnit.SECONDS));
                        try {
                            held.add(WriterFencingLease.acquire(
                                    volume, cluster, generation, UUID.randomUUID(), STALENESS));
                            winners.incrementAndGet();
                        } catch (final IllegalStateException heldByWinner) {
                            rejections.incrementAndGet();
                        }
                    } catch (final InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(ready.await(5, java.util.concurrent.TimeUnit.SECONDS));
            starting.countDown();
            assertTrue(done.await(30, java.util.concurrent.TimeUnit.SECONDS));
        } finally {
            for (final WriterFencingLease lease : held) {
                lease.close();
            }
        }
        assertEquals(1, winners.get(), "racing acquires must mint exactly one starting token");
        assertEquals(racers - 1, rejections.get(), "every loser must fail instead of minting a duplicate token");
    }

    /// Verifies a corrupt lease file fails closed instead of resetting the fencing token series.
    @Test
    void corruptLeaseFailsClosedInsteadOfResetting(@TempDir final Path volume) throws Exception {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final Path path = WriterFencingLease.leasePath(volume, cluster, generation);
        Files.write(path, new byte[]{9, 8, 7, 6, 5});
        assertThrows(IllegalStateException.class, () ->
                WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS));
        final var failure = assertThrows(IllegalStateException.class, () ->
                WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS));
        assertTrue(failure.getMessage().contains("refusing to reset the fencing token"),
                "corrupt lease must fail closed, was: %s".formatted(failure.getMessage()));
    }

    /// Verifies a deposed owner never runs its offer after a same-node takeover fences it.
    @Test
    void offerUnderOwnershipFailsAfterTakeoverWithoutOffering(@TempDir final Path volume) {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final UUID nodeId = UUID.randomUUID();
        try (final WriterFencingLease first =
                WriterFencingLease.acquire(volume, cluster, generation, nodeId, STALENESS)) {
            final AtomicInteger offers = new AtomicInteger();
            /* Same-node restart mints the next token immediately, fencing the
             * paused committer without waiting out staleness. Closing first
             * releases this JVM's ACTIVE guard so the successor can acquire. */
            first.close();
            try (final WriterFencingLease successor =
                    WriterFencingLease.acquire(volume, cluster, generation, nodeId, STALENESS)) {
                assertTrue(successor.fencingToken() > 1L);
                final var failure = assertThrows(IllegalStateException.class, () ->
                        first.executeUnderOwnership(() -> {
                            offers.incrementAndGet();
                            return 99L;
                        }));
                assertTrue(failure.getMessage().contains("fenced") || failure.getMessage().contains("closed"),
                        "unexpected failure: " + failure.getMessage());
                assertEquals(0, offers.get(), "a deposed writer must never run its offer");
            }
        }
    }

    /// Verifies close is idempotent and preserves the lease file so the token series survives restarts.
    @Test
    void closeIsIdempotentAndTheLeaseFileSurvives(@TempDir final Path volume) throws Exception {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final Path path = WriterFencingLease.leasePath(volume, cluster, generation);
        final UUID nodeId = UUID.randomUUID();
        final long heldToken;
        try (final WriterFencingLease holder =
                     WriterFencingLease.acquire(volume, cluster, generation, nodeId, STALENESS)) {
            heldToken = holder.fencingToken();
            assertTrue(holder.isCurrent());
            holder.close();
            assertFalse(holder.isCurrent(), "closed lease must not report current");
        }
        /* The file must survive release: deleting it would reset the series to
         * 1 and brick every reader whose persisted floor is above 1. */
        assertTrue(Files.exists(path),
                "close must keep the lease file so the token series survives restarts");
        assertEquals(heldToken, tokenAt(volume, cluster, generation),
                "the released file keeps the holder's token and node identity");
        /* Deleting the lease manually is an operator action that starts a new
         * series; a fresh acquire then mints token 1 and readers must reseed. */
        Files.deleteIfExists(path);
        try (final WriterFencingLease freshSeries =
                     WriterFencingLease.acquire(volume, cluster, generation, nodeId, STALENESS)) {
            assertEquals(1L, freshSeries.fencingToken(), "a manually deleted lease starts a new series");
        }
    }

    /// Reads the fencing token recorded in the lease file (magic, version, token...).
    private static long tokenAt(final Path volume, final UUID cluster, final UUID generation) throws Exception {
        final byte[] bytes = Files.readAllBytes(WriterFencingLease.leasePath(volume, cluster, generation));
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getLong(6);
    }

    /* Encoded layout: int magic, short version, long token, UUID node (16),
     * UUID holder (16), long heartbeat, int crc. */
    private static final int HEARTBEAT_OFFSET = Integer.BYTES + Short.BYTES + Long.BYTES + 16 + 16;

        /// Verifies a throwing commit offer still refreshes the heartbeat before the lock releases.
    ///
    /// Without the catch-path refresh, a failed offer leaves a stale-but-present
    /// lease that a successor can steal while the deposed writer still believes
    /// it holds the lease.
    @Test
    void failedOfferRefreshesHeartbeatBeforeLockRelease(@TempDir final Path volume) throws Exception {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        try (final WriterFencingLease holder =
                     WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS)) {
            holder.suspendHeartbeatForTest();
            final long before = heartbeatAt(volume, cluster, generation);
            Thread.sleep(5L);
            assertThrows(IllegalStateException.class, () -> holder.executeUnderOwnership(
                    () -> {
                        throw new IllegalStateException("injected offer failure");
                    }));
            assertTrue(heartbeatAt(volume, cluster, generation) > before,
                    "a throwing commit offer must refresh the heartbeat before releasing the interprocess lock");
            assertTrue(holder.isCurrent(), "the refreshed lease must still be current after a failed offer");
        }
    }

        /// Verifies a commit refreshes the heartbeat only when renewal is due.
    ///
    /// The per-commit fsync cost must stay proportional to renewal need, not
    /// to commit rate: a fresh heartbeat is reused, and a heartbeat past the
    /// renewal interval is rewritten before the next commit.
    @Test
    void heartbeatRefreshOnCommitIsRateLimited(@TempDir final Path volume) throws Exception {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        try (final WriterFencingLease holder =
                     WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS)) {
            holder.suspendHeartbeatForTest();
            final long acquired = heartbeatAt(volume, cluster, generation);
            holder.executeUnderOwnership(() -> 1L);
            assertEquals(acquired, heartbeatAt(volume, cluster, generation),
                    "a commit inside the renewal interval must not rewrite the heartbeat");
            Thread.sleep(STALENESS.toMillis() / 3L + 60L);
            holder.executeUnderOwnership(() -> 2L);
            assertTrue(heartbeatAt(volume, cluster, generation) > acquired,
                    "a commit past the renewal interval must refresh the heartbeat");
        }
    }

        /// Verifies the holder's monotonic pairing treats a wall-clock offset as fresh
    /// while a foreign acquirer still fails closed on the future-skew bound.
    @Test
    void ownLeaseTreatsWallClockOffsetAsFreshWhileForeignAcquireRejectsIt(@TempDir final Path volume) throws Exception {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final WriterFencingLease holder =
                WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS);
        holder.suspendHeartbeatForTest();
        /* Simulate a backward wall-clock step on the holder by making the file
         * heartbeat look far in the future. The holder pairs it with its own
         * monotonic age and still admits commits. */
        overwriteHeartbeat(volume, cluster, generation, System.currentTimeMillis() + STALENESS.toMillis());
        assertEquals(9L, holder.executeUnderOwnership(() -> 9L));
        holder.close();
        final IllegalStateException failure = assertThrows(IllegalStateException.class, () ->
                WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS));
        assertTrue(failure.getMessage().contains("clock-skew"),
                "a foreign acquirer must fail closed beyond the future-skew bound, was: " + failure.getMessage());
    }

        /// Verifies clock-skew-equivalent takeover stops exactly at the staleness boundary.
    ///
    /// The lease deliberately samples [`System#currentTimeMillis`] directly —
    /// there is no injectable clock seam to fake a skewed peer, and a foreign
    /// process would observe the same wall clock anyway. A successor whose
    /// clock runs ahead of the volume writer's is indistinguishable from a
    /// successor that compares the same heartbeat against a tighter staleness
    /// bound, so this cell replays one fixed heartbeat against two bounds:
    /// below the bound the skewed successor cannot steal and fails closed,
    /// above it the takeover succeeds with a strictly greater token. One
    /// heartbeat, two arithmetic comparisons — fully deterministic, no sleep.
    @Test
    void clockSkewEquivalentTakeoverStopsExactlyAtTheStalenessBoundary(@TempDir final Path volume) throws Exception {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final WriterFencingLease holder =
                WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), Duration.ofSeconds(30));
        holder.suspendHeartbeatForTest();
        holder.close();
        /* Pin the released heartbeat to a fixed past instant; its age defines
         * the boundary the "skewed" successor's tighter/looser bound
         * straddles, with wide margins around the test's own runtime. */
        overwriteHeartbeat(volume, cluster, generation, System.currentTimeMillis() - 10_000L);
        final long ageMillis = System.currentTimeMillis() - heartbeatAt(volume, cluster, generation);
        final IllegalStateException rejected = assertThrows(IllegalStateException.class, () ->
                WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(),
                        Duration.ofMillis(ageMillis + 5_000L)),
                "a skewed successor below the staleness boundary must not steal");
        assertTrue(rejected.getMessage().contains("held by node"),
                "below the boundary the lease still names its holder: " + rejected.getMessage());
        try (final WriterFencingLease successor =
                     WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(),
                             Duration.ofMillis(ageMillis - 500L))) {
            /* Past the boundary the same heartbeat is stale and the takeover
             * mints a strictly greater token, fencing the old writer. */
            assertEquals(2L, successor.fencingToken());
            assertTrue(successor.isCurrent());
        }
    }

    /// Verifies a closed lease never writes another heartbeat and rejects offers.
    @Test
    void closedLeaseNeverWritesHeartbeatAgain(@TempDir final Path volume) throws Exception {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final WriterFencingLease holder =
                WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS);
        holder.close();
        final long afterClose = heartbeatAt(volume, cluster, generation);
        final var failure = assertThrows(IllegalStateException.class, () ->
                holder.executeUnderOwnership(() -> 3L));
        assertTrue(failure.getMessage().contains("closed"), "closed lease must reject offers: " + failure.getMessage());
        Thread.sleep(STALENESS.toMillis() + 50L);
        assertEquals(afterClose, heartbeatAt(volume, cluster, generation),
                "a closed lease must never refresh its heartbeat again");
    }

        /// Verifies an overlapping interprocess lock fails closed within the bounded wait
    /// instead of blocking an unbounded [java.nio.channels.FileChannel#lock].
    @Test
    void overlappingLeaseLockFailsClosed(@TempDir final Path volume) throws Exception {
        final UUID cluster = UUID.randomUUID();
        final UUID generation = UUID.randomUUID();
        final Path lockPath = volume.resolve("writer-lease.lock");
        try (var channel = java.nio.channels.FileChannel.open(
                lockPath, java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.WRITE);
             var ignored = channel.lock()) {
            final long started = System.nanoTime();
            final var failure = assertThrows(IllegalStateException.class, () ->
                    WriterFencingLease.acquire(volume, cluster, generation, UUID.randomUUID(), STALENESS,
                            Duration.ofMillis(100)));
            assertTrue(System.nanoTime() - started < Duration.ofSeconds(5).toNanos(),
                    "a lock held elsewhere in the JVM must fail closed quickly");
            assertNotNull(failure.getCause());
        }
    }

    private static long heartbeatAt(final Path volume, final UUID cluster, final UUID generation) throws Exception {
        final byte[] bytes = Files.readAllBytes(WriterFencingLease.leasePath(volume, cluster, generation));
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getLong(HEARTBEAT_OFFSET);
    }

    private static void overwriteHeartbeat(final Path volume, final UUID cluster, final UUID generation,
                                           final long heartbeatMillis) throws Exception {
        final Path path = WriterFencingLease.leasePath(volume, cluster, generation);
        final byte[] bytes = Files.readAllBytes(path);
        final ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        buffer.putLong(HEARTBEAT_OFFSET, heartbeatMillis);
        buffer.putInt(bytes.length - Integer.BYTES,
                peruncs.datagrid.cluster.storage.Crc32C.compute(bytes, 0, bytes.length - Integer.BYTES));
        Files.write(path, bytes);
    }
}
