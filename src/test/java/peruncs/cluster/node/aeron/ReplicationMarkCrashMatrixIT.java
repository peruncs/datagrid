package peruncs.cluster.node.aeron;

import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.storage.aeron.mark.ReplicationMark;
import peruncs.cluster.test.ChildJava;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/// Kills real Store commits and verifies the named root and user data recover atomically.
@Timeout(value = 20, unit = TimeUnit.MINUTES)
class ReplicationMarkCrashMatrixIT {
    private static final int TRIALS = 200;

    @Test
    void namedMarkAndFourMegabytesOfStoreDataRecoverTogetherAfterProcessKill(@TempDir final Path root)
            throws Exception {
        final SplittableRandom delays = new SplittableRandom(0xA1_5_2026L);
        for (int trial = 0; trial < TRIALS; trial++) {
            final Path trialPath = root.resolve("trial-%03d".formatted(trial));
            final Path storePath = trialPath.resolve("store");
            final Path controlPath = trialPath.resolve("control");
            final UUID clusterId = UUID.randomUUID();
            final UUID generation = UUID.randomUUID();
            Files.createDirectories(controlPath);
            Process child = null;
            try {
                seed(storePath, clusterId, generation);
                child = startWriter(storePath, controlPath, clusterId, generation, trialPath.resolve("writer.log"));
                awaitCommitEntry(child, controlPath.resolve("commit-entered"),
                        trialPath.resolve("writer.log"));

                TimeUnit.MILLISECONDS.sleep(delays.nextLong(51L));
                final boolean killed = child.isAlive();
                if (killed) {
                    /* On Unix this is SIGKILL: Store cannot run a shutdown hook or flush another commit. */
                    child.destroyForcibly();
                    assertTrue(child.waitFor(15, TimeUnit.SECONDS), "killed writer did not exit");
                } else {
                    assertTrue(Files.exists(controlPath.resolve("commit-done")),
                            "writer exited before completing the commit: " + Files.readString(trialPath.resolve("writer.log")));
                }

                verifyRecovered(storePath, clusterId, generation);
            } finally {
                if (child != null && child.isAlive()) {
                    child.destroyForcibly();
                    assertTrue(child.waitFor(15, TimeUnit.SECONDS), "writer child did not stop during cleanup");
                }
                deleteTree(trialPath);
            }
        }
    }

    private static void seed(final Path storePath, final UUID clusterId, final UUID generation) {
        final var mark = new ReplicationMark(clusterId, generation, 1L, 17L);
        try (EmbeddedStorageManager storage = ReplicationMarkCrashChildMain.foundation(storePath, mark)
                .start(new ReplicationMarkCrashChildMain.CrashRoot())) {
            storage.storeAll(storage.root(), mark);
        }
    }

    private static Process startWriter(final Path storePath, final Path controlPath, final UUID clusterId,
                                       final UUID generation, final Path logPath)
            throws Exception {
        final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        return new ProcessBuilder(java, "--enable-preview", "--add-modules", "jdk.incubator.vector",
                "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-cp", ChildJava.classpath(), ReplicationMarkCrashChildMain.class.getName(),
                storePath.toString(), controlPath.toString(), clusterId.toString(), generation.toString())
                .redirectErrorStream(true)
                .redirectOutput(logPath.toFile())
                .start();
    }

    private static void awaitCommitEntry(final Process child, final Path entered, final Path log)
            throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L);
        while (child.isAlive() && !Files.exists(entered) && System.nanoTime() < deadline) {
            java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1L));
        }
        assertTrue(Files.exists(entered), "writer did not enter Store commit: " + Files.readString(log));
    }

    private static void verifyRecovered(final Path storePath, final UUID clusterId, final UUID generation) {
        final var mark = new ReplicationMark(clusterId, generation, 1L, 17L);
        try (EmbeddedStorageManager storage = ReplicationMarkCrashChildMain.foundation(storePath, mark).start()) {
            final var root = (ReplicationMarkCrashChildMain.CrashRoot) storage.root();
            assertEquals(clusterId, mark.clusterId());
            assertEquals(generation, mark.storeGeneration());
            if (mark.sequence() == -1L) {
                assertNull(root.latest, "uncommitted payload survived without its mark");
                return;
            }
            assertEquals(0L, mark.sequence(), "unexpected recovered mark sequence");
            assertNotNull(root.latest, "mark survived without its transaction");
            assertEquals(mark.sequence(), root.latest.sequence);
            assertEquals(ReplicationMarkCrashChildMain.ENTITY_COUNT, root.latest.entities.length);
            for (int index = 0; index < root.latest.entities.length; index++) {
                final var entity = root.latest.entities[index];
                assertEquals(mark.sequence(), entity.sequence);
                assertEquals(index, entity.index);
                assertEquals(ReplicationMarkCrashChildMain.ENTITY_BYTES, entity.payload.length);
                for (final byte value : entity.payload) {
                    if (value != (byte) index) fail("payload entity %s was only partly recovered".formatted(index));
                }
            }
        }
    }

    private static void deleteTree(final Path path) throws Exception {
        if (!Files.exists(path)) return;
        try (var paths = Files.walk(path)) {
            for (final Path entry : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(entry);
        }
    }
}
