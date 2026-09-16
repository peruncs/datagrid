package peruncs.datagrid.cluster.node.aeron;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/// Bounded writer/reader/index restart gate for the normal build.
///
/// The full three-reader Lucene/JVector restart scenario lives in
/// `AeronStoreIntegrationIT` behind the `crashmatrix` profile, which the
/// documented `mvn test` and `mvn package` commands never reach because they
/// stop before the Failsafe integration-test phase. This test keeps one
/// writer, one reader, and the embedded Lucene/JVector indexes in the
/// surefire gate across two forked phases ([AeronStoreSmokeChildMain]): the
/// reader imports one transaction, the processes restart, the reader resumes
/// from its persisted atomic cursor and imports the next transaction, and the
/// restarted Store still answers both indexes.
///
/// Each phase runs in its own JVM: several Stores, drivers, and archives in
/// one process must not share a fork with the classpath suites, and files
/// under the shared root carry stores, cursors, checkpoints, and archives
/// across the restart.
@Timeout(300)
class AeronStoreSmokeTest {
    @Test
    void writerReaderAndIndexStateSurviveARestart(@TempDir final Path root) throws Exception {
        final String clusterId = UUID.randomUUID().toString();
        final String generation = UUID.randomUUID().toString();
        final String writerNodeId = UUID.randomUUID().toString();
        final String readerNodeId = UUID.randomUUID().toString();
        /* Ports stay stable across the restart: the Archive recording pins
         * its live channel, so the restarted writer must publish on the same
         * channels. */
        final int controlPort = AeronStoreIntegrationIT.freePort();
        final int livePort = AeronStoreIntegrationIT.freePort();
        final int watermarkPort = AeronStoreIntegrationIT.freePort();

        final String seedOutput = runPhase("seed-import", root, clusterId, generation, writerNodeId, readerNodeId,
                controlPort, livePort, watermarkPort);
        assertTrue(seedOutput.contains("PHASE1-OK"), "seed/import phase failed: %s".formatted(seedOutput));

        final String resumeOutput = runPhase("resume-verify", root, clusterId, generation, writerNodeId, readerNodeId,
                controlPort, livePort, watermarkPort);
        assertTrue(resumeOutput.contains("SMOKE-OK"), "resume/verify phase failed: %s".formatted(resumeOutput));
    }

    private static String runPhase(
            final String phase,
            final Path root,
            final String clusterId,
            final String generation,
            final String writerNodeId,
            final String readerNodeId,
            final int controlPort,
            final int livePort,
            final int watermarkPort
    ) throws Exception {
        final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        final String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        final Process child = new ProcessBuilder(java, "--enable-preview", "--add-exports",
                "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-cp", classpath, AeronStoreSmokeChildMain.class.getName(),
                phase, root.toString(), clusterId, generation, writerNodeId, readerNodeId,
                Integer.toString(controlPort), Integer.toString(livePort), Integer.toString(watermarkPort))
                .redirectErrorStream(true).start();
        if (!child.waitFor(240, TimeUnit.SECONDS)) {
            child.destroyForcibly();
            fail("Store smoke phase %s timed out".formatted(phase));
        }
        final String output = new String(child.getInputStream().readAllBytes());
        assertEquals(0, child.exitValue(), "Store smoke phase %s failed: %s".formatted(phase, output));
        return output;
    }
}
