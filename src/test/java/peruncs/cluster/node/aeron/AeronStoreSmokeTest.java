package peruncs.cluster.node.aeron;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
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
/// surefire gate across three forked phases ([AeronStoreSmokeChildMain]): a
/// dedicated process seeds the writer Store, the next process imports one
/// transaction, and a third process restarts everything so the reader resumes
/// from its persisted atomic cursor, imports the next transaction, and the
/// restarted Store still answers both indexes.
///
/// Every phase runs in its own JVM, and a Store that one phase created is
/// only ever reopened by a later phase: closing then reopening the same Store
/// files inside one JVM races Store teardown and failed intermittently with
/// `BinaryBitmapIndex`. Files under the shared root carry stores, cursors,
/// checkpoints, and archives across the restarts.
@Timeout(300)
class AeronStoreSmokeTest {
    /// Verifies writer, reader, and index state survive a restart across the three forked smoke phases.
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

        final String seedOutput = runPhase("seed-store", root, clusterId, generation, writerNodeId, readerNodeId,
                controlPort, livePort, watermarkPort);
        assertTrue(seedOutput.contains("PHASE0-OK"), "seed phase failed: %s".formatted(seedOutput));

        final String seedImportOutput = runPhase("import-verify", root, clusterId, generation, writerNodeId, readerNodeId,
                controlPort, livePort, watermarkPort);
        assertTrue(seedImportOutput.contains("PHASE1-OK"), "import phase failed: %s".formatted(seedImportOutput));

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
        /* Child output goes to a file, not a pipe: a pipe nobody drains while
         * `waitFor` blocks fills up once Aeron or Store logging produces
         * enough output, the child blocks on write, and a healthy run turns
         * into a timeout. */
        final Path output = root.resolve(phase + ".log");
        final Process child = new ProcessBuilder(java, "--enable-preview", "--add-exports",
                "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-cp", classpath, AeronStoreSmokeChildMain.class.getName(),
                phase, root.toString(), clusterId, generation, writerNodeId, readerNodeId,
                Integer.toString(controlPort), Integer.toString(livePort), Integer.toString(watermarkPort))
                .redirectErrorStream(true)
                .redirectOutput(output.toFile())
                .start();
        if (!child.waitFor(240, TimeUnit.SECONDS)) {
            child.destroyForcibly();
            fail("Store smoke phase %s timed out".formatted(phase));
        }
        final String outputText = Files.readString(output);
        assertEquals(0, child.exitValue(), "Store smoke phase %s failed: %s".formatted(phase, outputText));
        return outputText;
    }
}
