package peruncs.cluster.storage.binary;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/// Confirms PerunCS-owned direct memory is reclaimed and remains inspectable through NMT.
class NativeMemoryTrackingTest {
    private static final long ALLOCATED_BYTES = 60L << 20;
    private static final long EXPECTED_RECLAIMED_BYTES = ALLOCATED_BYTES * 3L / 4L;
    private static final Pattern OTHER_COMMITTED = Pattern.compile(
            "^\\s*-\\s+Other \\(reserved=\\d+[KMG]B, committed=(\\d+)([KMG]B)\\)", Pattern.MULTILINE);
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void nativePoolMemoryReturnsNearItsBaseline() throws Exception {
        final Process child = new ProcessBuilder(
                javaExecutable(), "-XX:NativeMemoryTracking=summary", "--enable-preview",
                "--add-modules", "jdk.incubator.vector",
                "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "-cp", testClasspath(),
                NativeMemoryTrackingProbe.class.getName())
                .redirectErrorStream(true)
                .start();
        try (final BufferedReader output = new BufferedReader(
                     new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8));
             final BufferedWriter input = new BufferedWriter(
                     new OutputStreamWriter(child.getOutputStream(), StandardCharsets.UTF_8))) {
            readExpectedLine(output, "ALLOCATED");
            final String allocatedSummary = jcmd(child, "VM.native_memory", "summary");
            final long allocated = otherCommittedBytes(allocatedSummary);

            input.write("release\n");
            input.flush();
            readExpectedLine(output, "RELEASED");
            final String releasedSummary = jcmd(child, "VM.native_memory", "summary");
            assertTrue(allocatedSummary.contains("Native Memory Tracking:") &&
                            releasedSummary.contains("Native Memory Tracking:"),
                    "jcmd must return NMT summaries for both snapshots");
            final long released = otherCommittedBytes(releasedSummary);
            assertTrue(allocated - released >= EXPECTED_RECLAIMED_BYTES,
                    "NMT Other committed bytes should fall by at least 45 MiB after pool close; before=" +
                            allocated + ", after=" + released);

            input.write("exit\n");
            input.flush();
            assertTrue(child.waitFor(10, TimeUnit.SECONDS), "NMT child must exit");
            assertEquals(0, child.exitValue());
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                child.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    private static void readExpectedLine(final BufferedReader output, final String expected) throws Exception {
        CompletableFuture.runAsync(() -> {
            try {
                String line;
                while ((line = output.readLine()) != null) {
                    if (expected.equals(line)) return;
                }
                throw new IllegalStateException("child exited before %s".formatted(expected));
            } catch (final Exception failure) {
                throw new IllegalStateException(failure);
            }
        }).get(15, TimeUnit.SECONDS);
    }

    private static long otherCommittedBytes(final String summary) {
        final Matcher matcher = OTHER_COMMITTED.matcher(summary);
        assertTrue(matcher.find(), "NMT summary must report Other committed memory: " + summary);
        final long value = Long.parseLong(matcher.group(1));
        return switch (matcher.group(2)) {
            case "KB" -> value << 10;
            case "MB" -> value << 20;
            case "GB" -> value << 30;
            default -> throw new AssertionError("unexpected NMT unit " + matcher.group(2));
        };
    }

    private static String jcmd(final Process target, final String... args) throws Exception {
        final String[] command = new String[args.length + 2];
        command[0] = executable("jcmd");
        command[1] = Long.toString(target.pid());
        System.arraycopy(args, 0, command, 2, args.length);
        final Process diagnostic = new ProcessBuilder(command).redirectErrorStream(true).start();
        if (!diagnostic.waitFor(15, TimeUnit.SECONDS)) {
            diagnostic.destroyForcibly();
            fail("jcmd must return");
        }
        final String output = new String(diagnostic.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, diagnostic.exitValue(), output);
        return output;
    }

    private static String javaExecutable() {
        return executable("java");
    }

    private static String executable(final String name) {
        final String suffix = System.getProperty("os.name").startsWith("Windows") ? ".exe" : "";
        return Path.of(System.getProperty("java.home"), "bin", name + suffix).toString();
    }

    private static String testClasspath() {
        return System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    }
}
