package peruncs.cluster.storage.aeron.crashtest;

import peruncs.cluster.storage.io.AtomicFileWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/// Child process that holds an atomic metadata write open until the parent kills it.
public final class AeronCrashChildMain {
    private AeronCrashChildMain() {
    }

    static void main(final String[] arguments) throws Exception {
        final Path base = Path.of(System.getProperty("dg.crash.base"));
        final Path control = base.resolve("control");
        final Path state = Path.of(System.getProperty("dg.crash.state"));
        Files.createDirectories(control);
        if ("baseline".equals(System.getProperty("dg.crash.mode"))) {
            atomicWrite(state, channel -> write(channel, "baseline"));
            mark(control.resolve("ready"));
            return;
        }
        if ("crash-write".equals(System.getProperty("dg.crash.mode"))) {
            mark(control.resolve("ready"));
            atomicWrite(state, channel ->
            {
                write(channel, "replacement");
                mark(control.resolve("after-temp-write"));
                awaitKill(control.resolve("release"));
            });
            return;
        }
        if ("recover".equals(System.getProperty("dg.crash.mode"))) {
            final String value = Files.readString(state, StandardCharsets.UTF_8);
            Files.writeString(control.resolve("outcome"), "OUTCOME=%s".formatted(value),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);
            return;
        }
        throw new IllegalArgumentException("unknown dg.crash.mode");
    }

    private static void atomicWrite(final Path destination, final AtomicFileWriter.Encoder encoder)
            throws IOException {
        AtomicFileWriter.write(destination, encoder);
    }

    private static void write(final FileChannel channel, final String value) throws IOException {
        final ByteBuffer bytes = StandardCharsets.UTF_8.encode(value);
        while (bytes.hasRemaining()) {
            if (channel.write(bytes) == 0) throw new IOException("metadata write made no progress");
        }
    }

    private static void mark(final Path path) throws IOException {
        final Path temporary = path.resolveSibling("%s.tmp".formatted(path.getFileName()));
        Files.writeString(temporary, "ready", StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
        Files.move(temporary, path, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    private static void awaitKill(final Path release) throws IOException {
        while (!Files.exists(release)) {
            try {
                Thread.sleep(10L);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("crash child interrupted", interrupted);
            }
        }
    }
}
