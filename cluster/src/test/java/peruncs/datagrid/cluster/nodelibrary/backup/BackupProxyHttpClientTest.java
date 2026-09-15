package peruncs.datagrid.cluster.nodelibrary.backup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class BackupProxyHttpClientTest {
    private static void serveTwoDownloads(
            final ServerSocket server,
            final byte[] retryBody,
            final byte[] successfulBody,
            final AtomicReference<Throwable> failure
    ) {
        try {
            for (int attempt = 0; attempt < 2; attempt++) {
                try (Socket socket = server.accept()) {
                    readRequestHeaders(socket.getInputStream());
                    final byte[] body = attempt == 0 ? retryBody : successfulBody;
                    final String status = attempt == 0 ? "500 Internal Server Error" : "200 OK";
                    final OutputStream output = socket.getOutputStream();
                    output.write(("HTTP/1.1 %s\r\nContent-Length: %s\r\nConnection: close\r\n\r\n".formatted(status, body.length)).getBytes(StandardCharsets.US_ASCII));
                    output.write(body);
                    output.flush();
                }
            }
        } catch (final Throwable error) {
            failure.set(error);
        }
    }

    private static void readRequestHeaders(final InputStream input) throws IOException {
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        int previous = -1;
        int current;
        while ((current = input.read()) >= 0) {
            request.write(current);
            if (previous == '\r' && current == '\n' && request.size() >= 4) {
                final byte[] bytes = request.toByteArray();
                final int length = bytes.length;
                if (bytes[length - 4] == '\r' && bytes[length - 3] == '\n') return;
            }
            previous = current;
            if (request.size() > 16_384) throw new IOException("request headers are too large");
        }
        throw new IOException("truncated request headers");
    }

    @Test
    void retryTruncatesThePreviousResponseBody(@TempDir final Path root) throws Exception {
        final byte[] retryBody = new byte[512];
        final byte[] successfulBody = "short archive".getBytes(StandardCharsets.UTF_8);
        final AtomicReference<Throwable> serverFailure = new AtomicReference<>();

        try (ServerSocket server = new ServerSocket(0)) {
            final Thread serverThread = new Thread(() -> serveTwoDownloads(
                    server, retryBody, successfulBody, serverFailure), "backup-proxy-test-server");
            serverThread.setDaemon(true);
            serverThread.start();

            final BackupProxyHttpClient client = BackupProxyHttpClient.New(
                    URI.create("http://127.0.0.1:%s".formatted(server.getLocalPort())),
                    2,
                    Duration.ZERO,
                    Duration.ofSeconds(5L),
                    Duration.ofSeconds(1L));
            final Path destination = root.resolve("archive.tar.xz");

            client.download("1.tar.xz", destination);

            serverThread.join(5_000L);
            assertFalse(serverThread.isAlive(), "test server did not serve both attempts");
            if (serverFailure.get() != null) {
                throw new AssertionError("test HTTP server failed", serverFailure.get());
            }
            assertArrayEquals(successfulBody, Files.readAllBytes(destination));
        }
    }
}
