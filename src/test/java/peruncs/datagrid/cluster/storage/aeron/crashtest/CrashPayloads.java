package peruncs.datagrid.cluster.storage.aeron.crashtest;

import java.nio.charset.StandardCharsets;
import java.util.Random;

/// Deterministic transaction payloads for forked crash cells.
///
/// The forked child and its parent must agree byte-for-byte without sharing
/// heap state, so both sides generate through this single helper from
/// `(sequence, size, kind)`. Sizes target envelope, chunk, MTU, and
/// transaction boundaries; kinds cover compressible (`digest`) and
/// incompressible (`random`) bytes. The historical 64-byte digest payload is
/// `sized(sequence, 64, "digest")`.
public final class CrashPayloads {
    /// Default small payload preserving the historical crash-cell bytes.
    public static final int DEFAULT_SIZE = 64;
    /// Default compressible payload kind.
    public static final String DEFAULT_KIND = "digest";
    /// Largest payload a forked cell may use: comfortably under the child's
    /// 256 KiB `maxTransactionBytes` after envelope and chunk framing.
    public static final int MAX_SIZE = 200_000;

    private CrashPayloads() {
    }

    /// Generates the exact payload bytes for one crash transaction.
    ///
    /// @param sequence transaction sequence, selects the deterministic content
    /// @param size     payload bytes, from 1 to [#MAX_SIZE]
    /// @param kind     `digest` (compressible) or `random` (incompressible)
    /// @return fresh payload bytes
    public static byte[] sized(final int sequence, final int size, final String kind) {
        if (size < 1 || size > MAX_SIZE) {
            throw new IllegalArgumentException("crash payload size out of range: %s".formatted(size));
        }
        return switch (kind) {
            case "digest" -> digestFilled(sequence, size);
            case "random" -> randomFilled(sequence, size);
            default -> throw new IllegalArgumentException("unknown crash payload kind: %s".formatted(kind));
        };
    }

    private static byte[] digestFilled(final int sequence, final int size) {
        final byte[] seed = ("dg-crash:%s".formatted(sequence)).getBytes(StandardCharsets.UTF_8);
        final byte[] digest = digest(seed);
        final byte[] result = new byte[size];
        for (int i = 0; i < result.length; i++) result[i] = digest[i % digest.length];
        return result;
    }

    private static byte[] randomFilled(final int sequence, final int size) {
        final Random random = new Random(0xC0FFEEL ^ sequence);
        final byte[] result = new byte[size];
        random.nextBytes(result);
        return result;
    }

    private static byte[] digest(final byte[] value) {
        try {
            return java.security.MessageDigest.getInstance("SHA-256").digest(value);
        } catch (final java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }
}
