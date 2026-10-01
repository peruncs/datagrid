package peruncs.cluster.storage;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/// The one wire format for a UUID in node-owned binary formats: two big-endian longs, most significant first.
public final class UuidCodec {
    /// Encoded size of one UUID in bytes.
    public static final int LENGTH = 2 * Long.BYTES;

    private UuidCodec() {
    }

    /// Writes a UUID into a byte array and returns the offset after it.
    ///
    /// @param target array to write into
    /// @param offset first byte to write
    /// @param value  UUID to encode
    /// @return offset of the byte after the UUID
    public static int put(final byte[] target, final int offset, final UUID value) {
        Objects.requireNonNull(value, "value");
        putLong(target, offset, value.getMostSignificantBits());
        putLong(target, offset + Long.BYTES, value.getLeastSignificantBits());
        return offset + LENGTH;
    }

    /// Reads a UUID from a byte array.
    ///
    /// @param source array to read from
    /// @param offset first byte of the UUID
    /// @return the decoded UUID
    public static UUID get(final byte[] source, final int offset) {
        return new UUID(getLong(source, offset), getLong(source, offset + Long.BYTES));
    }

    /// Writes a UUID at the buffer's position and advances it.
    ///
    /// @param target big-endian buffer to write into
    /// @param value  UUID to encode
    public static void put(final ByteBuffer target, final UUID value) {
        Objects.requireNonNull(value, "value");
        target.putLong(value.getMostSignificantBits());
        target.putLong(value.getLeastSignificantBits());
    }

    /// Reads a UUID from the buffer's position and advances it.
    ///
    /// @param source big-endian buffer to read from
    /// @return the decoded UUID
    public static UUID get(final ByteBuffer source) {
        return new UUID(source.getLong(), source.getLong());
    }

    private static void putLong(final byte[] target, final int offset, final long value) {
        for (int index = 0; index < Long.BYTES; index++) {
            target[offset + index] = (byte) (value >>> (Long.BYTES - 1 - index) * Byte.SIZE);
        }
    }

    private static long getLong(final byte[] source, final int offset) {
        long value = 0L;
        for (int index = 0; index < Long.BYTES; index++) {
            value = value << Byte.SIZE | source[offset + index] & 0xffL;
        }
        return value;
    }
}
