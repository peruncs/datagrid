package peruncs.datagrid.cluster.storage.aeron.checkpoint;

import java.util.UUID;

/**
 * Big-endian binary codec shared by the Aeron checkpoint records.
 *
 * <p>The watermark and replication cursor use this shared package-private
 * implementation for integer, long, and UUID fields, keeping their wire
 * formats consistent.</p>
 */
final class AeronCheckpointCodec {
    /** Number of bytes in one UUID encoding. */
    static final int UUID_BYTES = 16;

    private AeronCheckpointCodec() {
    }

    /** Writes one big-endian integer and returns the next offset. */
    static int putInt(final byte[] target, final int offset, final int value) {
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
        return offset + Integer.BYTES;
    }

    /** Writes one big-endian short and returns the next offset. */
    static int putShort(final byte[] target, final int offset, final short value) {
        target[offset] = (byte) (value >>> 8);
        target[offset + 1] = (byte) value;
        return offset + Short.BYTES;
    }

    /** Writes one big-endian long and returns the next offset. */
    static int putLong(final byte[] target, final int offset, final long value) {
        for (int index = Long.BYTES - 1; index >= 0; index--) {
            target[offset + Long.BYTES - 1 - index] = (byte) (value >>> (index * Byte.SIZE));
        }
        return offset + Long.BYTES;
    }

    /** Writes one UUID as two big-endian longs and returns the next offset. */
    static int putUuid(final byte[] target, final int offset, final UUID value) {
        int cursor = putLong(target, offset, value.getMostSignificantBits());
        return putLong(target, cursor, value.getLeastSignificantBits());
    }

    /** Reads one big-endian integer. */
    static int getInt(final byte[] source, final int offset) {
        return (source[offset] & 0xff) << 24 | (source[offset + 1] & 0xff) << 16
               | (source[offset + 2] & 0xff) << 8 | source[offset + 3] & 0xff;
    }

    /** Reads one big-endian short. */
    static short getShort(final byte[] source, final int offset) {
        return (short) ((source[offset] & 0xff) << 8 | source[offset + 1] & 0xff);
    }

    /** Reads one big-endian long. */
    static long getLong(final byte[] source, final int offset) {
        long value = 0L;
        for (int index = 0; index < Long.BYTES; index++) {
            value = value << 8 | source[offset + index] & 0xffL;
        }
        return value;
    }

    /** Reads one UUID stored as two big-endian longs. */
    static UUID getUuid(final byte[] source, final int offset) {
        return new UUID(getLong(source, offset), getLong(source, offset + Long.BYTES));
    }
}
