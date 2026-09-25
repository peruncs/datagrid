package peruncs.cluster.storage.aeron.checkpoint;

import java.util.UUID;

/// Big-endian binary codec shared by the Aeron checkpoint records.
///
/// Every Aeron checkpoint frame starts with the same eight-byte header: a
/// magic int, a version short, and a flags short that must be zero until a
/// format revision assigns a bit meaning. The watermark, replication cursor,
/// and checkpoint store all encode and validate that header through this
/// shared codec so the three formats can never drift apart.
final class AeronCheckpointCodec {
    private AeronCheckpointCodec() {
    }
    /// Number of bytes in one UUID encoding.
    static final int UUID_BYTES = 16;

    /// Offset of the version field within the shared header.
    static final int VERSION_OFFSET = Integer.BYTES;

    /// Offset of the flags field within the shared header.
    static final int FLAGS_OFFSET = Integer.BYTES + Short.BYTES;

    /// Number of bytes in the shared magic, version, and flags header.
    static final int HEADER_LENGTH = Integer.BYTES + Short.BYTES * 2;

    /// Writes the shared `magic`, `version`, zero-`flags` header.
    ///
    /// @param target  destination array
    /// @param offset  first header byte
    /// @param magic   format magic
    /// @param version format version
    /// @return offset after the header
    static int putHeader(final byte[] target, final int offset, final int magic, final short version) {
        int cursor = putInt(target, offset, magic);
        cursor = putShort(target, cursor, version);
        return putShort(target, cursor, (short) 0);
    }

    /// Writes one byte and returns the next offset.
    ///
    /// @param target destination array
    /// @param offset destination offset
    /// @param value  byte value
    /// @return offset after the byte
    static int putByte(final byte[] target, final int offset, final byte value) {
        target[offset] = value;
        return offset + Byte.BYTES;
    }

    /// Reads one byte.
    ///
    /// @param source source array
    /// @param offset source offset
    /// @return byte value
    static byte getByte(final byte[] source, final int offset) {
        return source[offset];
    }

    /// Reads the flags half-word from the shared header.
    ///
    /// @param source frame bytes
    /// @return header flags
    static short headerFlags(final byte[] source) {
        return getShort(source, FLAGS_OFFSET);
    }

    /// Writes one big-endian integer and returns the next offset.
    static int putInt(final byte[] target, final int offset, final int value) {
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
        return offset + Integer.BYTES;
    }

    /// Writes one big-endian short and returns the next offset.
    static int putShort(final byte[] target, final int offset, final short value) {
        target[offset] = (byte) (value >>> 8);
        target[offset + 1] = (byte) value;
        return offset + Short.BYTES;
    }

    /// Writes one big-endian long and returns the next offset.
    static int putLong(final byte[] target, final int offset, final long value) {
        for (int index = Long.BYTES - 1; index >= 0; index--) {
            target[offset + Long.BYTES - 1 - index] = (byte) (value >>> (index * Byte.SIZE));
        }
        return offset + Long.BYTES;
    }

    /// Writes one UUID as two big-endian longs and returns the next offset.
    static int putUuid(final byte[] target, final int offset, final UUID value) {
        int cursor = putLong(target, offset, value.getMostSignificantBits());
        return putLong(target, cursor, value.getLeastSignificantBits());
    }

    /// Reads one big-endian integer.
    static int getInt(final byte[] source, final int offset) {
        return (source[offset] & 0xff) << 24 | (source[offset + 1] & 0xff) << 16
               | (source[offset + 2] & 0xff) << 8 | source[offset + 3] & 0xff;
    }

    /// Reads one big-endian short.
    static short getShort(final byte[] source, final int offset) {
        return (short) ((source[offset] & 0xff) << 8 | source[offset + 1] & 0xff);
    }

    /// Reads one big-endian long.
    static long getLong(final byte[] source, final int offset) {
        long value = 0L;
        for (int index = 0; index < Long.BYTES; index++) {
            value = value << 8 | source[offset + index] & 0xffL;
        }
        return value;
    }

    /// Reads one UUID stored as two big-endian longs.
    static UUID getUuid(final byte[] source, final int offset) {
        return new UUID(getLong(source, offset), getLong(source, offset + Long.BYTES));
    }

    /// Sequential reader over one checkpoint frame; the counterpart to the `put*` writers.
    ///
    /// Each `read*` call returns the value at the current offset and advances
    /// past it, so decoders name each field once instead of repeating a
    /// `get*` plus `offset += SIZE` pair per field.
    static final class FrameReader {
        private final byte[] source;
        private int offset;

        /// Starts reading at the first body byte; callers pass `0` or
        /// [#HEADER_LENGTH] depending on whether they validate the header themselves.
        ///
        /// @param source source frame
        /// @param offset first byte to read
        FrameReader(final byte[] source, final int offset) {
            this.source = source;
            this.offset = offset;
        }

        /// Reads one byte and advances past it.
        ///
        /// @return byte value
        byte readByte() {
            final byte value = getByte(source, offset);
            offset += Byte.BYTES;
            return value;
        }

        /// Reads one big-endian short and advances past it.
        ///
        /// @return short value
        short readShort() {
            final short value = getShort(source, offset);
            offset += Short.BYTES;
            return value;
        }

        /// Reads one big-endian integer and advances past it.
        ///
        /// @return int value
        int readInt() {
            final int value = getInt(source, offset);
            offset += Integer.BYTES;
            return value;
        }

        /// Reads one big-endian long and advances past it.
        ///
        /// @return long value
        long readLong() {
            final long value = getLong(source, offset);
            offset += Long.BYTES;
            return value;
        }

        /// Reads one UUID stored as two big-endian longs and advances past it.
        ///
        /// @return UUID value
        UUID readUuid() {
            final UUID value = getUuid(source, offset);
            offset += UUID_BYTES;
            return value;
        }

        /// Returns the offset of the next unread byte.
        ///
        /// @return current offset
        int offset() {
            return offset;
        }
    }
}
