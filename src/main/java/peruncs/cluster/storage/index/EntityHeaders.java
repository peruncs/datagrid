package peruncs.cluster.storage.index;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import peruncs.cluster.errors.CorruptReplicationDataException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/// Reads Store entity headers with bounds checks before native materialization.
public final class EntityHeaders {
    /* Serializer reads these headers through XMemory's native-order accessors. */
    private static final ValueLayout.OfLong LONG =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.nativeOrder());
    private static final int HEADER_BYTES = Binary.entityHeaderLength();

    private EntityHeaders() {
    }

    /// Validates all framing in a normalized direct buffer without copying it.
    ///
    /// @param buffer direct buffer whose position is zero and limit is the logical data length
    /// @throws IllegalArgumentException if the buffer is not direct or normalized
    /// @throws CorruptReplicationDataException if its framing is malformed
    public static void validateFraming(final ByteBuffer buffer) {
        scan(buffer, null);
    }

    static void forEach(final ByteBuffer buffer, final EntityVisitor visitor) {
        scan(buffer, Objects.requireNonNull(visitor, "visitor"));
    }

    /// Visits entity headers in Serializer's channel buffers without copying payload data.
    static void forEach(final Binary binary, final EntityVisitor visitor) {
        Objects.requireNonNull(binary, "binary");
        Objects.requireNonNull(visitor, "visitor");
        final boolean wrapped = binary instanceof ChunksWrapper;
        binary.iterateChannelChunks(channel -> {
            if (channel == null) throw invalid(0L, "null channel");
            for (final ByteBuffer source : channel.buffers()) {
                if (source == null) throw invalid(0L, "null channel buffer");
                /* Serializer's ChunksWrapper uses position as its written
                 * length; ordinary Binary channels use limit. In both cases
                 * entity bytes start at the buffer's zero offset. */
                final int logicalLength = wrapped ? source.position() : source.limit();
                final ByteBuffer view = source.duplicate();
                view.clear().limit(logicalLength);
                scan(view, visitor);
            }
        });
    }

    private static void scan(final ByteBuffer buffer, final EntityVisitor visitor) {
        Objects.requireNonNull(buffer, "buffer");
        if (!buffer.isDirect() || buffer.position() != 0) {
            throw new IllegalArgumentException("entity headers require a normalized direct buffer");
        }
        final MemorySegment segment = MemorySegment.ofBuffer(buffer);
        final long end = segment.byteSize();
        long offset = 0L;
        while (offset < end) {
            if (end - offset < Long.BYTES) throw invalid(offset, "truncated item length");
            final long itemLength;
            try {
                itemLength = segment.get(LONG, offset);
            } catch (final IndexOutOfBoundsException failure) {
                throw invalid(offset, "truncated item length");
            }
            if (itemLength == 0L || itemLength == Long.MIN_VALUE) {
                throw invalid(offset, "invalid item length");
            }
            if (itemLength < 0L) {
                final long commentLength = -itemLength;
                if (commentLength < Long.BYTES || commentLength > end - offset) {
                    throw invalid(offset, "comment exceeds buffer or length field");
                }
                offset += commentLength;
                continue;
            }
            if (itemLength < HEADER_BYTES || itemLength > end - offset) {
                throw invalid(offset, "entity exceeds buffer or header");
            }
            if (visitor != null) {
                final long typeId;
                final long objectId;
                try {
                    typeId = segment.get(LONG, offset + Long.BYTES);
                    objectId = segment.get(LONG, offset + 2L * Long.BYTES);
                } catch (final IndexOutOfBoundsException failure) {
                    throw invalid(offset, "truncated entity header");
                }
                visitor.entity(typeId, objectId, offset, itemLength);
            }
            offset += itemLength;
        }
    }

    private static CorruptReplicationDataException invalid(final long offset, final String reason) {
        return new CorruptReplicationDataException(
                "replicated entity framing is invalid at offset %s: %s".formatted(offset, reason));
    }

    @FunctionalInterface
    interface EntityVisitor {
        void entity(long typeId, long objectId, long offset, long length);
    }
}
