package peruncs.cluster.storage.index;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryEntityDataReader;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import peruncs.cluster.errors.CorruptReplicationDataException;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/// Reads Store entity headers with bounds checks before native materialization.
final class EntityHeaders {
    private static final int HEADER_BYTES = Binary.entityHeaderLength();
    private static final VarHandle NATIVE_LONG =
            MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());

    private EntityHeaders() {
    }

    /// Validates framing with native-order reads without copying the buffer.
    ///
    /// @param buffer buffer whose limit is the logical data length
    /// @throws CorruptReplicationDataException if its framing is malformed
    static void validateFraming(final ByteBuffer buffer) {
        scan(buffer, buffer.limit(), null);
    }

    static void forEach(final ByteBuffer buffer, final EntityVisitor visitor) {
        scan(buffer, buffer.limit(), Objects.requireNonNull(visitor, "visitor"));
    }

    /// Visits entity headers in Serializer's binary buffers without copying payload data.
    static void forEach(final Binary binary, final EntityVisitor visitor) {
        Objects.requireNonNull(binary, "binary");
        Objects.requireNonNull(visitor, "visitor");
        forEach(binary, visitor, null);
    }

    /// Visits entity type ids for the writer's bounded index pre-filter.
    static void forEachTypeId(final Binary binary, final TypeIdVisitor visitor) {
        Objects.requireNonNull(binary, "binary");
        Objects.requireNonNull(visitor, "visitor");
        forEach(binary, null, visitor);
    }

    static void forEachTypeId(final ByteBuffer buffer, final TypeIdVisitor visitor) {
        scanTypeIds(buffer, buffer.limit(), Objects.requireNonNull(visitor, "visitor"));
    }

    private static void forEach(final Binary binary, final EntityVisitor visitor,
                                final TypeIdVisitor typeVisitor) {
        final boolean wrapped = binary instanceof ChunksWrapper;
        final BinaryEntityDataReader reader = source -> {
            if (source == null) throw invalid(0L, "null binary buffer");
            /* ChunksWrapper uses position as written length; Store chunks use
             * limit. Both scanners read absolute offsets in Serializer's
             * native byte order without changing the supplied view. */
            final int logicalLength = wrapped ? source.position() : source.limit();
            if (typeVisitor == null) {
                scan(source, logicalLength, visitor);
            } else {
                scanTypeIds(source, logicalLength, typeVisitor);
            }
        };
        binary.iterateEntityData(reader);
    }

    private static void scan(final ByteBuffer buffer, final int end, final EntityVisitor visitor) {
        Objects.requireNonNull(buffer, "buffer");
        if (end < 0 || end > buffer.limit()) {
            throw new IllegalArgumentException("entity header scan length exceeds buffer bounds");
        }
        final boolean nativeOrder = buffer.order() == ByteOrder.nativeOrder();
        int offset = 0;
        while (offset < end) {
            final int remaining = end - offset;
            if (remaining < Long.BYTES) throw invalid(offset, "truncated item length");
            final long itemLength = nativeLong(buffer, offset, nativeOrder);
            if (itemLength > 0L) {
                if (itemLength < HEADER_BYTES || itemLength > remaining) {
                    throw invalid(offset, "entity exceeds buffer or header");
                }
                if (visitor != null) {
                    final long typeId = nativeLong(buffer, offset + Long.BYTES, nativeOrder);
                    final long objectId = nativeLong(buffer, offset + 2 * Long.BYTES, nativeOrder);
                    visitor.entity(typeId, objectId);
                }
                offset += (int) itemLength;
            } else {
                if (itemLength == Long.MIN_VALUE) throw invalid(offset, "invalid item length");
                final long commentLength = -itemLength;
                if (commentLength < Long.BYTES || commentLength > remaining) {
                    throw invalid(offset, "comment exceeds buffer or length field");
                }
                offset += (int) commentLength;
            }
        }
    }

    private static void scanTypeIds(final ByteBuffer buffer, final int end, final TypeIdVisitor visitor) {
        Objects.requireNonNull(buffer, "buffer");
        if (end < 0 || end > buffer.limit()) {
            throw new IllegalArgumentException("entity header scan length exceeds buffer bounds");
        }
        final boolean nativeOrder = buffer.order() == ByteOrder.nativeOrder();
        int offset = 0;
        while (offset < end) {
            final int remaining = end - offset;
            if (remaining < Long.BYTES) throw invalid(offset, "truncated item length");
            final long itemLength = nativeLong(buffer, offset, nativeOrder);
            if (itemLength < 0L) {
                if (itemLength == Long.MIN_VALUE) throw invalid(offset, "invalid item length");
                final long commentLength = -itemLength;
                if (commentLength < Long.BYTES || commentLength > remaining) {
                    throw invalid(offset, "comment exceeds buffer or length field");
                }
                offset += (int) commentLength;
                continue;
            }
            if (itemLength < HEADER_BYTES || itemLength > remaining) {
                throw invalid(offset, "entity exceeds buffer or header");
            }
            visitor.typeId(nativeLong(buffer, offset + Long.BYTES, nativeOrder));
            offset += (int) itemLength;
        }
    }

    private static long nativeLong(final ByteBuffer buffer, final int offset, final boolean bufferHasNativeOrder) {
        return bufferHasNativeOrder ? buffer.getLong(offset) : (long) NATIVE_LONG.get(buffer, offset);
    }

    private static CorruptReplicationDataException invalid(final long offset, final String reason) {
        return new CorruptReplicationDataException(
                "replicated entity framing is invalid at offset %s: %s".formatted(offset, reason));
    }

    @FunctionalInterface
    interface EntityVisitor {
        void entity(long typeId, long objectId);
    }

    @FunctionalInterface
    interface TypeIdVisitor {
        /// Examines one entity type id.
        void typeId(long typeId);
    }
}
