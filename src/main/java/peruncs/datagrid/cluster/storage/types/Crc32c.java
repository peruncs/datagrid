package peruncs.datagrid.cluster.storage.types;

import org.agrona.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.zip.CRC32C;

/// Shared CRC32C implementation for replication wire and checkpoint data.
public final class Crc32c {
    private Crc32c() {
    }

    /// Returns a fresh resettable CRC32C accumulator.
    ///
    /// The caller owns the returned accumulator and may use it without locking.
    ///
    /// @return reset CRC32C accumulator
    public static CRC32C accumulator() {
        return new CRC32C();
    }

    /// Returns the CRC32C of a byte range.
    ///
    /// @param bytes  source bytes
    /// @param offset first byte to include
    /// @param length number of bytes to include
    /// @return CRC32C value
    public static int compute(final byte[] bytes, final int offset, final int length) {
        return compute(bytes, offset, length, accumulator());
    }

    /// Returns the CRC32C of a byte range reusing caller-owned state.
    ///
    /// Hot paths must pass their own accumulator instead of allocating one
    /// per call. The accumulator is reset before use and is not retained.
    ///
    /// @param bytes  source bytes
    /// @param offset first byte to include
    /// @param length number of bytes to include
    /// @param reuse  caller-owned accumulator
    /// @return CRC32C value
    public static int compute(final byte[] bytes, final int offset, final int length, final CRC32C reuse) {
        Objects.requireNonNull(bytes, "bytes");
        if (offset < 0 || length < 0 || offset > bytes.length - length) {
            throw new IllegalArgumentException("invalid CRC32C range");
        }
        Objects.requireNonNull(reuse, "reuse");
        reuse.reset();
        reuse.update(bytes, offset, length);
        return (int) reuse.getValue();
    }

    /// Returns the CRC32C of the complete byte array.
    ///
    /// @param bytes source bytes
    /// @return CRC32C value
    public static int compute(final byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        return compute(bytes, 0, bytes.length);
    }

    /// Updates a caller-owned accumulator directly from an Agrona buffer.
    /// Native/direct buffers use the JDK zero-copy path; heap buffers use their
    /// existing backing array. No temporary heap scratch is allocated.
    public static void update(final CRC32C reuse, final DirectBuffer source,
                              final int offset, final int length) {
        Objects.requireNonNull(reuse, "reuse");
        Objects.requireNonNull(source, "source");
        if (offset < 0 || length < 0 || offset > source.capacity() - length) {
            throw new IllegalArgumentException("invalid CRC32C range");
        }
        final byte[] array = source.byteArray();
        if (array != null) {
            reuse.update(array, source.wrapAdjustment() + offset, length);
            return;
        }
        final ByteBuffer buffer = source.byteBuffer();
        if (buffer == null) throw new IllegalArgumentException("Agrona buffer has no accessible backing storage");
        final int start = source.wrapAdjustment() + offset;
        final ByteBuffer view = buffer.duplicate();
        view.position(start).limit(start + length);
        reuse.update(view);
    }

    /// Returns the CRC32C of an Agrona buffer range.
    public static int compute(final DirectBuffer source, final int offset, final int length) {
        final CRC32C reuse = accumulator();
        update(reuse, source, offset, length);
        return (int) reuse.getValue();
    }

}
