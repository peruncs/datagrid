package peruncs.datagrid.cluster.storage;

import org.agrona.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.zip.CRC32C;

/// Shared CRC32C implementation for replication wire and checkpoint data.
public final class Crc32C {
    private Crc32C() {
    }

    /// Reuses one checksum and direct-buffer view on a single caller thread.
    public static final class Context {
        private final CRC32C checksum = new CRC32C();
        private ByteBuffer source;
        private ByteBuffer view;

        public int compute(final DirectBuffer buffer, final int offset, final int length) {
            Objects.requireNonNull(buffer, "buffer");
            if (offset < 0 || length < 0 || offset > buffer.capacity() - length) {
                throw new IllegalArgumentException("invalid CRC32C range");
            }
            this.checksum.reset();
            final byte[] array = buffer.byteArray();
            if (array != null) {
                this.checksum.update(array, buffer.wrapAdjustment() + offset, length);
            } else {
                final ByteBuffer backing = buffer.byteBuffer();
                if (backing == null) throw new IllegalArgumentException("Agrona buffer has no accessible backing storage");
                if (backing != this.source) {
                    this.source = backing;
                    this.view = backing.duplicate();
                }
                final int start = buffer.wrapAdjustment() + offset;
                this.view.clear().position(start).limit(start + length);
                this.checksum.update(this.view);
            }
            return (int) this.checksum.getValue();
        }
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

}
