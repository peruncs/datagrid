package peruncs.cluster.storage.index;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataIterator;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.memory.XMemory;
import org.junit.jupiter.api.Test;
import peruncs.cluster.errors.CorruptReplicationDataException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityHeadersTest {
    @Test
    void readsEntityIdsAndSkipsSerializerComments() {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(8 + 24).order(ByteOrder.nativeOrder());
        buffer.putLong(-8L);
        buffer.putLong(24L).putLong(17L).putLong(29L);
        buffer.flip();
        final List<Long> ids = new ArrayList<>();

        EntityHeaders.forEach(buffer, (_, objectId, _, _) -> ids.add(objectId));

        assertEquals(List.of(29L), ids);
    }

    @Test
    void normalizesWrappedBinaryAtItsWrittenPosition() {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(64).order(ByteOrder.nativeOrder());
        buffer.putLong(24L).putLong(17L).putLong(29L);
        final List<Long> ids = new ArrayList<>();

        EntityHeaders.forEach(ChunksWrapper.New(buffer), (_, objectId, _, _) -> ids.add(objectId));

        assertEquals(List.of(29L), ids);
    }

    @Test
    void rejectsTruncatedOrInvalidFraming() {
        assertInvalid(ByteBuffer.allocateDirect(7));
        assertInvalid(item(0L, 8));
        assertInvalid(item(-7L, 8));
        assertInvalid(item(Long.MIN_VALUE, 8));
        assertInvalid(item(23L, 24));
        assertInvalid(item(25L, 24));
        assertInvalid(item(24L, 24 + 3));
    }

    @Test
    void matchesUpstreamEntityWalkForTenThousandEntities() {
        final int count = 10_000;
        final Random random = new Random(0xF1A11L);
        final ByteBuffer buffer = ByteBuffer.allocateDirect(count * 64).order(ByteOrder.nativeOrder());
        final List<Header> expected = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            final int length = Binary.entityHeaderLength() + random.nextInt(6) * Long.BYTES;
            final long typeId = random.nextLong();
            final long objectId = random.nextLong();
            buffer.putLong(length).putLong(typeId).putLong(objectId);
            expected.add(new Header(typeId, objectId));
            for (int padding = Binary.entityHeaderLength(); padding < length; padding++) {
                buffer.put((byte) random.nextInt());
            }
        }
        final Binary binary = ChunksWrapper.New(buffer);

        final List<Header> actual = new ArrayList<>(count);
        EntityHeaders.forEach(binary, (typeId, objectId, _, _) -> actual.add(new Header(typeId, objectId)));

        /* Serializer owns the framing walk; PerunCS owns only the bounds-checked header scan.
         * This upstream iterator reports entity boundaries, while the expected list above
         * independently verifies the type/object ids read at each boundary. */
        final BinaryEntityRawDataIterator upstream = BinaryEntityRawDataIterator.New();
        final int[] upstreamCount = {0};
        binary.iterateChannelChunks(channel -> {
            for (final ByteBuffer source : channel.buffers()) {
                final long length = source.position();
                final long start = XMemory.getDirectByteBufferAddress(source);
                assertEquals(0L, upstream.iterateEntityRawData(start, start + length, (entity, bound) -> {
                    upstreamCount[0]++;
                    return true;
                }));
            }
        });

        assertEquals(expected, actual);
        assertEquals(count, actual.size());
        assertEquals(count, upstreamCount[0]);
    }

    @Test
    void tenThousandRandomCorruptLengthsAreRejectedAsReplicationData() {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(64).order(ByteOrder.nativeOrder());
        buffer.putLong(24).putLong(17).putLong(29);
        buffer.putLong(-8).putLong(24).putLong(31).putLong(43);
        buffer.flip();
        final byte[] original = new byte[buffer.remaining()];
        buffer.duplicate().get(original);
        final Random random = new Random(0xC0FFEE);

        for (int iteration = 0; iteration < 10_000; iteration++) {
            buffer.clear().put(original).flip();
            long corruptLength = random.nextLong();
            if (corruptLength > 0L && corruptLength <= buffer.limit()) {
                corruptLength = buffer.limit() + 1L;
            } else if (corruptLength < 0L && corruptLength != Long.MIN_VALUE && -corruptLength <= buffer.limit()) {
                corruptLength = -buffer.limit() - 1L;
            }
            buffer.putLong(0, corruptLength);
            assertThrows(CorruptReplicationDataException.class, () -> EntityHeaders.validateFraming(buffer),
                    "mutation " + iteration);
            assertThrows(CorruptReplicationDataException.class,
                    () -> EntityHeaders.forEach(buffer, (_, _, _, _) -> { }), "mutation " + iteration);
        }
    }

    private static ByteBuffer item(final long length, final int capacity) {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(capacity).order(ByteOrder.nativeOrder());
        buffer.putLong(length);
        buffer.position(0);
        buffer.limit(capacity);
        return buffer;
    }

    private static void assertInvalid(final ByteBuffer buffer) {
        assertThrows(CorruptReplicationDataException.class, () -> EntityHeaders.validateFraming(buffer));
    }

    private record Header(long typeId, long objectId) {
    }

}
