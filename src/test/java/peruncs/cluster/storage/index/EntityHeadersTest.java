package peruncs.cluster.storage.index;

import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataIterator;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.types.PersistenceTarget;
import org.eclipse.store.storage.embedded.types.EmbeddedStorage;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.eclipse.store.storage.types.Storage;
import org.eclipse.store.storage.types.StorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import peruncs.cluster.errors.CorruptReplicationDataException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void matchesUpstreamWalkForRealSerializerOutput(@TempDir final Path directory) {
        final StorageConfiguration configuration = StorageConfiguration.Builder()
                .setStorageFileProvider(Storage.FileProvider(directory))
                .setChannelCountProvider(Storage.ChannelCountProvider(1))
                .createConfiguration();
        final var foundation = EmbeddedStorage.Foundation(configuration);
        final var connectionFoundation = foundation.getConnectionFoundation();
        final PersistenceTarget<Binary> delegate = connectionFoundation.getPersistenceTarget();
        final List<Header> captured = new ArrayList<>();
        connectionFoundation.setPersistenceTarget(new PersistenceTarget<>() {
            @Override
            public void write(final Binary binary) {
                final List<Header> headers = new ArrayList<>();
                EntityHeaders.forEach(binary, (typeId, objectId, _, _) -> headers.add(new Header(typeId, objectId)));
                final List<Header> upstreamHeaders = new ArrayList<>();
                final BinaryEntityRawDataIterator iterator = BinaryEntityRawDataIterator.New();
                final boolean wrapped = binary instanceof ChunksWrapper;
                binary.iterateChannelChunks(channel -> {
                    for (final ByteBuffer source : channel.buffers()) {
                        final int length = wrapped ? source.position() : source.limit();
                        final ByteBuffer view = source.duplicate().clear().order(ByteOrder.nativeOrder());
                        final long start = XMemory.getDirectByteBufferAddress(source);
                        assertEquals(0L, iterator.iterateEntityRawData(start, start + length,
                                (entityAddress, _) -> {
                                    final int offset = Math.toIntExact(entityAddress - start);
                                    upstreamHeaders.add(new Header(
                                            view.getLong(offset + Long.BYTES),
                                            view.getLong(offset + 2 * Long.BYTES)));
                                    return true;
                                }));
                    }
                });
                assertEquals(upstreamHeaders, headers,
                        "the bounded scanner must match Serializer's iterator on stored entities");
                captured.addAll(headers);
                delegate.write(binary);
            }

            @Override public boolean isWritable() { return delegate.isWritable(); }
            @Override public void prepareTarget() { delegate.prepareTarget(); }
            @Override public void closeTarget() { delegate.closeTarget(); }
        });

        final SerializerRoot root = new SerializerRoot();
        for (int index = 0; index < 10_000; index++) {
            root.entities.add(new SerializerEntity("serialized entity %d".formatted(index)));
        }
        try (EmbeddedStorageManager storage = foundation.start(root)) {
            storage.storeRoot();
        }

        assertTrue(captured.size() >= 10_000,
                "the test must compare at least 10,000 real serialized Store entities");
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

    public static final class SerializerRoot {
        public List<SerializerEntity> entities = new ArrayList<>();
    }

    public static final class SerializerEntity {
        public String value;

        SerializerEntity(final String value) {
            this.value = value;
        }
    }

}
