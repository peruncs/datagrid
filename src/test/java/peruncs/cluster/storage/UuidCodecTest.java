package peruncs.cluster.storage;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/// Verifies both encodings of a UUID agree and round-trip.
class UuidCodecTest {
    @Test
    void arrayAndBufferEncodingsAreIdenticalAndRoundTrip() {
        final UUID id = UUID.fromString("0f4e7a52-6b0a-4e16-8c63-0f6f5d3c1b11");
        final byte[] array = new byte[UuidCodec.LENGTH + 3];
        assertEquals(3 + UuidCodec.LENGTH, UuidCodec.put(array, 3, id));
        final ByteBuffer buffer = ByteBuffer.allocate(UuidCodec.LENGTH);
        UuidCodec.put(buffer, id);
        assertArrayEquals(buffer.array(), java.util.Arrays.copyOfRange(array, 3, array.length));
        assertEquals(id, UuidCodec.get(array, 3));
        buffer.flip();
        assertEquals(id, UuidCodec.get(buffer));
    }
}
