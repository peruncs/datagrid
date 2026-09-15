package peruncs.datagrid.cache.types;

import org.junit.jupiter.api.Test;
import peruncs.datagrid.cache.test.ClusteredCacheTestSupport;

import javax.cache.event.EventType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Verifies the timestamp message validation shared by every transport sender.
class TimestampsRegionUpdateMessageTest {
    @Test
    void validMessageIsAccepted() {
        new TimestampsRegionUpdateMessage("cache", "table", 0L);
    }

    @Test
    void nullCacheNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new TimestampsRegionUpdateMessage(null, "table", 1L));
    }

    @Test
    void blankCacheNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new TimestampsRegionUpdateMessage(" ", "table", 1L));
    }

    @Test
    void nullTableNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new TimestampsRegionUpdateMessage("cache", null, 1L));
    }

    @Test
    void blankTableNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new TimestampsRegionUpdateMessage("cache", "", 1L));
    }

    @Test
    void negativeTimestampIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new TimestampsRegionUpdateMessage("cache", "table", -1L));
    }

    @Test
    void fromEventConvertsAValidEvent() {
        final TimestampsRegionUpdateMessage message = TimestampsRegionUpdateMessage.fromEvent(
                ClusteredCacheTestSupport.event("cache", EventType.CREATED, "table", 42L));
        assertEquals("cache", message.cacheName());
        assertEquals("table", message.tableName());
        assertEquals(42L, message.timestamp());
    }

    @Test
    void fromEventRejectsNonStringKey() {
        assertThrows(IllegalArgumentException.class, () -> TimestampsRegionUpdateMessage.fromEvent(
                ClusteredCacheTestSupport.eventWith("cache", EventType.CREATED, 123, 42L)));
    }

    @Test
    void fromEventRejectsNonLongValue() {
        assertThrows(IllegalArgumentException.class, () -> TimestampsRegionUpdateMessage.fromEvent(
                ClusteredCacheTestSupport.eventWith("cache", EventType.CREATED, "table", "not-a-long")));
    }
}
