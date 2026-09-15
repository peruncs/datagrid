package peruncs.datagrid.cache.types;

import javax.cache.event.CacheEntryEvent;

/**
 * This message carries the newest timestamp known for one cache table.
 *
 * <p>Receivers compare the timestamp with their local value and never move it
 * backwards. The cache and table names identify the local entry to update.
 * {@link #fromEvent} derives the cache name from
 * {@link javax.cache.event.CacheEntryEvent#getSource()} and is intended for
 * created and updated events of the timestamp region.</p>
 *
 * @param cacheName local cache name
 * @param tableName timestamp table key
 * @param timestamp newest timestamp observed by the sender
 */
public record TimestampsRegionUpdateMessage(String cacheName, String tableName, long timestamp) {
    /**
     * Validates the message so a transport can never publish an empty name or a
     * negative timestamp.
     */
    public TimestampsRegionUpdateMessage {
        if (cacheName == null || cacheName.isBlank() || cacheName.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("cacheName must not be blank");
        }
        if (tableName == null || tableName.isBlank() || tableName.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("tableName must not be blank");
        }
        if (timestamp < 0) {
            throw new IllegalArgumentException("timestamp must not be negative");
        }
    }

    /**
     * Converts one timestamp-region cache event into a cluster update message.
     *
     * <p>The timestamp region stores one {@link Long} timestamp per
     * {@link String} table key. The key and value are validated here so every
     * transport sender applies the same rule; a sender that cannot convert an
     * event fails the local cache operation instead of publishing a malformed
     * message.</p>
     *
     * @param <K>   cache key type
     * @param <V>   cache value type
     * @param event cache event produced by the timestamp region
     * @return update message for the event
     * @throws IllegalArgumentException when the event key is not a table name or
     *                                  the event value is not a timestamp
     */
    public static <K, V> TimestampsRegionUpdateMessage fromEvent(
            final CacheEntryEvent<? extends K, ? extends V> event) {
        final Object key = event.getKey();
        final Object value = event.getValue();
        if (!(key instanceof final String tableName)) {
            throw new IllegalArgumentException("Event key is not of type " + String.class.getName());
        }
        if (!(value instanceof final Long timestamp)) {
            throw new IllegalArgumentException("Event value is not of type " + Long.class.getName());
        }
        return new TimestampsRegionUpdateMessage(event.getSource().getName(), tableName, timestamp);
    }
}
