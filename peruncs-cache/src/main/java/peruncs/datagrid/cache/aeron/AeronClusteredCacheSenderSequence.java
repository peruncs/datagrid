package peruncs.datagrid.cache.aeron;

import java.util.concurrent.ConcurrentHashMap;

/// Process-wide monotonic sequence shared by every sender of one configured
/// node identity on one channel.
///
/// Several providers on one node may share a configured `node-id`, so
/// their senders must draw from one sequence: a remote receiver keys gap
/// tracking by the wire identity, and two independent per-instance counters
/// would interleave (0, 0, 1, 1) and report false gaps for roughly half the
/// frames.
///
/// The sequence is keyed by the channel and stream id in addition to the
/// identity: the same node id used on two different channels has independent
/// sequences, because the receivers on each channel only ever see the frames
/// of that channel.
///
/// Only configured node identities enter this map. Entries remain for the
/// life of the JVM after the final provider lease closes, preserving sequence
/// continuity when a cache provider is recreated while remote receivers are
/// still attached. A provider without a configured node id owns a per-provider
/// sequence instead, because its random identity is unique per provider
/// instance.
final class AeronClusteredCacheSenderSequence {
    private static final ConcurrentHashMap<Key, Entry> SEQUENCES = new ConcurrentHashMap<>();

    private AeronClusteredCacheSenderSequence() {
    }

        /// Acquires a sequence lease for one configured identity.
    static SequenceLease acquire(final byte[] senderId, final String channel, final int streamId) {
        final Key key = new Key(channel, streamId, AeronClusteredCacheMessageCodec.senderIdOf(senderId));
        final Entry entry = SEQUENCES.computeIfAbsent(key, ignored -> new Entry());
        return new SequenceLease(entry);
    }

        /// Key of one shared sequence: an identity on one channel and stream.
    private record Key(String channel, int streamId, AeronClusteredCacheMessageCodec.SenderId sender) {
    }

    private static final class Entry {
        private final Object lock = new Object();
        private long sequence;
    }

        /// A sender-owned reference to the shared sequence.
    static final class SequenceLease implements AutoCloseable {
        private final Entry entry;

        private SequenceLease(final Entry entry) {
            this.entry = entry;
        }

                /// Creates an isolated sequence for a provider without a configured identity.
        static SequenceLease local() {
            return new SequenceLease(new Entry());
        }

        long current() {
            return this.entry.sequence;
        }

        void advance() {
            this.entry.sequence++;
        }

        Object lock() {
            return this.entry.lock;
        }

        @Override
        public void close() {
            /* Intentional no-op. The entry is retained after the final lease
             * closes: the sender identity is a process incarnation, so reusing
             * its sequence after provider recreation would otherwise create
             * a false gap in a still-live receiver. The bounded key set is one
             * entry per configured node/channel/stream. */
        }
    }
}
