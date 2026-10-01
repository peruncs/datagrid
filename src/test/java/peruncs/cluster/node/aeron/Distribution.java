package peruncs.cluster.node.aeron;

import peruncs.cluster.storage.binary.TypeDictionaryOutbox;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/// Test stand-in for the lifecycle state a writer node shares with its replication target: the
/// type-dictionary outbox and the flag that suppresses replication while the Store bootstraps.
final class Distribution {
    final TypeDictionaryOutbox outbox = new TypeDictionaryOutbox();
    private final AtomicBoolean ignored = new AtomicBoolean();

    BooleanSupplier enabled() {
        return () -> !this.ignored.get();
    }

    void ignoreDistribution(final boolean ignore) {
        this.ignored.set(ignore);
    }
}
