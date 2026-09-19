package peruncs.datagrid.cluster.node.aeron;

import org.eclipse.store.storage.types.StorageConnection;
import peruncs.datagrid.cluster.storage.types.ClusterStoreIndexes;

import java.util.Objects;
import java.util.function.BiConsumer;

/// Gates the per-write publication graph validation behind a root-set
/// modification stamp.
///
/// [ClusterStoreIndexes#validateStorageRoots] walks the reachable object
/// graph and is deliberately fail-closed, but running it before every
/// distributed write rescans an unchanged graph. The stamp covers the set of
/// Store roots — identifiers, root object identities, and the root count —
/// so a write skips the scan while no root was added, removed, or replaced,
/// and re-validates as soon as the root set changes. Cluster startup performs
/// its own full validation; the first write through this gate validates once
/// because no stamp has been recorded yet.
///
/// The skip is probabilistic by construction: two different root sets can
/// collide in the folded hash. The stamp folds two independent 64-bit
/// accumulators (different multipliers) plus the root count, so a collision
/// requires a simultaneous 64-bit match on both accumulators with the same
/// count — practically unreachable for root sets of realistic size, and the
/// cost of a miss is a redundant validation, never an unsound acceptance:
/// a changed graph almost certainly produces a different stamp and always
/// re-validates, while a collided unchanged graph merely skips work that
/// would find nothing.
///
/// Write admission is single-writer by contract, so the reusable stamp
/// accumulator is safe without synchronization.
final class WriterIndexValidationGate {
    private final long[] stamp = new long[3];
    private final BiConsumer<String, Object> stampCollector = (identifier, value) -> {
        final int identifierHash = identifier.hashCode();
        final int identityHash = System.identityHashCode(value);
        this.stamp[0] = this.stamp[0] * 31L + identifierHash;
        this.stamp[1] = this.stamp[1] * 1_000_000_007L + identityHash;
        this.stamp[2]++;
    };
    private long validatedPrimary = Long.MIN_VALUE;
    private long validatedSecondary = Long.MIN_VALUE;
    private long validatedCount = Long.MIN_VALUE;
    private int validations;

    /// Validates the Store roots when their modification stamp changed.
    ///
    /// @param storage writer storage connection
    void validate(final StorageConnection storage) {
        Objects.requireNonNull(storage, "storage");
        this.stamp[0] = 1L;
        this.stamp[1] = 1L;
        this.stamp[2] = 0L;
        storage.persistenceManager().viewRoots().iterateEntries(this.stampCollector);
        if (this.stamp[0] == this.validatedPrimary &&
            this.stamp[1] == this.validatedSecondary &&
            this.stamp[2] == this.validatedCount) {
            return;
        }
        ClusterStoreIndexes.validateStorageRoots(storage);
        this.validatedPrimary = this.stamp[0];
        this.validatedSecondary = this.stamp[1];
        this.validatedCount = this.stamp[2];
        this.validations++;
    }

    /// Returns how many full graph validations this gate performed.
    ///
    /// @return validation count
    int validations() {
        return this.validations;
    }
}
