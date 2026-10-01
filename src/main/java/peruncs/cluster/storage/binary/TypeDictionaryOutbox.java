package peruncs.cluster.storage.binary;

import java.util.concurrent.atomic.AtomicReference;

/// Holds the type dictionary that must travel with the writer's next transaction.
///
/// Receivers need the type definitions before they can materialize binary data that uses them.
/// The Store exports each dictionary change while it commits, and a restarted writer re-sends its
/// complete dictionary, so one slot is enough: cluster storage has one writer. The slot also
/// remembers whether its content is an authoritative restart snapshot, because a snapshot must
/// not be overwritten or cleared by a later incremental export.
public final class TypeDictionaryOutbox {
    /* The staged dictionary and its provenance live in one immutable value, so a reader can never
     * observe the text of one export with the kind of another. `null` means nothing is staged. */
    private final AtomicReference<Pending> pending = new AtomicReference<>();

    /// Creates an empty outbox.
    public TypeDictionaryOutbox() {
    }

    /// Stages the dictionary exported by a Store commit.
    ///
    /// A restart snapshot that is still waiting for its transaction is kept. `null` clears a
    /// staged incremental dictionary but never a snapshot.
    ///
    /// @param dictionary assembled type dictionary, or `null` to clear an incremental one
    public void stageIncremental(final String dictionary) {
        if (dictionary == null) {
            this.pending.updateAndGet(current -> current != null && current.snapshot() ? current : null);
            return;
        }
        this.pending.updateAndGet(current ->
                current != null && current.snapshot() ? current : new Pending(dictionary, false));
    }

    /// Stages the complete dictionary a restarted writer must re-send first.
    ///
    /// The snapshot replaces any incremental dictionary accumulated while distribution was
    /// disabled during startup.
    ///
    /// @param dictionary assembled complete type dictionary, or `null` to clear the slot
    public void stageSnapshot(final String dictionary) {
        this.pending.set(dictionary == null ? null : new Pending(dictionary, true));
    }

    /// Takes the staged dictionary and empties the slot.
    ///
    /// @return the staged dictionary, or `null` when none is pending
    public String consume() {
        final Pending staged = this.pending.getAndSet(null);
        return staged == null ? null : staged.dictionary();
    }

    private record Pending(String dictionary, boolean snapshot) {
    }
}
