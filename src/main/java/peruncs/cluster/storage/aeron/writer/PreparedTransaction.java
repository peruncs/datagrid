package peruncs.cluster.storage.aeron.writer;

import io.aeron.Aeron;

import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.zip.CRC32C;

/// Closeable handle that aborts an unfinished prepared transaction.
final class PreparedTransaction implements AutoCloseable {
    final AeronReplicationPublisher owner;
    final long sequence;
    final int dataLength;
    final int dataChunkCount;
    final int crc32c;
    final EnvelopeFramer framer;
    private volatile LongConsumer abortAction;
    private boolean abortActionInvoked;
    boolean abortAttempted;
    volatile boolean commitPending;
    private volatile LongConsumer commitAction;
    private long commitSequence;
    private boolean commitActionInvoked;
    private boolean locallyAccepted;
    volatile boolean terminal;
    long abortPosition = Aeron.NULL_VALUE;

    PreparedTransaction(final AeronReplicationPublisher owner, final long sequence, final int dataLength,
                                final int dataChunkCount, final int crc32c, final EnvelopeFramer framer) {
        this.owner = owner;
        this.sequence = sequence;
        this.dataLength = dataLength;
        this.dataChunkCount = dataChunkCount;
        this.crc32c = crc32c;
        this.framer = framer;
    }

    long sequence() {
        return this.sequence;
    }

    int dataLength() {
        return this.dataLength;
    }

    int dataChunkCount() {
        return this.dataChunkCount;
    }

    /// Returns the CRC32C of the Store binary carried by this transaction.
    int dataCrc32c() {
        return this.crc32c;
    }

    /// Records that the local Store accepted this transaction's bytes.
    ///
    /// From here on an abort would contradict the accepted Store state, so closing the
    /// token before COMMIT handling started fails the publisher closed instead of aborting.
    void markLocallyAccepted() {
        synchronized (this.owner) {
            this.locallyAccepted = true;
        }
    }

    void onCommit(final LongConsumer action, final long sequence) {
        this.commitAction = Objects.requireNonNull(action, "action");
        this.commitSequence = sequence;
    }

    void invokeCommitAction() {
        final LongConsumer action;
        final long sequence;
        synchronized (this.owner) {
            if (this.commitActionInvoked || this.commitAction == null) return;
            this.commitActionInvoked = true;
            action = this.commitAction;
            sequence = this.commitSequence;
        }
        action.accept(sequence);
    }

    /// Registers a callback for an abort whose publication has been attempted.
    /// The callback runs synchronously on the caller that completes the abort; a
    /// late registration is invoked immediately when the publisher already
    /// completed that path. A position of `-1` means the marker was offered
    /// but its durable Archive position is unknown.
    ///
    /// @param action receives the recorded abort position
    void onAbort(final LongConsumer action) {
        Objects.requireNonNull(action, "action");
        final boolean invoke;
        final long position;
        synchronized (this.owner) {
            if (this.abortAction != null && this.abortAction != action) {
                throw new IllegalStateException("abort callback is already registered");
            }
            this.abortAction = action;
            invoke = this.terminal && this.abortAttempted && !this.abortActionInvoked;
            if (invoke) this.abortActionInvoked = true;
            position = this.abortPosition;
        }
        if (invoke) action.accept(position);
    }

    void invokeAbortAction(final long position) {
        final LongConsumer action;
        synchronized (this.owner) {
            if (!this.abortAttempted || this.abortActionInvoked || this.abortAction == null) return;
            this.abortActionInvoked = true;
            action = this.abortAction;
        }
        action.accept(position);
    }

    /// Detaches this token without emitting an abort marker. This is used only
    /// after the local Store accepted data but a later step failed: an abort would
    /// contradict the accepted Store state, so the publisher is failed closed and
    /// the open prepared transaction and the Store mark are left for restart recovery.
    void abandonWithoutAbort() {
        synchronized (this.owner) {
            if (this.terminal) return;
            this.terminal = true;
            if (this.owner.pendingTransaction == this) this.owner.pendingTransaction = null;
            this.owner.failed = true;
        }
        this.framer.close();
    }

    /// Aborts an abandoned transaction so its sequence is terminated in the log.
    /// Closing after commit or abort has no effect.
    @Override
    public void close() {
        synchronized (this.owner) {
            if (this.terminal || this.commitPending) return;
            if (this.owner.lifecycle != AeronReplicationPublisher.Lifecycle.OPEN) {
                return;
            }
            if (this.locallyAccepted) {
                /* The Store already holds the bytes: leave the open transaction as restart
                 * recovery evidence and refuse further writes. */
                this.terminal = true;
                if (this.owner.pendingTransaction == this) this.owner.pendingTransaction = null;
                this.owner.failed = true;
                this.framer.close();
                return;
            }
            if (this.owner.failed) {
                this.terminal = true;
                if (this.owner.pendingTransaction == this) this.owner.pendingTransaction = null;
                this.framer.close();
                return;
            }
        }
        this.owner.abort(this);
    }
}
