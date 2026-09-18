package peruncs.datagrid.cluster.storage.aeron.reader;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.PersistentSubscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.concurrent.IdleStrategy;
import org.eclipse.serializer.typing.Disposable;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCursor;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.types.ReplicationRetry;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataClient;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataReceiver;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/// Reads committed Store transactions from an Archive and then from the live
/// publication.
///
/// Replay starts at the supplied durable position. The reader reports live
/// only after replay catches up, so a restart needs no separate snapshot path.
/// The subscription belongs to this reader; the caller remains responsible for
/// the shared Aeron and Archive clients.
public final class AeronArchiveReader implements Disposable {
    /// Immutable setup for one Archive replay and live reader.
    ///
    /// The builder groups subscription wiring, recovered cursor, and delivery
    /// callbacks so same-typed values cannot be swapped at the call site.
    ///
    /// @param aeron                    shared Aeron client
    /// @param archiveContext           Archive connection settings
    /// @param recordingId              Archive recording id
    /// @param startPosition            first replay position
    /// @param liveChannel              live publication channel
    /// @param liveStreamId             live publication stream
    /// @param replayChannel            replay channel
    /// @param replayStreamId           replay stream
    /// @param replicationConfiguration framing and timeout limits
    /// @param clusterId                expected cluster identity
    /// @param epoch                    expected writer epoch
    /// @param initialSequence          last sequence already applied
    /// @param initialPosition          last resolved Archive position
    /// @param receiver                 destination for complete Store binaries
    /// @param transactionResolved      callback after a transaction is delivered
    /// @param deliveryListener         callback around Store materialisation
    public record Configuration(
            Aeron aeron,
            AeronArchive.Context archiveContext,
            long recordingId,
            long startPosition,
            String liveChannel,
            int liveStreamId,
            String replayChannel,
            int replayStreamId,
            AeronReplicationConfiguration replicationConfiguration,
            UUID clusterId,
            long epoch,
            long initialSequence,
            long initialPosition,
            StorageBinaryDataReceiver receiver,
            Runnable transactionResolved,
            ReaderDeliveryListener deliveryListener
    ) {
        /// Validates required reader collaborators and recovered cursor bounds.
        public Configuration {
            Objects.requireNonNull(aeron, "aeron");
            Objects.requireNonNull(archiveContext, "archiveContext");
            Objects.requireNonNull(replicationConfiguration, "replicationConfiguration");
            Objects.requireNonNull(clusterId, "clusterId");
            Objects.requireNonNull(receiver, "receiver");
            Objects.requireNonNull(transactionResolved, "transactionResolved");
            if (initialSequence < -1 || initialSequence == Long.MAX_VALUE || initialPosition < -1) {
                throw new IllegalArgumentException("initial cursor must be sequence >= -1 and position >= -1");
            }
        }

        /// Starts a builder for an Archive reader.
        ///
        /// @return empty reader configuration builder
        public static Builder builder() {
            return new Builder();
        }

        /// Builds reader configuration without a long positional argument list.
        public static final class Builder {
            private Aeron aeron;
            private AeronArchive.Context archiveContext;
            private long recordingId;
            private long startPosition;
            private String liveChannel;
            private int liveStreamId;
            private String replayChannel;
            private int replayStreamId;
            private AeronReplicationConfiguration replicationConfiguration;
            private UUID clusterId;
            private long epoch;
            private long initialSequence = -1L;
            private long initialPosition = -1L;
            private StorageBinaryDataReceiver receiver;
            private Runnable transactionResolved = () -> {
            };
            private ReaderDeliveryListener deliveryListener;

            /// Creates an empty reader configuration builder.
            public Builder() {
            }

            /// Sets the shared Aeron client.
            ///
            /// @param value shared Aeron client
            /// @return this builder
            public Builder aeron(final Aeron value) { this.aeron = value; return this; }
            /// Sets the Archive connection settings.
            ///
            /// @param value Archive connection settings
            /// @return this builder
            public Builder archiveContext(final AeronArchive.Context value) { this.archiveContext = value; return this; }
            /// Sets the Archive recording id.
            ///
            /// @param value Archive recording id
            /// @return this builder
            public Builder recordingId(final long value) { this.recordingId = value; return this; }
            /// Sets the first replay position.
            ///
            /// @param value first replay position
            /// @return this builder
            public Builder startPosition(final long value) { this.startPosition = value; return this; }
            /// Sets the live channel.
            ///
            /// @param value live channel
            /// @return this builder
            public Builder liveChannel(final String value) { this.liveChannel = value; return this; }
            /// Sets the live stream id.
            ///
            /// @param value live stream id
            /// @return this builder
            public Builder liveStreamId(final int value) { this.liveStreamId = value; return this; }
            /// Sets the replay channel.
            ///
            /// @param value replay channel
            /// @return this builder
            public Builder replayChannel(final String value) { this.replayChannel = value; return this; }
            /// Sets the replay stream id.
            ///
            /// @param value replay stream id
            /// @return this builder
            public Builder replayStreamId(final int value) { this.replayStreamId = value; return this; }
            /// Sets the replication framing and timeout configuration.
            ///
            /// @param value replication framing and timeout configuration
            /// @return this builder
            public Builder replicationConfiguration(final AeronReplicationConfiguration value) { this.replicationConfiguration = value; return this; }
            /// Sets the cluster identity.
            ///
            /// @param value cluster identity
            /// @return this builder
            public Builder clusterId(final UUID value) { this.clusterId = value; return this; }
            /// Sets the writer epoch.
            ///
            /// @param value writer epoch
            /// @return this builder
            public Builder epoch(final long value) { this.epoch = value; return this; }
            /// Sets the already applied sequence.
            ///
            /// @param value already applied sequence
            /// @return this builder
            public Builder initialSequence(final long value) { this.initialSequence = value; return this; }
            /// Sets the already resolved Archive position.
            ///
            /// @param value already resolved Archive position
            /// @return this builder
            public Builder initialPosition(final long value) { this.initialPosition = value; return this; }
            /// Sets the Store binary receiver.
            ///
            /// @param value Store binary receiver
            /// @return this builder
            public Builder receiver(final StorageBinaryDataReceiver value) { this.receiver = value; return this; }
            /// Sets the post-transaction callback.
            ///
            /// @param value post-transaction callback
            /// @return this builder
            public Builder transactionResolved(final Runnable value) { this.transactionResolved = value; return this; }
            /// Sets the Store materialisation callback, or `null`.
            ///
            /// @param value Store materialisation callback, or `null`
            /// @return this builder
            public Builder deliveryListener(final ReaderDeliveryListener value) { this.deliveryListener = value; return this; }

            /// Builds the immutable reader configuration.
            ///
            /// @return immutable reader configuration
            public Configuration build() {
                return new Configuration(aeron, archiveContext, recordingId, startPosition, liveChannel,
                        liveStreamId, replayChannel, replayStreamId, replicationConfiguration, clusterId,
                        epoch, initialSequence, initialPosition, receiver, transactionResolved, deliveryListener);
            }
        }
    }

    private final PersistentSubscription subscription;
    private final TransactionAssembler assembler;
    private final long stopTimeoutNanos;
    private final IdleStrategy idleStrategy;
    private final AtomicBoolean active = new AtomicBoolean();
    private final AtomicLong stopDeadlineNanos = new AtomicLong();
    /* Sliding stop deadline: while a requested stop drains a replay backlog,
     * every newly resolved transaction — or newly materialized one, when the
     * Store receiver trails resolution under load — pushes the deadline out
     * by another stop timeout, so a slow-but-advancing reader is never timed
     * out. The overall cap still bounds a stop whose live tail perpetually
     * outruns replay. */
    private final AtomicLong stopRequestedNanos = new AtomicLong();
    private final AtomicLong stopProgressSequence = new AtomicLong(-1L);
    private final AtomicLong stopProgressApplied = new AtomicLong(-1L);
    private static final long STOP_OVERALL_TIMEOUT_MULTIPLIER = 10L;
    private volatile Thread thread;
    private volatile CountDownLatch stopped = new CountDownLatch(0);
    private volatile boolean disposed;
    /* Seeding closes on the first start: the stale-token floor must be fixed
     * before any frame is accepted, and a late seed would silently lower it. */
    private volatile boolean seedingClosed;
    /* A disposal request is permanent even when its bounded wait times out.  Keep
     * it separate from disposed so callers can retry disposal without starting a
     * second poller against the same subscription and assembler. */
    private volatile boolean disposeRequested;
    private volatile boolean live;
    private volatile boolean stopAtLatest;
    /* A terminal outcome (FAILED, TIMED_OUT, CLOSED) is sticky: no later state,
     * including disposal, may downgrade it to a success.  All transitions go
     * through the atomic reference so a polling-thread timeout cannot race a
     * concurrent disposal into the wrong final state. */
    private final AtomicReference<StorageBinaryDataClient.StopOutcome> stopOutcome =
            new AtomicReference<>(StorageBinaryDataClient.StopOutcome.NOT_STARTED);


    AeronArchiveReader(
            final PersistentSubscription subscription,
            final Configuration configuration) {
        this.subscription = Objects.requireNonNull(subscription, "subscription");
        try {
            final Configuration required = Objects.requireNonNull(configuration, "configuration");
            final AeronReplicationConfiguration requiredConfiguration = required.replicationConfiguration();
            this.stopTimeoutNanos = requiredConfiguration.readerStopTimeoutNanos();
            this.idleStrategy = requiredConfiguration.retryPolicy().idleStrategy();
            this.assembler = new TransactionAssembler(
                    requiredConfiguration, required.clusterId(), required.epoch(), required.initialSequence(),
                    required.initialPosition(), required.receiver(), required.transactionResolved(),
                    required.deliveryListener()
            );
        } catch (final RuntimeException | Error failure) {
            try {
                subscription.close();
            } catch (final RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

        /// Creates a reader with the supplied Archive, live, and cursor setup.
    /// The reader does not close the shared Aeron or Archive clients.
    ///
    /// @param configuration immutable reader configuration
    /// @return a reader that owns its subscription
    public static AeronArchiveReader New(final Configuration configuration) {
        final Configuration settings = Objects.requireNonNull(configuration, "configuration");
        final Aeron aeron = settings.aeron();
        final AeronArchive.Context subscriptionArchiveContext = settings.archiveContext().clone().aeron(aeron);
        final PersistentSubscription.Context subscriptionContext = new PersistentSubscription.Context()
                .aeron(aeron)
                .ownsAeronClient(false)
                .recordingId(settings.recordingId())
                .startPosition(settings.startPosition())
                .liveChannel(settings.liveChannel())
                .liveStreamId(settings.liveStreamId())
                .replayChannel(settings.replayChannel())
                .replayStreamId(settings.replayStreamId())
                .aeronArchiveContext(subscriptionArchiveContext);
        PersistentSubscription subscription = null;
        try {
            subscription = PersistentSubscription.create(subscriptionContext);
            return new AeronArchiveReader(subscription, settings);
        } catch (final RuntimeException | Error failure) {
            if (subscription != null) {
                try {
                    subscription.close();
                } catch (final RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            } else {
                try {
                    subscriptionContext.close();
                } catch (final RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
    }

        /// Sets the next stop outcome unless a terminal outcome already won.
    ///
    /// Every transition goes through here so a later event can never downgrade a
    /// reader that already failed, timed out, or closed to a cleaner-looking
    /// state. Non-terminal states (RUNNING, STOPPING, and the resolved boundary
    /// markers) are overwritten as usual.
    private void updateOutcome(final StorageBinaryDataClient.StopOutcome next) {
        this.stopOutcome.updateAndGet(current -> switch (current) {
            case FAILED, TIMED_OUT, CLOSED -> current;
            default -> next;
        });
    }

        /// Starts replay and live polling; repeated calls have no effect.
    public synchronized void start() {
        this.seedingClosed = true;
        if (this.disposed || this.disposeRequested) {
            throw new IllegalStateException("Aeron Archive reader is disposed or stopping for disposal");
        }
        if (this.assembler.failure() != null) {
            throw new IllegalStateException(
                    "cannot start a failed Aeron Archive reader; create a new reader from its durable cursor",
                    this.assembler.failure());
        }
        final Thread existing = this.thread;
        if (existing != null && existing.isAlive()) {
            throw new IllegalStateException("Aeron Archive reader is still stopping");
        }
        if (this.active.getAndSet(true)) {
            return;
        }
        this.stopAtLatest = false;
        this.stopDeadlineNanos.set(0L);
        this.stopRequestedNanos.set(0L);
        this.stopProgressSequence.set(-1L);
        this.stopProgressApplied.set(-1L);
        this.live = false;
        /* Preserve any terminal outcome: a restart after STOPPED/RESOLVED_BOUNDARY
         * becomes RUNNING, but a failed or timed-out reader never looks healthy. */
        this.updateOutcome(StorageBinaryDataClient.StopOutcome.RUNNING);
        this.stopped = new CountDownLatch(1);
        this.thread = Thread.ofVirtual().name("datagrid-aeron-archive-reader").unstarted(this::run);
        this.thread.start();
    }

    private void run() {
        final CountDownLatch lifecycleStopped = this.stopped;
        try {
            AeronReaderLifecycle.runPollingLoop(
                    this.active,
                    () -> this.subscription.hasFailed() || this.assembler.failure() != null,
                    () ->
                    {
                        final int work = this.subscription.controlledPoll((buffer, offset, length, header) ->
                        {
                            this.assembler.onFragment(buffer, offset, length, header);
                            return ControlledFragmentHandler.Action.CONTINUE;
                        }, 10);
                        this.live = this.subscription.isLive();
                        if (this.stopAtLatest) extendStopDeadline();
                        return work;
                    },
                    () -> this.stopAtLatest && this.live && !this.assembler.hasIncompleteTransaction(),
                    () -> this.stopAtLatest && ReplicationRetry.expired(this.stopDeadlineNanos.get()),
                    () ->
                    {
                        this.updateOutcome(StorageBinaryDataClient.StopOutcome.TIMED_OUT);
                        this.assembler.failure(new IllegalStateException(
                                "Timed out waiting for Aeron Archive replay to reach the live tail"));
                    },
                    this.idleStrategy
            );
            this.completeRun();
        } catch (final RuntimeException e) {
            this.assembler.failure(e);
        } catch (final Error e) {
            this.assembler.failure(new IllegalStateException("Aeron Archive reader polling failed", e));
            throw e;
        } finally {
            this.finishRun(lifecycleStopped);
        }
    }

    private synchronized void completeRun() {
        final StorageBinaryDataClient.StopOutcome current = this.stopOutcome.get();
        if (current == StorageBinaryDataClient.StopOutcome.TIMED_OUT ||
            current == StorageBinaryDataClient.StopOutcome.CLOSED) {
            return;
        }
        if (this.subscription.hasFailed()) {
            this.assembler.failure(new IllegalStateException(
                    "PersistentSubscription failed", this.subscription.failureReason()));
        }
        if (this.assembler.failure() != null) {
            this.updateOutcome(StorageBinaryDataClient.StopOutcome.FAILED);
            return;
        }
        this.updateOutcome(this.stopAtLatest && this.live
                ? StorageBinaryDataClient.StopOutcome.RESOLVED_BOUNDARY
                : StorageBinaryDataClient.StopOutcome.STOPPED);
    }

    private synchronized void finishRun(final CountDownLatch lifecycleStopped) {
        if (this.assembler.failure() != null) {
            this.updateOutcome(StorageBinaryDataClient.StopOutcome.FAILED);
        }
        this.active.set(false);
        this.live = false;
        lifecycleStopped.countDown();
    }

        /// Restarts a reader stopped at the live tail. A replay or validation failure
    /// is terminal; create a new reader from the durable cursor instead of
    /// reusing incomplete transaction state.
    public synchronized void resume() {
        final RuntimeException failure = this.failure();
        if (failure != null) {
            throw new IllegalStateException(
                    "cannot resume a failed Aeron Archive reader; create a new reader from its durable cursor", failure);
        }
        this.stopAtLatest = false;
        this.start();
    }

        /// Requests a stop after replay reaches the current live tail.
    public synchronized void stopAtLatestMessage() {
        if (this.disposed) {
            return;
        }
        if (this.disposeRequested) {
            throw new IllegalStateException("Aeron Archive reader is stopping for disposal");
        }
        if (this.stopOutcome.get() == StorageBinaryDataClient.StopOutcome.FAILED || this.failure() != null) {
            return;
        }
        /* The deadline precedes the flag: the polling thread tests the flag
         * first and the deadline second, so publishing the flag first would
         * let one iteration observe a live stop request against the reset
         * (already-expired) deadline and time out instantly. A reader that
         * observes the flag also observes this write: both sit in one
         * synchronized block ahead of the volatile flag publication. */
        this.stopDeadlineNanos.set(ReplicationRetry.deadlineNanos(this.stopTimeoutNanos));
        this.stopRequestedNanos.set(System.nanoTime());
        this.stopProgressSequence.set(this.assembler.lastResolvedSequence());
        this.stopProgressApplied.set(this.assembler.lastAppliedSequence());
        this.stopAtLatest = true;
        if (this.active.get()) this.updateOutcome(StorageBinaryDataClient.StopOutcome.STOPPING);
    }

        /// Pushes the stop deadline out while replay resolution or Store
    /// materialization advances.
    ///
    /// Each newly resolved transaction — or newly materialized one, when the
    /// receiver trails resolution — grants another full stop timeout, so a
    /// slow-but-advancing drain never times out. The extension is capped at an
    /// overall multiple of the stop timeout from the request: a stalled drain
    /// still fails at one timeout, and a live tail that perpetually outruns
    /// replay fails bounded instead of hanging a rolling restart.
    private void extendStopDeadline() {
        final long resolved = this.assembler.lastResolvedSequence();
        final long applied = this.assembler.lastAppliedSequence();
        final boolean resolving = resolved != this.stopProgressSequence.getAndSet(resolved);
        final boolean applying = applied != this.stopProgressApplied.getAndSet(applied);
        if (!resolving && !applying) return;
        final long extended = ReplicationRetry.deadlineNanos(this.stopTimeoutNanos);
        final long overall =
                this.stopRequestedNanos.get() + this.stopTimeoutNanos * STOP_OVERALL_TIMEOUT_MULTIPLIER;
        this.stopDeadlineNanos.set(Math.min(extended, overall));
    }

        /// Returns the last sequence delivered after commit and checksum validation.
    ///
    /// @return last resolved transaction sequence
    public long lastResolvedSequence() {
        return this.assembler.lastResolvedSequence();
    }

        /// Returns the last sequence materialized by the Store receiver.
    ///
    /// @return last applied transaction sequence
    public long lastAppliedSequence() {
        return this.assembler.lastAppliedSequence();
    }

        /// Returns whether the polling lifecycle is still active and has not failed.
    ///
    /// @return `true` when the reader is running
    public boolean isRunning() {
        final Thread pollingThread = this.thread;
        return !this.disposeRequested &&
               (this.active.get() || (pollingThread != null && pollingThread.isAlive())) &&
               this.failure() == null;
    }

        /// Returns whether replay has transitioned to the live subscription.
    ///
    /// @return `true` after replay reaches the live stream
    public boolean isLive() {
        return this.live;
    }

        /// Returns the Archive position of the last resolved commit.
    ///
    /// @return last resolved Archive position
    public long lastResolvedPosition() {
        return this.assembler.lastResolvedPosition();
    }

        /// Returns an atomic sequence/position snapshot for cursor persistence.
    ///
    /// @return current cursor snapshot
    public CursorSnapshot cursorSnapshot() {
        return this.assembler.cursorSnapshot();
    }

        /// Builds a cursor that can resume this reader from the same recording.
    ///
    /// @param nodeId          node that will own the resumed cursor
    /// @param storeGeneration Store image identity
    /// @param recordingId     Aeron Archive recording identity
    /// @return durable cursor for the current reader boundary
    public AeronReplicationCursor cursor(
            final UUID nodeId,
            final UUID storeGeneration,
            final long recordingId
    ) {
        final CursorSnapshot snapshot = this.assembler.cursorSnapshot();
        return new AeronReplicationCursor(
                this.assembler.clusterId(),
                nodeId,
                storeGeneration,
                this.assembler.epoch(),
                this.assembler.fencingToken(),
                recordingId,
                snapshot.position(),
                snapshot.sequence()
        );
    }

        /// Seeds the assembler's stale-token floor from the durable cursor.
    ///
    /// Call before [#start()] only. The floor must be fixed before the reader
    /// accepts any frame, so a restart never re-accepts history from a writer
    /// its cursor already moved past; seeding after start would silently move
    /// the floor under live validation and is rejected.
    ///
    /// @param fencingToken greatest token the persisted cursor accepted, or `0` for a new reader
    /// @throws IllegalStateException when the reader is already started
    public synchronized void seedFencingToken(final long fencingToken) {
        if (this.seedingClosed) {
            throw new IllegalStateException("Aeron Archive reader fencing seed is only accepted before start()");
        }
        this.assembler.startingFencingToken(fencingToken);
    }

        /// Returns the greatest writer fencing token accepted so far.
    ///
    /// @return greatest accepted fencing token
    public long fencingToken() {
        return this.assembler.fencingToken();
    }

        /// Returns the terminal polling failure, or `null` while healthy.
    ///
    /// @return terminal failure, or `null`
    public RuntimeException failure() {
        return this.assembler.failure();
    }

        /// Returns the stop boundary outcome and never infers success from a dead thread.
    ///
    /// @return current stop outcome
    public StorageBinaryDataClient.StopOutcome stopOutcome() {
        return this.stopOutcome.get();
    }

        /// Returns the terminal stop state and the last resolved sequence/position.
    ///
    /// @return current stop result
    public StorageBinaryDataClient.StopResult stopResult() {
        final CursorSnapshot cursor = this.assembler.cursorSnapshot();
        return new StorageBinaryDataClient.StopResult(this.stopOutcome.get(), cursor.sequence(), cursor.position());
    }

        /// Stops polling after a terminal Aeron client or MediaDriver failure.
    ///
    /// @param failure terminal failure
    public synchronized void fail(final RuntimeException failure) {
        this.assembler.failure(Objects.requireNonNull(failure, "failure"));
        this.active.set(false);
        this.live = false;
        this.updateOutcome(StorageBinaryDataClient.StopOutcome.FAILED);
    }

        /// Stops polling and releases this reader's subscriptions.
    ///
    /// If the polling thread does not terminate within the bounded shutdown
    /// window this method throws and leaves the subscription and assembler-owned
    /// buffers intact. A later call must retry after the thread has exited; this
    /// preserves native-buffer ownership and avoids closing a subscription under
    /// the polling thread.
    @Override
    public void dispose() {
        final Thread pollingThread;
        synchronized (this) {
            if (this.disposed) return;
            this.disposeRequested = true;
            if (this.active.get()) this.updateOutcome(StorageBinaryDataClient.StopOutcome.STOPPING);
            pollingThread = this.thread;
        }
        AeronReaderLifecycle.stopAndClose(this.active, pollingThread, this.stopped, this.subscription::close,
                this.stopTimeoutNanos);
        synchronized (this) {
            this.assembler.dispose();
            this.thread = null;
            this.disposed = true;
            /* A failed or timed-out reader must not be reported as a clean close. */
            this.updateOutcome(StorageBinaryDataClient.StopOutcome.CLOSED);
        }
    }

}
