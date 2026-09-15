package peruncs.datagrid.storage.distributed.aeron.reader;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.PersistentSubscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import peruncs.datagrid.storage.distributed.aeron.checkpoint.AeronReplicationCursor;
import peruncs.datagrid.storage.distributed.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.storage.distributed.types.ReplicationRetry;
import peruncs.datagrid.storage.distributed.types.StorageBinaryDataClient;
import peruncs.datagrid.storage.distributed.types.StorageBinaryDataReceiver;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reads committed Store transactions from an Archive and then from the live
 * publication.
 *
 * <p>Replay starts at the supplied durable position. The reader reports live
 * only after replay catches up, so a restart needs no separate snapshot path.
 * The subscription belongs to this reader; the caller remains responsible for
 * the shared Aeron and Archive clients.</p>
 */
public final class StorageBinaryDataClientAeronArchive implements StorageBinaryDataClient {
    private final PersistentSubscription subscription;
    private final TransactionAssembler assembler;
    private final long stopTimeoutNanos;
    private final AtomicBoolean active = new AtomicBoolean();
    private final AtomicLong stopDeadlineNanos = new AtomicLong();
    private volatile Thread thread;
    private volatile CountDownLatch stopped = new CountDownLatch(0);
    private volatile boolean disposed;
    /* A disposal request is permanent even when its bounded wait times out.  Keep
     * it separate from disposed so callers can retry disposal without starting a
     * second poller against the same subscription and assembler. */
    private volatile boolean disposeRequested;
    private volatile boolean live;
    private volatile boolean stopAtLatest;
    private volatile StorageBinaryDataClient.StopOutcome stopOutcome = StorageBinaryDataClient.StopOutcome.NOT_STARTED;


    StorageBinaryDataClientAeronArchive(
            final PersistentSubscription subscription,
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence,
            final long initialPosition,
            final StorageBinaryDataReceiver receiver,
            final Runnable transactionResolved,
            final ReaderDeliveryListener deliveryListener
    ) {
        this.subscription = java.util.Objects.requireNonNull(subscription, "subscription");
        try {
            this.stopTimeoutNanos = java.util.Objects.requireNonNull(configuration, "configuration").readerStopTimeoutNanos();
            this.assembler = new TransactionAssembler(
                    configuration, clusterId, epoch, initialSequence, initialPosition, receiver, transactionResolved,
                    deliveryListener
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

    /**
     * Creates a reader with no delivery durability callback.
     *
     * @param aeron               shared Aeron client, not owned by the reader
     * @param archiveContext      Archive connection settings
     * @param recordingId         recording to replay
     * @param startPosition       first Archive position to replay
     * @param liveChannel         live publication channel
     * @param liveStreamId        live publication stream
     * @param replayChannel       replay channel
     * @param replayStreamId      replay stream
     * @param configuration       shared framing and timeout limits
     * @param clusterId           expected cluster identity
     * @param epoch               expected writer epoch
     * @param initialSequence     last sequence already applied by the Store
     * @param receiver            destination for complete Store binaries
     * @param transactionResolved callback after a transaction is delivered
     * @return a reader that owns its subscriptions
     */
    public static StorageBinaryDataClientAeronArchive New(
            final Aeron aeron,
            final AeronArchive.Context archiveContext,
            final long recordingId,
            final long startPosition,
            final String liveChannel,
            final int liveStreamId,
            final String replayChannel,
            final int replayStreamId,
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence,
            final StorageBinaryDataReceiver receiver,
            final Runnable transactionResolved
    ) {
        return New(aeron, archiveContext, recordingId, startPosition, liveChannel, liveStreamId,
                replayChannel, replayStreamId, configuration, clusterId, epoch, initialSequence, receiver,
                transactionResolved, null, startPosition);
    }

    /**
     * Creates a reader with callbacks around Store materialisation.
     *
     * <p>The reader does not close {@code aeron} or the Archive client. Call
     * {@link #dispose()} when the reader is no longer needed.</p>
     *
     * @param aeron               shared Aeron client, not owned by the reader
     * @param archiveContext      Archive connection settings
     * @param recordingId         recording to replay
     * @param startPosition       first Archive position to replay
     * @param liveChannel         live publication channel
     * @param liveStreamId        live publication stream
     * @param replayChannel       replay channel
     * @param replayStreamId      replay stream
     * @param configuration       shared framing and timeout limits
     * @param clusterId           expected cluster identity
     * @param epoch               expected writer epoch
     * @param initialSequence     last sequence already applied by the Store
     * @param receiver            destination for complete Store binaries
     * @param transactionResolved callback after a transaction is delivered
     * @param deliveryListener    callback around Store materialisation
     * @return a reader that owns its subscriptions
     */
    public static StorageBinaryDataClientAeronArchive New(
            final Aeron aeron,
            final AeronArchive.Context archiveContext,
            final long recordingId,
            final long startPosition,
            final String liveChannel,
            final int liveStreamId,
            final String replayChannel,
            final int replayStreamId,
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence,
            final StorageBinaryDataReceiver receiver,
            final Runnable transactionResolved,
            final ReaderDeliveryListener deliveryListener
    ) {
        return New(aeron, archiveContext, recordingId, startPosition, liveChannel, liveStreamId,
                replayChannel, replayStreamId, configuration, clusterId, epoch, initialSequence, receiver,
                transactionResolved, deliveryListener, startPosition);
    }

    /**
     * Creates a reader with a complete recovered sequence and recording position.
     * The initial position is retained in the atomic cursor snapshot until the
     * first newly resolved transaction, so a reader that has not caught up cannot
     * expose an unrelated position.
     *
     * @param aeron               shared Aeron client, not owned by the reader
     * @param archiveContext      Archive connection settings
     * @param recordingId         recording to replay
     * @param startPosition       first Archive position to replay
     * @param liveChannel         live publication channel
     * @param liveStreamId        live publication stream
     * @param replayChannel       replay channel
     * @param replayStreamId      replay stream
     * @param configuration       shared framing and timeout limits
     * @param clusterId           expected cluster identity
     * @param epoch               expected writer epoch
     * @param initialSequence     last sequence already applied by the Store
     * @param receiver            destination for complete Store binaries
     * @param transactionResolved callback after a transaction is delivered
     * @param deliveryListener    callback around Store materialisation
     * @param initialPosition     last resolved Archive position, or {@code -1} for a new reader
     * @return a reader that owns its subscriptions
     */
    public static StorageBinaryDataClientAeronArchive New(
            final Aeron aeron,
            final AeronArchive.Context archiveContext,
            final long recordingId,
            final long startPosition,
            final String liveChannel,
            final int liveStreamId,
            final String replayChannel,
            final int replayStreamId,
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence,
            final StorageBinaryDataReceiver receiver,
            final Runnable transactionResolved,
            final ReaderDeliveryListener deliveryListener,
            final long initialPosition
    ) {
        final AeronArchive.Context subscriptionArchiveContext = archiveContext.clone().aeron(aeron);
        final PersistentSubscription.Context subscriptionContext = new PersistentSubscription.Context()
                .aeron(aeron)
                .ownsAeronClient(false)
                .recordingId(recordingId)
                .startPosition(startPosition)
                .liveChannel(liveChannel)
                .liveStreamId(liveStreamId)
                .replayChannel(replayChannel)
                .replayStreamId(replayStreamId)
                .aeronArchiveContext(subscriptionArchiveContext);
        PersistentSubscription subscription = null;
        try {
            subscription = PersistentSubscription.create(subscriptionContext);
            return new StorageBinaryDataClientAeronArchive(
                    subscription, configuration, clusterId, epoch, initialSequence, initialPosition, receiver,
                    transactionResolved, deliveryListener
            );
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

    /** Starts replay and live polling; repeated calls have no effect. */
    @Override
    public synchronized void start() {
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
        this.live = false;
        this.stopOutcome = StorageBinaryDataClient.StopOutcome.RUNNING;
        this.stopped = new CountDownLatch(1);
        this.thread = new Thread(this::run, "datagrid-aeron-archive-reader");
        this.thread.setDaemon(true);
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
                        return work;
                    },
                    () -> this.stopAtLatest && this.live && !this.assembler.hasIncompleteTransaction(),
                    () -> this.stopAtLatest && ReplicationRetry.expired(this.stopDeadlineNanos.get()),
                    () ->
                    {
                        this.stopOutcome = StorageBinaryDataClient.StopOutcome.TIMED_OUT;
                        this.assembler.failure(new IllegalStateException(
                                "Timed out waiting for Aeron Archive replay to reach the live tail"));
                    }
            );
            if (this.stopOutcome != StorageBinaryDataClient.StopOutcome.TIMED_OUT) {
                this.stopOutcome = this.stopAtLatest && this.live
                        ? StorageBinaryDataClient.StopOutcome.RESOLVED_BOUNDARY
                        : StorageBinaryDataClient.StopOutcome.STOPPED;
            }
            if (this.subscription.hasFailed()) {
                this.assembler.failure(new IllegalStateException(
                        "PersistentSubscription failed", this.subscription.failureReason()));
            }
        } catch (final RuntimeException e) {
            this.assembler.failure(e);
        } catch (final Error e) {
            this.assembler.failure(new IllegalStateException("Aeron Archive reader polling failed", e));
            throw e;
        } finally {
            if (this.assembler.failure() != null &&
                this.stopOutcome != StorageBinaryDataClient.StopOutcome.TIMED_OUT) {
                this.stopOutcome = StorageBinaryDataClient.StopOutcome.FAILED;
            }
            this.active.set(false);
            this.live = false;
            lifecycleStopped.countDown();
        }
    }

    /**
     * Restarts a reader stopped at the live tail. A replay or validation failure
     * is terminal; create a new reader from the durable cursor instead of
     * reusing incomplete transaction state.
     */
    public synchronized void resume() {
        final RuntimeException failure = this.failure();
        if (failure != null) {
            throw new IllegalStateException(
                    "cannot resume a failed Aeron Archive reader; create a new reader from its durable cursor", failure);
        }
        this.stopAtLatest = false;
        this.start();
    }

    /** Requests a stop after replay reaches the current live tail. */
    public synchronized void stopAtLatestMessage() {
        if (this.disposed) {
            return;
        }
        if (this.disposeRequested) {
            throw new IllegalStateException("Aeron Archive reader is stopping for disposal");
        }
        this.stopAtLatest = true;
        this.stopDeadlineNanos.set(ReplicationRetry.deadlineNanos(this.stopTimeoutNanos));
        if (this.active.get()) this.stopOutcome = StorageBinaryDataClient.StopOutcome.STOPPING;
    }

    /**
     * Returns the last sequence delivered after commit and checksum validation.
     *
     * @return last resolved transaction sequence
     */
    public long lastResolvedSequence() {
        return this.assembler.lastResolvedSequence();
    }

    /**
     * Returns the last sequence materialized by the Store receiver.
     *
     * @return last applied transaction sequence
     */
    public long lastAppliedSequence() {
        return this.assembler.lastAppliedSequence();
    }

    /**
     * Returns whether the polling thread is active and has not failed.
     *
     * @return {@code true} when the reader is running
     */
    public boolean isRunning() {
        return this.active.get() && !this.disposeRequested && this.failure() == null;
    }

    /**
     * Returns whether replay has transitioned to the live subscription.
     *
     * @return {@code true} after replay reaches the live stream
     */
    public boolean isLive() {
        return this.live;
    }

    /**
     * Returns the Archive position of the last resolved commit.
     *
     * @return last resolved Archive position
     */
    public long lastResolvedPosition() {
        return this.assembler.lastResolvedPosition();
    }

    /**
     * Returns an atomic sequence/position snapshot for cursor persistence.
     *
     * @return current cursor snapshot
     */
    public CursorSnapshot cursorSnapshot() {
        return this.assembler.cursorSnapshot();
    }

    /**
     * Builds a cursor that can resume this reader from the same recording.
     *
     * @param nodeId          node that will own the resumed cursor
     * @param storeGeneration Store image identity
     * @param recordingId     Aeron Archive recording identity
     * @return durable cursor for the current reader boundary
     */
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
                recordingId,
                snapshot.position(),
                snapshot.sequence()
        );
    }

    /**
     * Returns the terminal polling failure, or {@code null} while healthy.
     *
     * @return terminal failure, or {@code null}
     */
    public RuntimeException failure() {
        return this.assembler.failure();
    }

    /** Returns the stop boundary outcome and never infers success from a dead thread. */
    public StorageBinaryDataClient.StopOutcome stopOutcome() {
        return this.stopOutcome;
    }

    /** Returns the terminal stop state and the last resolved sequence/position. */
    @Override
    public StorageBinaryDataClient.StopResult stopResult() {
        final CursorSnapshot cursor = this.assembler.cursorSnapshot();
        return new StorageBinaryDataClient.StopResult(this.stopOutcome, cursor.sequence(), cursor.position());
    }

    /**
     * Stops polling after a terminal Aeron client or MediaDriver failure.
     *
     * @param failure terminal failure
     */
    public synchronized void fail(final RuntimeException failure) {
        this.assembler.failure(java.util.Objects.requireNonNull(failure, "failure"));
        this.active.set(false);
        this.live = false;
        if (this.stopOutcome != StorageBinaryDataClient.StopOutcome.CLOSED) {
            this.stopOutcome = StorageBinaryDataClient.StopOutcome.FAILED;
        }
    }

    /**
     * Stops polling and releases this reader's subscriptions.
     *
     * <p>If the polling thread does not terminate within the bounded shutdown
     * window this method throws and leaves the subscription and assembler-owned
     * buffers intact. A later call must retry after the thread has exited; this
     * preserves native-buffer ownership and avoids closing a subscription under
     * the polling thread.</p>
     */
    @Override
    public synchronized void dispose() {
        if (this.disposed) return;
        this.disposeRequested = true;
        if (this.active.get()) this.stopOutcome = StorageBinaryDataClient.StopOutcome.STOPPING;
        final Thread pollingThread = this.thread;
        AeronReaderLifecycle.stopAndClose(this.active, pollingThread, this.stopped, this.subscription::close);
        this.assembler.dispose();
        this.thread = null;
        this.disposed = true;
        this.stopOutcome = StorageBinaryDataClient.StopOutcome.CLOSED;
    }

}
