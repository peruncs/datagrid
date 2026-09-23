package peruncs.datagrid.cluster.storage.aeron.reader;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import org.eclipse.serializer.typing.Disposable;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCursor;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.aeron.config.AeronRetryPolicy;
import peruncs.datagrid.cluster.storage.binary.StorageBinaryDataReceiver;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/// Test-only live reader used by low-level UDP tests. Production clustering
/// uses [AeronArchiveReader]; both readers share
/// [TransactionAssembler] for commit-gated delivery and CRC validation.
public final class ReplicationApplierAeron implements Disposable {
    private final Subscription subscription;
    private final TransactionAssembler assembler;
    private final AtomicBoolean active = new AtomicBoolean();
    private final AtomicBoolean subscriptionClosed = new AtomicBoolean();
    private final int fragmentsPerPoll;
    private final long stopTimeoutNanos;
    private final FragmentAssembler fragmentAssembler;
    private volatile Thread thread;
    private volatile CountDownLatch stopped = new CountDownLatch(0);
    private volatile boolean disposed;

    /// Attaches a live reader to one subscription without starting polling.
    ///
    /// @param subscription live Aeron subscription to poll
    /// @param configuration framing and timeout limits
    /// @param clusterId expected cluster identity
    /// @param epoch expected writer epoch
    /// @param initialSequence last sequence already applied
    /// @param receiver destination for complete Store binaries
    public ReplicationApplierAeron(
            final Subscription subscription,
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence,
            final StorageBinaryDataReceiver receiver
    ) {
        this.subscription = subscription;
        try {
            this.fragmentsPerPoll = configuration.readerFragmentsPerPoll();
            this.stopTimeoutNanos = configuration.readerStopTimeoutNanos();
            this.assembler = TransactionAssemblerTestSupport.New(
                    configuration, clusterId, epoch, initialSequence, receiver);
            this.fragmentAssembler = new FragmentAssembler(this.assembler::onFragment);
        } catch (final RuntimeException | Error failure) {
            try {
                subscription.close();
            } catch (final RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    /// Starts the polling thread; repeated calls are ignored once running.
    public synchronized void start() {
        if (this.disposed) throw new IllegalStateException("Aeron reader is disposed");
        if (this.active.getAndSet(true)) return;
        this.stopped = new CountDownLatch(1);
        this.thread = Thread.ofVirtual().name("datagrid-aeron-reader").unstarted(this::run);
        this.thread.start();
    }

    private void run() {
        try {
            AeronReaderLifecycle.runPollingLoop(
                    this.active,
                    () -> false,
                    () ->
                    {
                        final int work = this.subscription.poll(this.fragmentAssembler, this.fragmentsPerPoll);
                        /* Same delivery-barrier flush point as the production
                         * Archive reader: an idle poll publishes staged
                         * transactions so live-tail latency stays bounded. */
                        if (work == 0) this.assembler.flushDeliveries();
                        return work;
                    },
                    () -> false,
                    () -> false,
                    () -> {
                    },
                    AeronRetryPolicy.Default().idleStrategy()
            );
        } catch (final RuntimeException e) {
            this.assembler.failure(e);
        } catch (final Error e) {
            this.assembler.failure(new IllegalStateException("Aeron reader polling failed", e));
        } finally {
            this.active.set(false);
            this.stopped.countDown();
        }
    }

    /// Returns the last fully resolved transaction sequence.
    ///
    /// @return last resolved sequence, or `-1` before the first commit
    public long lastResolvedSequence() {
        return this.assembler.lastResolvedSequence();
    }

    /// Snapshots the current replay boundary. The fencing token is fixed at
    /// one because low-level UDP tests run a single unfenced writer.
    ///
    /// @param nodeId reader node identity for the cursor
    /// @param storeGeneration Store generation for the cursor
    /// @param recordingId Archive recording the position refers to
    /// @return cursor at the last resolved sequence and position
    public AeronReplicationCursor cursor(final UUID nodeId, final UUID storeGeneration, final long recordingId) {
        final CursorSnapshot snapshot = this.assembler.cursorSnapshot();
        return new AeronReplicationCursor(
                this.assembler.clusterId(), nodeId, storeGeneration, this.assembler.epoch(), 1L, recordingId,
                snapshot.position(), snapshot.sequence());
    }

    /// Returns the latched terminal failure, if the reader failed.
    ///
    /// @return terminal failure, or `null` while healthy
    public RuntimeException failure() {
        return this.assembler.failure();
    }

    @Override
    public synchronized void dispose() {
        if (this.disposed) return;
        final Thread pollingThread = this.thread;
        AeronReaderLifecycle.stopAndClose(this.active, pollingThread, this.stopped, this.subscriptionClosed,
                this.subscription::close, this.stopTimeoutNanos);
        this.thread = null;
        this.assembler.dispose();
        this.disposed = true;
    }
}
