package peruncs.datagrid.cluster.storage.aeron.reader;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import org.eclipse.serializer.typing.Disposable;
import peruncs.datagrid.cluster.storage.aeron.checkpoint.AeronReplicationCursor;
import peruncs.datagrid.cluster.storage.aeron.config.AeronReplicationConfiguration;
import peruncs.datagrid.cluster.storage.aeron.config.AeronRetryPolicy;
import peruncs.datagrid.cluster.storage.types.StorageBinaryDataReceiver;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/// Test-only live reader used by low-level UDP tests. Production clustering
/// uses [AeronArchiveReader]; both readers share
/// [TransactionAssembler] for commit-gated delivery and CRC validation.
public final class StorageBinaryDataClientAeron implements Disposable {
    private final Subscription subscription;
    private final TransactionAssembler assembler;
    private final AtomicBoolean active = new AtomicBoolean();
    private final FragmentAssembler fragmentAssembler;
    private volatile Thread thread;
    private volatile CountDownLatch stopped = new CountDownLatch(0);
    private volatile boolean disposed;

    public StorageBinaryDataClientAeron(
            final Subscription subscription,
            final AeronReplicationConfiguration configuration,
            final UUID clusterId,
            final long epoch,
            final long initialSequence,
            final StorageBinaryDataReceiver receiver
    ) {
        this.subscription = subscription;
        try {
            this.assembler = new TransactionAssembler(configuration, clusterId, epoch, initialSequence, receiver);
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
                    () -> this.subscription.poll(this.fragmentAssembler, 10),
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

    public long lastResolvedSequence() {
        return this.assembler.lastResolvedSequence();
    }

    public AeronReplicationCursor cursor(final UUID nodeId, final UUID storeGeneration, final long recordingId) {
        final CursorSnapshot snapshot = this.assembler.cursorSnapshot();
        return new AeronReplicationCursor(
                this.assembler.clusterId(), nodeId, storeGeneration, this.assembler.epoch(), 1L, recordingId,
                snapshot.position(), snapshot.sequence());
    }

    public RuntimeException failure() {
        return this.assembler.failure();
    }

    @Override
    public synchronized void dispose() {
        if (this.disposed) return;
        final Thread pollingThread = this.thread;
        AeronReaderLifecycle.stopAndClose(this.active, pollingThread, this.stopped, this.subscription::close);
        this.thread = null;
        this.assembler.dispose();
        this.disposed = true;
    }
}
