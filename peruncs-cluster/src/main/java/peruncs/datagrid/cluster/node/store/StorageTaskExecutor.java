package peruncs.datagrid.cluster.node.store;

import org.eclipse.store.storage.types.StorageConnection;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.eclipse.serializer.util.X.notNull;

/// This executor runs storage maintenance work away from the caller thread.
///
/// Only one check task may run at a time. A later request while that task is
/// active is ignored, and the next request can start after the previous thread
/// has finished.
public interface StorageTaskExecutor extends AutoCloseable {
        /// Creates a storage task executor.
    ///
    /// @param connection Store connection
    /// @return task executor
    static StorageTaskExecutor New(final StorageConnection connection) {
        return new Default(notNull(connection));
    }

        /// Starts a storage check task.
    void runChecks();

        /// Reports whether a storage check is running.
    ///
    /// @return `true` when running
    boolean isRunningChecks();

        /// Reports the failure of the most recently completed storage check, if any.
    ///
    /// @return the most recent check failure, or `null` after a successful check
    Throwable failure();

        /// Stops outstanding maintenance work and releases executor state.
    @Override
    default void close() {
    }

        /// Implements the single-flight storage-check state machine.
    final class Default implements StorageTaskExecutor {
        private static final System.Logger LOGGER = System.getLogger(StorageTaskExecutor.class.getName());
        private static final long CLOSE_TIMEOUT_MILLIS = 5_000L;
        private final StorageConnection connection;
        private final ExecutorService executor;
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        private final AtomicReference<Future<?>> checksTask = new AtomicReference<>();
        private final AtomicBoolean closing = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();

                /// Creates the shared executor state.
        ///
        /// @param connection Store connection
        private Default(final StorageConnection connection) {
            this.connection = connection;
            this.executor = Executors.newSingleThreadExecutor(Thread.ofVirtual()
                    .name("EclipseStore-StorageChecks", 0L)
                    .factory());
        }

        @Override
        public void runChecks() {
            if (this.closed.get() || this.closing.get()) {
                throw new IllegalStateException("Storage task executor is closed");
            }
            while (true) {
                final Future<?> current = this.checksTask.get();
                if (current != null && !current.isDone()) return;
                LOGGER.log(System.Logger.Level.DEBUG, "Issuing new storage checks");
                final Future<?> next = this.executor.submit(this::runChecksTask);
                if (this.checksTask.compareAndSet(current, next)) return;
                next.cancel(false);
            }
        }

        @Override
        public boolean isRunningChecks() {
            final Future<?> task = this.checksTask.get();
            return task != null && !task.isDone();
        }

        @Override
        public Throwable failure() {
            return this.failure.get();
        }

        @Override
        public void close() {
            if (this.closed.get()) return;
            this.closing.set(true);
            final Future<?> task = this.checksTask.get();
            if (task != null) task.cancel(true);
            this.executor.shutdownNow();
            try {
                if (!this.executor.awaitTermination(CLOSE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                    throw new IllegalStateException(
                            "Storage checks did not stop within %s ms".formatted(CLOSE_TIMEOUT_MILLIS));
                }
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while stopping storage checks", interrupted);
            }
            this.closed.set(true);
            this.closing.set(false);
        }

        private void runChecksTask() {
            try {
                this.connection.issueFullGarbageCollection();
                this.connection.issueFullCacheCheck();
                this.connection.issueFullFileCheck();
                this.failure.set(null);
            } catch (final Exception failure) {
                this.failure.set(failure);
                LOGGER.log(System.Logger.Level.ERROR, "Storage checks failed", failure);
            } catch (final Error failure) {
                this.failure.set(failure);
                LOGGER.log(System.Logger.Level.ERROR, "Fatal storage-check failure", failure);
                throw failure;
            }
        }
    }
}
