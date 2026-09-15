package peruncs.datagrid.cluster.node.store;

import org.eclipse.store.storage.types.StorageConnection;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

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
        return new Default(Objects.requireNonNull(connection, "connection"));
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
        private volatile boolean closing;
        private volatile boolean closed;

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
            if (this.closed || this.closing) {
                throw new IllegalStateException("Storage task executor is closed");
            }
            while (true) {
                final Future<?> current = this.checksTask.get();
                if (current != null && !current.isDone()) return;
                LOGGER.log(System.Logger.Level.DEBUG, "Issuing new storage checks");
                final Future<?> next;
                try {
                    next = this.executor.submit(this::runChecksTask);
                } catch (final RejectedExecutionException rejected) {
                    /* Close won the race after the state check above and shut the
                     * executor down. Report closed instead of leaking the rejection. */
                    if (this.closed || this.closing) {
                        throw new IllegalStateException("Storage task executor is closed", rejected);
                    }
                    throw rejected;
                }
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
        public synchronized void close() {
            if (this.closed) return;
            this.closing = true;
            RuntimeException failure = null;
            try {
                final Future<?> task = this.checksTask.get();
                if (task != null) task.cancel(true);
                this.executor.shutdownNow();
                try {
                    if (!this.executor.awaitTermination(CLOSE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                        failure = new IllegalStateException(
                                "Storage checks did not stop within %s ms".formatted(CLOSE_TIMEOUT_MILLIS));
                    }
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    failure = new IllegalStateException("Interrupted while stopping storage checks", interrupted);
                }
            } finally {
                /* The executor is shut down and must never accept another task, so
                 * mark the terminal state even when the bounded wait timed out;
                 * otherwise a later runChecks would submit to a dead executor and
                 * leak the raw RejectedExecutionException. */
                this.closed = true;
                this.closing = false;
            }
            if (failure != null) {
                throw failure;
            }
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
