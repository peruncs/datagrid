package peruncs.cluster.node.store;

import org.eclipse.store.storage.types.StorageConnection;
import peruncs.cluster.api.NodeConfig;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.System.Logger.Level.ERROR;

/// Runs storage maintenance work away from the caller thread.
///
/// Only one check task may run at a time. A later request while that task is
/// active is ignored, and the next request can start after the previous thread
/// has finished.
public interface StorageTaskExecutor extends AutoCloseable {
        /// Creates a storage task executor.
    ///
    /// @param connection Store connection
    /// @return task executor
    static StorageTaskExecutor create(final StorageConnection connection) {
        return create(connection, NodeConfig.Operations.DEFAULT.storageCheckCloseTimeout());
    }

    /// Creates a storage task executor with a bounded close wait.
    ///
    /// @param connection Store connection
    /// @param closeTimeout maximum wait for the executor to stop
    /// @return task executor
    static StorageTaskExecutor create(final StorageConnection connection, final Duration closeTimeout) {
        return new Default(Objects.requireNonNull(connection, "connection"),
                Objects.requireNonNull(closeTimeout, "closeTimeout"));
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
    ///
    /// The shutdown starts with the first call. When the bounded wait fails,
    /// the failure is rethrown and a later call retries the wait; the
    /// executor never accepts new work once the shutdown has started.
    @Override
    default void close() {
    }

        /// Implements the single-flight storage-check state machine.
    final class Default implements StorageTaskExecutor {
        private static final System.Logger LOGGER = System.getLogger(StorageTaskExecutor.class.getName());
        private static final Future<?> SHUTDOWN = CompletableFuture.completedFuture(null);
        private final StorageConnection connection;
        private final ExecutorService executor;
        private final Duration closeTimeout;
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicReference<Future<?>> checksTask = new AtomicReference<>();

                /// Creates the shared executor state.
        ///
        /// @param connection Store connection
        private Default(final StorageConnection connection, final Duration closeTimeout) {
            if (closeTimeout.toMillis() <= 0L) {
                throw new IllegalArgumentException("closeTimeout must be positive");
            }
            this.connection = connection;
            this.closeTimeout = closeTimeout;
            this.executor = Executors.newSingleThreadExecutor(Thread.ofVirtual()
                    .name("EclipseStore-StorageChecks", 0L)
                    .factory());
        }

        @Override
        public void runChecks() {
            while (true) {
                final Future<?> current = this.checksTask.get();
                if (current == SHUTDOWN) {
                    throw new IllegalStateException("Storage task executor is closed");
                }
                if (current != null && !current.isDone()) return;
                final FutureTask<Void> next = new FutureTask<>(this::runChecksTask, null);
                if (!this.checksTask.compareAndSet(current, next)) continue;
                LOGGER.log(System.Logger.Level.DEBUG, "Issuing new storage checks");
                try {
                    this.executor.execute(next);
                    return;
                } catch (final RejectedExecutionException rejected) {
                    next.cancel(false);
                    /* Close atomically replaced this task slot with SHUTDOWN
                     * before shutting down the executor. */
                    if (this.checksTask.get() == SHUTDOWN) {
                        throw new IllegalStateException("Storage task executor is closed", rejected);
                    }
                    throw rejected;
                }
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
            final Future<?> task = this.checksTask.getAndSet(SHUTDOWN);
            if (task != null && task != SHUTDOWN) task.cancel(true);
            this.executor.shutdownNow();
            RuntimeException failure = null;
            try {
                if (!this.executor.awaitTermination(this.closeTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    failure = new IllegalStateException(
                            "Storage checks did not stop within %s ms".formatted(this.closeTimeout.toMillis()));
                }
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                failure = new IllegalStateException("Interrupted while stopping storage checks", interrupted);
            }
            /* The executor is shut down and must never accept another task.
             * A failed bounded wait leaves the task retryable, so a later
             * close retries the wait instead of losing the resource. */
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
                LOGGER.log(ERROR, "Storage checks failed", failure);
            } catch (final Error failure) {
                this.failure.set(failure);
                LOGGER.log(ERROR, "Fatal storage-check failure", failure);
                throw failure;
            }
        }
    }
}
