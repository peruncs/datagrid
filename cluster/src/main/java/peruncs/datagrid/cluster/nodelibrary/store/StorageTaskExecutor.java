package peruncs.datagrid.cluster.nodelibrary.store;


import org.eclipse.store.storage.types.StorageConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

        /// Stops outstanding maintenance work and releases executor state.
    @Override
    default void close() {
    }

        /// Implements the single-flight storage-check state machine.
    class Abstract implements StorageTaskExecutor {
        private static final Logger LOG = LoggerFactory.getLogger(Abstract.class);
        private static final long CLOSE_TIMEOUT_MILLIS = 5_000L;
        private final StorageConnection connection;
        private final ExecutorService executor;

        private Future<?> checksTask;
        private boolean closed;

                /// Creates the shared executor state.
        ///
        /// @param connection Store connection
        protected Abstract(final StorageConnection connection) {
            this.connection = connection;
            this.executor = Executors.newSingleThreadExecutor(task ->
            {
                final Thread thread = new Thread(task, "EclipseStore-StorageChecks");
                thread.setDaemon(true);
                return thread;
            });
        }

        @Override
        public synchronized void runChecks() {
            if (this.closed) throw new IllegalStateException("Storage task executor is closed");
            if (this.checksTask == null || this.checksTask.isDone()) {
                LOG.debug("Issuing new storage checks");
                this.checksTask = this.executor.submit(this::runChecksTask);
            }
        }

        @Override
        public synchronized boolean isRunningChecks() {
            return this.checksTask != null && !this.checksTask.isDone();
        }

        @Override
        public void close() {
            final Future<?> task;
            synchronized (this) {
                if (this.closed) return;
                this.closed = true;
                task = this.checksTask;
            }
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
        }

        private void runChecksTask() {
            try {
                this.connection.issueFullGarbageCollection();
                this.connection.issueFullCacheCheck();
                this.connection.issueFullFileCheck();
            } catch (final Throwable failure) {
                LOG.error("Storage checks failed", failure);
            }
        }
    }

        /// Provides the standard storage-check executor.
    final class Default extends Abstract implements StorageTaskExecutor {
        private Default(final StorageConnection connection) {
            super(connection);
        }
    }
}
