package peruncs.cluster.node.aeron;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.exceptions.AeronException;
import peruncs.cluster.storage.aeron.reader.AeronArchiveReader;
import peruncs.cluster.storage.aeron.writer.CrashHook;

import java.util.concurrent.atomic.AtomicReference;

import static java.lang.System.Logger.Level.WARNING;

/// Owns the embedded MediaDriver and Archive client runtime of one transport.
///
/// The runtime starts lazily on the first operation that needs it and is
/// closed by the transport's ordered close stages, never by an individual
/// reader, writer, or retention controller. The first asynchronous
/// MediaDriver failure is recorded here permanently: health must not hide it,
/// and a failed runtime is terminal for the owning transport.
final class AeronRuntimeOwner {
    private static final System.Logger LOGGER = System.getLogger(AeronRuntimeOwner.class.getName());

    private final AeronTransport facade;
    /// First asynchronous MediaDriver failure; health must not hide it.
    private final AtomicReference<RuntimeException> driverFailure = new AtomicReference<>();
    private volatile AeronRuntime runtime;

    AeronRuntimeOwner(final AeronTransport facade) {
        this.facade = facade;
    }

    private AeronSettings settings() {
        return this.facade.settings();
    }

    private AeronTransportShared shared() {
        return this.facade.shared();
    }

    /// Starts the runtime on first use, throwing once it has failed.
    ///
    /// Runs under the shared transport monitor: writer recovery, retention
    /// startup, and reader startup each serialize their runtime checks here.
    /// A failed watermark-channel setup tears the runtime down again before
    /// the failure escapes, so no half-started Archive survives.
    void ensure() {
        final AeronTransportShared shared = shared();
        synchronized (shared) {
            shared.ensureOpen();
            if (this.driverFailure.get() != null) {
                throw new IllegalStateException("Aeron runtime has failed; create a new transport", this.driverFailure.get());
            }
            if (this.runtime != null) return;
            if (this.settings().topology().role().isWriter()) shared.capacity().invalidate();
            this.runtime = AeronRuntime.start(this.settings(), this::recordDriverFailure,
                    this::recordSubscriberFailure,
                    () -> CrashHook.invoke("BEFORE_PUBLICATION_CONNECTED", -1L));
            try {
                this.facade.retentionOwner().ensureWatermarkChannel();
            } catch (final RuntimeException | Error failure) {
                final Throwable cleanupFailure = this.facade.closeRuntimeQuietly();
                if (cleanupFailure != null && cleanupFailure != failure) failure.addSuppressed(cleanupFailure);
                throw failure;
            }
        }
    }

    /// Records an asynchronous transport failure from an Aeron callback thread.
    ///
    /// This runs on the MediaDriver conductor thread, so it must never
    /// acquire the shared transport monitor: reader disposal, writer startup,
    /// and close all take that monitor, and blocking the conductor stalls
    /// every Aeron client sharing the driver. The method only writes
    /// volatile state and fails the installed reader through the lock-free
    /// [AeronArchiveReader#fail] path, which is safe to invoke from the
    /// error handler without touching transport lifecycle locks.
    private void recordDriverFailure(final Throwable failure) {
        final AeronTransportShared shared = shared();
        if (failure instanceof AeronException aeronFailure &&
            aeronFailure.category() == AeronException.Category.WARN) {
            /* Aeron uses the configured handler for non-terminal operational events
             * too. A control-response disconnect is different: it means the Archive
             * session can no longer validate or publish durable replication state. It
             * is terminal while the transport is live, but remains a normal shutdown
             * diagnostic after close has begun. */
            if (!AeronArchiveFailures.terminalControlResponseWarning(failure) || shared.closing() || shared.closed()) {
                LOGGER.log(WARNING, "Aeron transport warning", aeronFailure);
                return;
            }
            /* The embedded Archive can finish an older control session
             * after this transport has connected its own client. Its
             * disconnected response publication is not this writer's
             * durability channel. Never call Archive RPCs on the
             * conductor thread: the session id is an immutable snapshot. */
            final AeronRuntime current = this.runtime;
            final AeronArchive archive = current == null ? null : current.archive();
            if (archive == null || !AeronArchiveFailures.belongsToControlSession(
                    failure, archive.controlSessionId())) {
                LOGGER.log(WARNING, "Aeron warning for an inactive Archive control session", aeronFailure);
                return;
            }
        }
        final RuntimeException normalized = failure instanceof RuntimeException runtimeException
                ? runtimeException
                : new IllegalStateException("Aeron MediaDriver failed", failure);
        if (!this.driverFailure.compareAndSet(null, normalized) && this.driverFailure.get() != normalized) {
            /* Health keeps the first terminal cause, but later callbacks still carry
             * useful diagnostics (and must not disappear silently). */
            LOGGER.log(WARNING, "Additional Aeron transport failure", normalized);
        }
        this.facade.readerTransport().failCurrent(normalized);
    }

    private void recordSubscriberFailure(final Throwable failure) {
        /* Fragment-handler failures are application-level: the polling reader
         * already records them on its own assembler before rethrowing, and Aeron
         * forwards the same instance here. A failed Store import, CRC mismatch,
         * or dispose-time interrupt must never poison the shared
         * MediaDriver/Archive runtime used by subsequent readers, and must not
         * fail a replacement reader installed after the failing poll. Logging
         * here keeps the diagnostic; reader state stays with its own instance. */
        final AeronTransportShared shared = shared();
        if (shared.closing() || shared.closed()) {
            LOGGER.log(System.Logger.Level.DEBUG, "Aeron subscriber failure during shutdown", failure);
        } else {
            LOGGER.log(WARNING, "Aeron subscriber failure", failure);
        }
    }

    /// Stops the driver, idempotently, while the transport stays open.
    void stopDriver() {
        synchronized (shared()) {
            shared().ensureOpen();
            /* Stopping is idempotent: failure-injection and shutdown callers can race
             * without turning an already stopped runtime into a spurious failure. */
            if (this.runtime == null) return;
            this.runtime.stopDriver();
        }
    }

    /// Closes the runtime as an ordered transport close stage.
    void close() {
        this.runtime.close();
        this.runtime = null;
    }

    /// Closes the runtime during failed startup cleanup, reporting instead of throwing.
    ///
    /// @return the close failure, or `null` on success
    RuntimeException closeQuietly() {
        try {
            this.runtime.close();
            this.runtime = null;
            return null;
        } catch (final RuntimeException closeFailure) {
            return closeFailure;
        }
    }

    /// Reports whether the runtime has been started and not yet closed.
    ///
    /// @return `true` while the runtime exists
    boolean isStarted() {
        return this.runtime != null;
    }

    /// Returns the first terminal driver failure, or `null`.
    ///
    /// @return terminal driver failure, or `null` while healthy
    RuntimeException driverFailure() {
        return this.driverFailure.get();
    }

    Aeron aeron() {
        if (this.runtime == null) throw new IllegalStateException("Aeron runtime is not initialized");
        return this.runtime.aeron();
    }

    AeronArchive archive() {
        if (this.runtime == null) throw new IllegalStateException("Aeron runtime is not initialized");
        return this.runtime.archive();
    }

    AeronArchive.Context archiveContext() {
        if (this.runtime == null) throw new IllegalStateException("Aeron runtime is not initialized");
        return this.runtime.archiveContext();
    }

    /* AeronArchive's synchronous control client is not thread-safe.  Retention,
     * writer recovery, and reader discovery can run on different worker threads,
     * so every provider-owned request is serialized on the client itself.  The
     * lock covers only the request; callers retain their own retry/deadline logic
     * outside this helper. */
    long getStartPosition(final long recordingId) {
        final AeronArchive archive = this.archive();
        synchronized (archive) {
            return archive.getStartPosition(recordingId);
        }
    }

    long getStopPosition(final long recordingId) {
        final AeronArchive archive = this.archive();
        synchronized (archive) {
            return archive.getStopPosition(recordingId);
        }
    }

    long getRecordingPosition(final long recordingId) {
        final AeronArchive archive = this.archive();
        synchronized (archive) {
            return archive.getRecordingPosition(recordingId);
        }
    }

    int listRecordingsForUri(final String channelFragment, final int streamId,
                             final RecordingDescriptorConsumer consumer) {
        final AeronArchive archive = this.archive();
        synchronized (archive) {
            return archive.listRecordingsForUri(0L, 2, channelFragment,
                    streamId, consumer);
        }
    }
}
