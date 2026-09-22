package peruncs.datagrid.cluster.node.aeron;

import org.eclipse.serializer.typing.Disposable;

import java.util.Objects;
import java.util.function.Supplier;

/// Holds the current reader behind a dedicated lifecycle lock.
///
/// Reader replacement and disposal can join a polling thread for up to the
/// reader-stop timeout, so they must never run under the transport monitor:
/// health probes and asynchronous failure callbacks read [#current()] without
/// blocking behind that join. The replaced reader stays visible until its
/// dispose succeeded and the replacement reference is published last, so
/// lock-free observers see the disposing reader, an explicit starting state,
/// or the fully installed replacement. A dispose that throws
/// leaves the reader in place so a retried close reaches it again instead of
/// leaking a half-disposed instance.
///
/// @param <T> disposable reader type
final class AeronReaderSlot<T extends Disposable> {
    private enum State { EMPTY, LIVE, DISPOSING, STARTING }

    private final Object lock = new Object();
    private volatile T current;
    private volatile State state = State.EMPTY;

    /// Returns the installed reader, or `null` when none is installed.
    ///
    /// This is a volatile read: it never waits for a concurrent dispose.
    ///
    /// @return current reader, or `null`
    T current() {
        return this.current;
    }

    /// Whether a reader is installed or a replacement is in progress.
    boolean occupied() {
        return this.state != State.EMPTY;
    }

    /// Disposes the previous reader and publishes a fully created replacement.
    ///
    /// The factory runs under the lifecycle lock, after the previous reader is
    /// disposed and before the new reference is published, so a freshly
    /// started reader can never observe a partially replaced slot.
    ///
    /// @param factory creates the replacement reader
    /// @return installed replacement
    T replace(final Supplier<T> factory) {
        Objects.requireNonNull(factory, "factory");
        synchronized (this.lock) {
            final T previous = this.current;
            if (previous != null) {
                this.state = State.DISPOSING;
                try {
                    previous.dispose();
                } catch (final RuntimeException | Error failure) {
                    this.state = State.LIVE;
                    throw failure;
                }
            }
            this.current = null;
            this.state = State.STARTING;
            try {
                final T replacement = Objects.requireNonNull(factory.get(), "replacement");
                this.current = replacement;
                this.state = State.LIVE;
                return replacement;
            } catch (final RuntimeException | Error failure) {
                this.state = State.EMPTY;
                throw failure;
            }
        }
    }

    /// Disposes and clears the current reader.
    void dispose() {
        synchronized (this.lock) {
            this.disposeLocked();
        }
    }

    private void disposeLocked() {
        final T reader = this.current;
        if (reader == null) return;
        /* Null only after a successful dispose: a throwing dispose leaves the
         * reference in place so a retried close stage reaches it again
         * instead of leaking a half-disposed reader. */
        reader.dispose();
        this.current = null;
        this.state = State.EMPTY;
    }
}
