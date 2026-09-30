package peruncs.cluster.node.store;

import java.time.Duration;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/// Counts admitted application calls so node close can wait for them to leave.
final class ApplicationSections {
    private final NodeClose nodeClose;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition idle = this.lock.newCondition();
    private int active;

    ApplicationSections(final NodeClose nodeClose) {
        this.nodeClose = nodeClose;
    }

    void enter() {
        this.lock.lock();
        try {
            this.nodeClose.checkOpen();
            this.active++;
        } finally {
            this.lock.unlock();
        }
    }

    void exit() {
        this.lock.lock();
        try {
            if (--this.active == 0) this.idle.signalAll();
        } finally {
            this.lock.unlock();
        }
    }

    boolean awaitIdle(final Duration timeout) {
        long remaining = timeout.toNanos();
        this.lock.lock();
        try {
            while (this.active > 0) {
                if (remaining <= 0L) return false;
                try {
                    remaining = this.idle.awaitNanos(remaining);
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        } finally {
            this.lock.unlock();
        }
    }
}
