package peruncs.datagrid.cluster.storage.types;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies the structured graph-update boundary.
class ObjectGraphUpdateHandlerTest {
    private static final ScopedValue<String> CONTEXT = ScopedValue.newInstance();

    @Test
    void updateRunsInVirtualChildAndInheritsScopedContext() {
        final AtomicReference<Thread> updateThread = new AtomicReference<>();
        final AtomicReference<String> updateContext = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        ScopedValue.where(CONTEXT, "replication").run(() ->
        {
            try {
                ObjectGraphUpdateHandler.runStructured(
                        Runnable::run,
                        () ->
                        {
                            updateThread.set(Thread.currentThread());
                            updateContext.set(CONTEXT.get());
                        },
                        Duration.ofSeconds(5L));
            } catch (final Throwable error) {
                failure.set(error);
            }
        });

        assertNull(failure.get());
        assertEquals("replication", updateContext.get());
        assertTrue(updateThread.get().isVirtual());
        assertFalse(CONTEXT.isBound());
    }
}
