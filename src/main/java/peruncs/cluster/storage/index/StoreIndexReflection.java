package peruncs.cluster.storage.index;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import org.eclipse.store.gigamap.jvector.VectorIndex;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/// Invalidates Eclipse Store's transient JVector graph after replicated imports.
///
/// Replicated materialization bypasses `VectorIndex`'s mutation API, leaving its in-memory graph
/// stale. Eclipse Store exposes no public invalidation method yet, so this bridge reaches the
/// graph fields through `VarHandle`s: it takes the same `builderLock` write lock the index uses
/// for its own cleanup and uses volatile access, exactly as upstream does. It is temporary
/// pending [Eclipse Store PR #832](https://github.com/eclipse-store/store/pull/832), which proposes
/// the supported `VectorIndex.invalidateGraph()`; replace this class with that call once released.
/// This is the only permitted production use of reflection.
///
/// On the module path the Store's vector package must be opened to this module
/// (`--add-opens org.eclipes.store.gigamap.jvector/org.eclipse.store.gigamap.jvector=peruncs.cluster`);
/// [#verifyLayout()] fails node startup, not the first replicated batch, when it is not.
final class StoreIndexReflection {
    private StoreIndexReflection() {
    }

    /// Volatile handles to the transient graph state of one `VectorIndex` implementation.
    private record VectorGraphAccess(VarHandle builder, VarHandle graph, VarHandle rebuilt, VarHandle deferred,
                                     VarHandle lock) {
    }

    private static final ClassValue<VectorGraphAccess> ACCESS = new ClassValue<>() {
        @Override
        protected VectorGraphAccess computeValue(final Class<?> type) {
            return resolve(type);
        }
    };

    /// Checks that this Store version has the expected vector index layout and that it is accessible.
    ///
    /// @throws IllegalStateException when a field is missing, ambiguous or not accessible
    static void verifyLayout() {
        ACCESS.get(VectorIndex.Default.class);
    }

    /// Discards the transient vector graph before replicated imports become visible.
    ///
    /// The caller holds the graph write boundary.
    ///
    /// @param index index whose search graph to reset
    /// @throws IllegalStateException if the upstream field layout is not recognized
    static void invalidateVectorGraph(final VectorIndex<?> index) {
        final Object target = Objects.requireNonNull(index, "index");
        final VectorGraphAccess access = ACCESS.get(target.getClass());
        final ReentrantReadWriteLock lock = (ReentrantReadWriteLock) access.lock().getVolatile(target);
        if (lock == null) {
            /* The index never built a graph: nothing to reset. A graph without its lock is a layout surprise. */
            if (access.builder().getVolatile(target) != null || access.graph().getVolatile(target) != null) {
                throw new IllegalStateException(
                        "vector index %s has a graph but no builder lock".formatted(target.getClass().getName()));
            }
            return;
        }
        lock.writeLock().lock();
        try {
            if (access.deferred().getVolatile(target) instanceof ConcurrentLinkedQueue<?> queued) queued.clear();
            final GraphIndexBuilder builder = (GraphIndexBuilder) access.builder().getVolatile(target);
            if (builder != null) {
                try {
                    builder.close();
                } catch (final IOException failure) {
                    throw new IllegalStateException(
                            "cannot close vector search builder on %s".formatted(target.getClass().getName()), failure);
                }
                access.builder().setVolatile(target, (GraphIndexBuilder) null);
            }
            final OnHeapGraphIndex graph = (OnHeapGraphIndex) access.graph().getVolatile(target);
            if (graph != null) {
                graph.close();
                access.graph().setVolatile(target, (OnHeapGraphIndex) null);
            }
            access.rebuilt().setVolatile(target, false);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static VectorGraphAccess resolve(final Class<?> type) {
        VarHandle builder = null;
        VarHandle graph = null;
        VarHandle rebuilt = null;
        VarHandle deferred = null;
        VarHandle lock = null;
        for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
            for (final Field field : current.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) continue;
                switch (field.getName()) {
                    case "builder" -> {
                        if (GraphIndexBuilder.class.isAssignableFrom(field.getType())) {
                            builder = single(builder, field, type);
                        }
                    }
                    case "index" -> {
                        if (OnHeapGraphIndex.class.isAssignableFrom(field.getType())) graph = single(graph, field, type);
                    }
                    case "graphRebuilt" -> {
                        if (field.getType() == boolean.class) rebuilt = single(rebuilt, field, type);
                    }
                    case "deferredBuilderOps" -> {
                        if (ConcurrentLinkedQueue.class.isAssignableFrom(field.getType())) {
                            deferred = single(deferred, field, type);
                        }
                    }
                    case "builderLock" -> {
                        if (ReentrantReadWriteLock.class.isAssignableFrom(field.getType())) {
                            lock = single(lock, field, type);
                        }
                    }
                    default -> {
                    }
                }
            }
        }
        if (builder == null || graph == null || rebuilt == null || deferred == null || lock == null) {
            throw new IllegalStateException(
                    "cannot reset vector search graph on %s; unsupported Store version".formatted(type.getName()));
        }
        return new VectorGraphAccess(builder, graph, rebuilt, deferred, lock);
    }

    private static VarHandle single(final VarHandle existing, final Field field, final Class<?> type) {
        if (existing != null) {
            throw new IllegalStateException("multiple '%s' fields found on %s; unsupported Store version"
                    .formatted(field.getName(), type.getName()));
        }
        try {
            return MethodHandles.privateLookupIn(field.getDeclaringClass(), MethodHandles.lookup())
                    .unreflectVarHandle(field);
        } catch (final IllegalAccessException failure) {
            throw new IllegalStateException(
                    ("cannot access vector index field '%s' on %s; on the module path add "
                     + "--add-opens org.eclipes.store.gigamap.jvector/org.eclipse.store.gigamap.jvector=peruncs.cluster")
                            .formatted(field.getName(), field.getDeclaringClass().getName()), failure);
        }
    }
}
