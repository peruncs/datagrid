package peruncs.cluster.storage.index;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.store.gigamap.jvector.VectorIndex;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

/// Invalidates Eclipse Store's transient JVector graph after replicated imports.
///
/// Replicated materialization bypasses `VectorIndex`'s mutation API, leaving its in-memory graph
/// stale. Reflection is currently necessary because Eclipse Store exposes no public invalidation
/// method. This bridge retires that graph while the caller holds the graph write boundary. It is
/// temporary pending
/// [Eclipse Store PR #832](https://github.com/eclipse-store/store/pull/832), which proposes the
/// supported `VectorIndex.invalidateGraph()` API; replace this bridge with that API when the PR is
/// merged and released. This is the only permitted production use of reflection; all other
/// production reflection remains prohibited. Use Serializer's supported type-handler APIs for
/// validation.
final class StoreIndexReflection {
    private StoreIndexReflection() {
    }

    private record VectorGraphFields(Field builder, Field graph, Field rebuilt, Field deferred) {
    }

    private static final ClassValue<VectorGraphFields> VECTOR_GRAPH_FIELDS = new ClassValue<>() {
        @Override
        protected VectorGraphFields computeValue(final Class<?> type) {
            Field builder = null;
            Field graph = null;
            Field rebuilt = null;
            Field deferred = null;
            for (Class<?> current = type; current != null && current != Object.class;
                 current = current.getSuperclass()) {
                for (final Field field : current.getDeclaredFields()) {
                    if (Modifier.isStatic(field.getModifiers())) continue;
                    switch (field.getName()) {
                        case "builder" -> {
                            if (GraphIndexBuilder.class.isAssignableFrom(field.getType())) {
                                if (builder != null) throw ambiguous(type, "builder");
                                builder = field;
                            }
                        }
                        case "index" -> {
                            if (OnHeapGraphIndex.class.isAssignableFrom(field.getType())) {
                                if (graph != null) throw ambiguous(type, "index");
                                graph = field;
                            }
                        }
                        case "graphRebuilt" -> {
                            if (field.getType() == boolean.class) {
                                if (rebuilt != null) throw ambiguous(type, "graphRebuilt");
                                rebuilt = field;
                            }
                        }
                        case "deferredBuilderOps" -> {
                            if (ConcurrentLinkedQueue.class.isAssignableFrom(field.getType())) {
                                if (deferred != null) throw ambiguous(type, "deferredBuilderOps");
                                deferred = field;
                            }
                        }
                        default -> {
                        }
                    }
                }
                if (builder != null && graph != null && rebuilt != null && deferred != null) break;
            }
            if (builder == null || graph == null || rebuilt == null || deferred == null) {
                throw new IllegalStateException(
                        "cannot reset vector search graph on %s; unsupported Store version".formatted(type.getName()));
            }
            return new VectorGraphFields(builder, graph, rebuilt, deferred);
        }
    };

    /// Discards the transient vector graph before replicated imports become visible.
    ///
    /// The caller holds the graph write boundary. Replace this with the public
    /// `VectorIndex.invalidateGraph()` as soon as PR #832 is included.
    static void invalidateVectorGraph(final VectorIndex<?> index) {
        final Object target = Objects.requireNonNull(index, "index");
        final VectorGraphFields fields = VECTOR_GRAPH_FIELDS.get(target.getClass());
        final GraphIndexBuilder builder = (GraphIndexBuilder) read(target, fields.builder());
        final OnHeapGraphIndex graph = (OnHeapGraphIndex) read(target, fields.graph());
        final Object deferred = read(target, fields.deferred());
        if (deferred instanceof ConcurrentLinkedQueue<?> queued) queued.clear();
        if (builder != null) {
            try {
                builder.close();
            } catch (final IOException failure) {
                throw new IllegalStateException(
                        "cannot close vector search builder on %s".formatted(target.getClass().getName()), failure);
            }
            write(target, fields.builder(), null);
        }
        if (graph != null) {
            graph.close();
            write(target, fields.graph(), null);
        }
        writeBoolean(target, fields.rebuilt(), false);
    }

    private static IllegalStateException ambiguous(final Class<?> type, final String field) {
        return new IllegalStateException(
                "multiple '%s' fields found on %s; unsupported Store version".formatted(field, type.getName()));
    }

    private static Object read(final Object target, final Field field) {
        return XMemory.getObject(target, XMemory.objectFieldOffset(field));
    }

    private static void write(final Object target, final Field field, final Object value) {
        XMemory.setObject(target, XMemory.objectFieldOffset(field), value);
    }

    private static void writeBoolean(final Object target, final Field field, final boolean value) {
        XMemory.set_byte(target, XMemory.objectFieldOffset(field), (byte) (value ? 1 : 0));
    }
}
