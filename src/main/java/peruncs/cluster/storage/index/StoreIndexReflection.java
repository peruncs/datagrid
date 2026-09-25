package peruncs.cluster.storage.index;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.store.gigamap.lucene.LuceneContext;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/// Resolves and caches the upstream Store field layout used by index refresh
/// and validation.
///
/// This is deliberately the only place that knows the reflective layout:
/// a Store upgrade that moves a field fails here with a clear "unsupported
/// Store version" error instead of silently keeping a stale search view. The
/// cached fields are read and written through Store's offset-based memory
/// accessor, so no runtime `--add-opens` flag is required.
///
/// `ClassValue` caches let application classes unload with their class
/// loader. Resolution runs once per class, not once per index per batch; an
/// unrecognized layout throws instead of caching, so the next batch fails
/// closed again.
final class StoreIndexReflection {
    private StoreIndexReflection() {
    }

        /// Resolved transient fields of an upstream vector index.
    record VectorGraphFields(Field builder, Field graph, Field rebuilt, Field deferred) {
    }



    private static final ClassValue<VectorGraphFields> VECTOR_GRAPH_FIELDS = new ClassValue<>() {
        @Override
        protected VectorGraphFields computeValue(final Class<?> type) {
            Field builderField = null;
            Field graphField = null;
            Field rebuiltField = null;
            Field deferredField = null;
            for (Class<?> current = type; current != null && current != Object.class;
                 current = current.getSuperclass()) {
                for (final Field field : current.getDeclaredFields()) {
                    if (Modifier.isStatic(field.getModifiers())) continue;
                    switch (field.getName()) {
                        case "builder" -> {
                            if (GraphIndexBuilder.class.isAssignableFrom(field.getType())) {
                                if (builderField != null) throw ambiguous(type, "builder");
                                builderField = field;
                            }
                        }
                        case "index" -> {
                            if (OnHeapGraphIndex.class.isAssignableFrom(field.getType())) {
                                if (graphField != null) throw ambiguous(type, "index");
                                graphField = field;
                            }
                        }
                        case "graphRebuilt" -> {
                            if (field.getType() == boolean.class) {
                                if (rebuiltField != null) throw ambiguous(type, "graphRebuilt");
                                rebuiltField = field;
                            }
                        }
                        case "deferredBuilderOps" -> {
                            if (ConcurrentLinkedQueue.class.isAssignableFrom(field.getType())) {
                                if (deferredField != null) throw ambiguous(type, "deferredBuilderOps");
                                deferredField = field;
                            }
                        }
                        default -> {
                        }
                    }
                }
                if (builderField != null && graphField != null
                        && rebuiltField != null && deferredField != null) break;
            }
            if (builderField == null || graphField == null || rebuiltField == null || deferredField == null) {
                throw new IllegalStateException(
                        "cannot reset vector search graph on %s; unsupported Store version"
                                .formatted(type == null ? null : type.getName()));
            }
            return new VectorGraphFields(builderField, graphField, rebuiltField, deferredField);
        }
    };

    /* Only the persisted context field is resolved here: Lucene view
     * retirement itself uses the public, re-usable LuceneIndex.close() seam,
     * so no part of the transient handle layout (directory, writer, reader,
     * searcher, analyzer, readerStale) is touched reflectively. The injected
     * context field is needed by validation to reject external directory
     * configurations, which cannot survive replication. */
    private static final ClassValue<Field> LUCENE_CONTEXT_FIELD = new ClassValue<>() {
        @Override
        protected Field computeValue(final Class<?> type) {
            Field match = null;
            for (Class<?> current = type; current != null && current != Object.class;
                 current = current.getSuperclass()) {
                for (final Field field : current.getDeclaredFields()) {
                    if (Modifier.isStatic(field.getModifiers())) continue;
                    if (LuceneContext.class.isAssignableFrom(field.getType())) {
                        if (match != null) throw ambiguous(type, "LuceneContext");
                        match = field;
                    }
                }
            }
            if (match != null) return match;
            throw new IllegalStateException(
                    "cannot resolve the Lucene context on %s; unsupported Store version"
                            .formatted(type.getName()));
        }
    };

    private static IllegalStateException ambiguous(final Class<?> type, final String field) {
        return new IllegalStateException(
                "multiple '%s' fields found on %s; unsupported Store version".formatted(field, type.getName()));
    }

    /// Returns the cached transient vector-index fields for one index class.
    ///
    /// @param type vector index class
    /// @return resolved upstream fields
    static VectorGraphFields vectorGraphFields(final Class<?> type) {
        return VECTOR_GRAPH_FIELDS.get(type);
    }

    /// Cached reachable instance fields of one class hierarchy.
    ///
    /// The index-relevance walk visits every object of a validated graph;
    /// re-enumerating declared fields per visited object made every scan pay
    /// reflection setup proportional to the graph size. Resolved once per
    /// class and shared by every walk.
    private static final ClassValue<List<Field>> REACHABLE_FIELDS = new ClassValue<>() {
        @Override
        protected List<Field> computeValue(final Class<?> type) {
            final java.util.ArrayList<Field> fields = new java.util.ArrayList<>();
            for (Class<?> cursor = type; cursor != null && cursor != Object.class;
                 cursor = cursor.getSuperclass()) {
                for (final Field field : cursor.getDeclaredFields()) {
                    if (!Modifier.isStatic(field.getModifiers()) && !field.getType().isPrimitive()) {
                        fields.add(field);
                    }
                }
            }
            return List.copyOf(fields);
        }
    };

    /// Returns the cached reachable instance fields of one class hierarchy.
    ///
    /// @param type class whose instances the walk visits
    /// @return non-static, non-primitive fields, resolved once per class
    static List<Field> reachableFields(final Class<?> type) {
        return REACHABLE_FIELDS.get(type);
    }

    /// Cached `indexGroups` field of one upstream indices type.
    ///
    /// The group snapshot runs on every validated map; the superclass walk to
    /// locate the field is resolved once per indices class instead of per
    /// call. Every same-named candidate is collected across the whole
    /// hierarchy — a shadowing subclass field must fail closed as an
    /// ambiguous layout instead of silently binding the shadow, exactly like
    /// the vector and Lucene resolvers above.
    private static final ClassValue<Field> INDEX_GROUPS_FIELD = new ClassValue<>() {
        @Override
        protected Field computeValue(final Class<?> type) {
            Field match = null;
            for (Class<?> cursor = type; cursor != null && cursor != Object.class;
                 cursor = cursor.getSuperclass()) {
                for (final Field field : cursor.getDeclaredFields()) {
                    if (!Modifier.isStatic(field.getModifiers()) &&
                        "indexGroups".equals(field.getName()) &&
                        Iterable.class.isAssignableFrom(field.getType())) {
                        if (match != null) throw ambiguous(type, "indexGroups");
                        match = field;
                    }
                }
            }
            if (match != null) return match;
            throw new IllegalStateException(
                    "GigaMap index groups not found on %s; unsupported upstream layout"
                            .formatted(type.getName()));
        }
    };

    /// Returns the cached `indexGroups` field of one indices type.
    ///
    /// @param indicesType upstream GigaIndices class
    /// @return the resolved field
    static Field indexGroupsField(final Class<?> indicesType) {
        return INDEX_GROUPS_FIELD.get(indicesType);
    }

    /// Returns the cached persisted Lucene-context field of one index class.
    ///
    /// @param type Lucene index class
    /// @return resolved context field
    static Field luceneContextField(final Class<?> type) {
        return LUCENE_CONTEXT_FIELD.get(type);
    }

    /// Reads one object field through Store's offset-based accessor.
    ///
    /// @param target object to read
    /// @param field  resolved field
    /// @return field value
    static Object read(final Object target, final Field field) {
        return XMemory.getObject(target, XMemory.objectFieldOffset(field));
    }

    /// Writes one object field through Store's offset-based accessor.
    ///
    /// @param target object to mutate
    /// @param field  resolved field
    /// @param value  value to store
    static void write(final Object target, final Field field, final Object value) {
        XMemory.setObject(target, XMemory.objectFieldOffset(field), value);
    }

    /// Reads one boolean field through Store's offset-based accessor.
    ///
    /// @param target object to read
    /// @param field  resolved field
    /// @return field value
    static boolean readBoolean(final Object target, final Field field) {
        return XMemory.get_byte(target, XMemory.objectFieldOffset(field)) != 0;
    }

    /// Writes one boolean field through Store's offset-based accessor.
    ///
    /// @param target object to mutate
    /// @param field  resolved field
    /// @param value  value to store
    static void writeBoolean(final Object target, final Field field, final boolean value) {
        XMemory.set_byte(target, XMemory.objectFieldOffset(field), (byte) (value ? 1 : 0));
    }
}
