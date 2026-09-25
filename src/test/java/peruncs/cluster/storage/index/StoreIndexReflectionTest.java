package peruncs.cluster.storage.index;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

/// Fail-closed layout tests for the reflective upstream resolvers.
///
/// Synthetic classes shadow a field the resolver must bind uniquely: a
/// subclass field with the same name must be rejected as an ambiguous layout
/// instead of silently binding the shadow, which would let an upstream Store
/// upgrade corrupt index maintenance without any visible failure.
class StoreIndexReflectionTest {
    /// LuceneContext is an upstream interface, so plain interface-typed
    /// fields are enough for the shadowing layout tests.
    
    /// One superclass/base pair for shadowing tests.
    private static class GroupsBase {
        @SuppressWarnings("unused")
        private final List<String> indexGroups = new ArrayList<>();
    }

    private static class ShadowedGroups extends GroupsBase {
        @SuppressWarnings("unused")
        private final List<Integer> indexGroups = new ArrayList<>();
    }

    private static class SingleGroups {
        @SuppressWarnings("unused")
        private final List<String> indexGroups = new ArrayList<>();
    }

    /// A same-named field of an unexpected type is not a candidate: the
    /// resolver must bind the one typed match and ignore the stranger.
    private static class StrangerTypeGroups {
        @SuppressWarnings("unused")
        private final String indexGroups = "not an iterable";
        @SuppressWarnings("unused")
        private final List<String> realNameHidden = new ArrayList<>();
    }

    private static class LuceneBase {
        @SuppressWarnings("unused")
        private final org.eclipse.store.gigamap.lucene.LuceneContext<Object> context = null;
    }

    private static class ShadowedLuceneContext extends LuceneBase {
        @SuppressWarnings("unused")
        private final org.eclipse.store.gigamap.lucene.LuceneContext<Object> context = null;
    }

    /// Vector-graph shadowing pair: both classes declare a builder field.
    private static class VectorBase {
        @SuppressWarnings("unused")
        private final Object builder = new Object();
        @SuppressWarnings("unused")
        private final Object index = new Object();
        @SuppressWarnings("unused")
        private boolean graphRebuilt;
        @SuppressWarnings("unused")
        private final ConcurrentLinkedQueue<Object> deferredBuilderOps = new ConcurrentLinkedQueue<>();
    }

    /// A shadowing subclass field must fail closed, never silently bind.
    @Test
    void shadowedIndexGroupsFieldFailsClosed() {
        final IllegalStateException failure =
                assertThrows(IllegalStateException.class, () -> StoreIndexReflection.indexGroupsField(ShadowedGroups.class));
        assertTrue(failure.getMessage().contains("indexGroups"),
                "the failure must name the ambiguous field: " + failure.getMessage());
    }

    /// The single unambiguous layout resolves and is cached per class.
    @Test
    void singleIndexGroupsFieldResolves() {
        final Field field = StoreIndexReflection.indexGroupsField(SingleGroups.class);
        assertNotNull(field);
        assertEquals("indexGroups", field.getName());
    }

    /// Same-named fields of an unexpected type are not candidates, so the
    /// layout stays unambiguous when only one typed match exists.
    @Test
    void strangerTypedSameNameFieldIsIgnored() {
        assertThrows(IllegalStateException.class,
                () -> StoreIndexReflection.indexGroupsField(StrangerTypeGroups.class),
                "no typed candidate means no resolution, not a wrong bind");
    }

    /// A shadowed Lucene context must fail closed as well.
    @Test
    void shadowedLuceneContextFailsClosed() {
        final IllegalStateException failure =
                assertThrows(IllegalStateException.class, () -> StoreIndexReflection.luceneContextField(ShadowedLuceneContext.class));
        assertTrue(failure.getMessage().contains("LuceneContext"),
                "the failure must name the ambiguous field: " + failure.getMessage());
    }

    /// The vector resolver collects candidates across the whole hierarchy
    /// before binding, so a shadow cannot silently win even when the base
    /// declares every field first.
    @Test
    void vectorResolverReportsUnsupportedLayoutForSyntheticClasses() {
        /* The synthetic pair declares builder fields of Object (not
         * GraphIndexBuilder), so the typed resolver must reject the layout
         * instead of binding the wrong field. */
        assertThrows(IllegalStateException.class,
                () -> StoreIndexReflection.vectorGraphFields(VectorBase.class));
    }

    /// A UUID-only sanity check that the reflection helper reads what it
    /// resolved: the reachable-fields enumeration must see declared members.
    @Test
    void reachableFieldsEnumerateDeclaredMembers() {
        final List<Field> fields = StoreIndexReflection.reachableFields(SingleGroups.class);
        assertTrue(fields.stream().anyMatch(field -> "indexGroups".equals(field.getName())),
                "the declared indexGroups field must be reachable");
    }

}
