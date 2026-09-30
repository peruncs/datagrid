package peruncs.cluster.node.store;

import org.eclipse.serializer.persistence.types.PersistenceManager;
import org.eclipse.serializer.persistence.types.PersistenceRegisterer;
import org.eclipse.serializer.persistence.types.PersistenceStorer;
import org.eclipse.serializer.persistence.types.Storer;
import org.eclipse.store.storage.types.Database;
import org.eclipse.store.storage.types.StorageConnection;
import org.eclipse.store.storage.types.StorageManager;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/// Keeps the Store facade's public persistence-method classification complete.
class GateClassificationTest {
    private static final List<Class<?>> CONTRACTS = List.of(
            StorageManager.class, StorageConnection.class, Database.class, PersistenceManager.class,
            Storer.class, PersistenceStorer.class, PersistenceRegisterer.class);

    /* Overloads share a gate by method name; writer/reader differences are
     * explicit in the C7 table and exercised by StorageWriteGatingTest. */
    private static final Map<String, String> GATES = Map.ofEntries(
            Map.entry("store", "WRITE"), Map.entry("storeAll", "WRITE"),
            Map.entry("storeRoot", "WRITE"), Map.entry("setRoot", "WRITE"),
            Map.entry("commit", "WRITE"), Map.entry("ensureRoot", "ROLE_DEPENDENT"),
            Map.entry("updateMetadata", "ROLE_DEPENDENT"),
            Map.entry("updateCurrentObjectId", "ROLE_DEPENDENT"),
            Map.entry("write", "ROLE_DEPENDENT"),
            Map.entry("ensureObjectId", "ROLE_DEPENDENT"),
            Map.entry("ensureObjectIdGuaranteedRegister", "ROLE_DEPENDENT"),
            Map.entry("mergeEntries", "ROLE_DEPENDENT"),
            Map.entry("registerLocalRegistry", "ROLE_DEPENDENT"),
            Map.entry("register", "ROLE_DEPENDENT"), Map.entry("registerAll", "ROLE_DEPENDENT"),
            Map.entry("apply", "ROLE_DEPENDENT"),
            Map.entry("consolidate", "ROLE_DEPENDENT"),
            Map.entry("createRegisterer", "ROLE_DEPENDENT"),
            Map.entry("objectRegistry", "ROLE_DEPENDENT"),
            Map.entry("importData", "REJECT"), Map.entry("importFiles", "REJECT"),
            Map.entry("setStorage", "REJECT"), Map.entry("guaranteeNoActiveStorage", "REJECT"),
            Map.entry("exportAdjacencyData", "READ"), Map.entry("exportTypes", "READ"),
            Map.entry("issueFullBackup", "READ"), Map.entry("exportChannels", "READ"),
            Map.entry("getObject", "READ"), Map.entry("lookupObject", "READ"),
            Map.entry("lookupObjectId", "READ"),
            Map.entry("get", "READ"), Map.entry("collect", "READ"),
            Map.entry("createLoader", "READ"), Map.entry("typeDictionary", "READ"),
            Map.entry("viewRoots", "READ"), Map.entry("databaseName", "READ"),
            Map.entry("hasStorage", "READ"), Map.entry("toIdentifyingString", "READ"),
            Map.entry("root", "HANDLE"), Map.entry("persistenceManager", "HANDLE"),
            Map.entry("database", "HANDLE"), Map.entry("createStorer", "HANDLE"),
            Map.entry("createLazyStorer", "HANDLE"), Map.entry("createEagerStorer", "HANDLE"),
            Map.entry("createConnection", "HANDLE"), Map.entry("createBatchStorer", "HANDLE"),
            Map.entry("batchStorerBuilder", "HANDLE"),
            Map.entry("storage", "HANDLE"), Map.entry("guaranteeActiveStorage", "HANDLE"),
            Map.entry("source", "HANDLE"), Map.entry("target", "HANDLE"),
            Map.entry("start", "LIFECYCLE"), Map.entry("shutdown", "LIFECYCLE"),
            Map.entry("close", "LIFECYCLE"),
            Map.entry("checkAcceptingTasks", "ADMIN"), Map.entry("configuration", "ADMIN"),
            Map.entry("createStorageStatistics", "ADMIN"), Map.entry("issueGarbageCollection", "ADMIN"),
            Map.entry("issueCacheCheck", "ADMIN"), Map.entry("issueFileCheck", "ADMIN"),
            Map.entry("issueIntegrityCheck", "ADMIN"), Map.entry("issueTransactionsLogCleanup", "ADMIN"),
            Map.entry("issueStorageFlush", "ADMIN"), Map.entry("issueFullFileCheck", "ADMIN"),
            Map.entry("issueFullCacheCheck", "ADMIN"), Map.entry("issueFullGarbageCollection", "ADMIN"),
            Map.entry("issueFullIntegrityCheck", "ADMIN"), Map.entry("initializationTime", "ADMIN"),
            Map.entry("operationModeTime", "ADMIN"), Map.entry("isRunning", "ADMIN"),
            Map.entry("isActive", "ADMIN"), Map.entry("isAcceptingTasks", "ADMIN"),
            Map.entry("isShuttingDown", "ADMIN"), Map.entry("isStartingUp", "ADMIN"),
            Map.entry("getTargetByteOrder", "ADMIN"), Map.entry("currentObjectId", "ADMIN"),
            Map.entry("objectRegistryMonitor", "ADMIN"), Map.entry("accessUsageMarks", "ADMIN"),
            Map.entry("markUsedFor", "ADMIN"), Map.entry("unmarkUsedFor", "ADMIN"),
            Map.entry("markUsed", "ADMIN"), Map.entry("markUnused", "ADMIN"), Map.entry("isUsed", "ADMIN"),
            Map.entry("registerCommitListener", "ADMIN"),
            Map.entry("registerRegistrationListener", "ADMIN"),
            Map.entry("clear", "ADMIN"), Map.entry("skip", "ADMIN"),
            Map.entry("skipN", "ADMIN"), Map.entry("skipMapped", "ADMIN"),
            Map.entry("skipNulled", "ADMIN"), Map.entry("ensureCapacity", "ADMIN"),
            Map.entry("reinitialize", "ADMIN"), Map.entry("currentCapacity", "ADMIN"),
            Map.entry("size", "ADMIN"), Map.entry("isEmpty", "ADMIN"),
            Map.entry("isFull", "ADMIN"), Map.entry("maximumCapacity", "ADMIN"),
            Map.entry("initializationDuration", "ADMIN"), Map.entry("isShutdown", "ADMIN"),
            Map.entry("Clone", "ADMIN"), Map.entry("isByteOrderMismatch", "ADMIN"));

    /// Every public Store API method must have an explicit gate classification.
    @Test
    void everyPersistenceMethodHasAClassification() {
        final List<String> missing = new ArrayList<>();
        for (final Class<?> contract : CONTRACTS) {
            for (final Method method : contract.getMethods()) {
                final int modifiers = method.getModifiers();
                if (method.getDeclaringClass() != Object.class && Modifier.isPublic(modifiers) &&
                        !Modifier.isStatic(modifiers) && !method.isBridge() && !method.isSynthetic() &&
                        !GATES.containsKey(method.getName())) {
                    missing.add(contract.getSimpleName() + "#" + signature(method));
                }
            }
        }
        assertTrue(missing.isEmpty(), "unclassified Store methods: " + missing);
    }

    private static String signature(final Method method) {
        return method.getName() + "(" + String.join(",", java.util.Arrays.stream(method.getParameterTypes())
                .map(Class::getSimpleName).toList()) + ")";
    }
}
