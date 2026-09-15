package peruncs.datagrid.cluster.storage.types;

/** Applies one already-validated update to the object graph. */
@FunctionalInterface
public interface ObjectGraphUpdater {
    /** Applies the pending update to the object graph. */
    void updateObjectGraph();
}
