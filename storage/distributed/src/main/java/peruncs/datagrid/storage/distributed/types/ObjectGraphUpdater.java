package peruncs.datagrid.storage.distributed.types;

/** Applies one already-validated update to the object graph. */
@FunctionalInterface
public interface ObjectGraphUpdater {
    /** Applies the pending update to the object graph. */
    void updateObjectGraph();
}
