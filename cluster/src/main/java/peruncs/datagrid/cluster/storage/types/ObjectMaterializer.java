package peruncs.datagrid.cluster.storage.types;


import org.eclipse.serializer.collections.Set_long;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.BinaryEntityRawDataAcceptor;
import org.eclipse.serializer.persistence.types.*;

/// This acceptor collects remote object ids and materializes them as one batch.
///
/// Root objects are left untouched because each node owns its local roots.
/// Repeated object ids are collected once, which matters when one transaction
/// contains several versions of an object.
public class ObjectMaterializer implements BinaryEntityRawDataAcceptor {
    private final PersistenceTypeDictionary persistenceTypeDictionary;
    private final PersistenceObjectRegistry objectRegistry;
    private final PersistenceLoader loader;
    private final Set_long oids = Set_long.New();

        /// Creates a materializer for one persistence manager.
    ///
    /// @param persistenceManager manager that owns the target graph
    public ObjectMaterializer(final PersistenceManager<?> persistenceManager) {
        super();

        this.persistenceTypeDictionary = persistenceManager.typeDictionary();
        this.objectRegistry = persistenceManager.objectRegistry();
        this.loader = persistenceManager.createLoader();
    }

    @Override
    public boolean acceptEntityData(final long entityStartAddress, final long dataBoundAddress) {
        // check for incomplete entity header
        if (entityStartAddress + Binary.entityHeaderLength() > dataBoundAddress) {
            // signal to calling context that entity cannot be processed and header must be reloaded
            return false;
        }

        final PersistenceTypeDefinition ptd = this.persistenceTypeDictionary.lookupTypeById(
                Binary.getEntityTypeIdRawValue(entityStartAddress)
        );
        if (ptd == null) {
            throw new StorageBinaryDataException(
                    "Cannot materialize persisted entity with unknown type id %s".formatted(Binary.getEntityTypeIdRawValue(entityStartAddress)));
        }
        if (
                PersistenceRoots.class.isAssignableFrom(ptd.type())
                || PersistenceRootReference.class.isAssignableFrom(ptd.type())
        ) {
            // don't overwrite local roots
            return true;
        }

        final long oid = Binary.getEntityObjectIdRawValue(entityStartAddress);

        if (this.objectRegistry.containsClearedObject(oid)) {
            // no need to process known but not loaded objects
            return true;
        }

        /*
         * Now we know it is either a new object or an existing one which is loaded, so
         * we need to re-materialize it.
         *
         * Adding the oid to a set ensures that each object is only materialized once.
         * Multiple versions of the object may be in the same data set, mostly when the
         * replication happens in batch mode.
         */
        this.oids.add(oid);

        return true;
    }

        /// Materializes each object collected by [#acceptEntityData(long, long)].
    public void materialize() {
        try {
            // Batch-materializes all collected objects in the live graph
            this.loader.collect(obj ->
            {
                // no-op
            }, this.oids);
        } finally {
            // Help the GC even when materialization fails
            this.oids.truncate();
        }
    }

}
