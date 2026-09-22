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
class ObjectMaterializer implements BinaryEntityRawDataAcceptor {
    private final PersistenceTypeDictionary persistenceTypeDictionary;
    private final PersistenceObjectRegistry objectRegistry;
    private final PersistenceLoader loader;
    private final Set_long oids = Set_long.New();

        /// Creates a materializer for one persistence manager.
    ///
    /// @param persistenceManager manager that owns the target graph
    ObjectMaterializer(final PersistenceManager<?> persistenceManager) {
        super();

        this.persistenceTypeDictionary = persistenceManager.typeDictionary();
        this.objectRegistry = persistenceManager.objectRegistry();
        this.loader = persistenceManager.createLoader();
    }

        /// Collects one entity's object id, or fails on a malformed entity header.
    ///
    /// A truncated header means the containing batch is corrupt: the entity
    /// data cannot be skipped without silently dropping the rest of the
    /// buffer, so it fails with [StorageBinaryDataException] instead of
    /// stopping iteration.
    ///
    /// @param entityStartAddress entity header start address
    /// @param dataBoundAddress   end of the available entity data
    /// @return always `true`; a truncated header throws
    /// @throws StorageBinaryDataException if the entity header is truncated
    @Override
    public boolean acceptEntityData(final long entityStartAddress, final long dataBoundAddress) {
        if (entityStartAddress + Binary.entityHeaderLength() > dataBoundAddress) {
            throw new StorageBinaryDataException(
                    "truncated entity header at %s: %s bytes available, %s required"
                            .formatted(entityStartAddress, dataBoundAddress - entityStartAddress,
                                    Binary.entityHeaderLength()));
        }

        final PersistenceTypeDefinition ptd = this.persistenceTypeDictionary.lookupTypeById(
                Binary.getEntityTypeIdRawValue(entityStartAddress)
        );
        if (ptd == null) {
            throw new StorageBinaryDataException("Cannot materialize persisted entity with unknown type id %s".formatted(Binary.getEntityTypeIdRawValue(entityStartAddress)));
        }
        if (PersistenceRoots.class.isAssignableFrom(ptd.type()) || PersistenceRootReference.class.isAssignableFrom(ptd.type())) {// don't overwrite local roots
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
    void materialize() {
        this.materialize(this.loader);
    }

    /// Materializes through a loader whose source is selected by the caller.
    ///
    /// The distributed importer uses this overload to make the loader consume
    /// the just-received binary rather than asking Store's normal source for an
    /// entity that was already live before the import.
    void materialize(final PersistenceLoader sourceLoader) {
        try {
            // Batch-materializes all collected objects in the live graph
            sourceLoader.collect(_ ->
            {
                // no-op
            }, this.oids);
        } finally {
            // Help the GC even when materialization fails
            this.oids.truncate();
        }
    }

}
