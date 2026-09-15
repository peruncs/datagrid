package peruncs.datagrid.cache.clustered.types;

import org.eclipse.store.cache.hibernate.types.ConfigurationPropertyNames;

/** Names of the clustered-cache configuration properties. */
public interface ClusteredConfigurationPropertyNames {
    /** Prefix shared by clustered-cache properties. */
    String PREFIX = ConfigurationPropertyNames.PREFIX + "clustered.";
    /** Property that selects the serializer type provider. */
    String SERIALIZATION_TYPES_PROVIDER = PREFIX + "serialization-types-provider";
}
