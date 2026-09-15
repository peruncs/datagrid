package peruncs.datagrid.cache.types;

import org.eclipse.store.cache.hibernate.types.ConfigurationPropertyNames;

/// Names of the clustered-cache configuration properties.
public interface ClusteredConfigurationPropertyNames {
        /// Prefix shared by clustered-cache properties.
    String PREFIX = "%sclustered.".formatted(ConfigurationPropertyNames.PREFIX);
        /// Property that selects the serializer type provider.
    String SERIALIZATION_TYPES_PROVIDER = "%sserialization-types-provider".formatted(PREFIX);
}
