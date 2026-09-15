package peruncs.datagrid.cluster.node.backup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/// Immutable metadata returned by the remote backup service.
///
/// @param name remote object name
/// @param size remote object size in bytes
public record BackupMetadataDto(
        @JsonProperty("name") String name,
        @JsonProperty("size") long size
) {
        /// Creates metadata from the JSON properties returned by the backup service.
    @JsonCreator
    public BackupMetadataDto {
    }
}
