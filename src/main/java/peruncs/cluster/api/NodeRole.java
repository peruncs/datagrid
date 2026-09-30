package peruncs.cluster.api;

import java.util.Locale;

/// Fixed-topology node role selected from the transport and role settings.
///
/// @since 1.0
public enum NodeRole {
    /// Runs the local Store without a replication transport.
    STANDALONE,
    /// Owns the Store and the Aeron Archive recording; always distributes.
    WRITER,
    /// Replays the recording without recording; never distributes.
    READER,
    /// Replays like a reader and additionally serves backups.
    BACKUP_READER;

    /// Reports whether this role owns the write path.
    ///
    /// @return `true` for [NodeRole#WRITER]
    public boolean isWriter() {
        return this == WRITER;
    }

    /// Reports whether this role may create and update the local Store graph.
    ///
    /// @return `true` for standalone and writer nodes
    public boolean canWrite() {
        return this == STANDALONE || this == WRITER;
    }

    /// Returns the configuration spelling of this role.
    ///
    /// @return `writer`, `reader`, or `backup-reader`
    public String configName() {
        return switch (this) {
            case STANDALONE -> "standalone";
            case WRITER -> "writer";
            case READER -> "reader";
            case BACKUP_READER -> "backup-reader";
        };
    }

    /// Parses the configuration spelling of a role.
    ///
    /// @param configured role spelling, or `null`/blank for the default writer
    /// @return normalized role
    /// @throws IllegalArgumentException when the value names no known role
    public static NodeRole of(final String configured) {
        if (configured == null || configured.isBlank()) return WRITER;
        final String normalized = configured.trim().toLowerCase(Locale.ROOT);
        for (final NodeRole role : values()) {
            if (role.configName().equals(normalized)) return role;
        }
        throw new IllegalArgumentException(
                "unknown replication role '%s'; must be writer, reader, or backup-reader"
                        .formatted(configured));
    }

    /// Resolves the role selected by the replication transport and role settings.
    ///
    /// @param transport configured transport, or `null` for the local default
    /// @param configuredRole configured Aeron role
    /// @return standalone when replication is disabled, otherwise the configured Aeron role
    public static NodeRole resolve(final String transport, final String configuredRole) {
        return transport == null || "none".equalsIgnoreCase(transport.trim())
                ? STANDALONE
                : of(configuredRole);
    }

}
