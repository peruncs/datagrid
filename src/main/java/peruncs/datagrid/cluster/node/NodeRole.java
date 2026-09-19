package peruncs.datagrid.cluster.node;

import java.util.Locale;
import java.util.Objects;

/// Fixed-topology node role, normalized from the legacy and current settings.
///
/// Two settings describe the role: the legacy [NodeLibraryPropertiesProvider#isBackupNode]
/// flag and the `ECLIPSE_DATAGRID_REPLICATION_ROLE` value (`writer`, `reader`,
/// or `backup-reader`). Every decision point — startup path, manager guards,
/// root creation, executors, cursor handling, and transport setup — reads this
/// single value instead of combining the raw settings ad hoc, so a node can
/// never start down one role's path while its transport configures another.
///
/// @since 1.0
public enum NodeRole {
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

        /// Reports whether this role serves backups.
    ///
    /// @return `true` for [NodeRole#BACKUP_READER]
    public boolean isBackupReader() {
        return this == BACKUP_READER;
    }

        /// Returns the configuration spelling of this role.
    ///
    /// @return `writer`, `reader`, or `backup-reader`
    public String configName() {
        return switch (this) {
            case WRITER -> NodeLibraryPropertiesProvider.WRITER_ROLE;
            case READER -> NodeLibraryPropertiesProvider.READER_ROLE;
            case BACKUP_READER -> NodeLibraryPropertiesProvider.BACKUP_READER_ROLE;
        };
    }

        /// Parses a configured role value.
    ///
    /// @param configured raw role value, must name a known role
    /// @return normalized role
    /// @throws IllegalArgumentException when the value names no known role
    public static NodeRole parse(final String configured) {
        if (configured != null) {
            final String normalized = configured.trim().toLowerCase(Locale.ROOT);
            for (final NodeRole role : values()) {
                if (role.configName().equals(normalized)) {
                    return role;
                }
            }
        }
        throw new IllegalArgumentException(
                "unknown replication role '%s'; must be writer, reader, or backup-reader"
                        .formatted(configured));
    }

        /// Resolves the effective role from both settings, rejecting conflicts.
    ///
    /// A non-blank configured value is explicit and wins over the legacy flag,
    /// except that a legacy backup node combined with an explicit `writer` or
    /// `reader` is a misconfiguration and fails instead of silently picking a
    /// side. An unconfigured value inherits the legacy flag, defaulting to
    /// [NodeRole#WRITER].
    ///
    /// @param configured   raw `ECLIPSE_DATAGRID_REPLICATION_ROLE` value, or `null`
    /// @param legacyBackup legacy backup flag
    /// @return effective role
    /// @throws IllegalArgumentException for an unknown value or a legacy/new conflict
    public static NodeRole resolve(final String configured, final boolean legacyBackup) {
        if (configured != null && !configured.isBlank()) {
            final NodeRole role = parse(configured);
            if (legacyBackup && role != BACKUP_READER) {
                throw new IllegalArgumentException(
                        "ECLIPSE_DATAGRID_REPLICATION_ROLE=%s conflicts with a legacy backup node; use backup-reader or clear the legacy flag"
                                .formatted(configured.trim()));
            }
            return role;
        }
        return legacyBackup ? BACKUP_READER : WRITER;
    }

        /// Resolves the effective role for one provider.
    ///
    /// @param properties property provider, must not be `null`
    /// @return effective role
    /// @throws IllegalArgumentException for an unknown value or a legacy/new conflict
    public static NodeRole of(final NodeLibraryPropertiesProvider properties) {
        Objects.requireNonNull(properties, "properties");
        return resolve(properties.replicationRole(), properties.isBackupNode());
    }
}
