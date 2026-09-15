package peruncs.datagrid.cluster.nodelibrary.backup;


/// Identifies the local or remote place from which a backup is served.
public enum BackupTarget {
        /// Backup served by the hosted service.
    SAAS,
        /// Backup served by the local installation.
    ONPREM;

        /// Tries to parse the string into the appropriate backup target. If it fails
    /// `null` is returned.
    ///
    /// @param s target name
    /// @return parsed target, or `null`
    public static BackupTarget parse(final String s) {
        if (s == null) {
            return null;
        }

        return switch (s) {
            case "SAAS" -> SAAS;
            case "ONPREM" -> ONPREM;
            default -> null;
        };
    }
}
