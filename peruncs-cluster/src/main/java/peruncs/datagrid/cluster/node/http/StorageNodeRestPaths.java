package peruncs.datagrid.cluster.node.http;

/// The REST paths the node exposes under its root.
///
/// The node ships no HTTP server: the embedding application mounts these paths
/// on its own stack and delegates each one to
/// [ClusterRestRequestController]. Keeping the paths here gives embedders one
/// authoritative table instead of string literals, and keeps the route names
/// aligned with the controller methods. Media types are the embedder's choice;
/// the controller returns typed values and never renders JSON or Prometheus
/// text itself.
///
/// The mutating routes ([#BACKUP], [#GC], [#UPDATES], [#RESUME_UPDATES],
/// [#ACTIVATE_DISTRIBUTOR_START], and [#ACTIVATE_DISTRIBUTOR_FINISH]) are
/// privileged operations and carry no authentication of their own. The
/// embedding application must authenticate and authorize them before
/// delegating; only the health and readiness probes ([#HEALTH],
/// [#HEALTH_READY]) and the read-only metrics are safe to expose to a probe
/// endpoint.
public final class StorageNodeRestPaths {
        /// Root path shared by all node endpoints.
    public static final String ROOT_PATH = "/eclipse-datagrid";
        /// Reads whether this node is the distributor (`GET`).
    public static final String DISTRIBUTOR = "/distributor";
        /// Starts the distributor role transition (`POST`).
    public static final String ACTIVATE_DISTRIBUTOR_START = "/activate-distributor/start";
        /// Finishes the distributor role transition (`POST`).
    public static final String ACTIVATE_DISTRIBUTOR_FINISH = "/activate-distributor/finish";
        /// Liveness probe (`GET`).
    public static final String HEALTH = "/health";
        /// Readiness probe (`GET`).
    public static final String HEALTH_READY = "/health/ready";
        /// Reads the current storage size in bytes (`GET`).
    public static final String STORAGE_BYTES = "/storage-bytes";
        /// Reads the raw replication observability values (`GET`).
    public static final String REPLICATION_METRICS = "/replication-metrics";
        /// Starts a backup (`POST`) or reads whether one is running (`GET`).
    public static final String BACKUP = "/backup";
        /// Pauses replication (`POST`) or reads whether it is paused (`GET`).
    public static final String UPDATES = "/updates";
        /// Resumes replication after a controlled pause (`POST`).
    public static final String RESUME_UPDATES = "/resume-updates";
        /// Starts asynchronous storage checks and cleanup (`POST`) or reads whether they run (`GET`).
    public static final String GC = "/gc";

    private StorageNodeRestPaths() {
    }
}
