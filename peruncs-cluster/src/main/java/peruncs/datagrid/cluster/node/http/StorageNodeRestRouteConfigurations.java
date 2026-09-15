package peruncs.datagrid.cluster.node.http;

/// This class keeps the node REST paths and media types in one place.
///
/// These constants define the route table. The node ships no HTTP server; the
/// embedding application mounts these paths on its own stack and delegates to
/// the request controller, so every route here is public API even though this
/// repository never reads most of them. The request controller remains
/// responsible for the behavior behind each route.
public final class StorageNodeRestRouteConfigurations {
        /// Root path shared by all node endpoints.
    public static final String ROOT_PATH = "/eclipse-datagrid";

    private StorageNodeRestRouteConfigurations() {
    }

        /// Shared media types used by the node endpoints.
    public static final class MediaTypes {
        private static final String WILDCARD = "*/*";
        private static final String APPLICATION_JSON = "application/json";
        private static final String TEXT_PLAIN = "text/plain";

        private MediaTypes() {
        }
    }

        /// Reads whether this node is the distributor.
    public static final class GetDistributor {
                /// Distributor endpoint path.
        public static final String PATH = "/distributor";
                /// Distributor response media type.
        public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

        private GetDistributor() {
        }
    }

        /// Starts the distributor role transition.
    public static final class PostActivateDistributorStart {
                /// Activation-start endpoint path.
        public static final String PATH = "/activate-distributor/start";
                /// Activation-start request media type.
        public static final String CONSUMES = MediaTypes.WILDCARD;
                /// Activation-start response media type.
        public static final String PRODUCES = MediaTypes.WILDCARD;

        private PostActivateDistributorStart() {
        }
    }

        /// Finishes the distributor role transition.
    public static final class PostActivateDistributorFinish {
                /// Activation-finish endpoint path.
        public static final String PATH = "/activate-distributor/finish";
                /// Activation-finish request media type.
        public static final String CONSUMES = MediaTypes.WILDCARD;
                /// Activation-finish response media type.
        public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

        private PostActivateDistributorFinish() {
        }
    }

        /// Reads the node health state.
    public static final class GetHealth {
                /// Health endpoint path.
        public static final String PATH = "/health";
                /// Health response media type.
        public static final String PRODUCES = MediaTypes.WILDCARD;

        private GetHealth() {
        }
    }

        /// Reads whether the node is ready to serve.
    public static final class GetHealthReady {
                /// Readiness endpoint path.
        public static final String PATH = "/health/ready";
                /// Readiness response media type.
        public static final String PRODUCES = MediaTypes.WILDCARD;

        private GetHealthReady() {
        }
    }

        /// Reads the number of bytes used by storage.
    public static final class GetStorageBytes {
                /// Storage-size endpoint path.
        public static final String PATH = "/storage-bytes";
                /// Storage-size response media type.
        public static final String PRODUCES = MediaTypes.TEXT_PLAIN;

        private GetStorageBytes() {
        }
    }

        /// Reads replication metrics.
    public static final class GetReplicationMetrics {
                /// Replication-metrics endpoint path.
        public static final String PATH = "/replication-metrics";
                /// Replication-metrics response media type.
        public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

        private GetReplicationMetrics() {
        }
    }

        /// Starts a storage backup.
    public static final class PostBackup {
                /// Backup endpoint path.
        public static final String PATH = "/backup";
                /// Backup request media type.
        public static final String CONSUMES = MediaTypes.APPLICATION_JSON;
                /// Backup response media type.
        public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

        private PostBackup() {
        }

                /// Request body that selects the manual backup slot.
        ///
        /// @param useManualSlot whether the manual slot was requested
    public record Body(Boolean useManualSlot) {
    }
    }

        /// Reads whether a backup is running.
    public static final class GetBackup {
                /// Backup-status endpoint path.
        public static final String PATH = "/backup";
                /// Backup-status response media type.
        public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

        private GetBackup() {
        }
    }

        /// Stops replication at the latest safe message.
    public static final class PostUpdates {
                /// Update-pause endpoint path.
        public static final String PATH = "/updates";
                /// Update-pause request media type.
        public static final String CONSUMES = MediaTypes.WILDCARD;
                /// Update-pause response media type.
        public static final String PRODUCES = MediaTypes.WILDCARD;

        private PostUpdates() {
        }
    }

        /// Reads whether replication is paused.
    public static final class GetUpdates {
                /// Update-status endpoint path.
        public static final String PATH = "/updates";
                /// Update-status response media type.
        public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

        private GetUpdates() {
        }
    }

        /// Resumes replication after a controlled pause.
    public static final class PostResumeUpdates {
                /// Resume endpoint path.
        public static final String PATH = "/resume-updates";
                /// Resume request media type.
        public static final String CONSUMES = MediaTypes.WILDCARD;
                /// Resume response media type.
        public static final String PRODUCES = MediaTypes.WILDCARD;

        private PostResumeUpdates() {
        }
    }

        /// Starts asynchronous storage checks and cleanup.
    public static final class PostGc {
                /// Storage-check endpoint path.
        public static final String PATH = "/gc";
                /// Storage-check request media type.
        public static final String CONSUMES = MediaTypes.WILDCARD;
                /// Storage-check response media type.
        public static final String PRODUCES = MediaTypes.WILDCARD;

        private PostGc() {
        }
    }

        /// Reads whether storage checks are running.
    public static final class GetGc {
                /// Storage-check status path.
        public static final String PATH = "/gc";
                /// Storage-check status response media type.
        public static final String PRODUCES = MediaTypes.APPLICATION_JSON;

        private GetGc() {
        }
    }
}
