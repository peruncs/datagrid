import peruncs.cluster.api.ClusterNode;
import peruncs.cluster.api.ClusterStorageManager;
import peruncs.cluster.api.GraphBoundary;
import peruncs.cluster.api.NodeSettingsSource;

/// Embedded Eclipse Store clustering: one writer, N readers, replicated over Aeron.
///
/// This descriptor is the architecture decision record (ADR) of the module.
/// Each section states one decision and why; operational settings and
/// procedures live in `README.md` only. Package-level decisions live in the
/// packages' `package-info.java`.
///
/// # API surface
///
/// Only `peruncs.cluster.api` and `peruncs.cluster.errors` are exported. An
/// application opens a [ClusterNode], works through its
/// [ClusterStorageManager] (a `StorageManager`, so Eclipse Store code keeps
/// its API) and coordinates graph access with [GraphBoundary]. Configuration
/// enters through [NodeSettingsSource]. Everything else — Aeron, Store
/// adapters, cursors, checkpoints, backups, indexes — is internal, so it can
/// change without breaking applications. The module ships no HTTP, framework,
/// or configuration-system integration; embedding applications own those.
///
/// # Fixed roles, one writer
///
/// Roles are fixed at startup: `writer`, `reader`, `backup-reader`, or none
/// (standalone). There is no runtime promotion; a role change is a restart.
/// Only one writer may publish per cluster and Store generation: it holds a
/// renewable fencing lease carrying a monotonically increasing token, every
/// frame carries the token, and readers reject frames with a lower token than
/// they have accepted. The same node id restarting mints the next token at
/// once, so restarts never wait for lease expiry. Choosing the writer and
/// failing over belong to the deployment, not the library.
///
/// # Archive-first replication
///
/// A reader must never apply a transaction the writer did not durably record.
/// Each write therefore publishes its prepare chunks to the Aeron Archive,
/// then commits locally, then publishes the commit marker. Store `Binary`
/// bytes travel opaque inside a versioned envelope (identity, nonce,
/// sequence, chunking, CRC32C, commit/abort) — framing only, no second
/// serialization. Readers replay from the Archive and merge onto the live
/// stream; a live commit is applied only once the Archive has recorded it,
/// and a recording that stalls beyond budget fails the reader closed.
///
/// # Durable cursors, checkpoints, reseed
///
/// A reader persists a CRC-protected cursor (Store generation, sequence,
/// Archive position) after each applied barrier; the writer persists a
/// checkpoint binding cluster, generation, epoch, recording, and sequence
/// around every transaction. Startup reconciles both with the Archive and
/// fails closed with `ReseedRequiredException` whenever the boundary is
/// ambiguous, rather than guessing. Replication ships deltas that reference
/// writer-assigned object ids, so a reader cannot start from an empty Store:
/// it needs a seed (a compatible backup, or a copy of the writer's Store with
/// its cursor). Only the writer may create a root; a backup node may do so
/// only for an operator-uploaded Store it then publishes as the starter
/// backup. On-disk and wire formats are versioned and never migrated: an
/// incompatible upgrade is a reseed.
///
/// # Storage placement: local for live data, network only for backups
///
/// Every file on the replication or persistence hot path — the Store, the
/// reader cursor, the writer checkpoint, the Aeron Archive, and the Aeron
/// driver directory — must be on storage exclusive to the node: a local disk
/// or a node-exclusive block device (for example a cloud volume attached to
/// one host). A shared network file system (NFS, SMB, and similar) must not
/// hold them. The reasons:
///
/// - correctness rests on per-write `fsync`, atomic rename plus directory
///   `force`, and file locks, whose semantics and latency on network file
///   systems depend on mount options and server behaviour the library cannot
///   verify;
/// - the Aeron driver maps its buffers into memory and the Archive's
///   recorded-position counter is our durability proof, which assumes a
///   local file system honouring `fileSyncLevel`;
/// - every commit waits on these writes, so network round-trips would
///   become the throughput ceiling.
///
/// A shared network volume is used only for cold, latency-tolerant data:
/// backup archives, published by atomic rename. Consequence: a node's durable
/// state lives and dies with its local storage. Losing a reader's disk means
/// reseeding it. Losing the writer's disk loses its Archive, which forces a
/// new epoch and a reseed of every reader; acknowledged transactions not yet
/// applied by a reader or captured in a backup are lost with it.
///
/// Known exception, scheduled for removal: the writer fencing lease currently
/// lives on the shared backup volume (NFSv4) so that writers on different
/// hosts can fence each other, and it is consulted on every commit. Because
/// the writer is fixed by deployment, it is to be replaced by a node-local
/// writer lock plus a token carried in the Store (`OPUS_REVIEW.md`, A2).
///
/// # Quorum-gated Archive retention
///
/// Deleting Archive history a slow reader still needs destroys data no replay
/// can recover. Readers advertise their durable boundary as CRC-checked
/// watermarks, and the writer deletes only complete segments below the
/// minimum of the complete configured reader quorum: it pauses admission,
/// stops the recording, purges, and extends the same recording at the exact
/// stop position. An incomplete quorum or an active replay preserves history.
/// Watermarks are epoch-bound but carry no fencing token, so they can neither
/// fence nor un-fence a writer; any host on the replication network can
/// report progress. This is part of the deliberately unauthenticated protocol.
///
/// # No authentication, no encryption
///
/// The cluster protocol deliberately has neither node authentication nor
/// transport encryption, and neither may be added. The wire nonce, cluster
/// ids, and CRC32C detect accidental cross-wiring and corruption only; the
/// protocol does not require either feature. Cluster ids are checked inside
/// received frames, cursors, checkpoints, and watermarks, but they cannot
/// stop a subscriber from receiving another cluster's traffic; clusters that
/// share a network use separate channels and stream ids to avoid cross-wiring.
///
/// # Backups
///
/// A backup is one compressed archive (`storage/`, `manifest`, `ready`)
/// moved atomically into the backup volume after export completes, so a
/// half-written backup is never selectable. Extraction validates names,
/// links, uniqueness, sizes, and metadata before installing Store files.
/// Backups are taken by the backup-reader at a resolved replication boundary.
///
/// # Indexes live inside the object graph
///
/// Replication ships the object graph, so anything outside it would diverge
/// across nodes. Lucene data lives in the graph through the embedded
/// GraphDirectory and vectors in the persisted in-graph vector store;
/// external directories and on-disk indexes are rejected. Readers retire
/// stale search views after each applied batch.
///
/// # Graph coordination
///
/// Replication materializes into live objects, so application reads and
/// replicated writes share one fair read/write boundary per Store
/// ([GraphBoundary]): a read never observes a half-applied batch. A write
/// section neither persists nor rolls back by itself; a failure that may have
/// dirtied the graph latches it invalid, and later sections fail closed until
/// the node reloads or reseeds. Closing the node from inside a section is
/// rejected, because close joins workers that need the boundary.
///
/// @since 1.0
module peruncs.cluster
{
    /* Exported API signatures expose Store/Serializer types; re-export only
     * the modules whose types appear directly in exported signatures. */
    requires transitive org.eclipse.store.storage;
    requires transitive org.eclipse.serializer.base;
    requires transitive org.eclipse.store.storage.embedded;
    requires org.eclipse.serializer.persistence;
    requires org.eclipse.serializer.persistence.binary;
    requires org.eclipse.serializer.afs;
    requires org.eclipse.store.afs.nio;
    requires io.aeron.client;
    requires io.aeron.archive;
    requires io.aeron.driver;
    requires org.agrona;
    requires transitive org.eclipse.store.gigamap;
    requires transitive org.eclipse.store.gigamap.lucene;
    requires transitive org.apache.lucene.core;
    // The upstream module name is misspelled; keep the dependency aligned with
    // the published module descriptor.
    requires transitive org.eclipes.store.gigamap.jvector;
    requires transitive jvector;
    exports peruncs.cluster.api;
    exports peruncs.cluster.errors;
}
