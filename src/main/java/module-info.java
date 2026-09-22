/// Data Grid node with Aeron replication.
///
/// Applications create the node services, start them in dependency order,
/// and close them in reverse order. The node lifecycle, storage
/// adaptation, backup, and replication live in `...cluster.node`,
/// `...cluster.node.store`, `...cluster.node.backup`, and
/// `...cluster.node.replication`, implemented by the Aeron transport in
/// `...cluster.node.aeron`. Control operations are exposed programmatically
/// through the node managers; the module ships no HTTP surface — any HTTP,
/// MCP, or UI boundary belongs to the embedding application.
/// Store binary movement lives in
/// `...cluster.storage.types`, carried by
/// `...cluster.storage.aeron.*`, which also carries the embedded
/// Lucene/JVector index policy. The exported `cluster.api` package contains
/// the contracts embedding applications use; internal failures are translated
/// at that boundary.
/// Aeron is the only transport. Each provider owns its embedded MediaDriver
/// and Archive lifecycle, closed by the node lifecycle after maintenance stops.
///
/// # Trusted network boundary
///
/// Replication data and reader watermarks are not authenticated. Deploy them
/// only on an isolated, trusted network such as a private VPN. Aeron Archive
/// control authentication is a separate boundary and does not authenticate
/// replication frames.
///
/// # Archive-first replication
///
/// Readers must never apply a transaction the writer did not durably
/// record. Every write therefore follows Archive prepare chunks, then the
/// local Store enqueue, then the Archive commit. The transport keeps
/// Eclipse Serializer `Binary` bytes opaque behind a versioned envelope
/// carrying only cluster identity, a shared accidental-cross-wiring nonce,
/// sequence, chunking, CRC32C, and
/// commit/abort markers — framing only, no second object-graph encoding.
/// Readers replay from the Archive, join the live stream, and reconnect
/// from a durable cursor.
///
/// # Fixed roles with a fencing lease
///
/// Roles are fixed at configuration: `writer`, `reader`, or
/// `backup-reader`, or `none` for an unreplicated node. A reader owns a
/// persistent subscription with no writer publication path, so promotion is
/// rejected outright instead of producing a distributor that cannot
/// replicate. The single-writer invariant itself is enforced, not merely
/// configured: the writer holds a renewable fencing lease in the shared
/// backup volume carrying a monotonically increasing token. A different
/// writer for the same cluster/generation fails acquisition while the
/// holder's heartbeat is fresh; a stale lease is stolen with a greater
/// token, the deposed writer loses write admission, and readers fail closed
/// on any frame carrying a lower token. The same writer — the same stable
/// node id — restarting after a clean stop or a crash mints the next token
/// immediately instead of waiting out its own heartbeat; node identity is
/// the fencing principal, so restarts never black out on fencing. Every envelope, checkpoint, and cursor
/// carries the token, so interleaved history from two writers is rejected
/// instead of consumed. Reader watermarks are the deliberate exception: they
/// carry no fencing token (see the retention section). Automated failover
/// beyond lease stealing needs consensus, which stays a deployment
/// responsibility; the lease directory must be shared by all writers of one
/// cluster.
///
/// # Durable cursors and checkpoints
///
/// Every applied commit advances a durable `ReplicationCursor` of
/// transport, Store generation, logical sequence, and provider position.
/// The neutral cursor lives in the storage contract package
/// (`storage.types`), so the node layer persists it through
/// `ReplicationCursorStore` without importing the transport; the Aeron
/// provider encodes its recording identity inside the cursor's opaque
/// provider-position bytes. Writers additionally persist a checkpoint
/// binding cluster, Store generation, epoch, recording, and sequence.
/// Startup reconciles the two so a cursor
/// from another Store generation never resumes an unrelated recording.
/// Restarts stay routine instead of reseeds as long as Archive and cursor
/// survive together; cursor and checkpoint writes are CRC-protected and
/// atomic, and torn files are rejected. The checkpoint and cursor files are
/// checksum-protected, not authenticated: their trust boundary is the
/// filesystem, so the metadata directory must stay owner-only and local.
///
/// # Seeding and reseed
///
/// Replication ships deltas that reference object ids the writer created, so
/// a reader owns no authoritative Store image: a reader started against an
/// empty directory cannot reproduce pre-existing state and fails with
/// `ReseedRequiredException` instead of inventing a root. The writer is the
/// only role that may manufacture a fresh root; the backup node may do so
/// only for a user-uploaded Store it then publishes as the starter backup.
///
/// # Upgrade reseeds
///
/// The wire and restart formats are versioned and fail closed on mismatch:
/// envelopes require version 5 with a fixed 84-byte header plus the chunk
/// payload, and checkpoints and
/// cursors require version 2. A node upgraded from an older format must be
/// reseeded from a compatible backup or Store image with its cursor; old
/// files are rejected, never migrated in place.
///
/// # Quorum-gated retention
///
/// Deleting Archive segments a slow reader still needs destroys data no
/// replay can recover. Readers therefore advertise their durable boundary
/// as CRC-checked watermarks, and the writer deletes history only through
/// the complete configured reader quorum — pausing admission, stopping the
/// recording, purging only complete segments, and extending the same
/// recording at its exact stop position. With
/// an incomplete quorum, or during an active replay, history is preserved.
/// The quorum, not any single request, authorizes deletion.
///
/// The retention quorum is epoch-bound and excluded from token fencing:
/// watermarks report reader progress under one epoch but never carry
/// the writer fencing token, so a watermark can neither fence nor un-fence
/// a writer. Residual risk: token fencing does not cover retention. A
/// watermark replayed across epochs is rejected by its epoch binding, but
/// operators must still treat the isolated replication network as
/// the retention trust boundary: any host on it can report reader progress.
///
/// Multiple clusters may share a routed network only when their Aeron traffic
/// namespaces are separate. Give each cluster distinct live, replay, and
/// watermark channel endpoints (or otherwise distinct channel destinations)
/// and distinct stream IDs. Also assign a unique cluster ID to every cluster;
/// an explicitly configured wire nonce must match every participant.
/// The cluster ID is checked inside received envelopes, cursors, checkpoints,
/// and watermarks; it does not prevent a subscriber from receiving another
/// cluster's frame. Accidentally sharing a channel and stream therefore causes
/// a mismatch and fail-closed subscriber, while separate channels and streams
/// prevent cross-talk. CRC32C, cluster IDs, and the wire nonce do not
/// authenticate publishers;
/// firewall, VPN, or network-policy rules remain the live-channel trust
/// boundary.
///
/// # Filesystem backups
///
/// Backups are compressed ZIP archives on the configured filesystem volume.
/// A generated archive contains `storage/`, `manifest`, and `ready`, and is
/// atomically moved into the volume only after export completes. Archive
/// extraction validates names, links, entry uniqueness, size limits, and the
/// required metadata before atomically installing Store files. User-uploaded
/// storage uses `user-uploaded-storage.zip` as its local restore input.
///
/// # Indexes live inside the object graph
///
/// Replication ships the object graph, so anything kept outside it would
/// diverge across nodes. Lucene data must live in the graph through the
/// embedded GraphDirectory, and vector data in the persisted in-graph
/// vector store; external directories and on-disk indexes are rejected at
/// the registration boundary.
///
/// # Boundary, control, and entity
///
/// The node ships no HTTP server and no HTTP types. The embedding application
/// owns the entire boundary — HTTP and OpenAPI routes, MCP tools, a web UI,
/// Prometheus rendering, authentication, and authorization — and drives the
/// node through the control views (`StorageNodeControl`, `BackupNodeControl`)
/// borrowed from `ClusterFoundation`; the Store object graph beneath them is
/// the entity layer. The foundation owns both managers and closes them
/// exactly once, and both closes are idempotent. Roles stay fixed at startup
/// as described above, so there is deliberately no reader-to-distributor
/// promotion: a role change is a restart with a new role, never a runtime
/// transition.
///
/// @since 1.0
module peruncs.datagrid.cluster
{
    requires org.eclipse.store.storage.embedded;
    requires org.eclipse.serializer.base;
    requires org.eclipse.serializer.persistence;
    requires org.eclipse.serializer.persistence.binary;
    requires org.eclipse.store.storage;
    requires org.eclipse.serializer.afs;
    requires org.eclipse.store.afs.nio;
    requires io.aeron.client;
    requires io.aeron.archive;
    requires io.aeron.driver;
    requires org.agrona;
    requires org.eclipse.store.gigamap;
    requires org.eclipse.store.gigamap.lucene;
    // The upstream module name is misspelled; keep the dependency aligned with
    // the published module descriptor.
    requires org.eclipes.store.gigamap.jvector;
    requires jvector;
    exports peruncs.datagrid.cluster.api;
    exports peruncs.datagrid.cluster.errors;
}
