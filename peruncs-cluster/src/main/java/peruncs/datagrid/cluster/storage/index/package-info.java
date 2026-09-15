/// Keeps clustered text and vector search inside the Store object graph.
///
/// A replicated Store transaction is the only source of truth. An index
/// directory outside that transaction can advance independently, so it cannot
/// be made correct by copying or naming the directory. This package rejects
/// that configuration and provides the two supported registration paths.
///
/// Lucene uses an embedded GraphDirectory with manual commit at the
/// `GigaMap.store()` boundary. JVector uses its persisted vector store;
/// its transient search graph is rebuilt locally by each reader.
///
/// @since 1.0
package peruncs.datagrid.cluster.storage.index;
