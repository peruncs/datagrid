/// Embedded index facade, validation, and maintenance for the replicated Store.
///
/// Indexes live inside the object graph, so they replicate with it: Lucene
/// data through the embedded GraphDirectory and vector data in the persisted
/// in-graph vector store. The types here validate and maintain that policy
/// and pin the upstream storage layout the node relies on.
///
/// @since 1.0
package peruncs.cluster.storage.index;
