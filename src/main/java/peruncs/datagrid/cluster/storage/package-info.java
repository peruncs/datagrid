/// Store transport contracts and Aeron replication machinery.
///
/// This package holds no types itself. The `types` package defines the
/// neutral binary contracts — distributors, clients, mergers, cursors, and
/// the embedded index policy — so transport providers never duplicate the
/// API layer. The `aeron` package implements those contracts over Aeron
/// Archive recordings with fencing, checkpoints, and quorum-gated retention.
///
/// @since 1.0
package peruncs.datagrid.cluster.storage;
