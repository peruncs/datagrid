/**
 * This package defines the persistence replication contract.
 *
 * <p>These APIs carry Eclipse Store binary data and lifecycle callbacks without
 * depending on the Aeron client implementation. Writers
 * publish transaction boundaries; readers apply them in order; importers own
 * binary buffers until materialization completes.</p>
 *
 * <p>Implementations must not expose a mutable transport buffer after the
 * callback that consumes it returns.</p>
 *
 * @since 1.0
 */
package peruncs.datagrid.storage.distributed.types;
