/**
 * This package defines the settings that shape an Aeron storage stream.
 *
 * <p>Configuration values describe endpoints, stream identity, and frame
 * limits. Members that share a stream must use compatible values. The records
 * are immutable after construction so a running reader and writer see one
 * stable configuration.</p>
 *
 * @since 1.0
 */
package peruncs.datagrid.storage.distributed.aeron.config;
