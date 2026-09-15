/**
 * This package runs one cluster node.
 *
 * <p>It owns the foundation that assembles node services, the fixed-topology
 * node managers, the housekeeper, the configuration contract, and fatal error
 * handling. Node services move from construction to running, draining, and
 * closed states; a foundation belongs to one node and must not be reused after
 * that node is closed.</p>
 *
 * @since 1.0
 */
package peruncs.datagrid.cluster.nodelibrary.node;
