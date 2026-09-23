/// Assembles and runs one cluster node end to end.
///
/// The assembly wires every node service, the lifecycle starts and stops
/// them in dependency order, and the fixed-topology node managers expose
/// control views to the embedding application. The maintenance scheduler
/// runs periodic storage checks, the settings source supplies node-wide
/// configuration, and fatal error handling lives here. Node services move
/// from construction to running, draining, and closed states; an assembly
/// belongs to one node and must not be reused after that node is closed.
///
/// @since 1.0
package peruncs.datagrid.cluster.node;
