/// Data Grid cluster node: lifecycle, storage, and replication.
///
/// This package holds no types itself. The `node` package assembles and runs
/// one cluster node — assembly and lifecycle, fixed-topology managers,
/// maintenance scheduling, settings, and fatal errors. The `storage` package
/// carries Store binary movement and persisted index integration between
/// nodes. The exported `api` and `errors` packages are the application
/// boundary.
///
/// @since 1.0
package peruncs.datagrid.cluster;
