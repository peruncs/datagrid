/**
 * This module runs a Data Grid node with Aeron replication.
 *
 * <p>It defines the node lifecycle in {@code ...nodelibrary.node}, storage
 * adaptation in {@code ...nodelibrary.store}, backup in
 * {@code ...nodelibrary.backup}, replication in
 * {@code ...nodelibrary.replication}, and the HTTP surface in
 * {@code ...nodelibrary.http}, implemented by the Aeron transport in
 * {@code ...nodelibrary.aeron}. Aeron is the only transport.</p>
 *
 * <p>Applications create the node services, start them in dependency order,
 * and close them in reverse order. The exported packages contain the public
 * contracts and errors used at those boundaries.</p>
 *
 * @since 1.0
 */
module peruncs.datagrid.cluster.nodelibrary
{
    requires peruncs.datagrid.storage.distributed;
    requires org.eclipse.serializer.persistence;
    requires org.eclipse.serializer.persistence.binary;
    requires org.eclipse.store.storage;
    requires org.eclipse.store.storage.embedded;
    requires org.eclipse.serializer.afs;
    requires org.eclipse.store.afs.nio;
    requires com.fasterxml.jackson.core;
    requires com.fasterxml.jackson.databind;
    requires java.net.http;
    requires org.apache.commons.compress;
    requires io.aeron.client;
    requires io.aeron.archive;
    requires io.aeron.driver;
    requires org.agrona;

    exports peruncs.datagrid.cluster.nodelibrary.exceptions;
    exports peruncs.datagrid.cluster.nodelibrary.backup;
    exports peruncs.datagrid.cluster.nodelibrary.replication;
    exports peruncs.datagrid.cluster.nodelibrary.node;
    exports peruncs.datagrid.cluster.nodelibrary.store;
    exports peruncs.datagrid.cluster.nodelibrary.http;
    exports peruncs.datagrid.cluster.nodelibrary.aeron;
}
