/**
 * This module runs a Data Grid node with Aeron replication.
 *
 * <p>It defines the node lifecycle in {@code ...cluster.nodelibrary.node},
 * storage adaptation in {@code ...cluster.nodelibrary.store}, backup in
 * {@code ...cluster.nodelibrary.backup}, replication in
 * {@code ...cluster.nodelibrary.replication}, and the HTTP surface in
 * {@code ...cluster.nodelibrary.http}, implemented by the Aeron transport in
 * {@code ...cluster.nodelibrary.aeron}. Store binary movement lives in
 * {@code ...cluster.storage.types}, carried by
 * {@code ...cluster.storage.aeron.*}, with the embedded Lucene/JVector
 * index policy in {@code ...cluster.storage.index}. Aeron is the only
 * transport.</p>
 *
 * <p>Applications create the node services, start them in dependency order,
 * and close them in reverse order. The exported packages contain the public
 * contracts and errors used at those boundaries.</p>
 *
 * @since 1.0
 */
module peruncs.datagrid.cluster
{
    requires org.eclipse.store.storage.embedded;
    requires org.eclipse.serializer.base;
    requires org.eclipse.serializer.persistence;
    requires org.eclipse.serializer.persistence.binary;
    requires org.eclipse.store.storage;
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
    requires org.eclipse.store.gigamap;
    requires org.eclipse.store.gigamap.lucene;
    // The upstream module name is misspelled; keep the dependency aligned with
    // the published module descriptor.
    requires org.eclipes.store.gigamap.jvector;
    requires org.apache.lucene.core;
    /* Benchmark tests use com.sun.management.ThreadMXBean while production
     * classes have no runtime dependency on the management implementation. */
    requires static jdk.management;

    exports peruncs.datagrid.cluster.nodelibrary.exceptions;
    exports peruncs.datagrid.cluster.nodelibrary.backup;
    exports peruncs.datagrid.cluster.nodelibrary.replication;
    exports peruncs.datagrid.cluster.nodelibrary.node;
    exports peruncs.datagrid.cluster.nodelibrary.store;
    exports peruncs.datagrid.cluster.nodelibrary.http;
    exports peruncs.datagrid.cluster.nodelibrary.aeron;
    exports peruncs.datagrid.cluster.storage.types;
    exports peruncs.datagrid.cluster.storage.aeron.config;
    exports peruncs.datagrid.cluster.storage.aeron.checkpoint;
    exports peruncs.datagrid.cluster.storage.aeron.reader;
    exports peruncs.datagrid.cluster.storage.aeron.writer;
    exports peruncs.datagrid.cluster.storage.index;
}
