/**
 * This module moves Store binary data between nodes over Aeron.
 *
 * <p>It owns packet, transaction, reader, writer, import, and materialization
 * contracts in {@code ...distributed.types}, implemented by the Aeron
 * configuration, restart state, readers, and writers in
 * {@code ...distributed.aeron.*}.</p>
 *
 * <p>The transport keeps Eclipse Serializer/Eclipse Store {@code Binary} bytes
 * opaque behind a versioned envelope. Implementations must preserve transaction
 * boundaries and must release binary resources after the receiving object graph
 * has accepted them.</p>
 *
 * @since 1.0
 */
module peruncs.datagrid.storage.distributed
{
	requires org.eclipse.store.storage.embedded;
	requires org.eclipse.serializer.base;
	requires org.eclipse.serializer.persistence;
	requires org.eclipse.serializer.persistence.binary;
	requires org.agrona;
	requires io.aeron.client;
	requires io.aeron.archive;
	/* Benchmark tests use com.sun.management.ThreadMXBean while production
	 * classes have no runtime dependency on the management implementation. */
	requires static jdk.management;

	exports peruncs.datagrid.storage.distributed.types;
	exports peruncs.datagrid.storage.distributed.aeron.config;
	exports peruncs.datagrid.storage.distributed.aeron.checkpoint;
	exports peruncs.datagrid.storage.distributed.aeron.reader;
	exports peruncs.datagrid.storage.distributed.aeron.writer;
}
