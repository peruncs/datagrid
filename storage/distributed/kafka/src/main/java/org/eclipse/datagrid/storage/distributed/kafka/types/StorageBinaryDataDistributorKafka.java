package org.eclipse.datagrid.storage.distributed.kafka.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Kafka
 * %%
 * Copyright (C) 2025 - 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.datagrid.storage.distributed.types.Crc32c;
import org.eclipse.datagrid.storage.distributed.types.ReplicationRetry;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataChunker;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataDistributor;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;
import org.eclipse.serializer.memory.XMemory;
import org.eclipse.serializer.persistence.binary.types.Binary;
import org.eclipse.serializer.persistence.binary.types.ChunksWrapper;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.eclipse.serializer.chars.XChars.notEmpty;
import static org.eclipse.serializer.util.X.notNull;

/** Publishes neutral Store packets to a single Kafka partition. */
public interface StorageBinaryDataDistributorKafka extends StorageBinaryDataDistributor
{
	/** Stable key that keeps a stream on one Kafka partition. */
	String PARTITION_KEY = "eclipse-datagrid-replication";

	/** Creates a synchronous distributor.
	 * @param kafkaProperties Kafka producer properties
	 * @param topicName single-partition replication topic
	 * @return synchronous distributor
	 */
	static StorageBinaryDataDistributorKafka Sync(
		final Properties kafkaProperties, final String topicName)
	{
		return new Sync(notNull(kafkaProperties), notEmpty(topicName));
	}

	/** Creates an asynchronous distributor.
	 * @param kafkaProperties Kafka producer properties
	 * @param topicName single-partition replication topic
	 * @return asynchronous distributor
	 */
	static StorageBinaryDataDistributorKafka Async(
		final Properties kafkaProperties, final String topicName)
	{
		return new Async(notNull(kafkaProperties), notEmpty(topicName));
	}

	/** Common producer, sequencing, and lifecycle implementation. */
	abstract class Abstract implements StorageBinaryDataDistributorKafka
	{
		private static final System.Logger LOG =
			System.getLogger(StorageBinaryDataDistributorKafka.class.getName());
		private final Properties kafkaProperties;
		private final String topicName;
		private final Object sendLock = new Object();
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
		private final AtomicLong messageIndex = new AtomicLong(-1L);
		private final AtomicLong droppedAfterFailure = new AtomicLong();
		private final AtomicBoolean ignoreDistribution = new AtomicBoolean();
		private KafkaProducer<String, byte[]> kafkaProducer;
		private int activeActions;
		private boolean disposed;
		private boolean disposing;

		Abstract(final Properties kafkaProperties, final String topicName)
		{
			this.kafkaProperties = new Properties();
			this.kafkaProperties.putAll(kafkaProperties);
			this.topicName = topicName;
		}

		/** Executes on the caller or submits to the asynchronous worker.
		 * @param action distribution action
		 */
		protected abstract void execute(Runnable action);

		/** Returns whether a previously failed action may be dropped from a queue.
		 * @return {@code true} for an asynchronous queue-backed distributor
		 */
		protected abstract boolean asynchronous();

		private void distribute(final MessageType type, final Binary data, final Runnable cleanup)
		{
			Objects.requireNonNull(data, "data");
			final List<StorageBinaryDataChunker.Chunk> chunks;
			synchronized (this)
			{
				this.checkOpen();
				if (this.ignoreDistribution.get())
				{
					if (cleanup != null) cleanup.run();
					return;
				}
				this.activeActions++;
			}
			try
			{
				/* Packetization can copy a large Store transaction. Do not hold the
				 * lifecycle monitor while walking caller-owned buffers. */
				chunks = StorageBinaryDataChunker.chunk(data, StorageBinaryDistributedKafka.maxPacketSize());
				if (chunks.isEmpty()) throw new IllegalArgumentException("replication data must not be empty");
			}
			catch (final RuntimeException | Error failure)
			{
				this.actionFinished();
				throw failure;
			}
			finally
			{
				if (cleanup != null) cleanup.run();
			}

			try
			{
				this.execute(() ->
				{
					try
					{
						synchronized (this.sendLock)
						{
							this.tryExecute(type, chunks);
						}
					}
					finally
					{
						this.actionFinished();
					}
				});
			}
			catch (final RuntimeException | Error submissionFailure)
			{
				this.actionFinished();
				throw submissionFailure;
			}
		}

		private void checkOpen()
		{
			if (this.disposed || this.disposing)
				throw new IllegalStateException(this.disposed ? "Kafka distributor is disposed" : "Kafka distributor is stopping");
			final RuntimeException terminalFailure = this.failure.get();
			if (terminalFailure != null)
				throw new IllegalStateException("Kafka distributor has failed", terminalFailure);
		}

		private void actionFinished()
		{
			synchronized (this)
			{
				this.activeActions--;
				this.notifyAll();
			}
		}

		private void tryExecute(final MessageType type, final List<StorageBinaryDataChunker.Chunk> chunks)
		{
			final RuntimeException terminalFailure = this.failure.get();
			if (terminalFailure != null)
			{
				if (!this.asynchronous())
					throw new IllegalStateException("Kafka distributor has failed", terminalFailure);
				final long dropped = this.droppedAfterFailure.incrementAndGet();
				LOG.log(System.Logger.Level.WARNING, "Dropping queued Kafka replication action " + dropped);
				return;
			}
			try
			{
				this.executeDistribution(type, chunks);
			}
			catch (final RuntimeException | Error sendFailure)
			{
				this.recordFailure(sendFailure);
				throw sendFailure;
			}
		}

		private KafkaProducer<String, byte[]> ensureProducer()
		{
			if (this.kafkaProducer != null) return this.kafkaProducer;
			final Properties properties = new Properties();
			properties.putAll(this.kafkaProperties);
			properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
			properties.putIfAbsent(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name);
			properties.putIfAbsent(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
			properties.putIfAbsent(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "35000");
			KafkaProducer<String, byte[]> created = null;
			try
			{
				created = new KafkaProducer<>(properties);
				final int partitions = created.partitionsFor(this.topicName).size();
				if (partitions != 1)
					throw new IllegalStateException("Kafka replication topic must have exactly one partition; found " + partitions);
				return this.kafkaProducer = created;
			}
			catch (final RuntimeException | Error failure)
			{
				if (created != null)
				{
					try { created.close(); }
					catch (final RuntimeException | Error closeFailure) { failure.addSuppressed(closeFailure); }
				}
				throw failure;
			}
		}

		private void executeDistribution(final MessageType type, final List<StorageBinaryDataChunker.Chunk> chunks)
		{
			final long index = this.messageIndex.incrementAndGet();
			final var checksum = Crc32c.accumulator();
			for (final StorageBinaryDataChunker.Chunk chunk : chunks)
			{
				final byte[] bytes = chunk.bytes();
				checksum.update(bytes, 0, bytes.length);
			}
			final int crc = (int)checksum.getValue();
			final KafkaProducer<String, byte[]> producer = this.ensureProducer();
			for (final StorageBinaryDataChunker.Chunk chunk : chunks)
			{
				final ProducerRecord<String, byte[]> record =
					new ProducerRecord<>(this.topicName, 0, PARTITION_KEY, chunk.bytes());
				StorageBinaryDistributedKafka.addPacketHeaders(
					record.headers(), type, chunk.messageLength(), chunk.index(), chunk.count(), index, crc);
				try
				{
					producer.send(record).get(30L, TimeUnit.SECONDS);
				}
				catch (final InterruptedException interrupted)
				{
					Thread.currentThread().interrupt();
					throw new IllegalStateException("Interrupted while sending Kafka replication data", interrupted);
				}
				catch (final TimeoutException timeout)
				{
					throw new IllegalStateException("Timed out waiting for Kafka replication acknowledgement", timeout);
				}
				catch (final java.util.concurrent.ExecutionException sendFailure)
				{
					final Throwable cause = sendFailure.getCause();
					if (cause instanceof RuntimeException runtime) throw runtime;
					if (cause instanceof Error error) throw error;
					throw new IllegalStateException("Kafka replication send failed", cause);
				}
			}
		}

		/** Records the first terminal failure.
		 * @param failure terminal failure
		 */
		protected final void recordFailure(final Throwable failure)
		{
			this.failure.compareAndSet(null, failure instanceof RuntimeException runtime
				? runtime : new IllegalStateException("Kafka distributor failed", failure));
		}

		@Override
		public void distributeData(final Binary data)
		{
			this.distribute(MessageType.DATA, data, null);
		}

		@Override
		public void distributeTypeDictionary(final String value)
		{
			final ByteBuffer buffer = XMemory.toDirectByteBuffer(StorageBinaryDistributedKafka.serialize(value));
			final Runnable cleanup = releaseOnce(buffer);
			try
			{
				this.distribute(MessageType.TYPE_DICTIONARY, ChunksWrapper.New(buffer), cleanup);
			}
			catch (final RuntimeException | Error failure)
			{
				cleanup.run();
				throw failure;
			}
		}

		private static Runnable releaseOnce(final ByteBuffer buffer)
		{
			final AtomicBoolean released = new AtomicBoolean();
			return () ->
			{
				if (released.compareAndSet(false, true)) XMemory.deallocateDirectByteBuffer(buffer);
			};
		}

		/** Sets the next logical message index while the distributor is quiescent.
		 * @param index next logical message index
		 */
		public synchronized void messageIndex(final long index)
		{
			if (index < -1L || index == Long.MAX_VALUE)
				throw new IllegalArgumentException("message index must be in [-1, Long.MAX_VALUE)");
			if (this.activeActions != 0 || this.disposed || this.disposing)
				throw new IllegalStateException("Kafka message index can only change while quiescent");
			this.messageIndex.set(index);
		}

		/** Returns the latest logical message index.
		 * @return latest logical message index
		 */
		public long messageIndex() { return this.messageIndex.get(); }

		/** Returns the first terminal failure, or {@code null}.
		 * @return first terminal failure, or {@code null}
		 */
		public RuntimeException failure() { return this.failure.get(); }

		/** Returns whether new data is currently ignored. 
		 * @return {@code true} when distribution is ignored
		 */
		public boolean ignoreDistribution() { return this.ignoreDistribution.get(); }

		/** Enables or disables distribution without changing the producer state.
		 * @param ignore whether distribution should be ignored
		 */
		public void ignoreDistribution(final boolean ignore) { this.ignoreDistribution.set(ignore); }

		/** Marks the distributor stopping and closes the producer after active sends finish. */
		@Override
		public void dispose()
		{
			this.beginDisposal();
			this.closeAfterActions();
		}

		/** Marks this distributor as stopping before waiting for queued actions. */
		protected final synchronized void beginDisposal()
		{
			if (this.disposed) return;
			if (this.disposing) throw new IllegalStateException("Kafka distributor disposal is already in progress");
			this.disposing = true;
		}

		/** Reopens this distributor after a failed disposal attempt. */
		protected final synchronized void cancelDisposal()
		{
			this.disposing = false;
			this.notifyAll();
		}

		/** Closes the producer after all active actions have completed. */
		protected final void closeAfterActions()
		{
			try
			{
				final long deadline = ReplicationRetry.deadlineNanos(TimeUnit.SECONDS.toNanos(30L));
				synchronized (this)
				{
					while (this.activeActions != 0)
					{
						final long remaining = ReplicationRetry.remainingNanos(deadline);
						if (remaining <= 0L)
						{
							throw new IllegalStateException(
								"Kafka distributor has in-flight sends after disposal timeout");
						}
						TimeUnit.NANOSECONDS.timedWait(this, remaining);
					}
				}
				synchronized (this.sendLock)
				{
					if (this.kafkaProducer != null)
						this.kafkaProducer.close(java.time.Duration.ofSeconds(5L));
				}
				synchronized (this)
				{
					this.disposed = true;
					this.disposing = false;
				}
			}
			catch (final InterruptedException interrupted)
			{
				Thread.currentThread().interrupt();
				this.cancelDisposal();
				throw new IllegalStateException("Interrupted while stopping Kafka distributor", interrupted);
			}
			catch (final RuntimeException | Error failure)
			{
				this.cancelDisposal();
				throw failure;
			}
		}
	}

	/** Sends each packet synchronously on the caller thread. */
	final class Sync extends Abstract
	{
		private Sync(final Properties properties, final String topicName) { super(properties, topicName); }
		@Override
		protected void execute(final Runnable action) { action.run(); }
		@Override
		protected boolean asynchronous() { return false; }
	}

	/** Queues packets on one worker thread. */
	final class Async extends Abstract
	{
		private static final int MAX_QUEUED_ACTIONS = 1;
		private final ThreadPoolExecutor executor;

		private Async(final Properties properties, final String topicName)
		{
			super(properties, topicName);
			this.executor = new ThreadPoolExecutor(
				1, 1, 0L, TimeUnit.MILLISECONDS,
				new java.util.concurrent.ArrayBlockingQueue<>(MAX_QUEUED_ACTIONS),
				runnable ->
				{
					final Thread thread = new Thread(runnable, "datagrid-legacy-kafka-writer");
					thread.setDaemon(true);
					return thread;
				},
				new ThreadPoolExecutor.AbortPolicy());
		}

		@Override
		protected void execute(final Runnable action)
		{
			final Runnable guarded = () ->
			{
				try { action.run(); }
				catch (final RuntimeException failure) { this.recordFailure(failure); }
				catch (final Error failure) { this.recordFailure(failure); throw failure; }
			};
			try
			{
				this.executor.execute(guarded);
			}
			catch (final java.util.concurrent.RejectedExecutionException rejected)
			{
				if (this.executor.isShutdown()) throw rejected;
				try
				{
					if (!this.executor.getQueue().offer(guarded, 30L, TimeUnit.SECONDS))
						throw new java.util.concurrent.RejectedExecutionException(
							"Kafka distributor queue remained full", rejected);
				}
				catch (final InterruptedException interrupted)
				{
					Thread.currentThread().interrupt();
					throw new IllegalStateException(
						"Interrupted while waiting for Kafka distributor capacity", interrupted);
				}
			}
		}

		@Override
		protected boolean asynchronous() { return true; }

		@Override
		public void dispose()
		{
			this.beginDisposal();
			this.executor.shutdown();
			try
			{
				if (!this.executor.awaitTermination(30L, TimeUnit.SECONDS))
				{
					this.executor.shutdownNow();
					if (!this.executor.awaitTermination(5L, TimeUnit.SECONDS))
					{
						this.cancelDisposal();
						throw new IllegalStateException("Kafka distributor worker did not stop");
					}
				}
			}
			catch (final InterruptedException interrupted)
			{
				this.executor.shutdownNow();
				Thread.currentThread().interrupt();
				this.cancelDisposal();
				throw new IllegalStateException("Interrupted while stopping Kafka distributor", interrupted);
			}
			this.closeAfterActions();
		}
	}
}
