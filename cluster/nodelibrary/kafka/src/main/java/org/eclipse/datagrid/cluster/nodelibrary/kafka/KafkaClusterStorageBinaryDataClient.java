package org.eclipse.datagrid.cluster.nodelibrary.kafka;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
 * %%
 * Copyright (C) 2025 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.datagrid.cluster.nodelibrary.types.*;
import org.eclipse.datagrid.storage.distributed.types.Crc32c;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataMessage.MessageType;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacket;
import org.eclipse.serializer.collections.EqHashTable;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.common.IsolationLevel.READ_COMMITTED;
import static org.eclipse.serializer.util.X.notNull;

/**
 * This client reads ordered storage packets from Kafka and feeds the node
 * merger.
 *
 * <p>It buffers incomplete messages, applies complete messages in order, and
 * commits the consumer offset only after the acceptor and follow-up listener
 * have finished. Disposal stops the polling thread and closes its owned
 * resources.</p>
 */
public final class KafkaClusterStorageBinaryDataClient implements ClusterStorageBinaryDataClient
{
	/** Creates a Kafka storage-data client.
	 *
	 * @param packetAcceptor packet destination
	 * @param topicName Kafka topic
	 * @param groupId consumer group id
	 * @param offsetChangedListener listener invoked after a message is applied
	 * @param startingCursor starting replication cursor
	 * @param kafkaPropertiesProvider Kafka properties provider
	 * @param doCommitOffset whether Kafka offsets are committed after successful application
	 * @return Kafka storage-data client
	 */
	public static ClusterStorageBinaryDataClient New(
        final ClusterStorageBinaryDataPacketAcceptor packetAcceptor,
        final String topicName,
        final String groupId,
        final AfterDataMessageConsumedListener offsetChangedListener,
        final ReplicationCursor startingCursor,
        final KafkaPropertiesProvider kafkaPropertiesProvider,
        final boolean doCommitOffset
    )
    {
        return new KafkaClusterStorageBinaryDataClient(
            notNull(packetAcceptor),
            notNull(topicName),
            notNull(groupId),
            notNull(offsetChangedListener),
            notNull(startingCursor),
            notNull(kafkaPropertiesProvider),
            doCommitOffset
        );
    }

    private static final System.Logger LOG =
		System.getLogger(KafkaClusterStorageBinaryDataClient.class.getName());
	private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5L);
	private static final Duration OFFSET_LOOKUP_TIMEOUT = Duration.ofSeconds(30L);
	private static final int MAX_MESSAGE_BYTES = StorageBinaryDataMessage.MAX_MESSAGE_LENGTH;
	private static final int MAX_PACKET_COUNT =
		(MAX_MESSAGE_BYTES + KafkaHeaderCodec.maxPacketSize() - 1) /
			KafkaHeaderCodec.maxPacketSize();
	private static final int MAX_CACHED_PACKETS = StorageBinaryDataMessage.MAX_PACKET_COUNT;
	private static final long INCOMPLETE_MESSAGE_TIMEOUT_NANOS = Duration.ofMinutes(5L).toNanos();

        /**
         * List of packets that have been polled but not yet consumed as they are still
         * missing some packets to complete the set
         */
    private final Queue<CachedPacket> cachedPackets = new LinkedList<>();

    private final ClusterStorageBinaryDataPacketAcceptor packetAcceptor;
        private final String topicName;
        private final String groupId;
        private final AfterDataMessageConsumedListener offsetChangedListener;
        private final KafkaPropertiesProvider kafkaPropertiesProvider;
        private final boolean doCommitOffset;

		private final AtomicReference<ReplicationCursor> cursor;
	private long cachedSequence;
	/* Exact broker offset of the last packet included in a materialized message.
	 * Subtracting queue size from consumer.position() is incorrect when polls
	 * contain partial messages or offsets have gaps. */
	private long lastAppliedOffset = -1L;
	private long incompleteMessageSinceNanos = -1L;
	private int discardedPackets;
        private final AtomicBoolean stopAtLatestMessage = new AtomicBoolean();
        private final AtomicBoolean requestStop = new AtomicBoolean();
        private final AtomicBoolean running = new AtomicBoolean();

	private volatile Thread runner;
	private volatile KafkaConsumer<String, byte[]> consumer;
	private volatile boolean disposed;
	private volatile boolean disposeRequested;
	private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
	private boolean packetAcceptorDisposed;
	private boolean offsetListenerClosed;

    private KafkaClusterStorageBinaryDataClient(
            final ClusterStorageBinaryDataPacketAcceptor packetAcceptor,
            final String topicName,
            final String groupId,
            final AfterDataMessageConsumedListener offsetChangedListener,
            final ReplicationCursor startingCursor,
            final KafkaPropertiesProvider kafkaPropertiesProvider,
            final boolean doCommitOffset
        )
        {
            this.packetAcceptor = packetAcceptor;
            this.topicName = topicName;
            this.groupId = groupId;
            this.offsetChangedListener = offsetChangedListener;
            this.cursor = new AtomicReference<>(startingCursor);
			this.cachedSequence = startingCursor.logicalSequence();
            this.kafkaPropertiesProvider = kafkaPropertiesProvider;
            this.doCommitOffset = doCommitOffset;
    }

        @Override
        public boolean isRunning()
        {
            return this.running.get();
        }

        @Override
        public ReplicationCursor cursor()
        {
            return this.cursor.get();
        }

        @Override
	public synchronized void start()
	{
		if (this.disposed || this.disposeRequested) throw new IllegalStateException("Kafka data client is disposed or stopping");
		if (this.failure.get() != null) throw new IllegalStateException("Kafka data client has failed", this.failure.get());
		final Thread existing = this.runner;
		if (this.running.get()) return;
		if (existing != null && existing.isAlive())
		{
			throw new IllegalStateException("Kafka data client is still stopping");
		}
			LOG.log(System.Logger.Level.INFO,
				"Starting Kafka data client at sequence " + this.cursor.get().logicalSequence());
		this.running.set(true);
			try
			{
				this.runner = new Thread(this::tryRun, "datagrid-kafka-cluster-reader");
				this.runner.setDaemon(true);
				this.runner.start();
			}
			catch (final RuntimeException | Error failure)
			{
				this.running.set(false);
				this.runner = null;
				throw failure;
			}
        }

        private void tryRun()
        {
            try
            {
				this.run();
                this.running.set(false);
            }
		catch (final Throwable t)
		{
			this.running.set(false);
			if (!this.disposed && !this.requestStop.get())
			{
				this.failure.compareAndSet(null, t instanceof RuntimeException runtime
					? runtime
					: new IllegalStateException("Kafka data client stopped unexpectedly", t));
				LOG.log(System.Logger.Level.ERROR, "Kafka data client stopped unexpectedly", t);
				if (t instanceof Error error) throw error;
			}
		}
	}

	@Override
	public RuntimeException failure()
	{
		return this.failure.get();
	}

		private void run()
        {
            final var properties = this.kafkaPropertiesProvider.provide();
            properties.setProperty(GROUP_ID_CONFIG, this.groupId);
            properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
			properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.setProperty(ISOLATION_LEVEL_CONFIG, READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
            properties.setProperty(ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");

	try (final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties))
	{
		this.consumer = consumer;
		final var partitions = consumer.partitionsFor(this.topicName, Duration.ofSeconds(30L));
		if (partitions.size() != 1)
		{
			throw new IllegalStateException(
				"Kafka replication topic must have exactly one partition; found " + partitions.size()
			);
		}
		/* The topology is deliberately one partition. Explicit assignment avoids
		 * group rebalances while Store materialization blocks the polling thread;
		 * offsets are still committed to the configured group after application. */
		consumer.assign(Collections.singletonList(new TopicPartition(this.topicName, partitions.get(0).partition())));

				// Seek to the persisted offsets; a fresh client starts at the log beginning.
				final var startingCursor = this.cursor.get();
				final var startingOffsets = KafkaCursorCodec.decode(startingCursor, this.topicName);
				if (startingOffsets.isEmpty() && startingCursor.logicalSequence() >= 0L)
				{
					throw new IllegalStateException(
						"RESEED_REQUIRED: Kafka cursor has an applied sequence but no partition offset"
					);
				}
				final var beginningOffsets = consumer.beginningOffsets(consumer.assignment(), OFFSET_LOOKUP_TIMEOUT);
				for (final var entry : startingOffsets)
				{
					final var partition = entry.key();
					final long offset = entry.value();
					final Long beginning = beginningOffsets.get(partition);
					if (beginning == null || offset < beginning)
					{
						throw new IllegalStateException(
							"RESEED_REQUIRED: Kafka cursor points to records that retention has removed"
						);
					}
					LOG.log(System.Logger.Level.DEBUG, "Seeking partition " + partition + " to offset " + offset);
                    consumer.seek(partition, offset);
                }
                final var missingPartitions = consumer.assignment()
                    .stream()
                    .filter(a -> !startingOffsets.containsSearched(kv -> kv.key().equals(a)))
                    .toList();
                if (!missingPartitions.isEmpty())
                {
                    LOG.log(System.Logger.Level.DEBUG,
                        "Resetting offsets for partitions missing in the starting offsets: " + missingPartitions);
                    consumer.seekToBeginning(missingPartitions);
                }

                boolean run = true;
                while (run && !this.requestStop.get())
                {
					boolean progressed = false;
                    if (!this.stopAtLatestMessage.get())
                    {
						progressed = this.pollAndConsume(consumer);
                    }
                    else
                    {
                        LOG.log(System.Logger.Level.INFO, "Data client is now stopping at latest message.");
                        final long stopAt;
                        try (
	                            final var offsetProvider = KafkaCursorProvider.New(
                                this.topicName,
                                this.groupId + "-offsetgetter",
                                this.kafkaPropertiesProvider
                            )
                        )
                        {
                            offsetProvider.init();
	                            stopAt = offsetProvider.provideLatestSequence();
                        }
                        LOG.log(System.Logger.Level.INFO, "Stopping at message index " + stopAt);

						while (this.cachedSequence < stopAt && !this.requestStop.get())
                        {
							progressed |= this.pollAndConsume(consumer);
                        }
						LOG.log(System.Logger.Level.INFO,
							"Data client is now at latest sequence (" + this.cachedSequence + ")");

                        this.stopAtLatestMessage.set(false);
                        run = false;
                    }

					if (this.doCommitOffset && progressed)
					{
						consumer.commitSync(Duration.ofSeconds(30L));
					}
				}
			}
			finally
			{
				this.consumer = null;
			}

			if (!this.disposed)
			{
				this.requestStop.set(false);
			}
            LOG.log(System.Logger.Level.INFO, "DataClient run finished");
        }

        private ReplicationCursor createCursor(
			final KafkaConsumer<String, byte[]> consumer,
			final long sequence
		)
        {
			if (sequence % 10_000 == 0)
			{
				LOG.log(System.Logger.Level.DEBUG,
					"Polling and creating replication cursor at sequence " + sequence);
            }
            final EqHashTable<TopicPartition, Long> map = EqHashTable.New();
            for (final var partition : consumer.assignment())
            {
				long offset = consumer.position(partition);
				final CachedPacket pending = this.cachedPackets.peek();
				if (pending != null)
				{
					/* The first incomplete packet is the earliest record that must be
					 * replayed after restart. */
					offset = pending.offset();
				}
				else if (this.lastAppliedOffset >= 0)
				{
					offset = Math.min(offset, Math.addExact(this.lastAppliedOffset, 1L));
				}
                map.put(partition, offset);
            }
			final var cursor = new ReplicationCursor(
				"kafka", this.cursor.get().storeGeneration(), sequence,
				KafkaCursorCodec.encode(map.immure())
			);
			return cursor;
        }

		private void updateOffsets(final KafkaConsumer<String, byte[]> consumer)
		{
			final ReplicationCursor newCursor = this.createCursor(consumer, this.cachedSequence);
			this.offsetChangedListener.onApplied(newCursor);
			this.cursor.set(newCursor);
		}

        /**
         * Polls the Kafka consumer and consumes fully completed messages. Invalid
         * packets fail closed; incomplete messages remain cached until the next poll.
         */
		private boolean pollAndConsume(final KafkaConsumer<String, byte[]> consumer)
		{
            // only consume complete messages, to do this we need to look ahead to see if all packets
            // are here yet. If not read more, if it starts at 0 again then we know that something went
            // wrong on the writer side and that we should just skip all the packets in that series

			final ConsumerRecords<String, byte[]> records;
			try
			{
				records = consumer.poll(POLL_TIMEOUT);
			}
			catch (final OffsetOutOfRangeException failure)
			{
				throw new IllegalStateException(
					"RESEED_REQUIRED: Kafka cursor points to records that retention has removed", failure);
			}
			final long previousSequence = this.cachedSequence;
			this.cachedPackets.addAll(this.createPackets(records));
			if (this.cachedPackets.size() > MAX_CACHED_PACKETS)
			{
				throw new IllegalStateException("Kafka replication packet cache exceeded " + MAX_CACHED_PACKETS);
			}
			if (!this.cachedPackets.isEmpty() && this.incompleteMessageSinceNanos == -1L)
			{
				this.incompleteMessageSinceNanos = System.nanoTime();
			}

            if (this.cachedPackets.isEmpty())
            {
				if (!records.isEmpty())
				{
						this.updateOffsets(consumer);
					return true;
				}
				return false;
            }

			final var packets = new ArrayList<CachedPacket>(this.cachedPackets.size());

			outer:
			while (!this.cachedPackets.isEmpty())
            {
				final CachedPacket root = this.cachedPackets.peek();
				final var rootPacket = root == null ? null : root.packet();
                if (rootPacket == null)
                {
                    break;
                }

				if (rootPacket.packetIndex() != 0)
                {
					LOG.log(System.Logger.Level.WARNING,
						"First packet has index " + rootPacket.packetIndex() + ", expected 0; skipping packet");
					this.discard(this.cachedPackets.remove());
					continue;
				}
					if (rootPacket.packetCount() <= 0)
				{
					LOG.log(System.Logger.Level.WARNING, "Invalid packet count " + rootPacket.packetCount());
					this.discard(this.cachedPackets.remove());
						continue;
					}
					final int expectedPacketCount = (rootPacket.messageLength() +
						KafkaHeaderCodec.maxPacketSize() - 1) /
						KafkaHeaderCodec.maxPacketSize();
					if (rootPacket.messageLength() <= 0 || rootPacket.messageLength() > MAX_MESSAGE_BYTES ||
						rootPacket.packetCount() > MAX_PACKET_COUNT || rootPacket.packetCount() != expectedPacketCount)
					{
						throw new IllegalStateException("Invalid Kafka replication message bounds: length=" +
							rootPacket.messageLength() + ", packets=" + rootPacket.packetCount());
					}

                if (this.cachedPackets.size() < rootPacket.packetCount())
                {
                    //LOG.trace("Message Incomplete ({}/{})", this.cachedPackets.size(), rootPacket.packetCount());
                    break;
                }

				final var newMessagePackets = new ArrayList<CachedPacket>(rootPacket.packetCount());

                for (int i = 0; i < rootPacket.packetCount(); i++)
                {
					final CachedPacket cached = this.cachedPackets.peek();
					if (cached == null)
					{
						break outer;
					}
					final var packet = cached.packet();

				if (packet.packetIndex() != i)
				{
					LOG.log(System.Logger.Level.WARNING,
						"Unexpected packet index " + packet.packetIndex() + ", expected " + i + "; skipping packet");
					this.discard(this.cachedPackets.remove());
					continue outer;
                    }

				if (packet.packetCount() != rootPacket.packetCount())
                    {
						LOG.log(System.Logger.Level.WARNING,
							"Unexpected packet count " + packet.packetCount() + " at index " +
							packet.packetIndex() + ", expected " + rootPacket.packetCount() + "; skipping packet");
					this.discard(this.cachedPackets.remove());
					continue outer;
                    }

					newMessagePackets.add(this.cachedPackets.remove());
                }
				this.validateChecksum(newMessagePackets);

                packets.addAll(newMessagePackets);
            }

		this.consumeFullMessage(packets, consumer);
		if (this.cachedPackets.isEmpty())
		{
			this.incompleteMessageSinceNanos = -1L;
		}
		else if (this.incompleteMessageSinceNanos != -1L &&
			System.nanoTime() - this.incompleteMessageSinceNanos > INCOMPLETE_MESSAGE_TIMEOUT_NANOS)
		{
			throw new IllegalStateException("Kafka replication message remained incomplete for five minutes");
		}
		if (this.cachedSequence > previousSequence)
		{
			this.discardedPackets = 0;
		}
		if (this.cachedSequence <= previousSequence && this.cachedPackets.isEmpty() && !records.isEmpty())
		{
			/* Tombstones, discarded records, and already applied messages still advance
			 * the durable Kafka offset even when the Store sequence is unchanged. */
			this.updateOffsets(consumer);
			return true;
		}
		return this.cachedSequence > previousSequence;
		}

		private void discard(final CachedPacket packet)
		{
			this.lastAppliedOffset = Math.max(this.lastAppliedOffset, packet.offset());
			this.recordDiscardedPacket();
		}

		private void recordDiscardedPacket()
		{
			if (++this.discardedPackets > 1_024)
			{
				throw new IllegalStateException(
					"Kafka packet stream discarded more than 1024 packets without forward progress"
				);
			}
		}

		private List<CachedPacket> createPackets(final ConsumerRecords<String, byte[]> records)
		{
			final var list = new ArrayList<CachedPacket>();
			for (final var record : records)
			{
				if (record.value() == null)
				{
					/* Tombstones have no replication payload, but their offsets still
					 * belong to the cursor. */
					this.lastAppliedOffset = Math.max(this.lastAppliedOffset, record.offset());
					continue;
				}
				if (record.value().length == 0)
				{
					throw new IllegalStateException(
						"empty Kafka replication record is not a tombstone or packet");
				}
				if (record.value().length > KafkaHeaderCodec.maxPacketSize())
					throw new IllegalStateException("Kafka replication packet exceeds maximum payload size");
				list.add(this.createDataPacket(record, record.headers()));
			}
			return list;
        }

		private void consumeFullMessage(
			final Collection<CachedPacket> packets,
            final KafkaConsumer<String, byte[]> consumer
        )
        {
            final List<StorageBinaryDataPacket> newPackets = new ArrayList<>(packets.size());
			long lastNewSequence = this.cachedSequence;

			final List<CachedPacket> orderedPackets = new ArrayList<>(packets);
			for (int start = 0; start < orderedPackets.size();)
			{
				final long sequence = orderedPackets.get(start).packet().messageIndex();
				int end = start + 1;
				while (end < orderedPackets.size() &&
					orderedPackets.get(end).packet().messageIndex() == sequence) end++;
				if (sequence <= this.cachedSequence)
				{
					for (int index = start; index < end; index++)
					{
						this.lastAppliedOffset = Math.max(this.lastAppliedOffset, orderedPackets.get(index).offset());
					}
					LOG.log(System.Logger.Level.DEBUG,
						"Skipping already applied Kafka message index " + sequence);
				}
				else
				{
					if (sequence != lastNewSequence + 1L)
					{
						throw new IllegalStateException(
							"Kafka replication sequence gap: expected " + (lastNewSequence + 1L) +
							", received " + sequence);
					}
					lastNewSequence = sequence;
					for (int index = start; index < end; index++)
					{
						final CachedPacket cached = orderedPackets.get(index);
						this.lastAppliedOffset = Math.max(this.lastAppliedOffset, cached.offset());
						newPackets.add(cached.packet());
					}
				}
				start = end;
			}

            if (!newPackets.isEmpty())
            {
 				if (lastNewSequence % 10_000 == 0)
                {
					LOG.log(System.Logger.Level.DEBUG, "Applying packets at message index " + lastNewSequence);
				}
				acceptAndAwait(this.packetAcceptor, newPackets);
				final RuntimeException mergerFailure = this.packetAcceptor.failure();
				if (mergerFailure != null)
				{
					throw new IllegalStateException("Kafka reader merger has failed", mergerFailure);
				}
				final var newInfo = this.createCursor(consumer, lastNewSequence);
				this.offsetChangedListener.onApplied(newInfo);
				this.cursor.set(newInfo);
				this.cachedSequence = lastNewSequence;
            }
        }

		/**
		 * Completes the Store materialization boundary for one accepted message.
		 * Kafka offsets and cursor callbacks must be advanced only after this method
		 * returns successfully.
		 *
		 * @param packetAcceptor packet destination
		 * @param packets accepted packets
		 */
		static void acceptAndAwait(
			final ClusterStorageBinaryDataPacketAcceptor packetAcceptor,
			final List<StorageBinaryDataPacket> packets
		)
		{
			packetAcceptor.accept(packets);
			packetAcceptor.awaitApplied();
		}

		private CachedPacket createDataPacket(
            final ConsumerRecord<String, byte[]> record,
            final Headers headers
        )
        {
			final MessageType messageType = KafkaHeaderCodec.messageType(headers);
			final int messageLength = KafkaHeaderCodec.messageLength(headers);
			final int packetIndex = KafkaHeaderCodec.packetIndex(headers);
			final int packetCount = KafkaHeaderCodec.packetCount(headers);
			final long messageIndex = KafkaHeaderCodec.messageIndex(headers);
			final int messageCrc32c = KafkaHeaderCodec.messageCrc32c(headers);
			KafkaHeaderCodec.validateMetadata(messageLength, packetIndex, packetCount, messageIndex);
			return new CachedPacket(
				StorageBinaryDataPacket.New(
					messageType, messageLength, packetIndex, packetCount, messageIndex,
					ByteBuffer.wrap(record.value())
				), messageCrc32c, record.offset());
		}

		private void validateChecksum(final List<CachedPacket> packets)
		{
			final StorageBinaryDataPacket first = packets.get(0).packet();
			final int expected = packets.get(0).messageCrc32c();
			int totalLength = 0;
			final var checksum = Crc32c.accumulator();
			for (final CachedPacket cached : packets)
			{
				final StorageBinaryDataPacket packet = cached.packet();
				if (packet.messageType() != first.messageType() ||
					packet.messageLength() != first.messageLength() ||
					packet.packetCount() != first.packetCount() ||
					packet.messageIndex() != first.messageIndex())
				{
					throw new IllegalStateException("Kafka replication message metadata changed within a message");
				}
				if (cached.messageCrc32c() != expected)
				{
					throw new IllegalStateException("Kafka replication checksum header changed within a message");
				}
				final ByteBuffer payload = packet.buffer().duplicate();
				totalLength = Math.addExact(totalLength, payload.remaining());
				checksum.update(payload);
			}
			if (totalLength != first.messageLength())
				throw new IllegalStateException("Kafka replication message length mismatch");
			if ((int)checksum.getValue() != expected)
			{
				throw new IllegalStateException("Kafka replication message checksum mismatch");
			}
		}

	/** Packet plus its Kafka offset, retained until the complete message is applied. */
	private record CachedPacket(StorageBinaryDataPacket packet, int messageCrc32c, long offset)
	{
	}

        /**
         * Stops collecting updates after the latest complete message sequence in
         * Kafka has been applied.
         */
        @Override
        public void stopAtLatestMessage()
        {
            LOG.log(System.Logger.Level.INFO, "DataClient will stop at latest offset");
            this.stopAtLatestMessage.set(true);
        }

        @Override
	public void resume() throws NodelibraryException
        {
            if (this.stopAtLatestMessage.get())
            {
                throw new NodelibraryException(
				new IllegalStateException("Client is still reading up to the latest message index")
                );
            }
            if (this.isRunning())
            {
				throw new NodelibraryException(new IllegalStateException("Client is still active"));
            }
            this.start();
        }

	/**
	 * Wakes the Kafka poll before joining the reader. Downstream packet resources
	 * are closed only after the reader has stopped; a timeout leaves them open so
	 * the caller can retry disposal safely.
	 */
	@Override
	public synchronized void dispose()
	{
			if (this.disposed && this.packetAcceptorDisposed && this.offsetListenerClosed) return;
			this.disposed = true;
			this.disposeRequested = true;
			LOG.log(System.Logger.Level.DEBUG, "Disposing data client");
				this.requestStop.set(true);
			RuntimeException failure = null;
			final Thread current = this.runner;
			if (current == Thread.currentThread())
			{
				/* The polling thread cannot join itself and must not close the acceptor
				 * while callbacks are still using it.  Leave ownership retryable for the
				 * caller that can join this thread after the loop exits. */
				this.disposed = false;
				this.disposeRequested = false;
				throw new IllegalStateException("Kafka data client cannot be disposed from its polling thread");
			}
			if (current != null)
			{
				final KafkaConsumer<String, byte[]> consumer = this.consumer;
				if (consumer != null)
				{
					consumer.wakeup();
				}
				current.interrupt();
                try
                {
                    LOG.log(System.Logger.Level.DEBUG, "Waiting for runner to stop");
                    current.join(5_000L);
                }
				catch (final InterruptedException e)
				{
					Thread.currentThread().interrupt();
					failure = new NodelibraryException(e);
				}
				if (current.isAlive())
				{
					this.disposed = false;
					final IllegalStateException timeout = new IllegalStateException(
						"Kafka data client reader did not stop before disposal timeout"
					);
					if (failure != null) timeout.addSuppressed(failure);
					throw timeout;
				}
			}
			if (!this.packetAcceptorDisposed)
			{
				try
				{
					this.packetAcceptor.dispose();
					this.packetAcceptorDisposed = true;
				}
				catch (final RuntimeException disposeFailure)
				{
					if (failure == null) failure = disposeFailure;
					else failure.addSuppressed(disposeFailure);
				}
			}
			if (!this.offsetListenerClosed)
			{
				try
				{
					this.offsetChangedListener.close();
					this.offsetListenerClosed = true;
				}
				catch (final RuntimeException closeFailure)
				{
					if (failure == null) failure = closeFailure;
					else failure.addSuppressed(closeFailure);
				}
			}
			if (failure != null)
			{
				this.disposed = false;
				throw failure;
			}
        }
    }
