package org.eclipse.datagrid.cluster.nodelibrary.types;

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
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataPacket;
import org.eclipse.serializer.collections.EqHashTable;
import org.eclipse.serializer.concurrency.XThreads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.common.IsolationLevel.READ_COMMITTED;
import static org.eclipse.serializer.util.X.notNull;

public final class KafkaClusterStorageBinaryDataClient implements ClusterStorageBinaryDataClient
{
    public static ClusterStorageBinaryDataClient New(
        final ClusterStorageBinaryDataPacketAcceptor packetAcceptor,
        final String topicName,
        final String groupId,
        final AfterDataMessageConsumedListener offsetChangedListener,
        final MessageInfo startingMessageInfo,
        final KafkaPropertiesProvider kafkaPropertiesProvider,
        final boolean doCommitOffset
    )
    {
        return new KafkaClusterStorageBinaryDataClient(
            notNull(packetAcceptor),
            notNull(topicName),
            notNull(groupId),
            notNull(offsetChangedListener),
            notNull(startingMessageInfo),
            notNull(kafkaPropertiesProvider),
            doCommitOffset
        );
    }

    private static final Logger LOG = LoggerFactory.getLogger(KafkaClusterStorageBinaryDataClient.class);
        private static final long PARTITION_ASSIGNMENT_TIMEOUT_MS = Duration.ofSeconds(60L).toMillis();
        private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5L);

        /**
         * List of packets that have been polled but not yet consumed as they are still
         * missing some packets to complete the set
         */
    private final Queue<ClusterStorageBinaryDataPacket> cachedPackets = new LinkedList<>();

    private final ClusterStorageBinaryDataPacketAcceptor packetAcceptor;
        private final String topicName;
        private final String groupId;
        private final AfterDataMessageConsumedListener offsetChangedListener;
        private final KafkaPropertiesProvider kafkaPropertiesProvider;
        private final boolean doCommitOffset;

        private final AtomicReference<MessageInfo> messageInfo;
	private long cachedMessageIndex;
	private int discardedPackets;
        private final AtomicBoolean stopAtLatestMessage = new AtomicBoolean();
        private final AtomicBoolean requestStop = new AtomicBoolean();
        private final AtomicBoolean running = new AtomicBoolean();

	private volatile Thread runner;
	private volatile KafkaConsumer<String, byte[]> consumer;
	private volatile boolean disposed;
	private volatile RuntimeException failure;

    private KafkaClusterStorageBinaryDataClient(
            final ClusterStorageBinaryDataPacketAcceptor packetAcceptor,
            final String topicName,
            final String groupId,
            final AfterDataMessageConsumedListener offsetChangedListener,
            final MessageInfo startingMessageInfo,
            final KafkaPropertiesProvider kafkaPropertiesProvider,
            final boolean doCommitOffset
        )
        {
            this.packetAcceptor = packetAcceptor;
            this.topicName = topicName;
            this.groupId = groupId;
            this.offsetChangedListener = offsetChangedListener;
            this.messageInfo = new AtomicReference<>(startingMessageInfo);
            this.cachedMessageIndex = startingMessageInfo.messageIndex();
            this.kafkaPropertiesProvider = kafkaPropertiesProvider;
            this.doCommitOffset = doCommitOffset;
    }

        @Override
        public boolean isRunning()
        {
            return this.running.get();
        }

        @Override
        public MessageInfo messageInfo()
        {
            return this.messageInfo.get();
        }

        @Override
	public synchronized void start()
	{
		if (this.disposed) throw new IllegalStateException("Kafka data client is disposed");
		if (this.failure != null) throw new IllegalStateException("Kafka data client has failed", this.failure);
		if (this.running.get()) return;
            if (LOG.isInfoEnabled())
            {
                LOG.info("Starting kafka data client at message index {}", this.messageInfo.get().messageIndex());
            }
            this.runner = new Thread(this::tryRun, "datagrid-kafka-cluster-reader");
            this.runner.setDaemon(true);
            this.runner.start();
        }

        private void tryRun()
        {
            try
            {
                this.running.set(true);
                this.run();
                this.running.set(false);
            }
		catch (final Throwable t)
		{
			this.running.set(false);
			if (!this.disposed && !this.requestStop.get())
			{
				this.failure = t instanceof RuntimeException runtime
					? runtime
					: new IllegalStateException("Kafka data client stopped unexpectedly", t);
				LOG.error("Kafka data client stopped unexpectedly", t);
			}
		}
	}

	@Override
	public RuntimeException failure()
	{
		return this.failure;
	}

		private void run()
        {
            final var properties = this.kafkaPropertiesProvider.provide();
            properties.setProperty(GROUP_ID_CONFIG, this.groupId);
            properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
            properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "none"); // required so we can detect out-of-date consumers
            properties.setProperty(ISOLATION_LEVEL_CONFIG, READ_COMMITTED.toString().toLowerCase(Locale.ROOT));

	try (final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties))
	{
		this.consumer = consumer;
		final int partitionCount = consumer.partitionsFor(this.topicName).size();
		if (partitionCount != 1)
		{
			throw new IllegalStateException(
				"Kafka replication topic must have exactly one partition; found " + partitionCount
			);
		}
		consumer.subscribe(Collections.singletonList(this.topicName));

                // Wait for assignment
                LOG.trace("Waiting for partition assignment");
                final long startMs = System.currentTimeMillis();
                final long endMs = startMs + PARTITION_ASSIGNMENT_TIMEOUT_MS;
                while (consumer.assignment().isEmpty())
                {
                    if (System.currentTimeMillis() > endMs)
                    {
                        throw new RuntimeException("Timed out waiting for topic partition assignment");
                    }
                    consumer.seekToBeginning(Collections.emptyList());
                    try
                    {
                        consumer.poll(POLL_TIMEOUT);
                    }
                    catch (final InvalidOffsetException ignored)
                    {
                        // ignored for partition assignment
                        LOG.trace("Ignoring invalid offset in partition assignment loop");
                        XThreads.sleep(1000);
                    }
                }

                // Seek to correct offsets
                final var cachedMessageInfo = this.messageInfo.get();
				final var startingOffsets = KafkaCursorCodec.decode(cachedMessageInfo, this.topicName);
                for (final var entry : startingOffsets)
                {
                    final var partition = entry.key();
                    final long offset = entry.value();
                    LOG.debug("Seeking partition {} to offset {}", partition, offset);
                    consumer.seek(partition, offset);
                }
                final var missingPartitions = consumer.assignment()
                    .stream()
                    .filter(a -> !startingOffsets.containsSearched(kv -> kv.key().equals(a)))
                    .toList();
                if (!missingPartitions.isEmpty())
                {
                    LOG.debug(
                        "Resetting offsets for the following partitions missing in the starting offsets: {}",
                        missingPartitions
                    );
                    consumer.seekToBeginning(missingPartitions);
                }

                boolean run = true;
                while (run && !this.requestStop.get())
                {
                    if (!this.stopAtLatestMessage.get())
                    {
                        this.pollAndConsume(consumer);
                    }
                    else
                    {
                        LOG.info("Data client is now stopping at latest message.");
                        final long stopAt;
                        try (
                            final var offsetProvider = KafkaMessageInfoProvider.New(
                                this.topicName,
                                this.groupId + "-offsetgetter",
                                this.kafkaPropertiesProvider
                            )
                        )
                        {
                            offsetProvider.init();
                            stopAt = offsetProvider.provideLatestMessageIndex();
                        }
                        LOG.info("Stopping at message index {}", stopAt);

                        while (this.cachedMessageIndex < stopAt && !this.requestStop.get())
                        {
                            this.pollAndConsume(consumer);
                        }
                        LOG.info("Data client is now at latest offset ({})", this.cachedMessageIndex);

                        this.stopAtLatestMessage.set(false);
                        run = false;
                    }

					if (this.doCommitOffset)
					{
						consumer.commitSync();
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
            LOG.info("DataClient run finished");
        }

        private MessageInfo updateOffsets(final KafkaConsumer<String, byte[]> consumer)
        {
            if (LOG.isDebugEnabled() && this.cachedMessageIndex % 10_000 == 0)
            {
                LOG.debug("Polling and updating message info for message index {}", this.cachedMessageIndex);
            }
            final EqHashTable<TopicPartition, Long> map = EqHashTable.New();
            for (final var partition : consumer.assignment())
            {
                // since we don't know the exact offset for each partition we just subtract the amount of
                // missing cached packets to ensure that we read them again after the backup node restarts
                long offset = consumer.position(partition);
                offset = Math.max(offset - this.cachedPackets.size(), 0);
                map.put(partition, offset);
            }
            final var info = MessageInfo.New(
                this.cachedMessageIndex,
                "kafka",
                this.messageInfo.get().storeGeneration(),
                KafkaCursorCodec.encode(map.immure())
            );
            this.messageInfo.set(info);
            return info;
        }

        /**
         * Polls the kafka consumer and consumes fully completed messages. Invalid
         * packets are skipped and incomplete messages will be cached.
         */
		private void pollAndConsume(final KafkaConsumer<String, byte[]> consumer)
		{
            // only consume complete messages, to do this we need to look ahead to see if all packets
            // are here yet. If not read more, if it starts at 0 again then we know that something went
            // wrong on the writer side and that we should just skip all the packets in that series

            this.cachedPackets.addAll(this.createPackets(consumer.poll(Duration.ofSeconds(5))));

            if (this.cachedPackets.isEmpty())
            {
                return;
            }

            final var packets = new ArrayList<ClusterStorageBinaryDataPacket>(this.cachedPackets.size());

			outer:
			while (!this.cachedPackets.isEmpty())
            {
                final var rootPacket = this.cachedPackets.peek();
                if (rootPacket == null)
                {
                    break;
                }

				if (rootPacket.packetIndex() != 0)
                {
					LOG.error("First packet has index {}, expected 0 skipping packet...", rootPacket.packetIndex());
					this.cachedPackets.remove();
					this.recordDiscardedPacket();
					continue;
				}
				if (rootPacket.packetCount() <= 0)
				{
					LOG.error("Invalid packet count {}", rootPacket.packetCount());
					this.cachedPackets.remove();
					this.recordDiscardedPacket();
					continue;
				}

                if (this.cachedPackets.size() < rootPacket.packetCount())
                {
                    //LOG.trace("Message Incomplete ({}/{})", this.cachedPackets.size(), rootPacket.packetCount());
                    break;
                }

                final var newMessagePackets = new ArrayList<ClusterStorageBinaryDataPacket>(rootPacket.packetCount());

                for (int i = 0; i < rootPacket.packetCount(); i++)
                {
                    final var packet = this.cachedPackets.peek();
                    if (packet == null)
                    {
                        break outer;
                    }

				if (packet.packetIndex() != i)
				{
					LOG.error(
						"Unexpected Packet Index {}, expected {} skipping packet...",
						packet.packetIndex(), i
					);
					this.cachedPackets.remove();
					this.recordDiscardedPacket();
					continue outer;
                    }

				if (packet.packetCount() != rootPacket.packetCount())
                    {
                        LOG.error(
                            "Unexpected Packet Count {} of Packet at Index {}, expected {} skipping packet...",
                            packet.packetCount(),
                            packet.packetIndex(),
                            rootPacket.packetCount()
                        );
					this.cachedPackets.remove();
					this.recordDiscardedPacket();
					continue outer;
                    }

                    newMessagePackets.add(this.cachedPackets.remove());
                }

                packets.addAll(newMessagePackets);
            }

		final long previousMessageIndex = this.cachedMessageIndex;
		this.consumeFullMessage(packets, consumer);
		if (this.cachedMessageIndex > previousMessageIndex)
		{
			this.discardedPackets = 0;
		}
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

        private List<ClusterStorageBinaryDataPacket> createPackets(final ConsumerRecords<String, byte[]> records)
        {
            final var list = new ArrayList<ClusterStorageBinaryDataPacket>();
            for (final var record : records)
            {
                if (record.serializedValueSize() > 0)
                {
                    list.add(this.createDataPacket(record, record.headers()));
                }
                else
                {
                    LOG.warn("Encountered record with serialized value size 0");
                }
            }
            return list;
        }

        private void consumeFullMessage(
            final Collection<ClusterStorageBinaryDataPacket> packets,
            final KafkaConsumer<String, byte[]> consumer
        )
        {
            final List<StorageBinaryDataPacket> newPackets = new ArrayList<>(packets.size());

            for (final var packet : packets)
            {
                if (this.cachedMessageIndex >= packet.messageIndex())

                {
                    LOG.warn(
                        "Skipping packet with offset {} (current: {})",
                        packet.messageIndex(),
                        this.cachedMessageIndex
                    );
                    continue;
                }

                this.cachedMessageIndex = packet.messageIndex();

                if (LOG.isTraceEnabled() && this.cachedMessageIndex % 10_000 == 0)
                {
                    LOG.trace("Consuming packet with offset {}", this.cachedMessageIndex);
                }

                newPackets.add(packet);

            }

            if (!newPackets.isEmpty())
            {
                if (LOG.isDebugEnabled() && this.cachedMessageIndex % 10_000 == 0)
                {
                    LOG.debug("Applying packets at offset {}", this.cachedMessageIndex);
                }
				this.packetAcceptor.accept(newPackets);
				final RuntimeException mergerFailure = this.packetAcceptor.failure();
				if (mergerFailure != null)
				{
					throw new IllegalStateException("Kafka reader merger has failed", mergerFailure);
				}
				final var newInfo = this.updateOffsets(consumer);
                this.offsetChangedListener.onChange(newInfo);
            }
        }

        private ClusterStorageBinaryDataPacket createDataPacket(
            final ConsumerRecord<String, byte[]> record,
            final Headers headers
        )
        {
            return ClusterStorageBinaryDataPacket.New(
                ClusterStorageBinaryDistributedKafka.messageType(headers),
                ClusterStorageBinaryDistributedKafka.messageLength(headers),
                ClusterStorageBinaryDistributedKafka.packetIndex(headers),
                ClusterStorageBinaryDistributedKafka.packetCount(headers),
                ClusterStorageBinaryDistributedKafka.messageIndex(headers),
                ByteBuffer.wrap(record.value())
            );
        }

        /**
         * Stop collecting updates after the last available offset in kafka has been
         * reached.
         */
        @Override
        public void stopAtLatestMessage()
        {
            LOG.info("DataClient will stop at latest offset");
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
            if (this.disposed) return;
            this.disposed = true;
            LOG.trace("Disposing data client");
			this.requestStop.set(true);
            RuntimeException failure = null;
            final Thread current = this.runner;
			if (current != null && current != Thread.currentThread())
			{
				final KafkaConsumer<String, byte[]> consumer = this.consumer;
				if (consumer != null)
				{
					consumer.wakeup();
				}
				current.interrupt();
                try
                {
                    LOG.trace("Waiting for runner to stop");
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
            try
            {
                this.packetAcceptor.dispose();
            }
            catch (final RuntimeException disposeFailure)
            {
                if (failure == null) failure = disposeFailure;
                else failure.addSuppressed(disposeFailure);
            }
            try
            {
                this.offsetChangedListener.close();
            }
            catch (final RuntimeException closeFailure)
            {
                if (failure == null) failure = closeFailure;
                else failure.addSuppressed(closeFailure);
            }
            if (failure != null) throw failure;
        }
    }
