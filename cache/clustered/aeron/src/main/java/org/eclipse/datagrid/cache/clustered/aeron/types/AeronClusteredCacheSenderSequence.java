package org.eclipse.datagrid.cache.clustered.aeron.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered Aeron
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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-wide monotonic sequence shared by every sender of one configured
 * node identity on one channel.
 *
 * <p>Several providers on one node may share a configured {@code node-id}, so
 * their senders must draw from one sequence: a remote receiver keys gap
 * tracking by the wire identity, and two independent per-instance counters
 * would interleave (0, 0, 1, 1) and report false gaps for roughly half the
 * frames.</p>
 *
 * <p>The sequence is keyed by the channel and stream id in addition to the
 * identity: the same node id used on two different channels has independent
 * sequences, because the receivers on each channel only ever see the frames
 * of that channel.</p>
 *
 * <p>Only configured node identities enter this map, so the number of entries
 * is bounded by the distinct (channel, stream, node id) combinations used on
 * the node. A provider without a configured node id owns a per-provider
 * sequence instead, because its random identity is unique per provider
 * instance.</p>
 */
final class AeronClusteredCacheSenderSequence
{
	private static final ConcurrentHashMap<Key, AtomicLong> SEQUENCES = new ConcurrentHashMap<>();

	private AeronClusteredCacheSenderSequence()
	{
	}

	/** Key of one shared sequence: an identity on one channel and stream. */
	private record Key(String channel, int streamId, AeronClusteredCacheMessageCodec.SenderId sender)
	{
	}

	/** Returns the sequence shared by every sender of the given identity on one channel. */
	static AtomicLong shared(final byte[] senderId, final String channel, final int streamId)
	{
		return SEQUENCES.computeIfAbsent(
			new Key(channel, streamId, AeronClusteredCacheMessageCodec.senderIdOf(senderId)),
			ignored -> new AtomicLong());
	}
}