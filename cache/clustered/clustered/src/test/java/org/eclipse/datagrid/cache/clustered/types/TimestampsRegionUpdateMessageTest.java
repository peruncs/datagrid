package org.eclipse.datagrid.cache.clustered.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered
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

import org.eclipse.datagrid.cache.clustered.test.ClusteredCacheTestSupport;
import org.junit.jupiter.api.Test;

import javax.cache.event.EventType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies the timestamp message validation shared by every transport sender. */
class TimestampsRegionUpdateMessageTest
{
	@Test
	void validMessageIsAccepted()
	{
		new TimestampsRegionUpdateMessage("cache", "table", 0L);
	}

	@Test
	void nullCacheNameIsRejected()
	{
		assertThrows(IllegalArgumentException.class,
			() -> new TimestampsRegionUpdateMessage(null, "table", 1L));
	}

	@Test
	void blankCacheNameIsRejected()
	{
		assertThrows(IllegalArgumentException.class,
			() -> new TimestampsRegionUpdateMessage(" ", "table", 1L));
	}

	@Test
	void nullTableNameIsRejected()
	{
		assertThrows(IllegalArgumentException.class,
			() -> new TimestampsRegionUpdateMessage("cache", null, 1L));
	}

	@Test
	void blankTableNameIsRejected()
	{
		assertThrows(IllegalArgumentException.class,
			() -> new TimestampsRegionUpdateMessage("cache", "", 1L));
	}

	@Test
	void negativeTimestampIsRejected()
	{
		assertThrows(IllegalArgumentException.class,
			() -> new TimestampsRegionUpdateMessage("cache", "table", -1L));
	}

	@Test
	void fromEventConvertsAValidEvent()
	{
		final TimestampsRegionUpdateMessage message = TimestampsRegionUpdateMessage.fromEvent(
			ClusteredCacheTestSupport.event("cache", EventType.CREATED, "table", 42L));
		assertEquals("cache", message.cacheName());
		assertEquals("table", message.tableName());
		assertEquals(42L, message.timestamp());
	}

	@Test
	void fromEventRejectsNonStringKey()
	{
		assertThrows(IllegalArgumentException.class, () -> TimestampsRegionUpdateMessage.fromEvent(
			ClusteredCacheTestSupport.eventWith("cache", EventType.CREATED, 123, 42L)));
	}

	@Test
	void fromEventRejectsNonLongValue()
	{
		assertThrows(IllegalArgumentException.class, () -> TimestampsRegionUpdateMessage.fromEvent(
			ClusteredCacheTestSupport.eventWith("cache", EventType.CREATED, "table", "not-a-long")));
	}
}