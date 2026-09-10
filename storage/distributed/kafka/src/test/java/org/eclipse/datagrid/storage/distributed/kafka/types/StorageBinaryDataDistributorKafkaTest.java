package org.eclipse.datagrid.storage.distributed.kafka.types;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Kafka
 * %%
 * Copyright (C) 2026 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.eclipse.serializer.memory.XMemory.toDirectByteBuffer;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Verifies distributor failures remain visible without creating a Kafka broker. */
class StorageBinaryDataDistributorKafkaTest
{
	@Test
	void retainedFailureRejectsSubsequentDistribution()
	{
		final TestDistributor distributor = new TestDistributor();
		distributor.fail(new IllegalStateException("send failed"));

		assertThrows(IllegalStateException.class, () -> distributor.distributeData(
			org.eclipse.serializer.persistence.binary.types.ChunksWrapper.New(
				toDirectByteBuffer(new byte[] {1})
			)
		));
	}

	private static final class TestDistributor extends StorageBinaryDataDistributorKafka.Abstract
	{
		private TestDistributor()
		{
			super(new Properties(), "unused");
		}

		private void fail(final Throwable failure)
		{
			recordFailure(failure);
		}

		@Override
		protected void execute(final Runnable action)
		{
			action.run();
		}
	}
}
