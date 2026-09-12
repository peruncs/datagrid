package org.eclipse.datagrid.cluster.nodelibrary.kafka;

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Kafka Provider
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

/** Verifies deterministic Kafka configuration loading without a broker. */
class KafkaPropertiesProviderTest
{
	@Test
	void mergesConfigurationFilesInPathOrder(@TempDir final Path directory) throws Exception
	{
		Files.writeString(directory.resolve("10-base.properties"), "bootstrap.servers=first\nacks=1\n");
		Files.writeString(directory.resolve("20-override.properties"), "bootstrap.servers=second\n");

		final KafkaPropertiesProvider provider = KafkaPropertiesProvider.ConfigDirectory(directory);
		provider.init();

		assertEquals("second", provider.provide().getProperty("bootstrap.servers"));
		assertEquals("1", provider.provide().getProperty("acks"));
	}

	@Test
	void rejectsAnEmptyConfigurationDirectory(@TempDir final Path directory)
	{
		final KafkaPropertiesProvider provider = KafkaPropertiesProvider.ConfigDirectory(directory);

		assertThrows(NodelibraryException.class, provider::init);
	}
}
