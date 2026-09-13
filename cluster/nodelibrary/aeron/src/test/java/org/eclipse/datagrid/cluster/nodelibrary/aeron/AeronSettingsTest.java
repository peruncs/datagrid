package org.eclipse.datagrid.cluster.nodelibrary.aeron;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Aeron Provider
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

import org.eclipse.datagrid.cluster.nodelibrary.types.NodelibraryPropertiesProvider;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Direct validation tests for configuration combinations that must fail before runtime startup. */
class AeronSettingsTest
{
	@Test
	void acceptsTheValidatedEmbeddedWriterDefaults()
	{
		assertDoesNotThrow(() -> AeronSettings.fromEnvironment(properties(Map.of())));
	}

	@Test
	void externalArchiveWriterRejectsRetentionConfiguration()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
			"ECLIPSE_DATAGRID_AERON_EXTERNAL_ARCHIVE", "true",
			"ECLIPSE_DATAGRID_AERON_RETENTION_SECRET",
				Base64.getEncoder().encodeToString("sixteen-byte-key".getBytes()),
			"ECLIPSE_DATAGRID_AERON_RETENTION_READERS", UUID.randomUUID().toString()
		))));
	}

	@Test
	void externalArchiveWriterAcceptsItsPointToPointRecordingChannel()
	{
		assertDoesNotThrow(() -> AeronSettings.fromEnvironment(properties(Map.of(
			"ECLIPSE_DATAGRID_AERON_EXTERNAL_ARCHIVE", "true",
			"ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL", "aeron:udp?endpoint=localhost:40123"
		))));
	}

	@Test
	void rejectsShortRetentionSecret()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
			"ECLIPSE_DATAGRID_AERON_RETENTION_SECRET", Base64.getEncoder().encodeToString(new byte[8])
		))));
	}

	@Test
	void rejectsRecordingIdsBelowAeronNullValue()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
			"ECLIPSE_DATAGRID_AERON_RECORDING_ID", "-2"
		))));
	}

	@Test
	void rejectsADataStreamWithoutSpaceForDerivedStreams()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
			"ECLIPSE_DATAGRID_AERON_STREAM_ID", Integer.toString(Integer.MAX_VALUE)
		))));
	}

	@Test
	void rejectsAConflictingWatermarkStream()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
			"ECLIPSE_DATAGRID_AERON_WATERMARK_STREAM_ID", "1002"
		))));
	}

	@Test
	void rejectsDuplicateRetentionReaders()
	{
		final String reader = UUID.randomUUID().toString();
		assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
			"ECLIPSE_DATAGRID_AERON_RETENTION_READERS", reader + "," + reader
		))));
	}

	@Test
	void rejectsUnsupportedLocalDurableFirstMode()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
			"ECLIPSE_DATAGRID_AERON_REPLICATION_DURABILITY_MODE", "LOCAL_DURABLE_FIRST"
		))));
	}

	private static NodelibraryPropertiesProvider properties(final Map<String, String> overrides)
	{
		final Path root = Path.of(System.getProperty("java.io.tmpdir"), "aeron-settings-" + UUID.randomUUID());
		final UUID cluster = UUID.randomUUID();
		final UUID node = UUID.randomUUID();
		final UUID generation = UUID.randomUUID();
		return new NodelibraryPropertiesProvider.Env()
		{
			@Override public String replicationRole() { return "writer"; }
			@Override public boolean replicationRoleConfigured() { return true; }
			@Override public String replicationProperty(final String name)
			{
				final String override = overrides.get(name);
				if (override != null) return override;
				return switch (name)
				{
					case "ECLIPSE_DATAGRID_AERON_CLUSTER_ID" -> cluster.toString();
					case "ECLIPSE_DATAGRID_AERON_NODE_ID" -> node.toString();
					case "ECLIPSE_DATAGRID_AERON_STORE_GENERATION" -> generation.toString();
					case "ECLIPSE_DATAGRID_AERON_DIRECTORY" -> root.resolve("driver").toString();
					case "ECLIPSE_DATAGRID_AERON_ARCHIVE_DIRECTORY" -> root.resolve("archive").toString();
					case "ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH" ->
						root.resolve("checkpoint/writer.checkpoint").toString();
					default -> null;
				};
			}
		};
	}
}
