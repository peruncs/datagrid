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
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

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
	void readsRetentionSecretFromOwnerOnlyFile(@TempDir final Path temporaryDirectory) throws Exception
	{
		final byte[] secret = "sixteen-byte-key".getBytes(StandardCharsets.US_ASCII);
		final Path file = temporaryDirectory.resolve("retention.secret");
		Files.writeString(file, Base64.getEncoder().encodeToString(secret), StandardCharsets.US_ASCII);
		Files.setPosixFilePermissions(file, Set.of(PosixFilePermission.OWNER_READ));

		final AeronSettings settings = AeronSettings.fromEnvironment(properties(Map.of(
			"ECLIPSE_DATAGRID_AERON_RETENTION_SECRET_FILE", file.toString(),
			"ECLIPSE_DATAGRID_AERON_RETENTION_READERS", UUID.randomUUID().toString()
		)));

		assertArrayEquals(secret, settings.retentionSecret());
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

	@Test
	void rejectsUnsafeFilesystemSyncInProduction()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
			"ECLIPSE_DATAGRID_AERON_FILE_SYNC_LEVEL", "0"), true)));
	}

	@Test
	void rejectsIpv6WildcardInProduction()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
			"ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL", "aeron:udp?control=[::]:40123|control-mode=dynamic|fc=max",
			"ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL", "aeron:udp?endpoint=[::]:0|control=[::]:40123|control-mode=dynamic"
		), true)));
	}

	@Test
	void rejectsExpandedIpv6WildcardInProduction()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
			"ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
				"aeron:udp?control=[0:0:0:0:0:0:0:0]:40123|control-mode=dynamic|fc=max",
			"ECLIPSE_DATAGRID_AERON_REPLAY_CHANNEL",
				"aeron:udp?endpoint=[0:0:0:0:0:0:0:0]:0|control=[0:0:0:0:0:0:0:0]:40123|control-mode=dynamic"
		), true)));
	}

	@Test
	void rejectsFramingOverrideThatDisagreesWithReplication()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(
			"ECLIPSE_DATAGRID_AERON_LIVE_CHANNEL",
				"aeron:udp?control=localhost:40123|control-mode=dynamic|fc=max|term-length=8m"
		))));
	}

	@Test
	void rejectsLoopbackEndpointsInProduction()
	{
		assertThrows(IllegalArgumentException.class, () -> AeronSettings.fromEnvironment(properties(Map.of(), true)));
	}

	private static NodelibraryPropertiesProvider properties(final Map<String, String> overrides)
	{
		return properties(overrides, false);
	}

	private static NodelibraryPropertiesProvider properties(final Map<String, String> overrides, final boolean production)
	{
		final Path root = Path.of(System.getProperty("java.io.tmpdir"), "aeron-settings-" + UUID.randomUUID());
		final UUID cluster = UUID.randomUUID();
		final UUID node = UUID.randomUUID();
		final UUID generation = UUID.randomUUID();
		return new NodelibraryPropertiesProvider.Env()
		{
			@Override public boolean isProdMode() { return production; }
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
