package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
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

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.eclipse.store.storage.types.StorageConnection;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.*;

/** Verifies the backup manager's stop, durability, retention, and resume protocol. */
class StorageBackupManagerTest
{
	private static final ReplicationCursor CURSOR =
		new ReplicationCursor("test", null, 7L, new byte[] { 1, 2, 3 });

	@Test
	void createsPrunesRetainsAndResumesAfterAResolvedStop()
	{
		final FakeClient client = new FakeClient();
		client.running = true;
		final FakeBackend backend = new FakeBackend();
		backend.backups.addAll(List.of(
			new BackupMetadata(1L, false),
			new BackupMetadata(2L, false),
			new BackupMetadata(3L, false)));
		backend.previousCursor = CURSOR;
		final FakeRetention retention = new FakeRetention();
		retention.results.add(new ReplicationLogRetention.MaintenanceResult(
			ReplicationLogRetention.MaintenanceResult.Status.DELETED, 100L, "deleted"));

		final StorageBackupManager manager = manager(backend, client, retention, 2);

		manager.createStorageBackup(false);

		assertEquals(1, client.stopCalls);
		assertEquals(1, client.resumeCalls);
		assertEquals(1, backend.created.size());
		assertEquals(List.of(1L, 2L), backend.deleted.stream().map(BackupMetadata::timestamp).toList());
		assertEquals(List.of(CURSOR), retention.cursors);
		assertEquals(1, retention.calls);
	}

	@Test
	void retriesDeferredRetentionWithoutRepeatingTheBackup()
	{
		final FakeBackend backend = new FakeBackend();
		backend.backups.add(new BackupMetadata(1L, false));
		backend.previousCursor = CURSOR;
		final FakeRetention retention = new FakeRetention();
		retention.results.addAll(List.of(
			new ReplicationLogRetention.MaintenanceResult(
				ReplicationLogRetention.MaintenanceResult.Status.DEFERRED_ACTIVE_REPLAY, 10L, "active"),
			new ReplicationLogRetention.MaintenanceResult(
				ReplicationLogRetention.MaintenanceResult.Status.DEFERRED_ACTIVE_REPLAY, 10L, "active"),
			new ReplicationLogRetention.MaintenanceResult(
				ReplicationLogRetention.MaintenanceResult.Status.DELETED, 10L, "deleted")));

		final StorageBackupManager manager = manager(backend, new FakeClient(), retention, 1);

		manager.createStorageBackup(false);

		assertEquals(1, backend.created.size());
		assertEquals(3, retention.calls);
	}

	@Test
	void doesNotCreateOrResumeWhenTheReaderStopIsUnresolved()
	{
		final FakeClient client = new FakeClient();
		client.running = true;
		client.stopOutcome = org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataClient.StopOutcome.TIMED_OUT;
		final FakeBackend backend = new FakeBackend();

		final StorageBackupManager manager = manager(backend, client, new FakeRetention(), 1);

		assertThrows(IllegalStateException.class, () -> manager.createStorageBackup(false));
		assertEquals(1, client.stopCalls);
		assertEquals(0, client.resumeCalls);
		assertTrue(backend.created.isEmpty());
	}

	@Test
	void refusesToCreateAfterAReaderFailure()
	{
		final FakeClient client = new FakeClient();
		client.failure = new IllegalStateException("reader failed");
		final FakeBackend backend = new FakeBackend();

		final StorageBackupManager manager = manager(backend, client, new FakeRetention(), 1);

		final IllegalStateException failure = assertThrows(
			IllegalStateException.class, () -> manager.createStorageBackup(false));
		assertSame(client.failure, failure.getCause());
		assertEquals(0, client.stopCalls);
		assertTrue(backend.created.isEmpty());
	}

	@Test
	void preservesBackupFailureWhenResumeAlsoFails()
	{
		final FakeClient client = new FakeClient();
		client.running = true;
		client.resumeFailure = new NodelibraryException("resume failed");
		final FakeBackend backend = new FakeBackend();
		backend.createFailure = new IllegalStateException("backup failed");

		final StorageBackupManager manager = manager(backend, client, new FakeRetention(), 1);

		final IllegalStateException failure = assertThrows(
			IllegalStateException.class, () -> manager.createStorageBackup(false));
		assertEquals("backup failed", failure.getMessage());
		assertEquals(1, failure.getSuppressed().length);
		assertSame(client.resumeFailure, failure.getSuppressed()[0]);
	}

	@Test
	void manualBackupDeletesOnlyThePreviousManualSlotAndSkipsRetention()
	{
		final FakeBackend backend = new FakeBackend();
		backend.backups.addAll(List.of(
			new BackupMetadata(1L, false),
			new BackupMetadata(2L, true)));
		final FakeRetention retention = new FakeRetention();

		final StorageBackupManager manager = manager(backend, new FakeClient(), retention, 1);

		manager.createStorageBackup(true);

		assertEquals(List.of(2L), backend.deleted.stream().map(BackupMetadata::timestamp).toList());
		assertEquals(0, retention.calls);
	}

	private static StorageBackupManager manager(
		final FakeBackend backend,
		final FakeClient client,
		final FakeRetention retention,
		final int maxBackupCount
	)
	{
		return StorageBackupManager.New(
			storageConnection(), maxBackupCount, backend, () -> CURSOR, client, retention);
	}

	/** A proxy keeps this orchestration test independent of Store implementation details. */
	private static StorageConnection storageConnection()
	{
		return (StorageConnection)Proxy.newProxyInstance(
			StorageConnection.class.getClassLoader(),
			new Class<?>[] { StorageConnection.class },
			(proxy, method, arguments) ->
			{
				if (method.getReturnType() == boolean.class) return false;
				if (method.getReturnType() == byte.class) return (byte)0;
				if (method.getReturnType() == short.class) return (short)0;
				if (method.getReturnType() == int.class) return 0;
				if (method.getReturnType() == long.class) return 0L;
				if (method.getReturnType() == float.class) return 0.0f;
				if (method.getReturnType() == double.class) return 0.0d;
				if (method.getReturnType() == char.class) return '\0';
				return null;
			});
	}

	private static final class FakeClient implements ClusterStorageBinaryDataClient
	{
		private boolean running;
		private RuntimeException failure;
		private RuntimeException resumeFailure;
		private org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataClient.StopOutcome stopOutcome =
			org.eclipse.datagrid.storage.distributed.types.StorageBinaryDataClient.StopOutcome.RESOLVED_BOUNDARY;
		private int stopCalls;
		private int resumeCalls;

		@Override
		public void start()
		{
			this.running = true;
		}

		@Override
		public void stopAtLatestMessage()
		{
			this.stopCalls++;
			this.running = false;
		}

		@Override
		public ReplicationCursor cursor()
		{
			return CURSOR;
		}

		@Override
		public boolean isRunning()
		{
			return this.running;
		}

		@Override
		public RuntimeException failure()
		{
			return this.failure;
		}

		@Override
		public StopResult stopResult()
		{
			return new StopResult(this.stopOutcome, CURSOR.logicalSequence(), 42L);
		}

		@Override
		public void resume()
		{
			this.resumeCalls++;
			if (this.resumeFailure != null) throw this.resumeFailure;
			this.running = true;
		}

		@Override
		public void dispose()
		{
			this.running = false;
		}
	}

	private static final class FakeBackend implements StorageBackupBackend
	{
		private final List<BackupMetadata> backups = new ArrayList<>();
		private final List<BackupMetadata> created = new ArrayList<>();
		private final List<BackupMetadata> deleted = new ArrayList<>();
		private ReplicationCursor previousCursor;
		private RuntimeException createFailure;

		@Override
		public List<BackupMetadata> listBackups()
		{
			return List.copyOf(this.backups);
		}

		@Override
		public Optional<ReplicationCursor> getCursorFromPreviousBackup(final int skip)
		{
			return Optional.ofNullable(this.previousCursor);
		}

		@Override
		public void deleteBackup(final BackupMetadata backup)
		{
			this.deleted.add(backup);
			this.backups.remove(backup);
		}

		@Override
		public void createAndUploadBackup(
			final StorageConnection connection,
			final ReplicationCursor cursor,
			final BackupMetadata backup
		)
		{
			if (this.createFailure != null) throw this.createFailure;
			this.created.add(backup);
			this.backups.add(backup);
		}

		@Override public void downloadLatestBackup(final Path destination) { }
		@Override public void downloadBackup(final Path destination, final BackupMetadata backup) { }
		@Override public boolean hasUserUploadedStorage() { return false; }
		@Override public void downloadUserUploadedStorage(final Path destination) { }
		@Override public void deleteUserUploadedStorage() { }
	}

	private static final class FakeRetention implements ReplicationLogRetention
	{
		private final Queue<MaintenanceResult> results = new ArrayDeque<>();
		private final List<ReplicationCursor> cursors = new ArrayList<>();
		private int calls;

		@Override
		public MaintenanceResult deleteThrough(final ReplicationCursor cursor)
		{
			this.calls++;
			this.cursors.add(cursor);
			return this.results.isEmpty()
				? new MaintenanceResult(MaintenanceResult.Status.NOTHING_TO_DELETE, -1L, "none")
				: this.results.remove();
		}

		@Override
		public void close()
		{
		}
	}
}
