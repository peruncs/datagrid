package org.eclipse.datagrid.storage.distributed.aeron.crashtest;

/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Aeron
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

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Forked probe proving a dead driver is reported through the Aeron error handler. */
public final class AeronDriverFailureChildMain
{
	private AeronDriverFailureChildMain() { }

	public static void main(final String[] ignored) throws Exception
	{
		final Path root = Path.of(System.getProperty("dg.driver.failure.root"));
		final Path control = root.resolve("control");
		final Path aeronDirectory = root.resolve("aeron");
		Files.createDirectories(control);
		final CountDownLatch failed = new CountDownLatch(1);
		final AtomicReference<Throwable> failure = new AtomicReference<>();
		final MediaDriver.Context mediaContext = new MediaDriver.Context()
			.aeronDirectoryName(aeronDirectory.toString())
			.dirDeleteOnStart(true)
			.dirDeleteOnShutdown(true);
		try (MediaDriver driver = MediaDriver.launchEmbedded(mediaContext);
			Aeron aeron = Aeron.connect(new Aeron.Context()
				.aeronDirectoryName(aeronDirectory.toString())
				.driverTimeoutMs(250)
				.errorHandler(error ->
				{
					failure.compareAndSet(null, error);
					failed.countDown();
				})))
		{
			Files.writeString(control.resolve("connected"), "connected");
			driver.close();
			if (!failed.await(10, TimeUnit.SECONDS))
			{
				Files.writeString(control.resolve("outcome"), "NO_FAILURE");
				return;
			}
			final Throwable observed = failure.get();
			Files.writeString(control.resolve("outcome"), "FAILURE=" +
				(observed == null ? "unknown" : observed.getClass().getName()));
		}
	}
}
