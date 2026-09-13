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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Proves the production provider overrides Aeron's process-exiting timeout handler. */
class AeronProviderDriverFailureTest
{
	@Test
	void deadOwnedDriverFailsHealthWithoutExitingTheJvm(@TempDir final Path root) throws Exception
	{
		final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
		final String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
		final Process child = new ProcessBuilder(java, "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
			"-cp", classpath, "-Ddg.driver.failure.root=" + root,
			AeronProviderDriverFailureChildMain.class.getName()).redirectErrorStream(true).start();
		try
		{
			assertTrue(child.waitFor(15, TimeUnit.SECONDS), "provider driver-timeout child did not exit");
			final String output = new String(child.getInputStream().readAllBytes());
			assertEquals(0, child.exitValue(), output);
			assertEquals("connected", Files.readString(root.resolve("control/connected")));
			assertEquals("FAILED", Files.readString(root.resolve("control/outcome")));
		}
		finally
		{
			if (child.isAlive())
			{
				child.destroyForcibly();
				child.waitFor(5, TimeUnit.SECONDS);
			}
		}
	}
}
