package org.eclipse.datagrid.cluster.nodelibrary.types.crashtest;

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

/** Shared class-path construction for forked crash-test children. */
final class ChildJava
{
	private ChildJava()
	{
	}

	static String classpath()
	{
		final String testPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
		final String modulePath = System.getProperty("jdk.module.path");
		return modulePath == null || modulePath.isBlank()
			? testPath : testPath + java.io.File.pathSeparator + modulePath;
	}
}
