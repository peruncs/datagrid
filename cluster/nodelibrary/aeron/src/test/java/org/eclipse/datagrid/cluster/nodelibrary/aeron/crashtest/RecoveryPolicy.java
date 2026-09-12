package org.eclipse.datagrid.cluster.nodelibrary.aeron.crashtest;

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

/** Recovery result emitted by a provider crash child. */
enum RecoveryPolicy
{
	CONTINUE,
	REPLAY_FROM_ARCHIVE,
	RESEED_REQUIRED,
	FAIL_CLOSED,
	/** The child completed with an armed barrier that never fired. */
	HARNESS_ERROR
}
