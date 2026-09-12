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

/**
 * This package writes complete Store transactions to Aeron.
 *
 * <p>The writer publishes data in order and records the same stream for later
 * replay. A transaction end marker is the hand-off point: consumers may apply
 * the transaction only after that marker is durable. The writer must be closed
 * after the final transaction has been published.</p>
 */
package org.eclipse.datagrid.storage.distributed.aeron.writer;
