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
 * Ordered publication of Store transactions to Aeron and Archive.
 *
 * <p>The writer publishes data first and makes it visible with one terminal
 * commit marker. The coordinator then records the restart boundary. This
 * separation keeps transport I/O, Archive durability, and local Store
 * acceptance from being mistaken for the same event.</p>
 */
package org.eclipse.datagrid.storage.distributed.aeron.writer;
