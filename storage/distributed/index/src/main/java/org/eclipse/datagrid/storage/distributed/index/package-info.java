/*-
 * #%L
 * Eclipse Data Grid Store Index Integration
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
 * Keeps clustered text and vector search inside the Store object graph.
 *
 * <p>A replicated Store transaction is the only source of truth. An index
 * directory outside that transaction can advance independently, so it cannot
 * be made correct by copying or naming the directory. This package rejects
 * that configuration and provides the two supported registration paths.</p>
 *
 * <p>Lucene uses an embedded GraphDirectory with manual commit at the
 * {@code GigaMap.store()} boundary. JVector uses its persisted vector store;
 * its transient search graph is rebuilt locally by each reader.</p>
 *
 * @since 1.0
 */
package org.eclipse.datagrid.storage.distributed.index;
