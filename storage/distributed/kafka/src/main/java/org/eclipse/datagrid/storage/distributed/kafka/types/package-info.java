/*-
 * #%L
 * Eclipse Data Grid Storage Distributed Kafka
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
 * This package adapts neutral storage replication to Kafka.
 *
 * <p>The producer publishes packet metadata and binary fragments. The reader
 * consumes them and hands complete transactions to the neutral storage
 * contract. Packet order, transaction boundaries, and binary ownership remain
 * the responsibility of the neutral types package.</p>
 */
package org.eclipse.datagrid.storage.distributed.kafka.types;
