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
 * Neutral Store replication over one Kafka topic.
 *
 * <p>The adapter preserves packet order on one Kafka partition and validates
 * message metadata before forwarding complete messages to the neutral
 * receiver. The topic must have exactly one partition. Packet payloads are
 * limited to 1,000,000 bytes; configure the broker and topic record-size limits
 * for that payload plus Kafka-header overhead. Producers use Zstandard by
 * default.</p>
 *
 * @since 1.0
 */
package org.eclipse.datagrid.storage.distributed.kafka.types;
