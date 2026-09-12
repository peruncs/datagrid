/*-
 * #%L
 * Eclipse Data Grid Clustered Cache Kafka
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
 * This package carries clustered cache timestamps through Kafka.
 *
 * <p>The producer publishes invalidations and the consumer applies them to
 * the neutral clustered-cache contract. A consumer ignores messages sent by
 * its own client, and Kafka settings stay in this adapter package. Cache
 * semantics remain in the sibling transport-neutral package.</p>
 */
package org.eclipse.datagrid.cache.clustered.kafka.types;
