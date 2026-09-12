/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Kafka Provider
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
 * This package carries cluster replication through Kafka.
 *
 * <p>The provider owns the Kafka producer and consumer resources for one node.
 * The neutral cluster API owns lifecycle decisions and replication contracts.
 * A provider instance belongs to one node, and its topic and client settings
 * must match the other members that consume the same stream.</p>
 */
package org.eclipse.datagrid.cluster.nodelibrary.kafka;
