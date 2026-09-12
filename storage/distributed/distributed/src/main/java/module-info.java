/*-
 * #%L
 * Eclipse Data Grid Storage Distributed
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
 * This module defines the neutral contract for moving Store binary data.
 *
 * <p>It owns packet, transaction, reader, writer, import, and materialization
 * contracts. Optional Kafka and Aeron modules implement those contracts. This
 * module exports no provider implementation, so an application can use the
 * neutral API without pulling in a messaging client.</p>
 *
 * <p>Implementations must preserve transaction boundaries and must release
 * binary resources after the receiving object graph has accepted them.</p>
 *
 * @since 1.0
 */
module org.eclipse.datagrid.storage.distributed
{
	requires org.eclipse.store.storage.embedded;
	requires org.eclipse.serializer.base;
	requires org.eclipse.serializer.persistence;
	requires org.eclipse.serializer.persistence.binary;

	exports org.eclipse.datagrid.storage.distributed.types;
}
