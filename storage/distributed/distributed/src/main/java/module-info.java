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
 * Transport-neutral Store binary replication contracts for Eclipse Data Grid.
 *
 * <p>The module contains the SPI used by optional Kafka and Aeron providers.
 * It intentionally exports no provider implementation and therefore does not
 * pull a messaging client into applications that do not use clustering.</p>
 */
module org.eclipse.datagrid.storage.distributed
{
	requires org.eclipse.store.storage.embedded;
	requires org.eclipse.store.storage.embedded.configuration;
	requires org.eclipse.serializer.base;
	requires org.eclipse.serializer.persistence;
	requires org.eclipse.serializer.persistence.binary;

	exports org.eclipse.datagrid.storage.distributed.types;
}
