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
 * Validated limits shared by Aeron replication participants.
 *
 * <p>The writer and readers must agree on framing and transaction limits.
 * Validation happens before a publication or subscription is created, so a
 * bad deployment fails at startup rather than producing an unreadable stream.</p>
 */
package org.eclipse.datagrid.storage.distributed.aeron.config;
