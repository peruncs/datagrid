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
 * This package defines the private envelopes used on an Aeron stream.
 *
 * <p>An envelope carries one fragment of a replication transaction and its
 * framing information. Readers validate the version and boundaries before
 * passing data to the neutral storage contract. Application code should use
 * the reader and writer packages instead of depending on these wire classes.</p>
 */
package org.eclipse.datagrid.storage.distributed.aeron.wire;
