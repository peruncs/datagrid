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
 * This package reads complete Store transactions from Aeron.
 *
 * <p>A reader first replays the Archive and then follows the live stream. It
 * delivers a transaction only after its end marker arrives, so callers never
 * see a partial Store write. The reader owns its subscription and must be
 * closed before the Aeron client that created it.</p>
 */
package org.eclipse.datagrid.storage.distributed.aeron.reader;
