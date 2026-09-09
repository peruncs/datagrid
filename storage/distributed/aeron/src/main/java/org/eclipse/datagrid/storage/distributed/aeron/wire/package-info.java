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
 * The private envelope format used on the Aeron stream.
 *
 * <p>The checksum catches accidental corruption and framing mistakes. It does
 * not authenticate a sender. This package is intentionally not exported; an
 * external diagnostic tool should be added only when the wire format becomes
 * a supported operational interface.</p>
 */
package org.eclipse.datagrid.storage.distributed.aeron.wire;
