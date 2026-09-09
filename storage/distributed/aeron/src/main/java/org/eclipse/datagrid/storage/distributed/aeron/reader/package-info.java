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
 * Archive replay and live delivery of committed Store transactions.
 *
 * <p>The reader catches up from the recording before it accepts live data.
 * Transactions become visible only after their commit marker and checksum have
 * been verified. The assembler is shared with the test-only live reader so
 * both paths exercise the same ordering rules.</p>
 */
package org.eclipse.datagrid.storage.distributed.aeron.reader;
