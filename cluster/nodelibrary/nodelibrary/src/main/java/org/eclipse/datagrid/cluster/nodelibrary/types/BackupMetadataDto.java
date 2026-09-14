package org.eclipse.datagrid.cluster.nodelibrary.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary
 * %%
 * Copyright (C) 2025 MicroStream Software
 * %%
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * #L%
 */

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Immutable metadata returned by the remote backup service.
 *
 * @param name remote object name
 * @param size remote object size in bytes
 */
public record BackupMetadataDto(
	@JsonProperty("name") String name,
	@JsonProperty("size") long size
)
{
	/** Creates metadata from the JSON properties returned by the backup service. */
	@JsonCreator
	public BackupMetadataDto
	{
	}
}
