package org.eclipse.datagrid.cluster.nodelibrary.helidon;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Helidon
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


import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.HttpResponseException;

/**
 * This mapper turns neutral HTTP failures into Helidon responses.
 *
 * <p>The status and headers come from the exception, so the REST adapter does
 * not need to know which node rule caused the failure.</p>
 */
@ApplicationScoped
@Provider
public class NotADistributorMapper implements ExceptionMapper<HttpResponseException>
{
	/** Creates an exception mapper for neutral HTTP failures. */
	public NotADistributorMapper()
	{
	}

	@Override
	public Response toResponse(final HttpResponseException e)
	{
		final var response = Response.status(e.statusCode());
		for (final var header : e.extraHeaders())
		{
			response.header(header.key(), header.value());
		}
		return response.build();
	}
}
