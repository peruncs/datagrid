package org.eclipse.datagrid.cluster.nodelibrary.micronaut.types;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Micronaut
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


import io.micronaut.context.BeanContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.exceptions.DisabledBeanException;
import io.micronaut.core.reflect.InstantiationUtils;
import io.micronaut.eclipsestore.conf.EmbeddedStorageConfigurationProvider;
import io.micronaut.eclipsestore.conf.RootClassConfigurationProvider;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Singleton;
import org.eclipse.datagrid.cluster.nodelibrary.types.ClusterFoundation;
import org.eclipse.datagrid.storage.distributed.types.ObjectGraphUpdateHandler;

import java.util.function.Supplier;

/**
 * This factory builds the cluster foundation from Micronaut configuration.
 *
 * <p>The application must provide the named root-class configuration. The
 * factory fails during context creation when that root is missing, because a
 * cluster without a stable root cannot safely start storage.</p>
 */
@Factory
public class ClusterFoundationFactory
{
	private final BeanContext context;

	/**
	 * Creates a factory backed by the Micronaut bean context.
	 *
	 * @param context application bean context
	 */
	public ClusterFoundationFactory(final BeanContext context)
	{
		this.context = context;
	}

	/**
	 * Creates the cluster foundation from the named root configuration.
	 *
	 * @param configProvider embedded storage configuration
	 * @param async whether distribution may use asynchronous delivery
	 * @param objectGraphUpdateHandler handler for incoming graph updates
	 * @return configured cluster foundation
	 */
	@Singleton
	public ClusterFoundation<?> clusterFoundation(
		final EmbeddedStorageConfigurationProvider configProvider,
		@Property(name = "eclipsestore.distribution.async", defaultValue = "false") final boolean async,
		final ObjectGraphUpdateHandler objectGraphUpdateHandler
	)
	{
		final String name = "main";

		if (!this.context.containsBean(RootClassConfigurationProvider.class, Qualifiers.byName(name)))
		{
			throw new DisabledBeanException(
				"Please, define a bean of type " + RootClassConfigurationProvider.class.getSimpleName()
					+ " by name qualifier: " + name
			);
		}

		final RootClassConfigurationProvider configuration = this.context.getBean(
			RootClassConfigurationProvider.class,
			Qualifiers.byName(name)
		);

		if (configuration.getRootClass() == null)
		{
			throw new RuntimeException(
				"No EclipseStore storage root was found in the configuration for storage with name " + name
			);
		}

		final Supplier<Object> rootSupplier = () -> InstantiationUtils.instantiate(configuration.getRootClass());

		return ClusterFoundation.New()
			.setEnableAsyncDistribution(async)
			.setObjectGraphUpdateHandler(objectGraphUpdateHandler)
			.setRootSupplier(rootSupplier)
			.setEmbeddedStorageFoundation(configProvider.getBuilder().createEmbeddedStorageFoundation());
	}
}
