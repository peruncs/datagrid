package org.eclipse.datagrid.cache.clustered.types;

/*-
 * #%L
 * Eclipse Data Grid Cache Clustered
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

import java.util.Map;

/**
 * Parses the string-valued Hibernate cache properties shared by the transport
 * adapters.
 *
 * <p>Hibernate supplies the cache configuration as a raw {@code Map}; the
 * Kafka and Aeron providers read the same kinds of values (strings, bounded
 * integers, longs, booleans) from it. This class keeps that parsing in one
 * place so every adapter applies the same blank-value and minimum rules.</p>
 */
public final class ClusteredCachePropertyParsers
{
	private ClusteredCachePropertyParsers()
	{
	}

	/**
	 * Returns the trimmed value of a property, or the fallback when it is
	 * absent or blank.
	 *
	 * @param properties source properties
	 * @param name property name
	 * @param fallback value when the property is absent or blank
	 * @return property value or fallback
	 */
	@SuppressWarnings("rawtypes")
	public static String stringProperty(final Map properties, final String name, final String fallback)
	{
		final Object configured = properties.get(name);
		if (configured == null)
		{
			return fallback;
		}
		final String value = configured.toString().trim();
		return value.isEmpty() ? fallback : value;
	}

	/**
	 * Returns a property as an integer no smaller than {@code minimum}.
	 *
	 * @param properties source properties
	 * @param name property name
	 * @param fallback value when the property is absent or blank
	 * @param minimum smallest accepted value
	 * @return parsed value or fallback
	 * @throws IllegalArgumentException when the value is not an integer or is below the minimum
	 */
	@SuppressWarnings("rawtypes")
	public static int intProperty(final Map properties, final String name, final int fallback, final int minimum)
	{
		final String configured = stringProperty(properties, name, null);
		if (configured == null)
		{
			return fallback;
		}
		final int value;
		try
		{
			value = Integer.parseInt(configured);
		}
		catch (final NumberFormatException failure)
		{
			throw new IllegalArgumentException(name + " must be an integer: " + configured, failure);
		}
		if (value < minimum)
		{
			throw new IllegalArgumentException(name + " must be at least " + minimum + ": " + value);
		}
		return value;
	}

	/**
	 * Returns a property as a long no smaller than {@code minimum}.
	 *
	 * @param properties source properties
	 * @param name property name
	 * @param fallback value when the property is absent or blank
	 * @param minimum smallest accepted value
	 * @return parsed value or fallback
	 * @throws IllegalArgumentException when the value is not a long or is below the minimum
	 */
	@SuppressWarnings("rawtypes")
	public static long longProperty(final Map properties, final String name, final long fallback, final long minimum)
	{
		final String configured = stringProperty(properties, name, null);
		if (configured == null)
		{
			return fallback;
		}
		final long value;
		try
		{
			value = Long.parseLong(configured);
		}
		catch (final NumberFormatException failure)
		{
			throw new IllegalArgumentException(name + " must be a long: " + configured, failure);
		}
		if (value < minimum)
		{
			throw new IllegalArgumentException(name + " must be at least " + minimum + ": " + value);
		}
		return value;
	}

	/**
	 * Returns a property as a boolean.
	 *
	 * @param properties source properties
	 * @param name property name
	 * @param fallback value when the property is absent or blank
	 * @return parsed value or fallback
	 * @throws IllegalArgumentException when the value is not {@code true} or {@code false}
	 */
	@SuppressWarnings("rawtypes")
	public static boolean booleanProperty(final Map properties, final String name, final boolean fallback)
	{
		final String configured = stringProperty(properties, name, null);
		if (configured == null)
		{
			return fallback;
		}
		if (!"true".equalsIgnoreCase(configured) && !"false".equalsIgnoreCase(configured))
		{
			throw new IllegalArgumentException(name + " must be true or false: " + configured);
		}
		return Boolean.parseBoolean(configured);
	}
}