package peruncs.datagrid.cache.types;

import java.util.Map;

/// Parses the string-valued Hibernate cache properties used by the Aeron transport.
///
/// Hibernate supplies the cache configuration as a raw `Map`; the Aeron
/// provider reads strings, bounded integers, longs, and booleans from it. This class
/// keeps that parsing in one place so every setting applies the same blank-value
/// and minimum rules.
public final class ClusteredCachePropertyParsers {
    private ClusteredCachePropertyParsers() {
    }

        /// Returns the trimmed value of a property, or the fallback when it is
    /// absent or blank.
    ///
    /// @param properties source properties
    /// @param name       property name
    /// @param fallback   value when the property is absent or blank
    /// @return property value or fallback
    @SuppressWarnings("rawtypes")
    public static String stringProperty(final Map properties, final String name, final String fallback) {
        final Object configured = properties.get(name);
        if (configured == null) {
            return fallback;
        }
        final String value = configured.toString().trim();
        return value.isEmpty() ? fallback : value;
    }

        /// Returns a property as an integer no smaller than `minimum`.
    ///
    /// @param properties source properties
    /// @param name       property name
    /// @param fallback   value when the property is absent or blank
    /// @param minimum    smallest accepted value
    /// @return parsed value or fallback
    /// @throws IllegalArgumentException when the value is not an integer or is below the minimum
    @SuppressWarnings("rawtypes")
    public static int intProperty(final Map properties, final String name, final int fallback, final int minimum) {
        final String configured = stringProperty(properties, name, null);
        if (configured == null) {
            return fallback;
        }
        final int value;
        try {
            value = Integer.parseInt(configured);
        } catch (final NumberFormatException failure) {
            throw new IllegalArgumentException("%s must be an integer: %s".formatted(name, configured), failure);
        }
        if (value < minimum) {
            throw new IllegalArgumentException("%s must be at least %s: %s".formatted(name, minimum, value));
        }
        return value;
    }

        /// Returns a property as a long no smaller than `minimum`.
    ///
    /// @param properties source properties
    /// @param name       property name
    /// @param fallback   value when the property is absent or blank
    /// @param minimum    smallest accepted value
    /// @return parsed value or fallback
    /// @throws IllegalArgumentException when the value is not a long or is below the minimum
    @SuppressWarnings("rawtypes")
    public static long longProperty(final Map properties, final String name, final long fallback, final long minimum) {
        final String configured = stringProperty(properties, name, null);
        if (configured == null) {
            return fallback;
        }
        final long value;
        try {
            value = Long.parseLong(configured);
        } catch (final NumberFormatException failure) {
            throw new IllegalArgumentException("%s must be a long: %s".formatted(name, configured), failure);
        }
        if (value < minimum) {
            throw new IllegalArgumentException("%s must be at least %s: %s".formatted(name, minimum, value));
        }
        return value;
    }

        /// Returns a property as a boolean.
    ///
    /// @param properties source properties
    /// @param name       property name
    /// @param fallback   value when the property is absent or blank
    /// @return parsed value or fallback
    /// @throws IllegalArgumentException when the value is not `true` or `false`
    @SuppressWarnings("rawtypes")
    public static boolean booleanProperty(final Map properties, final String name, final boolean fallback) {
        final String configured = stringProperty(properties, name, null);
        if (configured == null) {
            return fallback;
        }
        if (!"true".equalsIgnoreCase(configured) && !"false".equalsIgnoreCase(configured)) {
            throw new IllegalArgumentException("%s must be true or false: %s".formatted(name, configured));
        }
        return Boolean.parseBoolean(configured);
    }
}
