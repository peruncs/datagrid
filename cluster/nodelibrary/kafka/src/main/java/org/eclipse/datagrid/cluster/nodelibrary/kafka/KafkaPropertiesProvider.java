package org.eclipse.datagrid.cluster.nodelibrary.kafka;

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

import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import static org.eclipse.serializer.util.X.notNull;

/** Loads and snapshots the Kafka client properties used by a transport. */
public interface KafkaPropertiesProvider
{
	/** Loads the provider's properties.
	 * @throws NodelibraryException if loading fails
	 */
    void init() throws NodelibraryException;

	/** Returns a defensive properties snapshot.
	 * @return Kafka properties
	 */
    Properties provide();

	/** Creates a provider for the default configuration directory.
	 * @return properties provider
	 */
    static KafkaPropertiesProvider ConfigDirectory()
    {
        return ConfigDirectory(Defaults.configDirectoryPath());
    }

	/** Creates a provider for a configuration directory.
	 * @param configDirectoryPath configuration directory
	 * @return properties provider
	 */
	static KafkaPropertiesProvider ConfigDirectory(final Path configDirectoryPath)
    {
        return new Default(notNull(configDirectoryPath));
    }

    /** Supplies the default location used by the configuration-directory factory. */
    interface Defaults
    {
		/** Returns the default configuration directory.
		 * @return default directory
		 */
        static Path configDirectoryPath()
        {
            return Paths.get("/kafka");
        }
    }

    /** Loads properties once and returns defensive snapshots to clients. */
    final class Default implements KafkaPropertiesProvider
    {
        private static final Logger LOG = LoggerFactory.getLogger(KafkaPropertiesProvider.class);
        private final Path configDirectoryPath;
		private volatile Properties properties;

        private Default(final Path configDirectoryPath)
        {
            this.configDirectoryPath = configDirectoryPath;
        }

        @Override
		public void init() throws NodelibraryException
		{
			final List<Path> configFiles;
			try (final var configDirectoryStream = Files.list(this.configDirectoryPath))
			{
				configFiles = configDirectoryStream
					.filter(Files::isRegularFile)
					.sorted(Comparator.comparing(Path::toString))
					.toList();
			}
			catch (final IOException e)
			{
				throw new NodelibraryException("Failed to list Kafka config files at " + this.configDirectoryPath, e);
			}
			if (configFiles.isEmpty())
			{
				throw new NodelibraryException("No Kafka configuration files found at " + this.configDirectoryPath);
			}

			final Properties loaded = new Properties();
			for (final Path configFile : configFiles)
			{
				LOG.debug("Reading Kafka config file at {}", configFile);
				try (final var fileInputStream = new FileInputStream(configFile.toFile()))
				{
					loaded.load(fileInputStream);
				}
				catch (final IOException e)
				{
					throw new NodelibraryException(
						"Failed to load Kafka properties config file at " + configFile, e
					);
				}
			}
			this.properties = loaded;
		}

        @Override
        public Properties provide()
        {
            if (this.properties == null)
            {
                throw new IllegalStateException("Properties file has not been initialized yet");
            }
            final var newProps = new Properties();
            newProps.putAll(this.properties);
            return newProps;
        }
    }
}
