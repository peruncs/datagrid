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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Comparator;
import java.util.EnumSet;
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
        private static final System.Logger LOG = System.getLogger(KafkaPropertiesProvider.class.getName());
        private final Path configDirectoryPath;
		private volatile Properties properties;

        private Default(final Path configDirectoryPath)
        {
            this.configDirectoryPath = configDirectoryPath;
        }

        @Override
		public void init() throws NodelibraryException
		{
			if (!Files.isDirectory(this.configDirectoryPath, LinkOption.NOFOLLOW_LINKS))
			{
				throw new NodelibraryException("Kafka configuration path is not a directory");
			}
			this.verifyOwnerOnly(this.configDirectoryPath, "Kafka configuration directory");
			final List<Path> configFiles;
			try (final var configDirectoryStream = Files.list(this.configDirectoryPath))
			{
				configFiles = configDirectoryStream
					.filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
					.filter(path -> path.getFileName().toString().endsWith(".properties"))
					.sorted(Comparator.comparing(Path::toString))
					.toList();
			}
			catch (final IOException e)
			{
				throw new NodelibraryException("Failed to list Kafka configuration files", e);
			}
			if (configFiles.isEmpty())
			{
				throw new NodelibraryException("No Kafka configuration files found");
			}

			final Properties loaded = new Properties();
			for (final Path configFile : configFiles)
			{
				this.verifyOwnerOnly(configFile, "Kafka configuration file");
				try
				{
					if (Files.size(configFile) > 1L << 20)
					{
						throw new NodelibraryException("Kafka properties file is too large");
					}
				}
				catch (final IOException e)
				{
					throw new NodelibraryException("Failed to inspect Kafka properties file", e);
				}
				LOG.log(System.Logger.Level.DEBUG, "Reading Kafka configuration file");
				try (final var fileInputStream = Files.newInputStream(configFile, LinkOption.NOFOLLOW_LINKS))
				{
					loaded.load(fileInputStream);
				}
				catch (final IOException e)
				{
					throw new NodelibraryException("Failed to load Kafka properties configuration file", e);
				}
			}
			final String bootstrapServers = loaded.getProperty("bootstrap.servers");
			if (bootstrapServers == null || bootstrapServers.isBlank())
			{
				throw new NodelibraryException("Kafka configuration must define bootstrap.servers");
			}
			this.properties = loaded;
		}

		private void verifyOwnerOnly(final Path path, final String description) throws NodelibraryException
		{
			try
			{
				final var permissions = Files.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS);
				final var forbidden = EnumSet.of(
					PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE,
					PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ,
					PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE);
				forbidden.retainAll(permissions);
				if (!forbidden.isEmpty())
				{
					throw new NodelibraryException(description + " must not be accessible by group or other users");
				}
			}
			catch (final UnsupportedOperationException ignored)
			{
				/* Non-POSIX file systems have no equivalent permission model. */
			}
			catch (final IOException e)
			{
				throw new NodelibraryException("Unable to verify Kafka configuration permissions", e);
			}
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
