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


import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.eclipse.serializer.util.X.notNull;

/**
 * This client moves backup archives through the remote backup service.
 *
 * <p>It translates HTTP failures and malformed metadata into
 * {@link org.eclipse.datagrid.cluster.nodelibrary.exceptions.NodelibraryException}.
 * The caller owns the paths passed to upload and download.</p>
 */
public interface BackupProxyHttpClient
{
	/** Uploads a backup file.
	 *
	 * @param s3Key remote backup key
	 * @param filePath local file path
	 * @throws NodelibraryException if upload fails
	 */
	void upload(final String s3Key, final Path filePath) throws NodelibraryException;

	/** Deletes a remote backup.
	 *
	 * @param s3Key remote backup key
	 * @throws NodelibraryException if deletion fails
	 */
	void delete(final String s3Key) throws NodelibraryException;

	/** Downloads a remote backup.
	 *
	 * @param s3Key remote backup key
	 * @param destinationFilePath local destination path
	 * @return destination path
	 * @throws NodelibraryException if download fails
	 */
	Path download(final String s3Key, final Path destinationFilePath) throws NodelibraryException;

	/** Lists remote backups.
	 *
	 * @return remote backup metadata
	 * @throws NodelibraryException if listing fails
	 */
	List<BackupMetadataDto> list() throws NodelibraryException;

	/** Creates a client for a backup service.
	 *
	 * @param baseUri backup service base URI
	 * @return backup client
	 */
	static BackupProxyHttpClient New(final URI baseUri)
	{
		return new Default(notNull(baseUri), 3, Duration.ofMillis(100L), Duration.ofSeconds(30L), Duration.ofSeconds(10L));
	}

	/** Creates a client with explicit retry and timeout policy.
	 *
	 * @param baseUri backup service base URI
	 * @param maxAttempts maximum number of attempts for one request
	 * @param retryBackoff delay between retryable failures
	 * @param requestTimeout per-request timeout
	 * @param connectTimeout connection-establishment timeout
	 * @return backup client
	 * @throws IllegalArgumentException if an attempt count or duration is invalid
	 */
	static BackupProxyHttpClient New(
		final URI baseUri,
		final int maxAttempts,
		final Duration retryBackoff,
		final Duration requestTimeout,
		final Duration connectTimeout
	)
	{
		return new Default(notNull(baseUri), maxAttempts, retryBackoff, requestTimeout, connectTimeout);
	}

	/** Implements backup transfers with the JDK HTTP client. */
	final class Default implements BackupProxyHttpClient
	{
		private static final Logger LOG = LoggerFactory.getLogger(BackupProxyHttpClient.class);
		private static final int MAX_METADATA_RESPONSE_BYTES = 1 << 20;
		private static final Pattern SAFE_OBJECT_KEY = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]*");
		private static final long MAX_RETRY_AFTER_SECONDS = 300L;

		private final HttpClient http;
		private final ObjectMapper mapper = new ObjectMapper();
		private final URI baseUri;
		private final int maxAttempts;
		private final Duration retryBackoff;
		private final Duration requestTimeout;

		private Default(final URI baseUri, final int maxAttempts, final Duration retryBackoff,
			final Duration requestTimeout, final Duration connectTimeout)
		{
			if (maxAttempts <= 0 || retryBackoff.isNegative() || requestTimeout.isNegative() || requestTimeout.isZero() ||
				connectTimeout.isNegative() || connectTimeout.isZero())
			{
				throw new IllegalArgumentException("invalid backup HTTP retry or timeout policy");
			}
			this.baseUri = normalizeBaseUri(baseUri);
			this.maxAttempts = maxAttempts;
			this.retryBackoff = retryBackoff;
			this.requestTimeout = requestTimeout;
			this.http = HttpClient.newBuilder()
				.connectTimeout(connectTimeout)
				.build();
		}

		private static URI normalizeBaseUri(final URI baseUri)
		{
			if (baseUri.getScheme() == null ||
				(!"http".equalsIgnoreCase(baseUri.getScheme()) && !"https".equalsIgnoreCase(baseUri.getScheme())) ||
			baseUri.getHost() == null || baseUri.getUserInfo() != null ||
			baseUri.getQuery() != null || baseUri.getFragment() != null)
			{
				throw new IllegalArgumentException("backup proxy URI must be an http(s) origin without user info, query, or fragment");
			}
			final String text = baseUri.toString();
			return URI.create(text.endsWith("/") ? text : text + "/");
		}

		@Override
		public void upload(final String s3Key, final Path filePath) throws NodelibraryException
		{
			LOG.trace("Uploading compressed storage archive");

			final HttpResponse<Void> res;
			try
			{
				res = this.send(
					HttpRequest.newBuilder()
						.PUT(BodyPublishers.ofFile(filePath))
						.uri(this.uri(s3Key))
						.timeout(this.requestTimeout)
						.build(),
					BodyHandlers.discarding(), "upload storage");
			}
			catch (final IOException e)
			{
				throw new NodelibraryException("Failed to open upload storage", e);
			}

			this.validateOkResponse(res.statusCode());
		}

		@Override
		public void delete(final String s3Key) throws NodelibraryException
		{
			final HttpResponse<Void> res = this.send(
				HttpRequest.newBuilder().DELETE().uri(this.uri(s3Key)).timeout(this.requestTimeout).build(),
				BodyHandlers.discarding(), "delete storage archive");

			this.validateOkResponse(res.statusCode());
		}

		@Override
		public Path download(final String s3Key, final Path destinationFilePath) throws NodelibraryException
		{
			LOG.trace("Downloading storage archive");

			try
			{
				final Path parent = destinationFilePath.toAbsolutePath().getParent();
				if (parent != null) Files.createDirectories(parent);
			}
			catch (final IOException e)
			{
				throw new NodelibraryException("Failed to create download destination", e);
			}
			final Path absoluteDestination = destinationFilePath.toAbsolutePath();
			final Path temporary = absoluteDestination.resolveSibling(
				absoluteDestination.getFileName() + ".part-" + UUID.randomUUID());
			boolean installed = false;
			try
			{
				final HttpResponse<Path> res = this.send(
					HttpRequest.newBuilder().GET().uri(this.uri(s3Key)).timeout(this.requestTimeout).build(),
					BodyHandlers.ofFile(temporary, StandardOpenOption.CREATE,
						StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE), "download storage");
				this.validateOkResponse(res.statusCode());
				try
				{
					Files.move(temporary, absoluteDestination, StandardCopyOption.ATOMIC_MOVE,
						StandardCopyOption.REPLACE_EXISTING);
				}
				catch (final AtomicMoveNotSupportedException unsupported)
				{
					Files.move(temporary, absoluteDestination, StandardCopyOption.REPLACE_EXISTING);
				}
				installed = true;
				return absoluteDestination;
			}
			catch (final IOException failure)
			{
				throw new NodelibraryException("Failed to install downloaded storage archive", failure);
			}
			finally
			{
				if (!installed)
				{
					try { Files.deleteIfExists(temporary); }
					catch (final IOException cleanupFailure) { LOG.warn("Failed to remove partial backup download {}", temporary, cleanupFailure); }
				}
			}
		}

		@Override
		public List<BackupMetadataDto> list() throws NodelibraryException
		{
			final HttpResponse<InputStream> res = this.send(
				HttpRequest.newBuilder().uri(this.baseUri).timeout(this.requestTimeout).build(),
				BodyHandlers.ofInputStream(), "list backups");

			final byte[] body;
		try (InputStream input = res.body())
			{
				if (res.statusCode() != 200)
				{
					throw new NodelibraryException(
						"Unexpected response from backup proxy. Status Code: " + res.statusCode());
				}
				body = readMetadata(input);
			}
		catch (final IOException failure)
		{
			throw new NodelibraryException("Failed to read backup metadata response body", failure);
		}

			try
			{
				return this.mapper.readValue(
					body,
					this.mapper.getTypeFactory().constructCollectionType(ArrayList.class, BackupMetadataDto.class)
				);
			}
			catch (final IOException e)
			{
				throw new NodelibraryException("Failed to parse backup metadata list response body", e);
			}
		}

		private static byte[] readMetadata(final InputStream input) throws IOException
		{
			final ByteArrayOutputStream output = new ByteArrayOutputStream();
			final byte[] buffer = new byte[8192];
			int total = 0;
			int read;
			while ((read = input.read(buffer)) != -1)
			{
				if (read > MAX_METADATA_RESPONSE_BYTES - total)
				{
					throw new NodelibraryException(
						"Backup metadata response exceeds " + MAX_METADATA_RESPONSE_BYTES + " bytes");
				}
				output.write(buffer, 0, read);
				total += read;
			}
			return output.toByteArray();
		}

		private void validateOkResponse(final int statusCode)
		{
			if (statusCode / 100 != 2)
			{
				throw new NodelibraryException("Unexpected response from backup proxy. Status Code: " + statusCode);
			}
		}

		private <T> HttpResponse<T> send(
			final HttpRequest request,
			final HttpResponse.BodyHandler<T> bodyHandler,
			final String operation
		) throws NodelibraryException
		{
			IOException lastFailure = null;
			for (int attempt = 1; attempt <= this.maxAttempts; attempt++)
			{
				try
				{
					final HttpResponse<T> response = this.http.send(request, bodyHandler);
					if (!isRetryable(response.statusCode()) || attempt == this.maxAttempts) return response;
					closeRetryBody(response.body(), operation);
					this.sleepBeforeRetry(response, attempt, operation);
					continue;
				}
				catch (final InterruptedException e)
				{
					Thread.currentThread().interrupt();
					throw new NodelibraryException("Interrupted while attempting to " + operation, e);
				}
				catch (final IOException e)
				{
					lastFailure = e;
					if (attempt == this.maxAttempts)
					{
						throw new NodelibraryException("Failed to " + operation, e);
					}
				}
				try
				{
					TimeUnit.MILLISECONDS.sleep(this.backoffMillis(attempt));
				}
				catch (final InterruptedException e)
				{
					Thread.currentThread().interrupt();
					throw new NodelibraryException("Interrupted while retrying " + operation, e);
				}
			}
			throw new NodelibraryException("Failed to " + operation, lastFailure);
		}

		private static void closeRetryBody(final Object body, final String operation)
			throws NodelibraryException
		{
			if (!(body instanceof AutoCloseable closeable)) return;
			try
			{
				closeable.close();
			}
			catch (final Exception failure)
			{
				throw new NodelibraryException("Failed to close retry response for " + operation, failure);
			}
		}

		private void sleepBeforeRetry(final HttpResponse<?> response, final int attempt, final String operation)
			throws NodelibraryException
		{
			try
			{
				final long retryAfter = response.headers().firstValue("Retry-After")
					.map(Default::retryAfterMillis)
					.orElse(-1L);
				final long delay = retryAfter < 0L ? this.backoffMillis(attempt) : retryAfter;
				TimeUnit.MILLISECONDS.sleep(delay);
			}
			catch (final InterruptedException e)
			{
				Thread.currentThread().interrupt();
				throw new NodelibraryException("Interrupted while retrying " + operation, e);
			}
		}

		private long backoffMillis(final int attempt)
		{
			final long base = this.retryBackoff.toMillis();
			return base > Long.MAX_VALUE / attempt ? Long.MAX_VALUE : base * attempt;
		}

		private static long retryAfterMillis(final String value)
		{
			try
			{
				final long seconds = Long.parseLong(value.trim());
				if (seconds < 0L) return -1L;
				return seconds > MAX_RETRY_AFTER_SECONDS
					? TimeUnit.SECONDS.toMillis(MAX_RETRY_AFTER_SECONDS)
					: TimeUnit.SECONDS.toMillis(seconds);
			}
			catch (final NumberFormatException ignored)
			{
				return -1L;
			}
		}

		private static boolean isRetryable(final int statusCode)
		{
			return statusCode == 408 || statusCode == 429 || statusCode >= 500;
		}

		private URI uri(final String key)
		{
			if (key == null || !SAFE_OBJECT_KEY.matcher(key).matches() || key.equals(".") || key.equals(".."))
			{
				throw new IllegalArgumentException("invalid backup object key");
			}
			return this.baseUri.resolve(key);
		}
	}
}
