package org.eclipse.datagrid.cluster.nodelibrary.aeron.crashtest;

/*-
 * #%L
 * Eclipse Data Grid Cluster Nodelibrary Aeron Provider
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

import java.util.HashMap;
import java.util.Map;

/** Typed, strict parser for a provider child outcome file. */
record CrashOutcome(
	RecoveryPolicy policy,
	String health,
	String error,
	String checkpointState,
	Long sequence,
	Long recordingId,
	Long recordingPosition,
	Integer crc32c,
	boolean storeValid
)
{
	static CrashOutcome parse(final String text)
	{
		final Map<String, String> values = new HashMap<>();
		for (final String line : text.split("\\R"))
		{
			if (line.isBlank()) continue;
			final int separator = line.indexOf('=');
			if (separator <= 0) throw new IllegalArgumentException("malformed crash outcome line: " + line);
			if (values.put(line.substring(0, separator), line.substring(separator + 1)) != null)
			{
				throw new IllegalArgumentException("duplicate crash outcome field: " + line.substring(0, separator));
			}
		}
		final RecoveryPolicy policy = RecoveryPolicy.valueOf(required(values, "OUTCOME"));
		return new CrashOutcome(
			policy,
			required(values, "HEALTH"),
			values.get("ERROR"),
			values.get("CHECKPOINT_STATE"),
			optionalLong(values, "SEQUENCE"),
			optionalLong(values, "RECORDING_ID"),
			optionalLong(values, "RECORDING_POSITION"),
			optionalInt(values),
			Boolean.parseBoolean(required(values, "PROOF_STORE_VALID"))
		);
	}

	private static String required(final Map<String, String> values, final String key)
	{
		final String value = values.get(key);
		if (value == null || value.isBlank()) throw new IllegalArgumentException("missing outcome field " + key);
		return value;
	}

	private static Long optionalLong(final Map<String, String> values, final String key)
	{
		final String value = values.get(key);
		return value == null ? null : Long.valueOf(value);
	}

	private static Integer optionalInt(final Map<String, String> values)
	{
		final String value = values.get("CRC32C");
		return value == null ? null : (int)Long.parseUnsignedLong(value);
	}
}
