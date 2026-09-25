package peruncs.cluster.node.aeron.crashtest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/// Typed, strict parser for a provider child outcome file.
record CrashOutcome(
        RecoveryPolicy policy,
        String health,
        String error,
        List<String> errorTypes,
        String checkpointState,
        Long sequence,
        Long recordingId,
        Long recordingPosition,
        Integer crc32c,
        boolean storeValid
) {
    static CrashOutcome parse(final String text) {
        final Map<String, String> values = new HashMap<>();
        final List<String> errorTypes = new ArrayList<>();
        for (final String line : text.split("\\R")) {
            if (line.isBlank()) continue;
            final int separator = line.indexOf('=');
            if (separator <= 0) throw new IllegalArgumentException("malformed crash outcome line: %s".formatted(line));
            final String key = line.substring(0, separator);
            if ("ERROR_TYPE".equals(key)) {
                /* The full cause chain, outermost first; repetition is the
                 * whole point, so it is collected, not keyed. */
                errorTypes.add(line.substring(separator + 1));
                continue;
            }
            if (values.put(key, line.substring(separator + 1)) != null) {
                throw new IllegalArgumentException("duplicate crash outcome field: %s".formatted(key));
            }
        }
        final RecoveryPolicy policy = RecoveryPolicy.valueOf(required(values, "OUTCOME"));
        return new CrashOutcome(
                policy,
                required(values, "HEALTH"),
                values.get("ERROR"),
                List.copyOf(errorTypes),
                values.get("CHECKPOINT_STATE"),
                optionalLong(values, "SEQUENCE"),
                optionalLong(values, "RECORDING_ID"),
                optionalLong(values, "RECORDING_POSITION"),
                optionalInt(values),
                Boolean.parseBoolean(required(values, "PROOF_STORE_VALID"))
        );
    }

    private static String required(final Map<String, String> values, final String key) {
        final String value = values.get(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("missing outcome field %s".formatted(key));
        return value;
    }

    private static Long optionalLong(final Map<String, String> values, final String key) {
        final String value = values.get(key);
        return value == null ? null : Long.valueOf(value);
    }

    private static Integer optionalInt(final Map<String, String> values) {
        final String value = values.get("CRC32C");
        return value == null ? null : (int) Long.parseUnsignedLong(value);
    }
}
