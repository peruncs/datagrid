# ADR 0004: Quorum-gated Archive retention

Status: accepted.

## Context

Deleting Archive segments a slow reader still needs destroys data that no
replay can recover.

## Decision

Readers advertise their durable boundary as HMAC-signed watermarks on a
dedicated stream. The writer deletes history only through the complete
configured reader quorum: it finds the least advanced reader, pauses
admission, stops the recording, purges only complete segments, and extends
the same recording at its exact stop position. Without the shared secret, an
incomplete quorum, or during an active replay, history is preserved and
retention reports unsupported.

## Consequences

- Retention can never outrun the slowest configured reader.
- The quorum, not any single maintenance request, authorizes deletion.
