# ADR 0001: Archive-first Store replication

Status: accepted.

## Context

Readers must never apply a transaction the writer did not durably record,
and a restarted writer must never leave readers pointed at bytes that no
longer exist.

## Decision

Every write follows Archive prepare chunks, then the local Store enqueue,
then the Archive commit. The transport keeps Eclipse Serializer `Binary`
bytes opaque behind a versioned envelope carrying only cluster identity,
sequence, chunking, CRC32C, and commit/abort markers. Readers replay from
the Archive, join the live stream, and reconnect from a durable cursor.

## Consequences

- A recorded transaction always exists in the Archive before any reader can
  observe it.
- The wire format carries framing only; there is no second object-graph
  encoding to version or secure.
