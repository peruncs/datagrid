# ADR 0003: Durable cursors and checkpoints

Status: accepted.

## Context

Late, restarted, or disconnected readers must resume exactly where the
cluster left off, and a restarted writer must prove which recording continues
its history.

## Decision

Every applied commit advances a durable `ReplicationCursor` (transport,
Store generation, logical sequence, provider position). Writers persist an
authenticated checkpoint binding cluster, Store generation, epoch, recording,
and sequence. Startup reconciles the two: a cursor from a different Store
generation never resumes an unrelated recording.

## Consequences

- Restarts are routine instead of reseeds, as long as the Archive and the
  cursor survive together.
- Cursor and checkpoint writes are CRC-protected and atomic; torn files are
  rejected rather than trusted.
