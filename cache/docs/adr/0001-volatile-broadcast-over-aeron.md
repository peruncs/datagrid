# ADR 0001: Volatile Aeron broadcast for cache invalidation

Status: accepted.

## Context

Clustered Hibernate nodes must learn when a peer updates query timestamps.
Invalidations are small, frequent, and idempotent-ish: each carries a table
name and a timestamp, and only the newest value matters.

## Decision

Carry invalidations on a volatile Aeron N-writer/N-reader broadcast. Each
frame holds a 16-byte sender identity, a per-sender sequence, and an Eclipse
Serializer payload. A node ignores frames carrying its own identity, or the
identity shared through a configured `node-id`. There is no log, no replay,
and no persistence.

## Consequences

- No broker or archive to operate on the invalidation path.
- A receiver that is down misses invalidations, so any detected loss must
  fail the node closed (see ADR 0003) rather than continue on stale state.
- Delivery has no durability guarantee and is never used to recover Store
  state.
