# ADR 0002: Synchronous, fail-fast invalidation sender

Status: accepted.

## Context

A JCache listener callback runs inside the local cache write. If the
invalidation is dropped while the local write succeeds, peers silently keep
stale timestamps.

## Decision

The sender offers each frame and waits until the publication accepts it,
retrying back pressure within the configured offer timeout (default 5 s). A
closed publication or an expired timeout raises
`CacheEntryListenerException`, failing the local cache operation. Publishing
one frame at a time under a per-identity sequence lock keeps the sender order
exact.

## Consequences

- Cache semantics never depend on transport timing: either peers can receive
  the invalidation or the write does not happen.
- A slow or unavailable Aeron driver blocks cache writes instead of degrading
  to stale reads.
