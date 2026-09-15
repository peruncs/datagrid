# ADR 0003: Fail-closed invalidation receiver

Status: accepted.

## Context

The broadcast from ADR 0001 cannot prove a bad frame was harmless and cannot
retransmit a missed one. A receiver that kept serving after such an event
would hand Hibernate timestamps it can no longer trust.

## Decision

The receiver stops on the first malformed or undeserializable frame, on a
valid message the acceptor cannot apply, and on any sender sequence gap. It
retains the terminal failure, and the timestamps region refuses every cache
operation until the node is restarted into a clean state.

## Consequences

- Stale query results are impossible by construction after transport faults.
- Operators treat any receiver failure as a node restart, not a transient to
  ride out.
