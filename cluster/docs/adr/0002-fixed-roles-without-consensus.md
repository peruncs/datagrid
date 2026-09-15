# ADR 0002: Fixed roles without consensus

Status: accepted.

## Context

Automatic writer election and failover need fencing and consensus, which is a
separate system with its own failure modes.

## Decision

Roles are fixed at configuration: `writer`, `reader`, or `backup-reader`.
Setting the transport to `none` runs a node unreplicated. A reader owns a
persistent subscription with no writer publication path, so promotion is
rejected outright instead of producing a distributor that cannot replicate.

## Consequences

- Writer fencing and manual promotion stay deployment responsibilities.
- No split-brain handling exists in the transport; there is only ever the
  configured writer.
