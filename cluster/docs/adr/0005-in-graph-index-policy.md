# ADR 0005: Indexes live inside the object graph

Status: accepted.

## Context

Replication ships the Store object graph. Anything kept outside it (files in
an application directory, on-disk index state) does not travel with a
transaction and diverges across nodes.

## Decision

Lucene data must live in the graph through the embedded GraphDirectory, and
vector data must use the persisted in-graph vector store. External Lucene
directories and on-disk vector indexes are rejected at the registration
boundary.

## Consequences

- Index state follows the same transaction as the entities it describes.
- Operators cannot trade correctness for out-of-graph index performance.
