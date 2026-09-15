# peruncs-cache

Clustered Hibernate second-level cache over Aeron (`peruncs.datagrid.cache`).

`ClusteredCacheRegionFactory` extends the Store Hibernate region factory. At
session-factory preparation it builds one serializer, one
`AeronClusteredCacheMessageComProvider`, one receiver, and one listener
configuration; the receiver starts before local cache events are redirected
to the cluster. On release it closes those resources before the local caches.

## Configuration

Settings live in the Hibernate cache properties under the `aeron.*`
clustered-cache keys:

- `channel` (default `aeron:ipc`, single-host), `stream-id`
- `node-id` (optional UUID shared by every provider of one node)
- `directory`, `embedded-driver`, `driver-timeout-millis`
- `offer-timeout-millis` (default 5000), `max-payload-bytes`

Multi-host deployments must configure a UDP channel with
`control-mode=dynamic`. Channels carry no authentication and must sit on an
isolated network.

## Failure behavior

- The sender is synchronous: a publish that cannot be accepted within the
  offer timeout fails the local cache write. Nothing is silently dropped.
- The receiver fails closed on malformed frames, unappliable messages, or
  sender sequence gaps. The timestamps region then refuses all operations
  rather than serving stale query results.

## Tests

`mvn -pl cache -am test`. The provider suite runs a real embedded
MediaDriver over `aeron:ipc`.

## Design

The architectural decisions — volatile broadcast, synchronous fail-fast
sending, fail-closed receiving — are recorded in the
[module documentation](src/main/java/module-info.java).
