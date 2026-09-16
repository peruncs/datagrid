# peruncs-cache

Clustered Hibernate second-level cache over Aeron (`peruncs.datagrid.cache`).

`ClusteredCacheRegionFactory` extends the Store Hibernate region factory. At
session-factory preparation it builds one serializer, one
`AeronClusteredCacheMessageCommunicationProvider`, one receiver, and one listener
configuration; the receiver starts before local cache events are redirected
to the cluster. On release it closes those resources before the local caches.

## Configuration

Settings live in the Hibernate cache properties under the
`hibernate.cache.eclipsestore.clustered.aeron.*` keys:

- `channel` (default `aeron:ipc`, single-host), `stream-id`
- `node-id` (optional UUID shared by every provider of one node)
- `directory`, `embedded-driver`, `driver-timeout-millis`
- `offer-timeout-millis` (default 5000), `max-payload-bytes`
- `heartbeat-interval-millis` and `freshness-timeout-millis`: idle senders
  publish sequence-consuming heartbeats and a receiver that hears nothing
  within the freshness timeout marks itself stale
- `cursor-directory`: where each receiver persists its per-sender sequence
  cursors so a restart validates its first sequence instead of trusting it
- `hmac-secret` / `hmac-secret-file` and `production-mode`,
  `allow-unsigned-frames`: frame authentication. Frames carry an HMAC tag
  when a secret is configured; production mode rejects unsigned frames unless
  they are explicitly allowed.
- `hmac-secret-previous` / `hmac-secret-previous-file`: the retiring key
  accepted during rotation overlap. Verification tries the primary first and
  falls back to this key while signing always uses the primary. Do not drop
  it as soon as every node runs the new primary: retain it until every
  receiver has drained past the rotation (freshness timeout plus a restart
  cycle), or bounce the receivers. The configuration object itself has no
  clear hook — its copies live with the region, while sender/receiver working
  copies are erased on close.

Multi-host deployments must configure a UDP channel with
`control-mode=dynamic` and a shared HMAC secret. The channel carries no
transport-level authentication, so it must still sit on an isolated network.
The secret authenticates holders, not nodes: any host with it can publish
valid-looking frames, so each environment needs its own secret.

## Failure behavior

- The sender is synchronous: a publish that cannot be accepted within the
  offer timeout fails the local cache write. Nothing is silently dropped.
- The receiver fails closed on malformed frames, a failed HMAC, unappliable
  messages, or sender sequence gaps. It also fails closed on silence past the
  freshness deadline, because a disconnected receiver cannot observe a missed
  frame unless a later one arrives.
- On start, a receiver invalidates the whole timestamps region before it is
  declared healthy, then validates its first sequence against the persisted
  cursor so invalidations missed while it was down are detected instead of
  trusted.
- The timestamps region refuses all operations after any of those terminal
  failures rather than serving stale query results.

## Tests

`mvn -pl peruncs-cache -am test`. The provider suite runs a real embedded
MediaDriver over `aeron:ipc`.

## Design

The architectural decisions — volatile broadcast, synchronous fail-fast
sending, fail-closed receiving — are recorded in the
[module documentation](src/main/java/module-info.java).
