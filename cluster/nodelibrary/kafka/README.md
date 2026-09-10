# DataGrid cluster Kafka provider

`cluster-nodelibrary-kafka` contains the existing Kafka distributor, reader,
position lookup, and retention implementations behind the neutral
`cluster-nodelibrary` SPI. It remains the compatibility provider when the
legacy `MSCNL_KAFKA_TOPIC_NAME` environment key is present, or can be selected
explicitly with:

```text
ECLIPSE_DATAGRID_REPLICATION_TRANSPORT=kafka
```

Applications that select Aeron do not need this module.

The Kafka replication topic is a single ordered log. Create it with exactly one
partition and do not increase its partition count after startup. The provider
rejects any other topology at producer, consumer, position, and retention
boundaries. When `MY_POD_NAME` is not set, each reader receives a unique
consumer-group identity for the lifetime of its JVM; this fallback is intended
for local development only. Production deployments must set `MY_POD_NAME` (or
another stable pod identity) so restarts reuse the same Kafka group.

The `writer`, `reader`, and `backup-reader` replication roles are accepted;
backup readers use the same ordered reader path and retain their durable cursor.

The provider's regression coverage is broker-free unit coverage for cursor
validation, configuration loading, retention fencing, and failure handling.
Broker-backed Kafka integration tests are intentionally out of scope for this
module; deployers should verify broker ACLs, topic creation, and partition
immutability in their environment.
