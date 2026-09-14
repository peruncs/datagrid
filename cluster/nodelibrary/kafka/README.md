# DataGrid cluster Kafka provider

`cluster-nodelibrary-kafka` contains the existing Kafka distributor, reader,
position lookup, and retention implementations behind the neutral
`cluster-nodelibrary` SPI. Select it explicitly with:

```text
ECLIPSE_DATAGRID_REPLICATION_TRANSPORT=kafka
```

Applications that select Aeron do not need this module. The provider uses the
same `storage-distributed` packet model as the neutral nodelibrary. The
independent `storage-distributed-kafka` artifact remains available for clients
that use the neutral storage API directly. It is maintained separately and is
not a drop-in replacement for this cluster provider; select one adapter
consistently for a deployment.

The Kafka replication topic is a single ordered log. Create it with exactly one
partition and do not increase its partition count after startup. The provider
rejects any other topology at producer, consumer, position, and retention
boundaries. The default consumer-group identity uses `MY_POD_NAME`, the
`eclipse.datagrid.node-id` system property, or the local hostname. Configure
`ECLIPSE_DATAGRID_NODE_ID` (or an explicit group id) when the hostname is not a
stable deployment identity; production readers fail fast when no stable
identity is available. The `writer`, `reader`, and `backup-reader` replication
roles are accepted;
backup readers use the same ordered reader path and retain their durable cursor.

The provider's regression coverage is broker-free unit coverage for cursor
validation, configuration loading, retention fencing, and failure handling.
Broker-backed Kafka integration tests are intentionally out of scope for this
module; deployers should verify broker ACLs, topic creation, and partition
immutability in their environment.
