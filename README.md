
# Eclipse Data Grid

## Description

Eclipse Data Grid is an in-memory data processing layer to speed up database applications and relieve the database.

Eclipse Data Grid can be used as a traditional distributed cache, but it is much more than a common cache. It enables you ultra-fast in-memory searching, as well as complex data processing, through the implementation of individual Java business logic. Unlike traditional caching solutions, which are built as key-value structures, Eclipse Data Grid is a native Java layer that utilizes the native Java object model. This allows you to work with native Java objects, Java types, and any complex Java object graphs, as well as integrate any Java libraries and implement complex logic in your in-memory data layer using Java.

Eclipse Data Grid is for everyone who needs an easy-to-use distributed cache, as well as for users who need much more than just a cache, combining caching, high-speed in-memory searching, and complex data manipulation by using Core Java concepts. Move your complex and performance-critical data and data operations to Eclipse Data Grid to significantly reduce database workloads and save costs, boost your application, and your business

Eclipse Data Grid is based on two other Eclipse projects:

- [Eclipse Serializer](https://github.com/eclipse-serializer/serializer)

    Powerful and highly secure Java serialization that enables dealing with any complex Java object graphs and avoids deserialization attacks by injecting and executing malicious code.

- [EclipseStore](https://github.com/eclipse-store/store)

  Java-native object graph persistence layer to store any complex Java object graphs or individual subgraphs transaction-safe into any binary data storage, and restore them in RAM on demand. Using a traditional database and thus OR-Mapping, JSON conversion, or any other mappings are completely superfluous. EclipseStore is ACID-compliant, provides lazy-loading, indexing, GigaMap for fully automated lazy-loading, and provides a smart concept for schema migration. EclipseStore is built as a persistence layer to be used for a single JVM run on a single node.

Eclipse Data Grid itself provides you with the code to generate a cluster environment to run, scale, and maintain an Eclipse Data Grid application based on Kubernetes, as well as important cluster features such as replication, elastic scale-out / scale-in, and backups. Eclipse Data Grid is based on a single-writer approach. While the consistency model on each cluster node is full consistency, the standard cluster consistency model is eventual consistency.

### Optional replication transports

The core cluster and storage artifacts are transport-neutral. Select exactly
one provider for Store binary replication:

- Kafka: `storage-distributed` plus `cluster-nodelibrary-kafka`.
- Aeron: `storage-distributed-aeron` plus `cluster-nodelibrary-aeron`, with
  `ECLIPSE_DATAGRID_REPLICATION_TRANSPORT=aeron` and a stable
  `ECLIPSE_DATAGRID_AERON_CLUSTER_ID`.

The Aeron provider embeds MediaDriver/Aeron Archive and uses reliable UDP; it
does not require Kafka infrastructure. The fixed-writer/no-consensus model is
intentional. See [the Aeron integration plan](docs/aeron-clustering-integration-plan.md)
and the provider READMEs for configuration and current production gates.

This checkout is aligned with the locally installed Eclipse Store/Serializer
`5.0.0-SNAPSHOT` artifacts. The replication tests assert Store 5's coalesced
type-dictionary export contract, while Serializer supplies the crash-safe
dictionary-file swap; the snapshot should still be treated as pre-release.

The provider is an explicit dependency; framework adapters do not pull Kafka or
Aeron transitively. For example, add the neutral SPI and one provider:

```xml
<dependency>
  <groupId>org.eclipse.datagrid</groupId>
  <artifactId>storage-distributed</artifactId>
</dependency>
<dependency>
  <groupId>org.eclipse.datagrid</groupId>
  <artifactId>storage-distributed-aeron</artifactId>
</dependency>
<dependency>
  <groupId>org.eclipse.datagrid</groupId>
  <artifactId>cluster-nodelibrary-aeron</artifactId>
</dependency>
```

Aeron transports the bytes produced by Eclipse Serializer directly. Its small
68-byte envelope carries only cluster/epoch/sequence, chunk, and CRC metadata;
there is no second SBE object-graph encoding layer. `term-length`, `mtu-length`,
`chunk-size`, `max-transaction-bytes`, `offer-timeout-nanos`, and durability
mode are configurable with the
`eclipsestore.distribution.aeron.*` provider properties and corresponding
`ECLIPSE_DATAGRID_AERON_*` environment variables. Aeron deployments must also
set an explicit `ECLIPSE_DATAGRID_REPLICATION_ROLE`; the provider refuses to
guess whether a node is a writer or reader.
Keep the same values on the writer and readers. Clustered text and vector
indexes must stay inside the Eclipse Store object graph so their state follows
the same Store transaction as the entities. The `storage-distributed-index`
module provides the supported registration API: Lucene uses an embedded
GraphDirectory and JVector uses its persisted vector store. External Lucene
directories and JVector on-disk indexes are rejected; they are not supported by
either Kafka or Aeron.
Writer restart safety additionally requires stable `ECLIPSE_DATAGRID_AERON_NODE_ID`,
`ECLIPSE_DATAGRID_AERON_STORE_GENERATION`, and a durable
`ECLIPSE_DATAGRID_AERON_CHECKPOINT_PATH`; archive and checkpoint directories
must be outside the MediaDriver directory.

Authenticated Archive retention additionally requires a shared secret on the
writer and every reader (`ECLIPSE_DATAGRID_AERON_RETENTION_SECRET`), plus the
fixed reader set on the writer (`ECLIPSE_DATAGRID_AERON_RETENTION_READERS`).
The secret may instead be supplied through
`ECLIPSE_DATAGRID_AERON_RETENTION_SECRET_FILE`, an owner-only regular file
containing the base64 key. Generate one 256-bit secret and configure the same
value on all nodes.
Without Java (openssl):

```bash
openssl rand -base64 32
```

```bash
export ECLIPSE_DATAGRID_AERON_RETENTION_SECRET="<generated value>"
```

Cluster monitoring exposes the same transport-neutral endpoint for every
framework adapter: `GET /eclipse-datagrid/replication-metrics`. It emits
Prometheus gauges for provider (`aeron`/`kafka`/`none`), current and latest
sequence, transaction lag, lifecycle state (`replaying`, `live`, `failed`,
etc.), readiness, and health. Aeron Archive/replay failures therefore appear
in the existing health/metrics surface instead of requiring Kafka-specific
monitoring.

## License

Eclipse Data Grid is available under [Eclipse Public License - v 2.0](LICENSE).
