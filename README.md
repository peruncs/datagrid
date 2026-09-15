# Peruncs Data Grid

Peruncs Data Grid is an in-memory data processing layer to speed up database
applications and relieve the database. It combines distributed caching,
high-speed in-memory searching, and complex data manipulation on the native
Java object model, persisted transaction-safe by Eclipse Store and moved
between nodes over Aeron. It is based on a single-writer approach: each node
is fully consistent locally, while the cluster model is eventual consistency.

## Modules

| Module | Artifact | Contents |
|---|---|---|
| [`cache`](cache/README.md) | `peruncs-cache` | Clustered Hibernate second-level cache over Aeron |
| [`cluster`](cluster/README.md) | `peruncs-cluster` | Node lifecycle, backup, Store replication, and indexes over Aeron |

Each module keeps its own `README.md` and architecture decision records in
`docs/adr/`. `cache` and `cluster` are independent; `cluster` is
self-contained.

## Build

Requires Java 26 and Maven 3.9+.

```bash
mvn test
mvn package
```

The checkout is aligned with the locally installed Eclipse Store/Serializer
`5.0.0-SNAPSHOT` artifacts; the snapshot should still be treated as
pre-release.

## Use

```xml
<dependency>
    <groupId>peruncs</groupId>
    <artifactId>peruncs-cluster</artifactId>
</dependency>
<dependency>
    <groupId>peruncs</groupId>
    <artifactId>peruncs-cache</artifactId>
</dependency>
```
