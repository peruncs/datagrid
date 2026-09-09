# DataGrid Aeron transport

`storage-distributed-aeron` is an optional provider. The neutral
`storage-distributed` artifact does not depend on Aeron; applications choose
this module or the existing Kafka provider.

The transport keeps Eclipse Serializer/Eclipse Store `Binary` bytes opaque and
adds a 64-byte versioned envelope for cluster identity, sequence, chunking,
CRC32C, and commit/abort markers. A writer should use
`AeronStorageBinaryTargetDistributing` with an
`AeronReplicationWriteCoordinator` so the ordering is:

```text
Archive prepare chunks -> local Store enqueue -> Archive commit
```

For a recorded writer publication:

```java
try (AeronArchiveReplicationPublisher writer =
         AeronArchiveReplicationPublisher.New(
             archive, liveChannel, 1001, configuration, clusterId, epoch, sequence)) {
    AeronReplicationWriteCoordinator coordinator =
        new AeronReplicationWriteCoordinator(writer.replicationPublisher());
    // install coordinator in AeronStorageBinaryTargetDistributing
}
```

Readers use `StorageBinaryDataClientAeronArchive.New(...)` for replay, live
join, and reconnect. The live-only reader is retained under test sources for
low-level UDP coverage, not shipped as a production API. Persist the neutral
DataGrid cursor/checkpoint after each completed commit. `AeronReplicationCheckpointStore` is provided for
deployments that persist the Aeron-specific identity and replay boundary;
the neutral nodelibrary callback remains the integration point.

The envelope is deliberately not an SBE-generated second payload format:
Eclipse Serializer's `Binary` bytes remain the authoritative Store payload,
while the fixed header supplies only framing and validation. Configure
`AeronReplicationConfiguration` (or the provider environment variables) for
term length, MTU, chunk size, transaction limit, and offer timeout. Chunk size
must remain below `min(termLength / 8, 16 MiB) - 64`; Aeron fragments each
envelope as needed for the selected MTU.

CRC32C detects corruption but does not authenticate a sender. Bind UDP and
Archive-control channels to private interfaces and restrict them with firewall
or network-policy rules; do not enable ACK-driven retention on an untrusted
network. Transport-level authentication/encryption must be supplied by the
deployment (for example, an authenticated network overlay).

Run the transport and UDP/Archive integration tests with:

```text
mvn -pl storage/distributed/aeron -am verify
```
