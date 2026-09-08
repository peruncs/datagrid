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
