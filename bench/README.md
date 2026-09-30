# Benchmarks

Run the F1 parser comparison with:

```sh
./bench/run-f1.sh 'peruncs.cluster.storage.index.EntityHeadersBenchmark' -f 2 -wi 5 -i 5 -w 1s -r 1s -rf json -rff target/f1.json
```

Limit the run to the two acceptance sizes with `-p payloadBytes=65536,1048576`.
`-Pbench` adds `src/bench/java` to test compilation and supplies JMH; the runner
builds the test classpath and starts JMH with the Java 27 runtime flags used by
the project.

The benchmark captures one real Store commit containing the requested byte-array payload and its
replication mark. It measures the production writer prefilter against Serializer's raw walk. Writer
bytes are Store-generated; received reader bytes still use the separate bounded scanner before
Serializer's native materializer. For each payload size, compare
`productionWriterCommitPrefilter` with `serializerRawWriterCommitScan`; the gate passes when
prefilter throughput is at least 95% of the raw scan.

Measure pool reuse and fresh arena allocations with:

```sh
./bench/run-f1.sh 'peruncs.cluster.storage.binary.NativeBufferPoolBenchmark' -f 2 -wi 5 -i 5 -w 1s -r 1s -prof gc -rf json -rff target/f2.json
```
