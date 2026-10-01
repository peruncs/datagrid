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

Run the D-25 full-path comparison on the historical `9975d95` checkout and the current tree with
the same Java runtime and machine:

```sh
./bench/run-fullpath.sh --commit=9975d95 --output=bench/results/9975d95.json
./bench/run-fullpath.sh --commit=working-tree --output=bench/results/working-tree.json
./bench/compare bench/results/9975d95.json bench/results/working-tree.json
```

The full-path runner defaults to the A1.10 workload: four concurrent Store callers, three real
Aeron readers, a 60-second warm-up, five 60-second windows, and 1 KiB / 64 KiB payloads. It writes
writer p50/p99 and throughput, per-reader import-start-to-mark p99, writer-return-to-visible-mark
p99, read-boundary p99 at idle and under write load, and a JFR event/GC summary. Short overrides
are for runner checks only; `bench/compare` rejects results that do not use the acceptance workload.
On developer machines, compare like-for-like environment metadata. Linux/NVMe execution is waived.

Measure pool reuse and fresh arena allocations with:

```sh
./bench/run-f1.sh 'peruncs.cluster.storage.binary.NativeBufferPoolBenchmark' -f 2 -wi 5 -i 5 -w 1s -r 1s -prof gc -rf json -rff target/f2.json
```

Compare staged and gathered Aeron offers with four source buffers using:

```sh
./bench/run-f1.sh 'peruncs.cluster.storage.aeron.writer.AeronGatherOfferBenchmark' \
  -p payloadBytes=65536,131072,262144,1048576 -p sourceBufferCount=4 \
  -f 3 -wi 3 -i 5 -w 1s -r 1s -rf json -rff target/p1-6.json
```

The trial setup checks both wire bytes and CRC results. The benchmark includes transaction and
chunk CRCs, a staging copy or reusable vectors, and Aeron's term-buffer copy loop; it excludes the
driver, back pressure, and network. P1-6 uses gathering at or above 128 KiB based on this comparison.
