# Benchmarks

Run the F1 parser comparison with:

```sh
./bench/run-f1.sh -f 2 -wi 5 -i 5 -w 1s -r 1s -rf json -rff target/f1.json
python3 bench/compare-f1.py target/f1.json
```

Limit the run to the two acceptance sizes with `-p payloadBytes=65536,1048576`.
`-Pbench` adds `src/bench/java` to test compilation and supplies JMH; the runner
builds the test classpath and starts JMH with the Java 27 runtime flags used by
the project.

The benchmark captures one real Serializer commit containing the requested byte-array
payload, runs the production `Binary` header scanner over it, and compares it with Serializer's raw
iterator. Both sides checksum the same type and object ids. The production `Binary` path gates the
selected implementation. The comparator fails when it is more than 5% slower than Serializer's raw
scan.
