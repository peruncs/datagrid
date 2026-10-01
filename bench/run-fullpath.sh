#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
mvn -B -DskipTests test-compile dependency:build-classpath \
  -Dmdep.includeScope=test -Dmdep.outputFile=target/fullpath-classpath.txt

classpath="target/test-classes:target/classes:$(cat target/fullpath-classpath.txt)"
exec java --enable-preview --add-modules jdk.incubator.vector,jdk.jfr \
  --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
  -cp "$classpath" peruncs.cluster.node.aeron.AeronFullPathBenchmark "$@"
