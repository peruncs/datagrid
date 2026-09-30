#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
mvn -Pbench -DskipTests test-compile dependency:build-classpath \
  -Dmdep.includeScope=test -Dmdep.outputFile=target/f1-classpath.txt

classpath="target/test-classes:target/classes:$(cat target/f1-classpath.txt)"
exec java --enable-preview --add-modules jdk.incubator.vector \
  --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
  -cp "$classpath" org.openjdk.jmh.Main "$@"
