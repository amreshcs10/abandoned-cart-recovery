#!/usr/bin/env sh
# Build and run with only a JDK (17+). No Maven, Gradle or network needed.
#   ./run.sh          build, run the tests, then the walkthrough + soak
#   ./run.sh test     build and run the tests only
#   ./run.sh demo     build and run the walkthrough + soak only
set -e
cd "$(dirname "$0")"
rm -rf out out-test
javac --release 17 -d out $(find src/main -name '*.java')
# PipelineJUnitTest is for Maven; the JDK-only build runs the same scenarios via PipelineTests.
javac --release 17 -cp out -d out-test $(find src/test -name '*.java' ! -name '*JUnitTest.java')
case "${1:-all}" in
  test) java -cp out:out-test com.example.cartrecovery.PipelineTests ;;
  demo) java -cp out com.example.cartrecovery.verify.Verifier ;;
  *)    java -cp out:out-test com.example.cartrecovery.PipelineTests && echo && java -cp out com.example.cartrecovery.verify.Verifier ;;
esac
