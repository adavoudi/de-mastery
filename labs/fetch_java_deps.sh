#!/usr/bin/env bash
# Fetch the JARs the JVM track (JVM-01..06) and Kafka Streams (KFK-08) exercises
# compile+test against, into a gitignored labs/.jars/java/ directory, each
# verified against a pinned sha256 from Maven Central.
#
# WHY: Java exercises stay pytest-driven — the de_testkit `run_junit` helper
# javac-compiles the learner's .java plus JUnit 5 / AssertJ test sources against
# this fixed classpath and runs the JUnit Platform console launcher, all with
# the vendored JDK 21 (labs/.jdk). No network at test time, no Gradle needed for
# the ordinary case (Gradle is only for JVM-03 — see labs/fetch_gradle.sh).
#
# kafka-clients / kafka-streams / kafka-streams-test-utils are pinned to the
# SAME 4.3.1 line as the native broker tarball (labs/fetch_kafka.sh) so the
# TopologyTestDriver and the real brokers agree.
#
# `de doctor` reports whether these are present and prints this command as the fix.
#
# Usage:  bash labs/fetch_java_deps.sh [--check]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JARS_DIR="${HERE}/.jars/java"
MAVEN="${MAVEN:-https://repo1.maven.org/maven2}"

# name|relative-maven-path|sha256
JARS=(
  "junit-platform-console-standalone-1.11.4.jar|org/junit/platform/junit-platform-console-standalone/1.11.4/junit-platform-console-standalone-1.11.4.jar|b016ef6b1c3454d6d7c2c88ce081dabf289699686af6622d6e4e2e1b54b4a2fc"
  "assertj-core-3.27.3.jar|org/assertj/assertj-core/3.27.3/assertj-core-3.27.3.jar|5b8a26205f6d5ea60ad9ce65ce4a40a2afe4c48abeec61bd0740a088c24e89f5"
  "kafka-clients-4.3.1.jar|org/apache/kafka/kafka-clients/4.3.1/kafka-clients-4.3.1.jar|52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36"
  "kafka-streams-4.3.1.jar|org/apache/kafka/kafka-streams/4.3.1/kafka-streams-4.3.1.jar|bd73f3805e116f711e8456ee89a8a5b43ff5dcf5a5bb865afae29deae89e7c4b"
  "kafka-streams-test-utils-4.3.1.jar|org/apache/kafka/kafka-streams-test-utils/4.3.1/kafka-streams-test-utils-4.3.1.jar|e84547c830b86dffdc4f5556d9d58fd1de5020a820b0de34d837b27aa4452d8c"
  "avro-1.12.0.jar|org/apache/avro/avro/1.12.0/avro-1.12.0.jar|1f5fbfff9b7427d6832cc3031a3c81413d3e21d9e2551847d9231c28493f9492"
  "jackson-databind-2.18.2.jar|com/fasterxml/jackson/core/jackson-databind/2.18.2/jackson-databind-2.18.2.jar|4b364e6850dc89172fcf1d4dd26b8ff5488eda44ff4657e22dd265203dd5ab3c"
  "jackson-core-2.18.2.jar|com/fasterxml/jackson/core/jackson-core/2.18.2/jackson-core-2.18.2.jar|d8054ae7c0d1c2d2f55d28e46026ebe5892881f3fab5f439233184381c3b4a1f"
  "jackson-annotations-2.18.2.jar|com/fasterxml/jackson/core/jackson-annotations/2.18.2/jackson-annotations-2.18.2.jar|581bd61000ef7648943f781ca05689e56d03f6052748365a8e2b3a9b5d3fa32f"
  "slf4j-api-2.0.16.jar|org/slf4j/slf4j-api/2.0.16/slf4j-api-2.0.16.jar|a12578dde1ba00bd9b816d388a0b879928d00bab3c83c240f7013bf4196c579a"
  "slf4j-simple-2.0.16.jar|org/slf4j/slf4j-simple/2.0.16/slf4j-simple-2.0.16.jar|effc32018658bea09d1e08c7d1060ccad46c086960f583d07dd7ffe9c1172a47"
)

CHECK_ONLY=0
[ "${1:-}" = "--check" ] && CHECK_ONLY=1
mkdir -p "${JARS_DIR}"
missing=0

sha_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}';
  else shasum -a 256 "$1" | awk '{print $1}'; fi
}

for entry in "${JARS[@]}"; do
  name="${entry%%|*}"; rest="${entry#*|}"
  path="${rest%%|*}"; want="${rest##*|}"
  dest="${JARS_DIR}/${name}"
  if [ -f "${dest}" ] && [ "$(sha_of "${dest}")" = "${want}" ]; then
    echo "ok     ${name}"; continue
  fi
  if [ "${CHECK_ONLY}" = "1" ]; then echo "MISSING ${name}"; missing=1; continue; fi
  echo "fetch  ${name}"
  curl -fsSL -o "${dest}" "${MAVEN}/${path}"
  got="$(sha_of "${dest}")"
  if [ "${got}" != "${want}" ]; then
    echo "ERROR: sha256 mismatch for ${name}" >&2
    echo "  want ${want}" >&2
    echo "  got  ${got}" >&2
    rm -f "${dest}"; exit 1
  fi
  echo "ok     ${name} (verified)"
done

if [ "${CHECK_ONLY}" = "1" ] && [ "${missing}" = "1" ]; then
  echo "Some Java jars are missing. Run: bash labs/fetch_java_deps.sh" >&2
  exit 1
fi
echo "All Java jars present in ${JARS_DIR}"
