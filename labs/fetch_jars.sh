#!/usr/bin/env bash
# Fetch the Spark runtime JARs that Iceberg/Delta exercise tests need, into a
# gitignored labs/.jars/ directory, each verified against a pinned sha256.
#
# WHY: tests must NOT resolve `--packages` at runtime (needs a writable ivy cache
# + network, and is non-deterministic). The `spark_iceberg` / Delta fixtures put
# these local JARs on the Spark classpath via `spark.jars`. `de doctor` reports
# whether they are present and prints this command as the fix.
#
# Versions are pinned to the Spark 4.0.0 line (see labs/versions.lock):
#   iceberg-spark-runtime-4.0_2.13:1.10.0  + iceberg-aws-bundle:1.10.0
#   delta-spark_2.13:4.0.0                 + delta-storage:4.0.0
#
# Usage:  bash labs/fetch_jars.sh [--check]
#   (no args)  download any missing/!matching JARs, verify sha256
#   --check    only report presence+checksum status; exit 1 if anything missing
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JARS_DIR="${HERE}/.jars"
MAVEN="${MAVEN:-https://repo1.maven.org/maven2}"

# name|relative-maven-path|sha256
JARS=(
  "iceberg-spark-runtime-4.0_2.13-1.10.0.jar|org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.0/iceberg-spark-runtime-4.0_2.13-1.10.0.jar|0480f1248e0a8b50ae2a730d7ad3e1a727351c362ca63f4a0c35182087a49323"
  "iceberg-aws-bundle-1.10.0.jar|org/apache/iceberg/iceberg-aws-bundle/1.10.0/iceberg-aws-bundle-1.10.0.jar|172370cfc32a5f1ae5399b210e86fc04a366dc29a112d97ac8ffc74df7525596"
  "delta-spark_2.13-4.0.0.jar|io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar|538511702aae0ef6973a6a70af3d4543c9009f8edbed786a00737e2d3cd7f04e"
  "delta-storage-4.0.0.jar|io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar|9bdb9fb450f1e119eba53feb427f331b0d09072d26485b8273883ad72c9a2e1d"
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
    echo "ok     ${name}"
    continue
  fi
  if [ "${CHECK_ONLY}" = "1" ]; then
    echo "MISSING ${name}"
    missing=1
    continue
  fi
  echo "fetch  ${name}"
  curl -fsSL -o "${dest}" "${MAVEN}/${path}"
  got="$(sha_of "${dest}")"
  if [ "${got}" != "${want}" ]; then
    echo "ERROR: sha256 mismatch for ${name}" >&2
    echo "  want ${want}" >&2
    echo "  got  ${got}" >&2
    rm -f "${dest}"
    exit 1
  fi
  echo "ok     ${name} (verified)"
done

if [ "${CHECK_ONLY}" = "1" ] && [ "${missing}" = "1" ]; then
  echo "Some JARs are missing. Run: bash labs/fetch_jars.sh" >&2
  exit 1
fi
echo "All JARs present in ${JARS_DIR}"
