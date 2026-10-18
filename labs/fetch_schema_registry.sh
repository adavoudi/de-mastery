#!/usr/bin/env bash
# OPTIONAL: fetch the Confluent Community distribution (Confluent Community
# License — permitted for local learning/eval) to get a NATIVE Schema Registry
# that runs on the vendored JDK 21 against the native Kafka broker. Unpacked
# into the gitignored labs/.schema-registry/.
#
# WHY optional: KFK-06 grades WITHOUT any registry process, using the fastavro
# schema-resolution compatibility checker (de_testkit.check_compatibility /
# the check_avro_compatibility fixture). This script only adds a LIVE registry
# for exercises that want to demonstrate the real HTTP API via the
# `schema_registry` fixture (which SKIPs when this is absent).
#
# Confluent does NOT publish a checksum file for the community tarball (only a
# GPG .asc), so we pin the sha512 OBSERVED on 2026-10-04 and verify against it
# (same model as a lockfile). Recorded in labs/versions.lock (schema_registry).
#
# Usage:  bash labs/fetch_schema_registry.sh [--check]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SR_DIR="${HERE}/.schema-registry"

VERSION="7.9.0"
MINOR="7.9"
TARBALL="confluent-community-${VERSION}.tar.gz"
URL="https://packages.confluent.io/archive/${MINOR}/${TARBALL}"
SHA512="25450ee5cfb6188d3ad3e19651b67cb9b3006603eb57ccac79b9845986d517857156dabdbb806b78728dc4b71bd230b43977ba05e0190b48f34bb10462e13144"
# the community tarball unpacks to confluent-<version>/
MARKER="${SR_DIR}/confluent-${VERSION}/bin/schema-registry-start"

sha_of() {
  if command -v sha512sum >/dev/null 2>&1; then sha512sum "$1" | awk '{print $1}';
  else shasum -a 512 "$1" | awk '{print $1}'; fi
}

CHECK_ONLY=0
[ "${1:-}" = "--check" ] && CHECK_ONLY=1

if [ -x "${MARKER}" ]; then
  echo "ok     native Schema Registry present at ${SR_DIR}/confluent-${VERSION}"
  exit 0
fi
if [ "${CHECK_ONLY}" = "1" ]; then
  echo "MISSING native Schema Registry. Run: bash labs/fetch_schema_registry.sh" >&2
  exit 1
fi

mkdir -p "${SR_DIR}"
dest="${SR_DIR}/${TARBALL}"
if [ ! -f "${dest}" ] || [ "$(sha_of "${dest}")" != "${SHA512}" ]; then
  echo "fetch  ${TARBALL} (~360 MB)"
  curl -fSL -o "${dest}" "${URL}"
fi
got="$(sha_of "${dest}")"
if [ "${got}" != "${SHA512}" ]; then
  echo "ERROR: sha512 mismatch for ${TARBALL}" >&2
  echo "  want ${SHA512}" >&2
  echo "  got  ${got}" >&2
  rm -f "${dest}"; exit 1
fi
echo "ok     ${TARBALL} (sha512 verified)"
echo "unpack confluent-${VERSION} -> ${SR_DIR}/confluent-${VERSION}"
tar -xzf "${dest}" -C "${SR_DIR}"
rm -f "${dest}"
[ -x "${MARKER}" ] || { echo "ERROR: expected ${MARKER}" >&2; exit 1; }
echo "All set: native Schema Registry at ${SR_DIR}/confluent-${VERSION}"
