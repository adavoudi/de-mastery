#!/usr/bin/env bash
# Fetch the native Apache Kafka 4.x binary distribution (Scala 2.13) into a
# gitignored labs/.kafka/ directory, verified against the official sha512.
#
# WHY native (no Docker): the no-Docker rule — every graded Kafka exercise must
# have a graded part that runs WITHOUT Docker. The de_testkit `kafka_broker` /
# `kafka_cluster` fixtures start single-node/multi-node KRaft brokers straight
# from this tarball using the vendored JDK 21 (labs/.jdk). `de doctor` reports
# whether the tarball is present and prints this command as the fix.
#
# Native Kafka also bundles the CLI tools (kafka-topics.sh, kafka-configs.sh,
# kafka-acls.sh, connect-standalone.sh, connect-mirror-maker.sh, ...) and the
# Kafka Connect runtime, so KFK-07 (native Connect) and KFK-13 (MirrorMaker 2)
# need no extra downloads.
#
# Version pinned to the latest stable 4.x (see labs/versions.lock, phase3_kafka):
#   kafka_2.13-4.3.1.tgz   (released 2026-06-23)
#
# Usage:  bash labs/fetch_kafka.sh [--check]
#   (no args)  download + verify + unpack if missing/mismatched
#   --check    only report presence/checksum status; exit 1 if missing
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_DIR="${HERE}/.kafka"

VERSION="4.3.1"
SCALA="2.13"
NAME="kafka_${SCALA}-${VERSION}"
TARBALL="${NAME}.tgz"
SHA512="c7d7b2318cb51aa0c61d3246a51c349210073c5c9b754947ef965a439f2f939e8600f204e134a75ac31faf3829c9370960ef7c6a9886c8a1dbf0339a21f4c54c"

# Prefer the fast mirror; fall back to the permanent archive.
PRIMARY="https://dlcdn.apache.org/kafka/${VERSION}/${TARBALL}"
ARCHIVE="https://archive.apache.org/dist/kafka/${VERSION}/${TARBALL}"

MARKER="${KAFKA_DIR}/${NAME}/bin/kafka-topics.sh"

sha_of() {
  if command -v sha512sum >/dev/null 2>&1; then sha512sum "$1" | awk '{print $1}';
  else shasum -a 512 "$1" | awk '{print $1}'; fi
}

CHECK_ONLY=0
[ "${1:-}" = "--check" ] && CHECK_ONLY=1

if [ -x "${MARKER}" ]; then
  echo "ok     native Kafka ${VERSION} present at ${KAFKA_DIR}/${NAME}"
  exit 0
fi

if [ "${CHECK_ONLY}" = "1" ]; then
  echo "MISSING native Kafka ${VERSION}. Run: bash labs/fetch_kafka.sh" >&2
  exit 1
fi

mkdir -p "${KAFKA_DIR}"
dest="${KAFKA_DIR}/${TARBALL}"

if [ ! -f "${dest}" ] || [ "$(sha_of "${dest}")" != "${SHA512}" ]; then
  echo "fetch  ${TARBALL}"
  if ! curl -fSL -o "${dest}" "${PRIMARY}"; then
    echo "       primary mirror failed; trying archive.apache.org"
    curl -fSL -o "${dest}" "${ARCHIVE}"
  fi
fi

got="$(sha_of "${dest}")"
if [ "${got}" != "${SHA512}" ]; then
  echo "ERROR: sha512 mismatch for ${TARBALL}" >&2
  echo "  want ${SHA512}" >&2
  echo "  got  ${got}" >&2
  rm -f "${dest}"
  exit 1
fi
echo "ok     ${TARBALL} (sha512 verified)"

echo "unpack ${NAME} -> ${KAFKA_DIR}/${NAME}"
rm -rf "${KAFKA_DIR}/${NAME}"
tar -xzf "${dest}" -C "${KAFKA_DIR}"
# Keep only the unpacked tree to save ~130 MB; the tarball is reproducible.
rm -f "${dest}"

if [ ! -x "${MARKER}" ]; then
  echo "ERROR: expected ${MARKER} after unpack" >&2
  exit 1
fi
echo "All set: native Kafka ${VERSION} at ${KAFKA_DIR}/${NAME}"
