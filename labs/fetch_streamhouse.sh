#!/usr/bin/env bash
# Fetch the native Apache Fluss server distribution + the Paimon / Fluss Flink
# connector / tiering jars the STH (streamhouse) track needs, into gitignored
# labs/.fluss/ and labs/.jars/streamhouse/, each verified against a pinned
# checksum. Reuses the vendored Flink 2.2.1 dist + JDK 21 (labs/fetch_flink.sh,
# labs/setup_jdk.sh) and the Iceberg/hadoop jars already in labs/.jars/flink.
#
# WHY native (no Docker): the no-Docker rule — every graded STH exercise must
# have a graded part that runs WITHOUT Docker. The de_testkit streamhouse
# fixtures:
#   * run Paimon/Iceberg Flink jobs on an embedded MiniCluster (batch reads!),
#   * start a native Fluss local cluster (ZK + coordinator + tablet server on
#     OS-assigned free ports, small heaps) straight from labs/.fluss, and
#   * submit the Fluss->Paimon tiering service to a native Flink local cluster,
# all on the vendored Temurin JDK 21 (labs/.jdk). Serving reads use pure-Python
# pypaimon / pyiceberg (no Docker). The fixtures SKIP cleanly when an artifact
# or the JDK is missing.
#
# Versions pinned (see labs/versions.lock, phase5_streamhouse):
#   Apache Paimon 2.0.0            — paimon-flink-2.2 (connector) + paimon-bundle
#                                    (lake format) + paimon-flink-action (compact
#                                    /CDC actions). Works with Flink 2.2.1/JDK 21.
#   Apache Fluss 1.0.0            — server tarball (bundles ZooKeeper + the
#                                    paimon/iceberg lake plugins) + fluss-flink-2.2
#                                    (Flink 2.2 connector) + fluss-flink-tiering
#                                    (lake tiering service job). fluss-flink-2.2
#                                    is published for 1.0.0 (NOT only 0.9.x — the
#                                    search.maven.org index is stale; the Maven
#                                    repo metadata.xml lists 1.0.0).
#   iceberg-flink-runtime-2.2 1.12.0 — already fetched by fetch_flink.sh
#                                    (labs/.jars/flink); STH-04 reuses it.
#
# Usage:  bash labs/fetch_streamhouse.sh [--check]
#   (no args)  download + verify + unpack if missing/mismatched (idempotent;
#              a second run is a no-op)
#   --check    only report presence/checksum status; exit 1 if a HARD artifact
#              is missing
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLUSS_DIR="${HERE}/.fluss"
JARS_DIR="${HERE}/.jars/streamhouse"
MAVEN="${MAVEN:-https://repo1.maven.org/maven2}"
APACHE_DL="${APACHE_DL:-https://downloads.apache.org/fluss}"
APACHE_ARCHIVE="https://archive.apache.org/dist/fluss"
APACHE_CDN="https://dlcdn.apache.org/fluss"

# -------- Fluss server distribution -----------------------------------------
FLUSS_VERSION="1.0.0"
FLUSS_NAME="fluss-${FLUSS_VERSION}"
FLUSS_TARBALL="fluss-${FLUSS_VERSION}-bin.tgz"
FLUSS_SHA512="4cc134d1c5e7b9b07754b4d38903b9c6e4075d66aa7cf04eb3b053fa5095266f1d141e147417d292b3e93b642b24ea06630709de85562c3a829b640ccb7d52ca"
FLUSS_MARKER="${FLUSS_DIR}/${FLUSS_NAME}/bin/local-cluster.sh"

# -------- Jars (not in any dist; labs/.jars/streamhouse, sha256) ------------
#   name|relative-maven-path|sha256
JARS=(
  "paimon-flink-2.2-2.0.0.jar|org/apache/paimon/paimon-flink-2.2/2.0.0/paimon-flink-2.2-2.0.0.jar|bf344b7c619dcce3fcae8b5cfd5130dd3a9592ce232057d6ce7363111a987d76"
  "paimon-bundle-2.0.0.jar|org/apache/paimon/paimon-bundle/2.0.0/paimon-bundle-2.0.0.jar|7a755b7b11b500147cd113802565d19d0f84947b6444bc8dabad85005c930561"
  "paimon-flink-action-2.0.0.jar|org/apache/paimon/paimon-flink-action/2.0.0/paimon-flink-action-2.0.0.jar|cd435d1136fbf18b627f895d2f5ac04de14a2a2fa1c7ff3a7bcd662933f7381f"
  "fluss-flink-2.2-1.0.0.jar|org/apache/fluss/fluss-flink-2.2/1.0.0/fluss-flink-2.2-1.0.0.jar|8cad6d1342b7e2deaf3db7fe7fbc1911c05823a538517b58da32cc0e8601ed8e"
  "fluss-flink-tiering-1.0.0.jar|org/apache/fluss/fluss-flink-tiering/1.0.0/fluss-flink-tiering-1.0.0.jar|f0b85db9b6cfdad8658406e31ce3a2aae6ffa0cdff81ce22fffc919e4032160e"
)

sha512_of() {
  if command -v sha512sum >/dev/null 2>&1; then sha512sum "$1" | awk '{print $1}';
  else shasum -a 512 "$1" | awk '{print $1}'; fi
}
sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}';
  else shasum -a 256 "$1" | awk '{print $1}'; fi
}

CHECK_ONLY=0
[ "${1:-}" = "--check" ] && CHECK_ONLY=1
missing=0

# -------- 1. Fluss server distribution --------------------------------------
if [ -x "${FLUSS_MARKER}" ]; then
  echo "ok     Fluss ${FLUSS_VERSION} present at ${FLUSS_DIR}/${FLUSS_NAME}"
else
  if [ "${CHECK_ONLY}" = "1" ]; then
    echo "MISSING Fluss ${FLUSS_VERSION}. Run: bash labs/fetch_streamhouse.sh" >&2
    missing=1
  else
    mkdir -p "${FLUSS_DIR}"
    dest="${FLUSS_DIR}/${FLUSS_TARBALL}"
    if [ ! -f "${dest}" ] || [ "$(sha512_of "${dest}")" != "${FLUSS_SHA512}" ]; then
      echo "fetch  ${FLUSS_TARBALL}"
      if ! curl -fSL -o "${dest}" "${APACHE_DL}/${FLUSS_NAME}/${FLUSS_TARBALL}"; then
        echo "       downloads.apache.org failed; trying dlcdn"
        if ! curl -fSL -o "${dest}" "${APACHE_CDN}/${FLUSS_NAME}/${FLUSS_TARBALL}"; then
          echo "       dlcdn failed; trying archive.apache.org"
          curl -fSL -o "${dest}" "${APACHE_ARCHIVE}/${FLUSS_NAME}/${FLUSS_TARBALL}"
        fi
      fi
    fi
    got="$(sha512_of "${dest}")"
    if [ "${got}" != "${FLUSS_SHA512}" ]; then
      echo "ERROR: sha512 mismatch for ${FLUSS_TARBALL}" >&2
      echo "  want ${FLUSS_SHA512}" >&2
      echo "  got  ${got}" >&2
      rm -f "${dest}"; exit 1
    fi
    echo "ok     ${FLUSS_TARBALL} (sha512 verified)"
    echo "unpack ${FLUSS_NAME} -> ${FLUSS_DIR}/${FLUSS_NAME}"
    rm -rf "${FLUSS_DIR}/${FLUSS_NAME}"
    tar -xzf "${dest}" -C "${FLUSS_DIR}"
    rm -f "${dest}"
    [ -x "${FLUSS_MARKER}" ] || { echo "ERROR: expected ${FLUSS_MARKER}" >&2; exit 1; }
    echo "All set: Fluss ${FLUSS_VERSION} at ${FLUSS_DIR}/${FLUSS_NAME}"
  fi
fi

# -------- 2. Paimon + Fluss connector / tiering jars ------------------------
mkdir -p "${JARS_DIR}"
for entry in "${JARS[@]}"; do
  name="${entry%%|*}"; rest="${entry#*|}"
  path="${rest%%|*}"; want="${rest##*|}"
  dest="${JARS_DIR}/${name}"
  if [ -f "${dest}" ] && [ "$(sha256_of "${dest}")" = "${want}" ]; then
    echo "ok     ${name}"; continue
  fi
  if [ "${CHECK_ONLY}" = "1" ]; then echo "MISSING ${name}"; missing=1; continue; fi
  echo "fetch  ${name}"
  curl -fsSL -o "${dest}" "${MAVEN}/${path}"
  got="$(sha256_of "${dest}")"
  if [ "${got}" != "${want}" ]; then
    echo "ERROR: sha256 mismatch for ${name}" >&2
    echo "  want ${want}" >&2
    echo "  got  ${got}" >&2
    rm -f "${dest}"; exit 1
  fi
  echo "ok     ${name} (sha256 verified)"
done

# -------- 3. Note Iceberg-on-Flink reuse ------------------------------------
ICE="${HERE}/.jars/flink/iceberg-flink-runtime-2.2-1.12.0.jar"
if [ -f "${ICE}" ]; then
  echo "ok     iceberg-flink-runtime-2.2-1.12.0.jar present (STH-04 reuses labs/.jars/flink)"
else
  echo "note   iceberg-flink-runtime-2.2-1.12.0.jar not found; run bash labs/fetch_flink.sh"
  echo "       (STH-04 'Iceberg as streaming storage' needs it)"
fi

if [ "${CHECK_ONLY}" = "1" ] && [ "${missing}" = "1" ]; then
  echo "Some streamhouse artifacts are missing. Run: bash labs/fetch_streamhouse.sh" >&2
  exit 1
fi
echo "Streamhouse artifacts present (Fluss in ${FLUSS_DIR}, jars in ${JARS_DIR})."
echo ""
echo "Serving layer (STH-07) is pure-Python, no Docker:"
echo "  * Paimon reads  : pypaimon==2.0.0 in a dedicated venv (de_testkit builds it)."
echo "  * Iceberg reads : pyiceberg (main venv) -> Arrow -> DuckDB SQL."
echo "  Optional Docker-only serving (StarRocks / Trino) is an extra layer only."
