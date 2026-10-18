#!/usr/bin/env bash
# Fetch the native Apache Flink 2.x binary distribution + the extra connector /
# Iceberg jars the FLK track needs, into gitignored labs/.flink/ and
# labs/.jars/flink/, each verified against a pinned checksum.
#
# WHY native (no Docker): the no-Docker rule — every graded FLK exercise must
# have a graded part that runs WITHOUT Docker. The de_testkit flink fixtures
#   * run Java DataStream/Table jobs on an embedded MiniCluster, and
#   * (optionally) start a native local Flink cluster (JobManager + TaskManager)
# straight from labs/.flink on OS-assigned free ports with small heaps, all on
# the vendored Temurin JDK 21 (labs/.jdk, from labs/setup_jdk.sh).
#
# `de doctor` reports whether the dist + jars are present and prints this command
# as the fix. The fixtures SKIP cleanly (never fail) when the dist/jars/JDK are
# missing.
#
# Versions pinned (see labs/versions.lock, phase4_flink):
#   Apache Flink 2.2.1 (Scala 2.12)  — newest 2.x line WITH a released
#                                      flink-connector-kafka (5.0.0-2.2).
#   flink-sql-connector-kafka 5.0.0-2.2 (fat; bundles the DataStream connector
#                                        AND the SQL/Table 'kafka'+'upsert-kafka').
#   iceberg-flink-runtime-2.2 1.12.0    — Iceberg 1.10.0 has NO Flink-2.2 runtime
#                                         (only Flink-2.0); 1.12.0 is the closest
#                                         supported runtime for Flink 2.2 (FLK-10).
#   flink-json / flink-csv / flink-connector-base / flink-table-* ship IN the
#   dist (lib/ + opt/); no separate download. debezium-json format is part of
#   flink-json (built in), so the FLK-10 no-Docker CDC path needs no extra jar.
#
# Usage:  bash labs/fetch_flink.sh [--check]
#   (no args)  download + verify + unpack if missing/mismatched
#   --check    only report presence/checksum status; exit 1 if missing
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLINK_DIR="${HERE}/.flink"
JARS_DIR="${HERE}/.jars/flink"
MAVEN="${MAVEN:-https://repo1.maven.org/maven2}"

FLINK_VERSION="2.2.1"
FLINK_SCALA="2.12"
FLINK_NAME="flink-${FLINK_VERSION}"
FLINK_TARBALL="flink-${FLINK_VERSION}-bin-scala_${FLINK_SCALA}.tgz"
FLINK_SHA512="9beb619520a9b9a94d71e165f51c2c2bf67d180fee2a6f88a85b05f23b513efd12897669b6b8b000a7f16c8c8835c3bdb1e7b83133ef63056dc98a2a67105f60"

FLINK_PRIMARY="https://dlcdn.apache.org/flink/${FLINK_NAME}/${FLINK_TARBALL}"
FLINK_ARCHIVE="https://archive.apache.org/dist/flink/${FLINK_NAME}/${FLINK_TARBALL}"

FLINK_MARKER="${FLINK_DIR}/${FLINK_NAME}/bin/start-cluster.sh"

# Extra jars (not in the dist): name|relative-maven-path|sha256
#   flink-json / flink-csv / flink-connector-files / flink-table-* ship in the
#   dist's lib/; the SQL 'debezium-json' format is part of flink-json (built in).
#   hadoop-client-api / hadoop-client-runtime provide org.apache.hadoop.conf.*
#   + LocalFileSystem, which the Iceberg Flink hadoop catalog (FLK-10) needs and
#   the Flink dist does NOT ship. Two shaded uber jars, self-contained.
JARS=(
  "flink-sql-connector-kafka-5.0.0-2.2.jar|org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar|5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616"
  "iceberg-flink-runtime-2.2-1.12.0.jar|org/apache/iceberg/iceberg-flink-runtime-2.2/1.12.0/iceberg-flink-runtime-2.2-1.12.0.jar|c79bbee8792b1115c321e4be99fab3fb36db0bf8fa2bba24b481e6da14416de3"
  "hadoop-client-api-3.4.2.jar|org/apache/hadoop/hadoop-client-api/3.4.2/hadoop-client-api-3.4.2.jar|607c0299f4df33013bd0a0f37c8cf255c635bbaddd247b8fec68c210cc17d19f"
  "hadoop-client-runtime-3.4.2.jar|org/apache/hadoop/hadoop-client-runtime/3.4.2/hadoop-client-runtime-3.4.2.jar|18d071c7f72f8a946677b398a57af9c7afa2dd41a81023c310f99373fec200b3"
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

# -------- 1. Flink binary distribution --------------------------------------
if [ -x "${FLINK_MARKER}" ]; then
  echo "ok     native Flink ${FLINK_VERSION} present at ${FLINK_DIR}/${FLINK_NAME}"
else
  if [ "${CHECK_ONLY}" = "1" ]; then
    echo "MISSING native Flink ${FLINK_VERSION}. Run: bash labs/fetch_flink.sh" >&2
    missing=1
  else
    mkdir -p "${FLINK_DIR}"
    dest="${FLINK_DIR}/${FLINK_TARBALL}"
    if [ ! -f "${dest}" ] || [ "$(sha512_of "${dest}")" != "${FLINK_SHA512}" ]; then
      echo "fetch  ${FLINK_TARBALL} (542 MB)"
      if ! curl -fSL -o "${dest}" "${FLINK_PRIMARY}"; then
        echo "       primary mirror failed; trying archive.apache.org"
        curl -fSL -o "${dest}" "${FLINK_ARCHIVE}"
      fi
    fi
    got="$(sha512_of "${dest}")"
    if [ "${got}" != "${FLINK_SHA512}" ]; then
      echo "ERROR: sha512 mismatch for ${FLINK_TARBALL}" >&2
      echo "  want ${FLINK_SHA512}" >&2
      echo "  got  ${got}" >&2
      rm -f "${dest}"; exit 1
    fi
    echo "ok     ${FLINK_TARBALL} (sha512 verified)"
    echo "unpack ${FLINK_NAME} -> ${FLINK_DIR}/${FLINK_NAME}"
    rm -rf "${FLINK_DIR}/${FLINK_NAME}"
    tar -xzf "${dest}" -C "${FLINK_DIR}"
    rm -f "${dest}"
    [ -x "${FLINK_MARKER}" ] || { echo "ERROR: expected ${FLINK_MARKER}" >&2; exit 1; }
    echo "All set: native Flink ${FLINK_VERSION} at ${FLINK_DIR}/${FLINK_NAME}"
  fi
fi

# -------- 2. Extra connector / Iceberg jars ---------------------------------
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
  echo "ok     ${name} (verified)"
done

if [ "${CHECK_ONLY}" = "1" ] && [ "${missing}" = "1" ]; then
  echo "Some Flink artifacts are missing. Run: bash labs/fetch_flink.sh" >&2
  exit 1
fi
echo "Flink artifacts present (dist in ${FLINK_DIR}, jars in ${JARS_DIR})."

# -------- 3. PyFlink isolated venv (FLK-08 Python side) ---------------------
# apache-flink pulls apache-beam, which hard-caps pyarrow/numpy/protobuf below
# what the main uv project pins (pyarrow 18.1.0 etc.). So PyFlink is NOT added
# to pyproject.toml; instead it lives in a DEDICATED, gitignored venv and the
# de_testkit `pyflink_runner` fixture shells out to its interpreter. This keeps
# the main lock clean. The PyFlink wheel bundles its own Java jars (same 2.2.1),
# run on the vendored JDK 21.
PYFLINK_VERSION="2.2.1"
PYFLINK_VENV="${FLINK_DIR}/.pyflink-venv"
PYFLINK_MARKER="${PYFLINK_VENV}/bin/python"

pyflink_ok() {
  [ -x "${PYFLINK_MARKER}" ] && \
    "${PYFLINK_MARKER}" -c "import pyflink; print(pyflink.version.__version__)" 2>/dev/null \
      | grep -q "^${PYFLINK_VERSION}$"
}

if pyflink_ok; then
  echo "ok     PyFlink ${PYFLINK_VERSION} venv present at ${PYFLINK_VENV}"
else
  if [ "${CHECK_ONLY}" = "1" ]; then
    echo "note   PyFlink venv not built (optional; FLK-08 Python side will SKIP)."
    # PyFlink is OPTIONAL — do NOT set missing=1 (dist/jars are the hard gate).
  elif ! command -v uv >/dev/null 2>&1; then
    echo "skip   uv not found; cannot build the PyFlink venv (FLK-08 Python side"
    echo "       will SKIP). Install uv, then re-run bash labs/fetch_flink.sh."
  else
    echo "venv   building isolated PyFlink ${PYFLINK_VERSION} venv (apache-beam"
    echo "       pins conflict with the main project, so it is kept separate)..."
    rm -rf "${PYFLINK_VENV}"
    uv venv --python 3.12 "${PYFLINK_VENV}" >/dev/null
    if uv pip install --python "${PYFLINK_MARKER}" "apache-flink==${PYFLINK_VERSION}" >/dev/null 2>&1; then
      if pyflink_ok; then
        echo "ok     PyFlink ${PYFLINK_VERSION} venv ready at ${PYFLINK_VENV}"
      else
        echo "WARN   PyFlink installed but import check failed; FLK-08 Python side SKIPs."
      fi
    else
      echo "WARN   PyFlink install failed (network/resolver). FLK-08 Python side SKIPs"
      echo "       cleanly; the Java/Table side is unaffected."
    fi
  fi
fi
