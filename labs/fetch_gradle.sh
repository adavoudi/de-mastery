#!/usr/bin/env bash
# Fetch a pinned, sha256-verified Gradle distribution into a gitignored
# labs/.gradle-dist/ directory, for the ONE module that genuinely exercises a
# Gradle Kotlin DSL build + shaded/fat jar: JVM-03 (package a Flink job jar).
#
# WHY vendored + offline: Gradle is slow and network-hungry the first time it
# resolves plugins/dependencies. We vendor the Gradle runtime here and point the
# JVM-03 tests at an OFFLINE, repo-local Gradle user home (labs/.gradle-home,
# also gitignored) so the single build stays reproducible and bounded. The build
# runs with `--offline` once the dependency jars are already in the shared
# labs/.jars/java (fetch_java_deps.sh) — JVM-03's build.gradle.kts resolves from
# a flatDir repo pointing at labs/.jars/java, so no Maven Central hit at grade
# time.
#
# Every OTHER Java exercise uses the fast javac+JUnit path (run_junit); only
# JVM-03 pays the Gradle cost (~15-30 s warm, documented in the contract).
#
# `de doctor` reports whether this is present (optional check).
#
# Usage:  bash labs/fetch_gradle.sh [--check]
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIST_DIR="${HERE}/.gradle-dist"

VERSION="8.14.3"
NAME="gradle-${VERSION}"
ZIP="${NAME}-bin.zip"
SHA256="bd71102213493060956ec229d946beee57158dbd89d0e62b91bca0fa2c5f3531"
URL="https://services.gradle.org/distributions/${ZIP}"
MARKER="${DIST_DIR}/${NAME}/bin/gradle"

sha_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}';
  else shasum -a 256 "$1" | awk '{print $1}'; fi
}

CHECK_ONLY=0
[ "${1:-}" = "--check" ] && CHECK_ONLY=1

if [ -x "${MARKER}" ]; then
  echo "ok     Gradle ${VERSION} present at ${DIST_DIR}/${NAME}"
  exit 0
fi
if [ "${CHECK_ONLY}" = "1" ]; then
  echo "MISSING Gradle ${VERSION}. Run: bash labs/fetch_gradle.sh" >&2
  exit 1
fi

mkdir -p "${DIST_DIR}"
dest="${DIST_DIR}/${ZIP}"
if [ ! -f "${dest}" ] || [ "$(sha_of "${dest}")" != "${SHA256}" ]; then
  echo "fetch  ${ZIP}"
  curl -fSL -o "${dest}" "${URL}"
fi
got="$(sha_of "${dest}")"
if [ "${got}" != "${SHA256}" ]; then
  echo "ERROR: sha256 mismatch for ${ZIP}" >&2
  echo "  want ${SHA256}" >&2
  echo "  got  ${got}" >&2
  rm -f "${dest}"; exit 1
fi
echo "ok     ${ZIP} (sha256 verified)"
echo "unpack ${NAME} -> ${DIST_DIR}/${NAME}"
rm -rf "${DIST_DIR}/${NAME}"
if command -v unzip >/dev/null 2>&1; then
  unzip -q "${dest}" -d "${DIST_DIR}"
else
  (cd "${DIST_DIR}" && jar xf "${dest}")
fi
rm -f "${dest}"
[ -x "${MARKER}" ] || { echo "ERROR: expected ${MARKER}" >&2; exit 1; }
echo "All set: Gradle ${VERSION} at ${DIST_DIR}/${NAME}"
