#!/usr/bin/env bash
# Download Eclipse Temurin JDK 21 (LTS) into labs/.jdk/ for Spark.
#
# Why: Spark 4.x supports Java 17/21/25. If the system Java is a newer feature
# release (e.g. 24), Spark may crash. This installs a known-good JDK 21 locally,
# with NO sudo and NO system changes. The `de` CLI and the test `spark` fixture
# auto-detect labs/.jdk and set JAVA_HOME to it.
#
# Usage:  bash labs/setup_jdk.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JDK_DIR="${HERE}/.jdk"

ARCH="$(uname -m)"
case "${ARCH}" in
  x86_64|amd64) A_ARCH="x64" ;;
  aarch64|arm64) A_ARCH="aarch64" ;;
  *) echo "Unsupported architecture: ${ARCH}" >&2; exit 1 ;;
esac

OS="linux"
case "$(uname -s)" in
  Linux) OS="linux" ;;
  Darwin) OS="mac" ;;
  *) echo "Unsupported OS: $(uname -s)" >&2; exit 1 ;;
esac

API="https://api.adoptium.net/v3/assets/feature_releases/21/ga?architecture=${A_ARCH}&image_type=jdk&os=${OS}&vendor=eclipse&page_size=1"

echo "Querying Adoptium for Temurin JDK 21 (${OS}/${A_ARCH})..."
META="$(curl -fsSL "${API}")"

# Extract link + sha256 using python3 (always available in this project).
read -r LINK SHA256 RELEASE < <(python3 - "$META" <<'PY'
import json, sys
d = json.loads(sys.argv[1])
b = d[0]["binaries"][0]["package"]
print(b["link"], b["checksum"], d[0]["release_name"])
PY
)

echo "Release: ${RELEASE}"
echo "URL:     ${LINK}"
echo "sha256:  ${SHA256}"

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT
TARBALL="${TMP}/jdk.tar.gz"

echo "Downloading..."
curl -fSL "${LINK}" -o "${TARBALL}"

echo "Verifying sha256..."
echo "${SHA256}  ${TARBALL}" | sha256sum -c -

echo "Extracting into ${JDK_DIR} ..."
rm -rf "${JDK_DIR}"
mkdir -p "${JDK_DIR}"
tar -xzf "${TARBALL}" -C "${JDK_DIR}"

# The tarball expands to a single top-level dir (jdk-21.x.y+z[/Contents/Home on mac]).
TOP="$(find "${JDK_DIR}" -maxdepth 1 -mindepth 1 -type d | head -n1)"
if [ "${OS}" = "mac" ] && [ -d "${TOP}/Contents/Home" ]; then
  TOP="${TOP}/Contents/Home"
fi

if [ ! -x "${TOP}/bin/java" ]; then
  echo "ERROR: java not found after extraction under ${TOP}" >&2
  exit 1
fi

"${TOP}/bin/java" -version
echo
echo "JDK 21 installed at: ${TOP}"
echo "The de CLI and tests will auto-detect labs/.jdk and set JAVA_HOME."
