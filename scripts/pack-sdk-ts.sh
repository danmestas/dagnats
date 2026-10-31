#!/usr/bin/env bash
# scripts/pack-sdk-ts.sh VERSION DEST_DIR
# Packs sdk/ts (@dagnats/bridge) into DEST_DIR/dagnats-bridge-VERSION.tgz,
# the release asset it ships as: dagnats publishes it to neither npmjs nor
# GitHub Packages. VERSION is the dagnats tag without the v, set into
# package.json for the pack only; package.json and its lockfile are
# restored on exit so a release build leaves the tree clean.
#
# Fails loudly without node/npm: a release without the asset would be
# missing a file its notes promise, so there is no skip.
set -euo pipefail

version="${1:?usage: pack-sdk-ts.sh VERSION DEST_DIR}"
dest="${2:?usage: pack-sdk-ts.sh VERSION DEST_DIR}"
version="${version#v}"

for tool in node npm; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "pack-sdk-ts: $tool not found; install Node 22.19 or later to build the release" >&2
    exit 1
  fi
done

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$dest"
dest="$(cd "$dest" && pwd)"
cd "$root/sdk/ts"

backup="$(mktemp -d)"
cp package.json "$backup/package.json"
cp package-lock.json "$backup/package-lock.json"
restore() {
  cp "$backup/package.json" package.json
  cp "$backup/package-lock.json" package-lock.json
  rm -rf "$backup"
}
trap restore EXIT INT TERM

npm ci --no-audit --no-fund
npm pkg set version="$version"
rm -rf dist
# prepack runs the build.
npm pack --pack-destination "$dest" --silent >/dev/null

out="$dest/dagnats-bridge-$version.tgz"
if [ ! -f "$out" ]; then
  echo "pack-sdk-ts: expected $out was not produced" >&2
  exit 1
fi
echo "packed $out"
