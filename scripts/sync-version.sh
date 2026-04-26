#!/usr/bin/env bash
# Read the latest git tag and write it into docs/site/hugo.yaml's
# params.version so the landing page picks it up at build time. Run by
# the Cloudflare build before Hugo invokes; safe to run locally too —
# it only edits the working tree, never commits.
#
# Falls back to whatever value is already in hugo.yaml if no tags are
# reachable (e.g., very early dev with no releases yet).
set -euo pipefail

HUGO_YAML="docs/site/hugo.yaml"

# Cloudflare Workers Builds does a shallow clone with no tags; pull just
# the tag refs cheaply so 'git describe' below has something to look at.
git fetch --tags --depth=1 >/dev/null 2>&1 || true

if TAG=$(git describe --tags --abbrev=0 2>/dev/null); then
    VERSION="${TAG#v}"
else
    echo "sync-version: no git tags reachable, leaving hugo.yaml untouched" >&2
    exit 0
fi

# Replace the version line. Use awk + temp file to stay portable across
# GNU sed (Linux/CF) and BSD sed (macOS).
awk -v v="${VERSION}" '
    /^  version: "/ { print "  version: \"" v "\""; next }
    { print }
' "${HUGO_YAML}" > "${HUGO_YAML}.tmp"
mv "${HUGO_YAML}.tmp" "${HUGO_YAML}"

echo "sync-version: hugo.yaml params.version set to ${VERSION}"
