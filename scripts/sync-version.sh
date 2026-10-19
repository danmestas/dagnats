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

# Cloudflare Workers Builds does a shallow clone with no tags, so
# 'git describe' finds nothing: no tag is reachable from a single commit.
# Take the highest release tag by version instead, from local tags if any,
# else from the remote's tag list (the repo is public).
latest_tag() {
    git tag -l 'v*' | sort -V | tail -n 1
}
remote_latest_tag() {
    git ls-remote --tags --refs "$1" 'v*' 2>/dev/null \
        | sed 's#.*refs/tags/##' | sort -V | tail -n 1
}

TAG=$(latest_tag)
if [ -z "${TAG}" ]; then
    TAG=$(remote_latest_tag origin)
fi
if [ -z "${TAG}" ]; then
    TAG=$(remote_latest_tag https://github.com/danmestas/dagnats.git)
fi
if [ -z "${TAG}" ]; then
    echo "sync-version: no release tags found, leaving hugo.yaml untouched" >&2
    exit 0
fi
VERSION="${TAG#v}"

# Replace the version line. Use awk + temp file to stay portable across
# GNU sed (Linux/CF) and BSD sed (macOS).
awk -v v="${VERSION}" '
    /^  version: "/ { print "  version: \"" v "\""; next }
    { print }
' "${HUGO_YAML}" > "${HUGO_YAML}.tmp"
mv "${HUGO_YAML}.tmp" "${HUGO_YAML}"

echo "sync-version: hugo.yaml params.version set to ${VERSION}"
