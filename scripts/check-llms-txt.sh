#!/usr/bin/env bash
# Regenerates llms.txt and llms-full.txt into a scratch dir and validates them.
# Run from the repo root (CI and `make docs-check-llms` do). Exits non-zero on the
# first class of failure, printing every offender in that class.
#
# Checks: generator succeeds and is deterministic; no Hugo shortcode remains; every
# content page appears exactly once in llms-full.txt; every page except a section's
# own landing page is linked exactly once from llms.txt; every site link in llms.txt
# is a real page; llms.txt follows the llmstxt.org shape; the curated header still
# matches dag/ step types and the package directories it names.
set -euo pipefail

SITE_DIR="${SITE_DIR:-docs/site}"
BASE_URL="${BASE_URL:-https://dagnats-docs.daniel-mestas.workers.dev}"
CONTENT_DIR="$SITE_DIR/content"
HEADER="$SITE_DIR/llms-header.md"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT INT TERM
fail=0
problem() { echo "check-llms-txt: $*" >&2; fail=1; }

OUT_DIR="$work/a" ./scripts/gen-llms-txt.sh >/dev/null
OUT_DIR="$work/b" ./scripts/gen-llms-txt.sh >/dev/null
cmp -s "$work/a/llms.txt" "$work/b/llms.txt" || problem "llms.txt is not deterministic"
cmp -s "$work/a/llms-full.txt" "$work/b/llms-full.txt" || problem "llms-full.txt is not deterministic"
llms="$work/a/llms.txt"
full="$work/a/llms-full.txt"

# Shortcodes are `{{<` / `{{%`; plain `{{` appears legitimately in Go and CI snippets.
for out in "$llms" "$full"; do
  if grep -nE '\{\{[<%]' "$out" >&2; then problem "shortcode remnants in $(basename "$out")"; fi
done

# Expected page URLs, derived from content paths the way Hugo does.
: > "$work/urls.txt"
: > "$work/landing.txt"
while IFS= read -r file; do
  rel="${file#"$CONTENT_DIR"/}"
  [ "$rel" = "_index.md" ] && continue
  url_path="${rel%.md}"
  url_path="${url_path%_index}"
  url_path="${url_path%/}"
  echo "$BASE_URL/$url_path/" >> "$work/urls.txt"
  # docs/<section>/_index.md only carries a card grid; llms.txt uses it as the H2.
  case "$rel" in docs/*/_index.md) [ "$(echo "$rel" | tr -cd / | wc -c)" -eq 2 ] \
    && echo "$BASE_URL/$url_path/" >> "$work/landing.txt";; esac
done < <(find "$CONTENT_DIR" -name '*.md' -type f | LC_ALL=C sort)
sort -o "$work/urls.txt" "$work/urls.txt"
[ -s "$work/urls.txt" ] || problem "no content pages found under $CONTENT_DIR"

grep '^URL: ' "$full" | sed 's/^URL: //' | sort > "$work/full_urls.txt"
dups="$(uniq -d "$work/full_urls.txt")"
[ -z "$dups" ] || problem "pages repeated in llms-full.txt: $dups"
diff_out="$(diff "$work/urls.txt" "$work/full_urls.txt" || true)"
[ -z "$diff_out" ] || problem "llms-full.txt pages differ from content ('<' missing, '>' extra): $diff_out"

grep -oE '\]\(https?://[^)]+\)' "$llms" | sed 's/^](//; s/)$//' | sort > "$work/links.txt" || true
grep -v '/llms-full.txt$' "$work/links.txt" > "$work/page_links.txt" || true
dups="$(uniq -d "$work/page_links.txt")"
[ -z "$dups" ] || problem "pages linked twice in llms.txt: $dups"
bad="$(comm -13 "$work/urls.txt" "$work/page_links.txt")"
[ -z "$bad" ] || problem "llms.txt links to non-pages: $bad"
sort "$work/landing.txt" > "$work/landing_sorted.txt"
comm -23 "$work/urls.txt" "$work/landing_sorted.txt" > "$work/expected_links.txt"
missing="$(comm -23 "$work/expected_links.txt" "$work/page_links.txt")"
[ -z "$missing" ] || problem "pages missing from llms.txt: $missing"
grep -qx "$BASE_URL/llms-full.txt" "$work/links.txt" || problem "llms.txt must link llms-full.txt"

# llmstxt.org shape: H1, then a blockquote summary, then H2 sections.
[ "$(sed -n 1p "$llms" | cut -c1-2)" = "# " ] || problem "llms.txt must start with an H1"
grep -q '^> ' "$llms" || problem "llms.txt needs a blockquote summary"
grep -q '^## ' "$llms" || problem "llms.txt needs H2 sections"

# Header claims that can rot: step types and package directories.
types_src="$(sed -n '/^var stepTypeStrings/,/^}/p' dag/types.go | grep -oE '"[a-z_]+"' | tr -d '"')"
count=0
for t in $types_src; do
  count=$((count + 1))
  grep -q "\`$t\`" "$HEADER" || problem "step type $t (dag/types.go) is not named in $HEADER"
done
grep -q "There are $count step types" "$HEADER" || problem "$HEADER must say 'There are $count step types'"
for pkg in $(sed -n '/^## Go packages/,/^## /p' "$HEADER" | grep -oE '^- `[a-z/]+`' | tr -d '`- '); do
  [ -d "$pkg" ] || problem "$HEADER names package $pkg but ./$pkg does not exist"
done

if [ "$fail" -ne 0 ]; then exit 1; fi
echo "check-llms-txt: ok ($(wc -l < "$work/urls.txt" | tr -d ' ') pages)"
