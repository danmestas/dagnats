#!/usr/bin/env bash
# Generates llms.txt and llms-full.txt from the Hugo content tree.
#
#   llms.txt       docs/site/llms-header.md (curated) + a section index with one
#                  "- [Title](url): description" line per page (llmstxt.org format).
#   llms-full.txt  every page's body, in sidebar order, with its canonical URL.
#
# Pages are ordered like the hextra sidebar: a section's _index.md first, then its
# children by weight (unset = last), then title. Only a leading YAML frontmatter
# block is stripped. Hextra shortcodes are converted (cards -> link list, callout
# -> inner text, ref -> URL); an unknown shortcode or a page without a title is an
# error. Output is deterministic: no timestamps.
#
# Env: SITE_DIR (docs/site), OUT_DIR ($SITE_DIR/static), BASE_URL.
set -euo pipefail

SITE_DIR="${SITE_DIR:-docs/site}"
OUT_DIR="${OUT_DIR:-$SITE_DIR/static}"
BASE_URL="${BASE_URL:-https://dagnats-docs.daniel-mestas.workers.dev}"
CONTENT_DIR="$SITE_DIR/content"
HEADER="$SITE_DIR/llms-header.md"

[ -d "$CONTENT_DIR" ] || { echo "gen-llms-txt: no $CONTENT_DIR" >&2; exit 1; }
[ -f "$HEADER" ] || { echo "gen-llms-txt: no $HEADER" >&2; exit 1; }
mkdir -p "$OUT_DIR"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT INT TERM

# Pass 1: per-page frontmatter -> rel <TAB> weight <TAB> title <TAB> description.
# The description falls back to the first sentence of the first prose paragraph.
meta_awk='
function clean(v) {
  sub(/^[ \t]+/, "", v); sub(/[ \t]+$/, "", v)
  if (v ~ /^".*"$/ || v ~ /^\047.*\047$/) v = substr(v, 2, length(v) - 2)
  return v
}
function plain(s,   t) {
  while (match(s, /\[[^]]*\]\([^)]*\)/)) {
    t = substr(s, RSTART, RLENGTH); sub(/\]\(.*/, "", t); sub(/^\[/, "", t)
    s = substr(s, 1, RSTART - 1) t substr(s, RSTART + RLENGTH)
  }
  gsub(/\*\*|`/, "", s)
  return s
}
function sentence(s,   i) {
  i = index(s, ". ")
  if (i > 0) s = substr(s, 1, i)
  if (length(s) > 220) s = substr(s, 1, 217) "..."
  return s
}
# Generated pages (gomarkdoc SDK reference) have no frontmatter; Hugo would title them
# "Api" from the filename, so use their first H1 instead.
NR == 1 { if ($0 == "---") { infm = 1; next } ; nofm = 1 }
infm && $0 == "---" { infm = 0; next }
infm {
  if (folded && $0 ~ /^[ \t]+/) { desc = desc " " clean($0); next }
  folded = 0
  if ($0 ~ /^title:/)  { title = clean(substr($0, 7)) }
  else if ($0 ~ /^weight:/) { weight = clean(substr($0, 8)) }
  else if ($0 ~ /^description:/) {
    v = clean(substr($0, 13))
    if (v ~ /^[>|]-?$/) { folded = 1; desc = "" } else desc = v
  }
  next
}
/^(```|~~~)/ { fence = !fence; next }
nofm && title == "" && /^# / { title = clean(substr($0, 3)); next }
fence || done { next }
/^[ \t]*$/ { if (para != "") done = 1; next }
/^(#|<|[{][{]|\||-|\*|>|!|[0-9]+\.|import |func |type |const |var )/ { if (para == "") next }
{ para = (para == "" ? $0 : para " " $0) }
END {
  if (title == "") { printf "gen-llms-txt: %s: missing frontmatter title\n", FILENAME > "/dev/stderr"; exit 1 }
  if (weight == "" || weight + 0 == 0) weight = 99999
  if (nofm) { desc = "Generated Go API reference for the " title " package: exported types, functions and methods."; title = title " package API" }
  else if (desc == "" && para != "") desc = sentence(plain(para))
  gsub(/\t/, " ", title); gsub(/\t/, " ", desc)
  printf "%s\t%s\t%s\t%s\n", FILENAME, weight, title, desc
}'

: > "$work/meta.tsv"
failed=0
while IFS= read -r file; do
  awk "$meta_awk" "$file" >> "$work/meta.tsv" || failed=1
done < <(find "$CONTENT_DIR" -name '*.md' -type f | LC_ALL=C sort)
[ "$failed" -eq 0 ] || exit 1

# Pass 2: sort key (ancestor sections by weight/title, _index first) and URL.
awk -F'\t' -v OFS='\t' -v content="$CONTENT_DIR/" -v base="$BASE_URL" '
function key_of(rel,   n, parts, i, d, k, w) {
  n = split(rel, parts, "/")
  k = ""; d = ""
  for (i = 1; i < n; i++) {
    d = (d == "" ? parts[i] : d "/" parts[i])
    k = k sprintf("%06d\001%s\001%s", sw[d], st[d] == "" ? parts[i] : st[d], d) "\002"
  }
  if (parts[n] == "_index.md") return k
  return k sprintf("%06d\001%s\001%s", W[rel], T[rel], rel)
}
function url_of(rel,   u) {
  u = rel; sub(/_index\.md$/, "", u); sub(/\.md$/, "", u)
  sub(/\/$/, "", u)
  return base "/" u (u == "" ? "" : "/")
}
{
  rel = substr($1, length(content) + 1)
  R[NR] = rel; W[rel] = $2; T[rel] = $3; D[rel] = $4
  if (rel ~ /(^|\/)_index\.md$/) {
    dir = rel; sub(/\/?_index\.md$/, "", dir)
    sw[dir] = $2; st[dir] = $3
  }
}
END {
  for (i = 1; i <= NR; i++) {
    rel = R[i]
    if (rel == "_index.md") continue   # landing page has no body content
    print key_of(rel), rel, url_of(rel), T[rel], D[rel]
  }
}' "$work/meta.tsv" | LC_ALL=C sort -t $'\t' -k1,1 > "$work/pages.tsv"

[ -s "$work/pages.tsv" ] || { echo "gen-llms-txt: no pages found" >&2; exit 1; }

# Pass 3: llms-full.txt.
body_awk='
function attr(s, name,   re) {
  re = name "=\"[^\"]*\""
  if (match(s, re)) return substr(s, RSTART + length(name) + 2, RLENGTH - length(name) - 3)
  return ""
}
function resolve(target,   t, frag) {
  if (target ~ /^[a-z]+:/) return target
  frag = ""
  if (match(target, /#.*/)) { frag = substr(target, RSTART); target = substr(target, 1, RSTART - 1) }
  sub(/\.md$/, "", target); sub(/\/$/, "", target)
  if (target ~ /^\//) return base target "/" frag
  return base "/" dir (dir == "" ? "" : "/") target "/" frag
}
function fail(msg) { printf "gen-llms-txt: %s:%d: %s\n", rel, NR, msg > "/dev/stderr"; bad = 1 }
BEGIN { dir = rel; sub(/\/?[^\/]*$/, "", dir) }
NR == 1 { if ($0 == "---") { infm = 1; next } ; nofm = 1 }
infm { if ($0 == "---") infm = 0; next }
nofm && !h1done && /^# / { h1done = 1; next }   # title is printed by the caller
/^(```|~~~)/ { fence = !fence; print; next }
fence { print; next }
/^[ \t]*[{][{][<%][ \t]*\/?(cards|callout)[^}]*[>%][}][}][ \t]*$/ { next }
/^[ \t]*[{][{][<%][ \t]*card[ \t]/ {
  link = resolve(attr($0, "link")); t = attr($0, "title"); s = attr($0, "subtitle")
  if (t == "" || attr($0, "link") == "") { fail("card needs link and title"); next }
  print "- [" t "](" link ")" (s == "" ? "" : ": " s)
  next
}
/^[ \t]*[{][{][<%][ \t]*\/?tabs[^}]*[>%][}][}][ \t]*$/ { next }
/^[ \t]*[{][{][<%][ \t]*tab[ \t]/ { n = attr($0, "name"); print "**" n "**"; print ""; next }
/^[ \t]*[{][{][<%][ \t]*\/tab[ \t]*[>%][}][}][ \t]*$/ { next }
{
  line = $0
  while (match(line, /[{][{][<%] *ref +"[^"]*" *[>%][}][}]/)) {
    rs = RSTART; rl = RLENGTH   # resolve() calls match() and clobbers RSTART/RLENGTH
    ref = substr(line, rs, rl)
    ref = substr(ref, index(ref, "\"") + 1); ref = substr(ref, 1, index(ref, "\"") - 1)
    line = substr(line, 1, rs - 1) resolve(ref) substr(line, rs + rl)
  }
  if (line ~ /[{][{][<%]/) fail("unsupported shortcode: " line)
  print line
}
END { exit bad }'

{
  echo "# DagNats Documentation (Full)"
  echo
  echo "> Every documentation page concatenated in sidebar order. Curated overview: $BASE_URL/llms.txt"
  echo
} > "$work/full.txt"

while IFS=$'\t' read -r _key rel url title _desc; do
  {
    echo "---"
    echo
    echo "# $title"
    echo
    echo "URL: $url"
    echo
  } >> "$work/full.txt"
  awk -v rel="$rel" -v base="$BASE_URL" "$body_awk" "$CONTENT_DIR/$rel" >> "$work/full.txt"
  echo >> "$work/full.txt"
done < "$work/pages.tsv"

# llms.txt: curated header + one H2 per top-level docs section.
awk -F'\t' '
{
  rel = $2
  n = split(rel, p, "/")
  sec = (p[1] == "docs" && n > 2) ? p[2] : "_overview"
  if (!(sec in seen)) { seen[sec] = 1; order[++count] = sec }
  if (n > 2 && p[n] == "_index.md" && n == 3) { name[sec] = $4; next }   # section landing: just the H2
  if (sec == "_overview") name[sec] = "Overview"
  items[sec] = items[sec] "- [" $4 "](" $3 ")" ($5 == "" ? "" : ": " $5) "\n"
}
END {
  for (i = 1; i <= count; i++) {
    s = order[i]
    if (items[s] == "") continue
    printf "\n## %s\n\n%s", (name[s] == "" ? s : name[s]), items[s]
  }
}' "$work/pages.tsv" > "$work/index.txt"

{
  cat "$HEADER"
  cat "$work/index.txt"
  printf '\n## Optional\n\n- [Full documentation in one file](%s/llms-full.txt): every page above, concatenated in order\n' "$BASE_URL"
} > "$work/llms.txt"

mv "$work/llms.txt" "$OUT_DIR/llms.txt"
mv "$work/full.txt" "$OUT_DIR/llms-full.txt"
echo "gen-llms-txt: wrote $(wc -l < "$work/pages.tsv" | tr -d ' ') pages to $OUT_DIR/"
