#!/usr/bin/env bash
# Renders every ```mermaid block in the given Markdown files with the Mermaid
# CLI, which uses the same parser as GitHub, and fails on the first that
# doesn't parse. Needs Node (npx); the CLI downloads on first use.
set -euo pipefail

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

status=0
for doc in "${@:-README.md}"; do
    python3 - "$doc" "$work" <<'PY'
import pathlib, re, sys
doc, out = sys.argv[1], pathlib.Path(sys.argv[2])
blocks = re.findall(r"```mermaid\n(.*?)```", pathlib.Path(doc).read_text(), re.S)
for i, block in enumerate(blocks):
    (out / f"{pathlib.Path(doc).stem}-{i}.mmd").write_text(block)
PY
done

for diagram in "$work"/*.mmd; do
    [ -e "$diagram" ] || continue
    if npx -y -p @mermaid-js/mermaid-cli mmdc -q -i "$diagram" -o "${diagram%.mmd}.svg" >"$diagram.log" 2>&1; then
        echo "ok      $(basename "$diagram")"
    else
        echo "FAILED  $(basename "$diagram")"
        grep -m3 -i error "$diagram.log" || true
        status=1
    fi
done
exit $status
