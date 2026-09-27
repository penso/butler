default:
    @just --list

format:
    cargo fmt --all
    taplo fmt

format-check:
    cargo fmt --all -- --check
    taplo fmt --check

# Every feature combination, so a cfg gate that only breaks one is caught.
lint:
    cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
    cargo clippy --locked -p butler-jobs --all-targets --no-default-features -- -D warnings
    cargo clippy --locked -p butler-jobs --all-targets --no-default-features --features tokio -- -D warnings
    cargo clippy --locked -p butler-jobs --all-targets --no-default-features --features redis -- -D warnings
    cargo clippy --locked -p butler-jobs --all-targets --no-default-features --features sqlite -- -D warnings

# The Redis test is skipped when no server is reachable; see `just redis`.
test:
    cargo test --locked --workspace
    cargo test --locked -p butler-jobs --no-default-features

ci: format-check lint test

# Advisory, duplicate-version, and source checks over the dependency graph.
audit-deps:
    cargo deny check advisories bans sources

# Audit the GitHub workflows for injection, over-broad permissions, and unpinned actions.
audit-workflows:
    actionlint .github/workflows/*.yml
    zizmor .github/

# How the worker uses cores: I/O-bound and CPU-bound jobs, in memory.
bench:
    cargo run --locked --release -p demo --bin bench

# Package and verify both crates the way crates.io would, without uploading.
publish-dry-run:
    cargo publish --locked --workspace --dry-run

# Publish main's HEAD to crates.io through the Publish workflow, which checks
# that CI passed on it, then tags v<version>. Bump the workspace version first.
release:
    #!/usr/bin/env bash
    set -euo pipefail
    git fetch origin main
    sha=$(git rev-parse origin/main)
    echo "Publishing $sha"
    gh workflow run publish.yml --ref main -f expected_sha="$sha"

# Parse every mermaid diagram in the README the way GitHub does. Needs Node.
check-diagrams:
    bash scripts/check-mermaid.sh README.md

# The dashboard in a real browser (Playwright, Chromium), as CI runs it.
# Needs Node; builds and starts its own seeded server for each test.
e2e:
    cd e2e && npm ci --no-audit --no-fund && npx playwright install chromium && npm test

# Rebuild the dashboard's stylesheet after changing templates or ui/input.css.
# Needs the standalone Tailwind CSS v4 CLI (`tailwindcss`).
web-css:
    cd crates/butler-web && tailwindcss -i ui/input.css -o assets/app.css --minify

# The dashboard, reading ./butler.toml like the demo worker: http://127.0.0.1:9090
web:
    cargo run --locked -p butler-web

# A throwaway Redis for the demo and the Redis test.
redis:
    docker run -d --rm --name butler-redis -p 6379:6379 redis:8-alpine

redis-stop:
    docker stop butler-redis

# Run each in its own terminal, from the repository root.
worker:
    cargo run --locked -p demo --bin worker

injector:
    cargo run --locked -p demo --bin injector
