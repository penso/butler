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
    cargo clippy --locked -p butler --all-targets --no-default-features -- -D warnings
    cargo clippy --locked -p butler --all-targets --no-default-features --features tokio -- -D warnings
    cargo clippy --locked -p butler --all-targets --no-default-features --features redis -- -D warnings
    cargo clippy --locked -p butler --all-targets --no-default-features --features sqlite -- -D warnings

# The Redis test is skipped when no server is reachable; see `just redis`.
test:
    cargo test --locked --workspace
    cargo test --locked -p butler --no-default-features

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

# Publishes butler-macros, then butler. Needs `cargo login` and a license in Cargo.toml.
publish:
    cargo publish --locked --workspace

# Parse every mermaid diagram in the README the way GitHub does. Needs Node.
check-diagrams:
    bash scripts/check-mermaid.sh README.md

# Rebuild the dashboard's stylesheet after changing templates or ui/input.css.
# Needs the standalone Tailwind CSS v4 CLI (`tailwindcss`).
web-css:
    cd crates/butler-web && tailwindcss -i ui/input.css -o assets/app.css --minify

# The dashboard, reading ./config.toml like the demo worker: http://127.0.0.1:9090
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
