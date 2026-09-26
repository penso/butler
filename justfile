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

# The Redis test is skipped when no server is reachable; see `just redis`.
test:
    cargo test --locked --workspace
    cargo test --locked -p butler --no-default-features

ci: format-check lint test

# License, advisory, and source checks over the dependency graph.
audit-deps:
    cargo deny check

# Audit the GitHub workflows for injection, over-broad permissions, and unpinned actions.
audit-workflows:
    actionlint .github/workflows/*.yml
    zizmor .github/

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
