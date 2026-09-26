#![allow(clippy::unwrap_used)]

//! Exercise default discovery in child processes so current-directory and
//! environment changes cannot interfere with other tests.

use std::{
    process::Command,
    sync::atomic::{AtomicU32, Ordering},
};

use butler::{BackendKind, Config, Error};

fn assert_load(files: &[(&str, &str)], env: &[(&str, &str)], expected: &str) {
    static RUN: AtomicU32 = AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "butler-config-loading-{}-{}",
        std::process::id(),
        RUN.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&dir).unwrap();
    for (name, contents) in files {
        std::fs::write(dir.join(name), contents).unwrap();
    }
    let mut child = Command::new(std::env::current_exe().unwrap());
    child
        .args(["--ignored", "--exact", "load_config_child", "--nocapture"])
        .current_dir(&dir);
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("BUTLER_") {
            child.env_remove(key);
        }
    }
    let output = child
        .envs(env.iter().copied())
        .env("CONFIG_TEST_EXPECT", expected)
        .output()
        .unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    assert!(
        output.status.success(),
        "child failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "child process for isolated configuration loading"]
fn load_config_child() {
    let expected = std::env::var("CONFIG_TEST_EXPECT").unwrap();
    let loaded = Config::load();
    if expected == "missing" {
        assert!(matches!(loaded, Err(Error::Config(_))));
        return;
    }
    let config = loaded.unwrap();
    if expected == "defaults" {
        assert_eq!(config.queue.backend, BackendKind::File);
        assert_eq!(
            config.worker.concurrency,
            Config::default().worker.concurrency
        );
    } else {
        assert_eq!(config.queue.backend, BackendKind::Memory);
        assert_eq!(
            config.worker.concurrency,
            expected.parse::<usize>().unwrap()
        );
    }
}

#[test]
fn discovers_butler_toml_without_reading_host_config() {
    assert_load(
        &[
            ("config.toml", "this is not Butler configuration"),
            (
                "butler.toml",
                "[queue]\nbackend = 'memory'\n[worker]\nconcurrency = 7\n",
            ),
        ],
        &[],
        "7",
    );
}

#[test]
fn absent_default_config_uses_defaults() {
    assert_load(&[], &[], "defaults");
}

#[test]
fn host_config_alone_does_not_override_defaults() {
    assert_load(
        &[(
            "config.toml",
            "[queue]\nbackend = 'memory'\n[worker]\nconcurrency = 7\n",
        )],
        &[],
        "defaults",
    );
}

#[test]
fn explicit_config_path_overrides_default_discovery() {
    assert_load(
        &[
            ("butler.toml", "invalid default configuration"),
            (
                "config.toml",
                "[queue]\nbackend = 'memory'\n[worker]\nconcurrency = 9\n",
            ),
        ],
        &[("BUTLER_CONFIG", "config.toml")],
        "9",
    );
}

#[test]
fn environment_overrides_butler_toml() {
    assert_load(
        &[(
            "butler.toml",
            "[queue]\nbackend = 'file'\n[worker]\nconcurrency = 7\n",
        )],
        &[
            ("BUTLER_QUEUE__BACKEND", "memory"),
            ("BUTLER_WORKER__CONCURRENCY", "11"),
        ],
        "11",
    );
}

#[test]
fn explicit_missing_config_is_an_error() {
    assert_load(
        &[("butler.toml", "[queue]\nbackend = 'memory'\n")],
        &[("BUTLER_CONFIG", "missing.toml")],
        "missing",
    );
}
