use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

fn engine() -> Command {
    Command::new(env!("CARGO_BIN_EXE_engine"))
}

fn config_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/copytrade.json")
}

fn run_manifest(command: &mut Command) -> Output {
    command
        .arg("--config")
        .arg(config_path())
        .arg("--print-build-manifest")
        .output()
        .unwrap()
}

fn run_manifest_with_timeout(command: &mut Command, timeout: Duration) -> Output {
    let mut child = command
        .arg("--config")
        .arg(config_path())
        .arg("--print-build-manifest")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if std::time::Instant::now() >= deadline {
            child.kill().unwrap();
            let _ = child.wait();
            panic!("engine attempted to open a key-path trap or failed to exit");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn temporary_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("hl1c-{name}-{}-{nonce}", std::process::id()))
}

#[test]
fn engine_startup_is_successful_and_reproducible() {
    let first = run_manifest(&mut engine());
    let second = run_manifest(&mut engine());
    assert!(first.status.success());
    assert_eq!(first.stdout, second.stdout);
    assert!(first.stderr.is_empty());
}

#[test]
fn plausible_key_environment_and_files_do_not_change_engine_behavior() {
    let baseline = run_manifest(&mut engine());
    let key_path_trap = temporary_path("key-path-trap");
    let status = Command::new("mkfifo").arg(&key_path_trap).status().unwrap();
    assert!(status.success());

    let trapped = run_manifest_with_timeout(
        engine()
            .env("HYPERLIQUID_PRIVATE_KEY", "trap-private-value")
            .env("HYPERLIQUID_MASTER_PRIVATE_KEY", "trap-master-value")
            .env("HYPERLIQUID_AGENT_KEY_OUT", &key_path_trap)
            .env("HYPERLIQUID_MASTER_PRIVATE_KEY_FILE", &key_path_trap),
        Duration::from_secs(2),
    );
    fs::remove_file(&key_path_trap).unwrap();

    assert!(trapped.status.success());
    assert_eq!(baseline.stdout, trapped.stdout);
    assert_eq!(baseline.stderr, trapped.stderr);
    assert!(!trapped.stdout.windows(4).any(|window| window == b"trap"));
}

#[test]
fn invalid_configuration_fails_closed() {
    let invalid = temporary_path("invalid-config");
    fs::write(&invalid, b"{}").unwrap();
    let output = engine()
        .arg("--config")
        .arg(&invalid)
        .arg("--validate-config")
        .output()
        .unwrap();
    fs::remove_file(&invalid).unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("failed closed"));
}

#[test]
fn mutation_capable_arguments_are_not_registered() {
    for argument in [
        "--approve-agent",
        "--copytrade",
        "--submit-order",
        "--cancel-order",
        "--withdraw",
        "--transfer",
        "--private-key",
    ] {
        let output = engine().arg(argument).output().unwrap();
        assert!(
            !output.status.success(),
            "{argument} unexpectedly succeeded"
        );
    }
}

#[test]
fn production_binary_excludes_research_commands() {
    for command in [
        "observe",
        "plan",
        "shadow",
        "qualify-mainnet",
        "qualify-transport",
        "qualify-profitability",
        "qualify-micro-density",
        "finalize-qualification",
        "aggregate-qualification",
        "replay-density",
        "audit-candidates",
        "--execution-mode",
        "--duration",
    ] {
        let output = engine().arg(command).output().unwrap();
        assert!(!output.status.success(), "{command} unexpectedly succeeded");
    }
}

#[test]
fn model_dependency_closure_cannot_reach_execution_authority() {
    let compiler = Command::new("rustc").arg("-vV").output().unwrap();
    assert!(compiler.status.success());
    let compiler = String::from_utf8(compiler.stdout).unwrap();
    let host = compiler
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .unwrap();
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version=1", "--locked", "--offline"])
        .args(["--filter-platform", host])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let graph: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let packages = graph["packages"].as_array().unwrap();
    let members = graph["workspace_members"].as_array().unwrap();
    let workspace: std::collections::BTreeMap<_, _> = packages
        .iter()
        .filter(|package| members.contains(&package["id"]))
        .map(|package| (package["name"].as_str().unwrap(), package))
        .collect();
    assert_eq!(
        workspace.keys().copied().collect::<Vec<_>>(),
        ["engine", "mfce"]
    );
    let binaries: Vec<_> = workspace
        .values()
        .flat_map(|package| {
            package["targets"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|target| {
                    target["kind"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|kind| kind == "bin")
                })
                .map(|target| target["name"].as_str().unwrap())
        })
        .collect();
    assert_eq!(binaries, ["engine"]);

    let nodes = graph["resolve"]["nodes"].as_array().unwrap();
    for (root, forbidden) in [
        (
            "mfce",
            &[
                "engine",
                "ethers",
                "reqwest",
                "tokio",
                "hyper",
                "rusqlite",
                "tokio-tungstenite",
            ][..],
        ),
        ("engine", &["hyperliquid_rust_sdk", "dotenv"][..]),
    ] {
        let mut pending = vec![workspace[root]["id"].as_str().unwrap()];
        let mut visited = std::collections::BTreeSet::new();
        while let Some(id) = pending.pop() {
            if !visited.insert(id) {
                continue;
            }
            let package = packages.iter().find(|package| package["id"] == id).unwrap();
            let name = package["name"].as_str().unwrap();
            assert!(
                !forbidden.contains(&name),
                "{root} depends on forbidden package {name}"
            );
            let node = nodes.iter().find(|node| node["id"] == id).unwrap();
            pending.extend(
                node["dependencies"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|id| id.as_str().unwrap()),
            );
        }
    }
}
