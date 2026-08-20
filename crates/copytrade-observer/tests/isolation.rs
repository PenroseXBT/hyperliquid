use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

fn observer() -> Command {
    Command::new(env!("CARGO_BIN_EXE_copytrade-observer"))
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
            panic!("observer attempted to open a key-path trap or failed to exit");
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
fn observer_startup_is_successful_and_reproducible() {
    let first = run_manifest(&mut observer());
    let second = run_manifest(&mut observer());
    assert!(first.status.success());
    assert_eq!(first.stdout, second.stdout);
    assert!(first.stderr.is_empty());
}

#[test]
fn plausible_key_environment_and_files_do_not_change_observer_behavior() {
    let baseline = run_manifest(&mut observer());
    let key_path_trap = temporary_path("key-path-trap");
    let status = Command::new("mkfifo").arg(&key_path_trap).status().unwrap();
    assert!(status.success());

    let trapped = run_manifest_with_timeout(
        observer()
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
    let output = observer()
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
        let output = observer().arg(argument).output().unwrap();
        assert!(
            !output.status.success(),
            "{argument} unexpectedly succeeded"
        );
    }
}

#[test]
#[cfg(not(feature = "research-cli"))]
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
    ] {
        let output = observer().arg(command).output().unwrap();
        assert!(!output.status.success(), "{command} unexpectedly succeeded");
    }
}

#[test]
#[cfg(feature = "research-cli")]
fn read_only_observation_schedules_all_candidates_without_transport() {
    let policy = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/read-api-policy.json");
    let output = observer()
        .arg("observe")
        .arg("--config")
        .arg(config_path())
        .arg("--request-policy")
        .arg(policy)
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("candidates=169"));
    assert!(stdout.contains("enqueued=169"));
    assert!(stdout.contains("pending=169"));
}

#[test]
#[cfg(feature = "research-cli")]
fn invalid_read_policy_fails_closed() {
    let invalid = temporary_path("invalid-read-policy");
    fs::write(&invalid, b"{}").unwrap();
    let output = observer()
        .arg("observe")
        .arg("--config")
        .arg(config_path())
        .arg("--request-policy")
        .arg(&invalid)
        .output()
        .unwrap();
    fs::remove_file(&invalid).unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("failed closed"));
}

#[test]
#[cfg(feature = "research-cli")]
fn fixture_plan_is_inert_and_reproducible() {
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/accepted-snapshots.json");
    let run = || {
        observer()
            .arg("plan")
            .arg("--config")
            .arg(config_path())
            .arg("--fixture")
            .arg(&fixture)
            .output()
            .unwrap()
    };
    let first = run();
    let second = run();
    assert!(first.status.success());
    assert_eq!(first.stdout, second.stdout);
    let stdout = String::from_utf8_lossy(&first.stdout);
    assert!(stdout.contains("planned_only=true"));
    assert!(stdout.contains("submission_capable=false"));
    assert!(stdout.contains("retry_generation=0"));
    assert!(!stdout.contains("submitted"));
    assert!(!stdout.contains("filled"));
}

#[test]
#[cfg(feature = "research-cli")]
fn fixture_shadow_is_deterministic_and_unsigned() {
    let fixture =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/accepted-snapshots.json");
    let run = || {
        observer()
            .arg("shadow")
            .arg("--config")
            .arg(config_path())
            .arg("--fixture")
            .arg(&fixture)
            .output()
            .unwrap()
    };
    let first = run();
    let second = run();
    assert!(first.status.success());
    assert_eq!(first.stdout, second.stdout);
    let stdout = String::from_utf8_lossy(&first.stdout);
    assert!(stdout.contains("shadow_only=true"));
    assert!(stdout.contains("submission_capable=false"));
    assert!(stdout.contains("scenario=expected"));
}
