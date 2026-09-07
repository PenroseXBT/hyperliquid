#![cfg(unix)]

use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT: AtomicUsize = AtomicUsize::new(0);

#[test]
fn container_and_wrapper_use_only_the_engine_executable() {
    let docker = include_str!("../../../Dockerfile.railway");
    let wrapper = include_str!("../../../scripts/run_su6_railway.sh");
    assert!(docker.contains("railway-frozen/engine /app/bin/engine"));
    assert!(docker.contains("sha256sum --check /app/bin/engine.sha256"));
    assert!(wrapper.contains("readonly ENGINE=\"/app/bin/engine\""));
    for old in ["/app/bin/copytrade-observer", "/app/bin/copytrade-signer"] {
        assert!(!docker.contains(old));
        assert!(!wrapper.contains(old));
    }
}

#[test]
fn production_start_has_no_offline_model_prerequisite() {
    let wrapper = include_str!("../../../scripts/run_su6_railway.sh");
    assert!(!wrapper.contains("offline-mfce-gate"));
    assert!(!wrapper.contains("MFCE_OFFLINE_GATE_PATH"));
    assert!(!wrapper.contains("--offline-gate"));
    assert!(wrapper.contains("\"$ENGINE\" continuous"));
}

#[test]
fn production_fatal_exit_requires_an_explicit_operator_restart() {
    let railway = include_str!("../../../railway.toml");
    let wrapper = include_str!("../../../scripts/run_su6_railway.sh");
    assert!(railway.contains("restartPolicyType = \"NEVER\""));
    assert!(railway.contains("restartPolicyMaxRetries = 1"));
    assert!(!railway.contains("restartPolicyType = \"ALWAYS\""));
    assert!(wrapper.contains("operator_restart_required=true"));
    assert!(!wrapper.contains("railway_restart=true"));
}

struct Launcher(PathBuf);

impl Launcher {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "live-launcher-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(root.join("app/bin")).unwrap();
        fs::create_dir(root.join("volume")).unwrap();
        let script = include_str!("../../../scripts/run_su6_railway.sh")
            .replace("/app/", &format!("{}/app/", root.display()));
        fs::write(root.join("launcher.sh"), script).unwrap();
        for (name, script) in [
            (
                "engine",
                "#!/bin/bash\nprintf 'deterministic failure\\n' >&2\nexit 23\n",
            ),
            // Production uses GNU coreutils sync -f. Record the durability
            // boundaries here so this regression also runs on macOS.
            (
                "sync",
                "#!/bin/bash\nprintf '%s\\n' \"$*\" >> \"$SYNC_CALLS\"\n",
            ),
        ] {
            let path = root.join("app/bin").join(name);
            fs::write(&path, script).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        Self(root)
    }

    fn failure(&self) -> PathBuf {
        self.0.join("volume/live/failure")
    }

    fn run(&self) -> Output {
        Command::new("bash")
            .arg(self.0.join("launcher.sh"))
            .env("RAILWAY_VOLUME_MOUNT_PATH", self.0.join("volume"))
            .env("SU6_DATA_ROOT_NAME", "live")
            .env("SYNC_CALLS", self.0.join("sync-calls"))
            .env(
                "PATH",
                format!(
                    "{}/app/bin:{}",
                    self.0.display(),
                    std::env::var("PATH").unwrap()
                ),
            )
            .output()
            .unwrap()
    }
}

impl Drop for Launcher {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn failure_survives_restart_with_exact_status_stderr_and_process_identity() {
    let launcher = Launcher::new();
    let first = launcher.run();
    assert_eq!(first.status.code(), Some(23), "{first:?}");
    let first_meta = fs::read_to_string(launcher.failure().join("last-exit.meta")).unwrap();
    let first_run = first_meta
        .lines()
        .find_map(|line| line.strip_prefix("run_id="))
        .unwrap();
    for field in [
        "exit_code=23",
        "previous_run_id=none",
        "started_at=",
        "exited_at=",
        "engine_pid=",
        "wrapper_pid=",
        "binary_sha256=",
        "stderr_sha256=",
    ] {
        assert!(first_meta.contains(field), "{first_meta}");
    }
    assert_eq!(
        fs::read_to_string(launcher.failure().join("last-exit.stderr")).unwrap(),
        "deterministic failure\n"
    );
    let second = launcher.run();
    assert_eq!(second.status.code(), Some(23), "{second:?}");
    let output = String::from_utf8(second.stdout).unwrap();
    assert!(output.contains(&first_meta));
    assert!(output.contains("deterministic failure"));
    let second_meta = fs::read_to_string(launcher.failure().join("last-exit.meta")).unwrap();
    assert!(second_meta.contains(&format!("previous_run_id={first_run}\n")));
    assert_ne!(first_meta, second_meta);
    let syncs = fs::read_to_string(launcher.0.join("sync-calls")).unwrap();
    assert_eq!(syncs.lines().count(), 6);
    assert!(syncs.lines().all(|line| line.starts_with("-f ")));
}

#[test]
fn diagnostic_schema_rejects_unknown_entries_wrong_types_and_symlinks() {
    for (name, kind) in [
        ("unknown", 0),
        ("last-exit.meta", 1),
        ("process.stderr", 2),
        ("last-exit.stderr.tmp", 2),
        ("current-run.meta.tmp", 2),
    ] {
        let launcher = Launcher::new();
        fs::create_dir_all(launcher.failure()).unwrap();
        let path = launcher.failure().join(name);
        let outside = launcher.0.join("outside");
        fs::write(&outside, b"untouched").unwrap();
        match kind {
            0 => fs::write(path, b"unknown").unwrap(),
            1 => fs::create_dir(path).unwrap(),
            _ => symlink(&outside, path).unwrap(),
        }
        let result = launcher.run();
        assert_eq!(result.status.code(), Some(1), "{result:?}");
        assert!(String::from_utf8(result.stdout)
            .unwrap()
            .contains("invalid_failure_artifact="));
        assert_eq!(fs::read(outside).unwrap(), b"untouched");
        assert!(!launcher.0.join("sync-calls").exists());
    }
}
