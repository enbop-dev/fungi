use anyhow::{Context, Result, bail};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::process::process_is_running;
use crate::state::{LabState, ProcessSpec};

pub(crate) const STATE_FILE: &str = "state.json";
pub(crate) const STATE_VERSION: u32 = 2;
const OWNER_FILE: &str = ".fungi-lab";
const OWNER: &str = "fungi-lab v2\n";

pub(crate) fn lock_lab(root: &Path, initialize: bool) -> Result<File> {
    if fs::symlink_metadata(root).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        bail!("refusing symlinked lab directory {}", root.display());
    }
    if initialize {
        fs::create_dir_all(root)?;
        if !root.join(OWNER_FILE).exists() {
            if fs::read_dir(root)?.next().is_some() {
                bail!(
                    "lab directory is not empty and is not owned by this fungi-lab version; choose a new --lab-dir"
                );
            }
            let mut marker = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(root.join(OWNER_FILE))?;
            marker.write_all(OWNER.as_bytes())?;
            marker.sync_all()?;
        }
    }
    validate_owner(root)?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.join(OWNER_FILE))?;
    file.try_lock()
        .context("another command is managing this lab; try again when it finishes")?;
    Ok(file)
}

fn validate_owner(root: &Path) -> Result<()> {
    if fs::symlink_metadata(root)?.file_type().is_symlink()
        || fs::symlink_metadata(root.join(OWNER_FILE))
            .context("unrecognized lab directory; use the previous tool for old labs, or select a fresh --lab-dir")?
            .file_type()
            .is_symlink()
        || fs::read_to_string(root.join(OWNER_FILE))? != OWNER
    {
        bail!(
            "refusing unrecognized or symlinked lab directory {}",
            root.display()
        );
    }
    Ok(())
}

pub(crate) fn wait_ready_with_bin(
    fungi_bin: &Path,
    repo: &Path,
    fungi_dir: &Path,
    timeout: Duration,
) -> Result<()> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        let output = bounded_output(
            Command::new(fungi_bin)
                .current_dir(repo)
                .arg("--fungi-dir")
                .arg(fungi_dir)
                .arg("info")
                .arg("version"),
            None,
        )
        .context("failed to probe daemon readiness")?;

        if output.status.success() {
            return Ok(());
        }

        thread::sleep(Duration::from_millis(300));
    }

    bail!("daemon did not become ready within {:?}", timeout)
}

pub(crate) fn wait_peer_id(
    fungi_bin: &Path,
    repo: &Path,
    fungi_dir: &Path,
    timeout: Duration,
) -> Result<String> {
    let started = Instant::now();
    let mut last = String::new();
    while started.elapsed() < timeout {
        let output = bounded_output(
            Command::new(fungi_bin)
                .current_dir(repo)
                .arg("--fungi-dir")
                .arg(fungi_dir)
                .arg("info")
                .arg("id"),
            None,
        )
        .context("failed to query daemon peer id")?;
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            if let Some(peer_id) = parse_peer_id(&stdout) {
                return Ok(peer_id);
            }
            last = stdout.to_string();
        } else {
            last = String::from_utf8_lossy(&output.stderr).to_string();
        }
        thread::sleep(Duration::from_millis(300));
    }
    bail!(
        "daemon RPC did not become ready for {}\n{}",
        fungi_dir.display(),
        last
    )
}

pub(crate) fn run_cli_capture<I, S>(
    fungi_bin: &Path,
    repo: &Path,
    fungi_dir: &Path,
    args: I,
    input: Option<&str>,
) -> Result<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let output = run_cli_output(fungi_bin, repo, fungi_dir, args, input)?;
    if !output.status.success() {
        bail!(
            "fungi command failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub(crate) fn run_cli_status<I, S>(
    fungi_bin: &Path,
    repo: &Path,
    fungi_dir: &Path,
    args: I,
    input: Option<&str>,
) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let output = run_cli_output(fungi_bin, repo, fungi_dir, args, input)?;
    if !output.status.success() {
        bail!(
            "fungi command failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

pub(crate) fn run_cli_output<I, S>(
    fungi_bin: &Path,
    repo: &Path,
    fungi_dir: &Path,
    args: I,
    input: Option<&str>,
) -> Result<std::process::Output>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut command = Command::new(fungi_bin);
    command.current_dir(repo).arg("--fungi-dir").arg(fungi_dir);
    for arg in args {
        command.arg(arg.as_ref());
    }
    bounded_output(&mut command, input)
}

fn bounded_output(command: &mut Command, input: Option<&str>) -> Result<std::process::Output> {
    // Files avoid blocking on a full pipe while enforcing a subprocess deadline.
    let mut stdout = tempfile::tempfile()?;
    let mut stderr = tempfile::tempfile()?;
    command.stdin(if input.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    command
        .stdout(stdout.try_clone()?)
        .stderr(stderr.try_clone()?);
    let mut child = command.spawn().context("failed to run fungi command")?;
    if let Some(input) = input {
        use std::io::Write;
        if let Err(error) = child
            .stdin
            .take()
            .context("failed to open command stdin")?
            .write_all(input.as_bytes())
        {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error.into());
        }
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            bail!("fungi command timed out after 10 seconds: {command:?}");
        }
        thread::sleep(Duration::from_millis(50));
    };
    stdout.rewind()?;
    stderr.rewind()?;
    let mut out = Vec::new();
    let mut err = Vec::new();
    stdout.read_to_end(&mut out)?;
    stderr.read_to_end(&mut err)?;
    Ok(std::process::Output {
        status,
        stdout: out,
        stderr: err,
    })
}

pub(crate) fn wait_relay_peer_id_from_log(
    log: &Path,
    offset: u64,
    timeout: Duration,
) -> Result<String> {
    let started = Instant::now();
    let mut last = String::new();
    while started.elapsed() < timeout {
        let mut contents = String::new();
        if let Ok(mut file) = File::open(log) {
            file.seek(SeekFrom::Start(offset))?;
            file.read_to_string(&mut contents)?;
            if contents.contains("Added external addresses:")
                && let Some(peer_id) = contents.lines().find_map(|line| {
                    line.trim()
                        .strip_prefix("Local peer id: ")
                        .map(ToOwned::to_owned)
                })
            {
                return Ok(peer_id);
            }
            last = contents
                .lines()
                .rev()
                .take(20)
                .collect::<Vec<_>>()
                .join("\n");
        }
        thread::sleep(Duration::from_millis(100));
    }
    bail!(
        "timed out waiting for relay peer id in {}\n{}",
        log.display(),
        last
    )
}

pub(crate) fn write_node_config(
    fungi_dir: &Path,
    rpc_port: u16,
    tcp_port: u16,
    udp_port: u16,
    relay_addrs: &[String],
) -> Result<()> {
    let relay_list = relay_addrs
        .iter()
        .map(|addr| format!("\"{addr}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let config = format!(
        "version = 3\n\n[rpc]\nlisten_address = \"127.0.0.1:{rpc_port}\"\n\n[network]\nlisten_tcp_port = {tcp_port}\nlisten_udp_port = {udp_port}\nrelay_enabled = true\nuse_community_relays = false\ncustom_relay_addresses = [{relay_list}]\n\n[runtime]\ndisable_docker = false\ndisable_wasmtime = false\n"
    );
    fs::write(fungi_dir.join("config.toml"), config).with_context(|| {
        format!(
            "failed to write {}",
            fungi_dir.join("config.toml").display()
        )
    })
}

pub(crate) fn wait_for_state(
    root: &Path,
    manager: &mut Child,
    manager_log: &Path,
    timeout: Duration,
) -> Result<LabState> {
    let started = Instant::now();
    let mut last_error = None;
    while started.elapsed() < timeout {
        match read_state(root) {
            Ok(state) if state.ready && state.manager_pid == Some(manager.id()) => {
                return Ok(state);
            }
            Ok(_) => {}
            Err(error) => last_error = Some(error.to_string()),
        }
        if let Some(status) = manager
            .try_wait()
            .context("failed to inspect fungi-lab manager process")?
        {
            bail!(
                "fungi-lab manager exited before startup completed ({status}){}",
                log_tail_suffix(manager_log)
            );
        }
        thread::sleep(Duration::from_millis(250));
    }
    bail!(
        "timed out waiting for local lab startup{}{}",
        last_error
            .map(|error| format!(": {error}"))
            .unwrap_or_default(),
        log_tail_suffix(manager_log)
    )
}

fn log_tail_suffix(log: &Path) -> String {
    let Ok(contents) = fs::read_to_string(log) else {
        return String::new();
    };
    let mut lines = contents.lines().rev().take(40).collect::<Vec<_>>();
    lines.reverse();
    if lines.is_empty() {
        String::new()
    } else {
        format!("\nmanager log ({}):\n{}", log.display(), lines.join("\n"))
    }
}

pub(crate) fn read_state(root: &Path) -> Result<LabState> {
    validate_owner(root)?;
    let path = root.join(STATE_FILE);
    let raw =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let value: serde_json::Value = serde_json::from_str(&raw)
        .with_context(|| format!("invalid state {}; retained for inspection", path.display()))?;
    if value.get("version").and_then(|v| v.as_u64()) != Some(STATE_VERSION as u64) {
        bail!(
            "unsupported lab state version {}; supported version is {STATE_VERSION}. Use the matching old tool to stop/clean, or choose a new --lab-dir",
            value["version"]
        );
    }
    let state: LabState = serde_json::from_value(value)?;
    if state.root != root
        || state.node_a.dir != root.join("nodes/a/fungi")
        || state.node_b.dir != root.join("nodes/b/fungi")
        || state.relay.home != root.join("relay-home")
        || state.relay.log != root.join("relay.log")
    {
        bail!("state paths do not match this lab's fixed layout; refusing to manage it");
    }
    for relative in [
        "nodes",
        "nodes/a",
        "nodes/b",
        "nodes/a/fungi",
        "nodes/b/fungi",
        "nodes/a/Fungi",
        "nodes/a/FungiDev",
        "nodes/b/Fungi",
        "nodes/b/FungiDev",
        "relay-home",
        "relay.log",
        "node-a.log",
        "node-b.log",
        "manager.log",
        STATE_FILE,
    ] {
        if let Ok(metadata) = fs::symlink_metadata(root.join(relative))
            && metadata.file_type().is_symlink()
        {
            bail!("symlink in lab layout: {relative}; refusing to manage it");
        }
    }
    Ok(state)
}

pub(crate) fn write_state(state: &LabState) -> Result<()> {
    fs::create_dir_all(&state.root)?;
    let path = state.root.join(STATE_FILE);
    let raw = serde_json::to_string_pretty(state)?;
    let temporary = state.root.join(format!("state-{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    let result = (|| -> Result<()> {
        file.write_all(raw.as_bytes())?;
        file.sync_all()?;
        fs::rename(&temporary, &path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.with_context(|| format!("failed to write {}", path.display()))
}

pub(crate) fn default_root() -> Result<PathBuf> {
    Ok(find_repo_root()?.join("target/local-lab"))
}

pub(crate) fn selected_root(root: Option<PathBuf>) -> Result<PathBuf> {
    let root = root.map_or_else(default_root, Ok)?;
    let root = std::path::absolute(root)?;
    if root
        .components()
        .any(|part| part == std::path::Component::ParentDir)
    {
        bail!("lab directory must not contain '..'");
    }
    Ok(root)
}

pub(crate) fn find_repo_root() -> Result<PathBuf> {
    let mut candidates = Vec::new();
    candidates.push(std::env::current_dir()?);
    if let Ok(exe) = std::env::current_exe()
        && let Some(parent) = exe.parent()
    {
        candidates.push(parent.to_path_buf());
    }
    for start in candidates {
        for path in start.ancestors() {
            if path.join("Cargo.toml").exists() && path.join("fungi/Cargo.toml").exists() {
                return Ok(path.to_path_buf());
            }
        }
    }
    bail!("could not find fungi repo root")
}

pub(crate) fn print_started_summary(state: &LabState) {
    println!("Fungi local lab started.");
    println!("  lab-dir: {}", state.root.display());
    println!("  trust:  {}", state.trust.as_arg());
    println!("  node-a: {}", state.node_a.dir.display());
    println!("  node-b: {}", state.node_b.dir.display());
    println!(
        "  manager log: {}",
        state.root.join("manager.log").display()
    );
    println!("  A peer: {}", state.node_a.peer_id);
    println!("  B peer: {}", state.node_b.peer_id);
    println!("  recorded lab paths:");
    println!("    {}", state.root.display());
    println!("    {}", state.node_a.dir.display());
    println!("    {}", state.node_b.dir.display());
    println!(
        "  rollback: fungi-lab --lab-dir {} trust none",
        shell_quote_path(&state.root)
    );
    println!();
    println!(
        "Use: ./target/debug/fungi -f {} service list",
        display_path_arg(&state.repo, &state.node_a.dir)
    );
    println!(
        "Use: ./target/debug/fungi -f {} service list",
        display_path_arg(&state.repo, &state.node_b.dir)
    );
}

pub(crate) fn print_process(
    name: &str,
    pid: Option<u32>,
    peer_id: Option<&str>,
    log: Option<&Path>,
    spec: &ProcessSpec,
) {
    let state = if process_is_running(pid, spec) {
        "running"
    } else {
        "stopped"
    };
    println!(
        "  {name}: {state} pid={}",
        pid.map_or("-".to_string(), |pid| pid.to_string())
    );
    if let Some(peer_id) = peer_id {
        println!("    peer_id: {peer_id}");
    }
    if let Some(log) = log {
        println!("    log: {}", log.display());
    }
}

pub(crate) fn parse_peer_id(text: &str) -> Option<String> {
    text.split_whitespace()
        .find(|part| part.starts_with("16Uiu"))
        .map(ToOwned::to_owned)
}

pub(crate) fn circuit_addr(relay_addr: &str, peer_id: &str) -> String {
    format!("{relay_addr}/p2p-circuit/p2p/{peer_id}")
}

pub(crate) fn epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn shell_quote(value: impl AsRef<str>) -> String {
    let value = value.as_ref();
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

pub(crate) fn shell_quote_path(value: &Path) -> String {
    shell_quote(value.display().to_string())
}

pub(crate) fn display_path_arg<'a>(repo: &'a Path, path: &'a Path) -> std::path::Display<'a> {
    path.strip_prefix(repo).unwrap_or(path).display()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(root: &Path) -> LabState {
        use crate::state::{NodeState, RelayState, TrustMode};
        LabState {
            version: STATE_VERSION,
            repo: root.to_path_buf(),
            root: root.to_path_buf(),
            fungi_bin: root.join("unused-binary"),
            manager_bin: root.join("unused-manager"),
            manager_pid: None,
            ready: false,
            created_at_epoch_secs: 0,
            expires_at_epoch_secs: 0,
            trust: TrustMode::None,
            node_a: NodeState::empty("a", root.join("nodes/a/fungi")),
            node_b: NodeState::empty("b", root.join("nodes/b/fungi")),
            relay: RelayState {
                pid: None,
                home: root.join("relay-home"),
                log: root.join("relay.log"),
                peer_id: String::new(),
                tcp_port: 1001,
                udp_port: 1002,
                tcp_addr: String::new(),
                udp_addr: String::new(),
            },
        }
    }

    #[test]
    fn refuses_unowned_directories_and_concurrent_commands() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("keep"), "user data").unwrap();
        assert!(lock_lab(temp.path(), true).is_err());
        assert_eq!(
            fs::read_to_string(temp.path().join("keep")).unwrap(),
            "user data"
        );
        let root = temp.path().join("lab");
        let lock = lock_lab(&root, true).unwrap();
        assert!(lock_lab(&root, false).is_err());
        drop(lock);
        assert!(lock_lab(&root, false).is_ok());
    }

    #[test]
    fn state_rejects_corruption_versions_and_external_paths() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let _lock = lock_lab(root, true).unwrap();
        fs::write(root.join(STATE_FILE), "broken").unwrap();
        assert!(read_state(root).is_err());
        let mut state = fixture(root);
        state.version = 1;
        write_state(&state).unwrap();
        assert!(
            read_state(root)
                .unwrap_err()
                .to_string()
                .contains("unsupported lab state version")
        );
        state.version = STATE_VERSION;
        state.node_a.dir = root.join("../outside");
        write_state(&state).unwrap();
        assert!(read_state(root).is_err());
    }

    #[test]
    fn state_readers_only_observe_complete_writes() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let _lock = lock_lab(root, true).unwrap();
        let state = fixture(root);
        write_state(&state).unwrap();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let mut state = state.clone();
                for i in 0..100 {
                    state.created_at_epoch_secs = i;
                    write_state(&state).unwrap();
                }
            });
            for _ in 0..100 {
                read_state(root).unwrap();
            }
        });
    }

    #[cfg(unix)]
    #[test]
    fn state_rejects_symlinked_node_storage() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("target");
        fs::create_dir(&target).unwrap();
        let alias = temp.path().join("alias");
        std::os::unix::fs::symlink(&target, &alias).unwrap();
        assert!(lock_lab(&alias, true).is_err());
        assert!(fs::read_dir(&target).unwrap().next().is_none());
        let root = temp.path().join("lab");
        let _lock = lock_lab(&root, true).unwrap();
        write_state(&fixture(&root)).unwrap();
        std::os::unix::fs::symlink(temp.path(), root.join("nodes")).unwrap();
        assert!(read_state(&root).is_err());
    }

    #[test]
    fn relay_readiness_ignores_previous_log_runs() {
        let temp = tempfile::tempdir().unwrap();
        let log = temp.path().join("relay.log");
        let previous = "Local peer id: old\n";
        fs::write(
            &log,
            format!("{previous}Local peer id: new\nAdded external addresses:\n"),
        )
        .unwrap();
        assert_eq!(
            wait_relay_peer_id_from_log(&log, previous.len() as u64, Duration::from_secs(1))
                .unwrap(),
            "new"
        );
    }

    #[test]
    fn generated_node_config_disables_community_relays() {
        let dir = tempfile::tempdir().unwrap();
        write_node_config(
            dir.path(),
            1111,
            2222,
            3333,
            &["/ip4/127.0.0.1/tcp/4444/p2p/relay".to_string()],
        )
        .unwrap();

        let content = fs::read_to_string(dir.path().join("config.toml")).unwrap();
        assert!(content.contains("listen_address = \"127.0.0.1:1111\""));
        assert!(content.contains("listen_tcp_port = 2222"));
        assert!(content.contains("listen_udp_port = 3333"));
        assert!(content.contains("relay_enabled = true"));
        assert!(content.contains("use_community_relays = false"));
        assert!(content.contains("/ip4/127.0.0.1/tcp/4444/p2p/relay"));
    }

    #[test]
    fn peer_id_parser_finds_libp2p_peer_id() {
        let peer_id = "16Uiu2HAmGXFS6aYsKKYRkEDo1tNigZKN8TAYrsfSnEdC5sZLNkiE";
        assert_eq!(
            parse_peer_id(&format!("Local Peer ID: {peer_id}")),
            Some(peer_id.to_string())
        );
    }

    #[test]
    fn log_tail_suffix_is_bounded_and_keeps_order() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("manager.log");
        let contents = (1..=45)
            .map(|line| format!("line {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&log, contents).unwrap();

        let suffix = log_tail_suffix(&log);
        assert!(!suffix.contains("line 5\n"));
        assert!(suffix.contains("line 6\nline 7"));
        assert!(suffix.ends_with("line 45"));
    }
}
