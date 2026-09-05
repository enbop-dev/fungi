use anyhow::{Context, Result, anyhow, bail};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::Duration,
};

use crate::cli::{ManagerArgs, StartArgs, StatusArgs};
use crate::process::{
    detach_process_group, get_fungi_binary_path, process_is_running, reserve_tcp_port,
    reserve_udp_port, stop_pid,
};
use crate::state::{
    LabState, NodeCommand, NodeName, NodeState, ProcessCommand, RelayState, TrustMode,
};
use crate::util::{
    circuit_addr, display_path_arg, epoch_secs, find_repo_root, print_process,
    print_started_summary, read_state, run_cli_capture, run_cli_status, shell_quote,
    shell_quote_path, wait_for_state, wait_peer_id, wait_relay_peer_id_from_log, write_node_config,
    write_state,
};

const STATE_FILE: &str = crate::util::STATE_FILE;
const MANAGER_LOG: &str = "manager.log";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const TTL_SECS: u64 = 2 * 60 * 60;

pub(crate) fn start_background_lab(args: StartArgs) -> Result<()> {
    let repo = find_repo_root()?;
    let repo = repo.canonicalize().unwrap_or(repo);
    let fungi_bin = match args.fungi_bin {
        Some(path) => path,
        None => get_fungi_binary_path()?,
    };
    let fungi_bin = fungi_bin.canonicalize().unwrap_or(fungi_bin);
    let root = args.root.unwrap_or_else(|| repo.join("target/local-lab"));

    if root.join(STATE_FILE).exists() {
        let state = read_state(&root)?;
        if process_is_running(state.manager_pid, &state.process_spec_for_manager()) {
            bail!("local lab is already running. Use `fungi-lab stop` first.");
        }
        stop_lab(&state).context("failed to stop previous local lab; refusing to start over it")?;
        fs::remove_file(root.join(STATE_FILE))?;
    }
    if root.join("cancel").exists() {
        fs::remove_file(root.join("cancel"))?;
    }

    fs::create_dir_all(&root)?;
    let manager_log = root.join(MANAGER_LOG);
    let stdout = open_log(&manager_log)?;
    let stderr = stdout
        .try_clone()
        .with_context(|| format!("failed to clone {}", manager_log.display()))?;

    let current_exe = std::env::current_exe().context("failed to locate fungi-lab binary")?;
    let mut command = Command::new(current_exe);
    command
        .arg("manager")
        .arg("--repo")
        .arg(&repo)
        .arg("--fungi-bin")
        .arg(&fungi_bin)
        .arg("--lab-dir")
        .arg(&root)
        .arg("--trust")
        .arg(args.trust.as_arg())
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    detach_process_group(&mut command);
    let mut child = command
        .spawn()
        .context("failed to start fungi-lab manager")?;

    let state = await_startup(&root, &mut child, &manager_log, STARTUP_TIMEOUT)?;
    print_started_summary(&state);
    Ok(())
}

fn await_startup(
    root: &Path,
    child: &mut std::process::Child,
    manager_log: &Path,
    timeout: Duration,
) -> Result<LabState> {
    match wait_for_state(root, child, manager_log, timeout) {
        Ok(state) => Ok(state),
        Err(error) => {
            // Let bounded startup operations finish and roll back. Killing the
            // manager between spawn and PID recording can orphan a child.
            fs::write(root.join("cancel"), b"cancel startup")?;
            let deadline = std::time::Instant::now() + Duration::from_secs(45);
            while child.try_wait()?.is_none() {
                if std::time::Instant::now() >= deadline {
                    bail!(
                        "{error:#}\nmanager is still cleaning up; inspect {}",
                        manager_log.display()
                    );
                }
                thread::sleep(Duration::from_millis(100));
            }
            let cleanup = cleanup_partial_state(root);
            Err(with_cleanup_result(error, cleanup))
        }
    }
}

pub(crate) fn run_manager(args: ManagerArgs) -> Result<()> {
    fs::create_dir_all(&args.root)?;
    let now = epoch_secs();
    let mut state = LabState {
        version: crate::util::STATE_VERSION,
        repo: args.repo.clone(),
        root: args.root.clone(),
        fungi_bin: args.fungi_bin.clone(),
        manager_bin: std::env::current_exe()?,
        manager_pid: Some(std::process::id()),
        ready: false,
        created_at_epoch_secs: now,
        expires_at_epoch_secs: now.saturating_add(TTL_SECS),
        trust: args.trust,
        relay: RelayState {
            pid: None,
            home: args.root.join("relay-home"),
            log: args.root.join("relay.log"),
            peer_id: String::new(),
            tcp_port: reserve_tcp_port()?,
            udp_port: reserve_udp_port()?,
            tcp_addr: String::new(),
            udp_addr: String::new(),
        },
        node_a: NodeState::empty("a", args.root.join("nodes/a/fungi")),
        node_b: NodeState::empty("b", args.root.join("nodes/b/fungi")),
    };
    let startup = (|| -> Result<()> {
        write_state(&state)?;
        start_relay(&mut state)?;
        state.node_a = start_node(&mut state, NodeName::A)?;
        write_state(&state)?;
        state.node_b = start_node(&mut state, NodeName::B)?;
        write_state(&state)?;

        add_lab_devices(&state)?;
        apply_trust_mode_to_state(&state, state.trust)?;
        check_cancelled(&state.root)?;
        state.ready = true;
        write_state(&state)?;
        Ok(())
    })();
    if let Err(error) = startup {
        let cleanup = stop_lab_processes(&state, true);
        state.ready = false;
        state.manager_pid = None;
        if cleanup.is_ok() {
            state.relay.pid = None;
            state.node_a.pid = None;
            state.node_b.pid = None;
        }
        let state_update = write_state(&state);
        let error = with_cleanup_result(error, cleanup);
        return match state_update {
            Ok(()) => Err(error),
            Err(state_error) => Err(anyhow!(
                "{error:#}\nfailed to record rolled-back state: {state_error:#}"
            )),
        };
    }

    supervise(state)
}

fn supervise(mut state: LabState) -> Result<()> {
    loop {
        thread::sleep(Duration::from_secs(1));
        if let Ok(latest) = read_state(&state.root) {
            state = latest;
        }
        if epoch_secs() >= state.expires_at_epoch_secs || state.root.join("cancel").exists() {
            // The starting CLI holds the lock until cancellation has finished.
            let _lock = if state.root.join("cancel").exists() {
                None
            } else {
                let Ok(lock) = crate::util::lock_lab(&state.root, false) else {
                    continue;
                };
                Some(lock)
            };
            let mut latest = match read_state(&state.root) {
                Ok(latest) => latest,
                Err(error) => {
                    stop_lab_processes(&state, true)?;
                    return Err(error.context(
                        "stopped last known lab processes; invalid state retained for inspection",
                    ));
                }
            };
            stop_lab_processes(&latest, true)?;
            latest.ready = false;
            latest.manager_pid = None;
            latest.relay.pid = None;
            latest.node_a.pid = None;
            latest.node_b.pid = None;
            write_state(&latest)?;
            return Ok(());
        }
    }
}

pub(crate) fn print_status(root: &Path, args: StatusArgs) -> Result<()> {
    let state = read_state(root)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&state)?);
        return Ok(());
    }

    println!("Fungi local lab");
    println!("  root: {}", state.root.display());
    println!("  ready: {}", state.ready);
    println!("  expires_at_epoch_secs: {}", state.expires_at_epoch_secs);
    print_process(
        "manager",
        state.manager_pid,
        None,
        None,
        &state.process_spec_for_manager(),
    );
    print_process(
        "relay",
        state.relay.pid,
        Some(&state.relay.peer_id),
        Some(&state.relay.log),
        &state.process_spec_for_relay(),
    );
    print_process(
        "node-a",
        state.node_a.pid,
        Some(&state.node_a.peer_id),
        Some(&state.node_a.log),
        &state.process_spec_for_node(NodeName::A),
    );
    print_process(
        "node-b",
        state.node_b.pid,
        Some(&state.node_b.peer_id),
        Some(&state.node_b.log),
        &state.process_spec_for_node(NodeName::B),
    );
    println!(
        "  fungi a: {} -f {}",
        state.fungi_bin.display(),
        display_path_arg(&state.repo, &state.node_a.dir)
    );
    println!(
        "  fungi b: {} -f {}",
        state.fungi_bin.display(),
        display_path_arg(&state.repo, &state.node_b.dir)
    );
    Ok(())
}

pub(crate) fn stop_selected_lab(root: &Path) -> Result<()> {
    let state = read_state(root)?;
    stop_lab(&state)?;
    let mut state = state;
    state.ready = false;
    state.manager_pid = None;
    state.relay.pid = None;
    state.node_a.pid = None;
    state.node_b.pid = None;
    write_state(&state)?;
    println!("Stopped Fungi local lab processes.");
    Ok(())
}

pub(crate) fn clean_lab(root: &Path) -> Result<()> {
    let state = read_state(root)?;
    let repo = find_repo_root()?;
    ensure_safe_removal_target(root, "lab directory", &[&repo, &state.fungi_bin])?;
    stop_lab(&state)?;
    if root.exists() {
        fs::remove_dir_all(root)
            .with_context(|| format!("failed to remove lab root {}", root.display()))?;
    }
    println!("Removed Fungi local lab directories.");
    Ok(())
}

pub(crate) fn print_env(root: &Path) -> Result<()> {
    let state = read_state(root)?;
    println!("export FUNGI_BIN={}", shell_quote_path(&state.fungi_bin));
    println!("export FUNGI_LAB_DIR={}", shell_quote_path(&state.root));
    println!("export FUNGI_A_DIR={}", shell_quote_path(&state.node_a.dir));
    println!("export FUNGI_B_DIR={}", shell_quote_path(&state.node_b.dir));
    println!(
        "export FUNGI_A_PEER_ID={}",
        shell_quote(&state.node_a.peer_id)
    );
    println!(
        "export FUNGI_B_PEER_ID={}",
        shell_quote(&state.node_b.peer_id)
    );
    println!(
        "export FUNGI_RELAY_TCP_ADDR={}",
        shell_quote(&state.relay.tcp_addr)
    );
    println!(
        "export FUNGI_RELAY_UDP_ADDR={}",
        shell_quote(&state.relay.udp_addr)
    );
    Ok(())
}

pub(crate) fn manage_node(root: &Path, command: NodeCommand) -> Result<()> {
    let mut state = read_state(root)?;
    match command {
        NodeCommand::Stop { node } => {
            let spec = state.process_spec_for_node(node);
            let node_state = state.node_mut(node);
            stop_pid(node_state.pid, &spec, true)?;
            node_state.pid = None;
        }
        NodeCommand::Start { node } => {
            let current_pid = state.node(node).pid;
            if process_is_running(current_pid, &state.process_spec_for_node(node)) {
                println!("node {:?} is already running.", node);
                return Ok(());
            }
            require_manager(&state)?;
            let updated = start_node(&mut state, node)?;
            *state.node_mut(node) = updated;
        }
        NodeCommand::Restart { node } => {
            {
                let spec = state.process_spec_for_node(node);
                let node_state = state.node_mut(node);
                stop_pid(node_state.pid, &spec, true)?;
                node_state.pid = None;
            }
            require_manager(&state)?;
            let updated = start_node(&mut state, node)?;
            *state.node_mut(node) = updated;
        }
    }
    write_state(&state)?;
    println!("Updated node {:?}.", command.node());
    Ok(())
}

pub(crate) fn manage_relay(root: &Path, command: ProcessCommand) -> Result<()> {
    let mut state = read_state(root)?;
    match command {
        ProcessCommand::Stop => {
            stop_pid(state.relay.pid, &state.process_spec_for_relay(), true)?;
            state.relay.pid = None;
        }
        ProcessCommand::Start => {
            if process_is_running(state.relay.pid, &state.process_spec_for_relay()) {
                println!("relay is already running.");
                return Ok(());
            }
            require_manager(&state)?;
            start_relay(&mut state)?;
        }
        ProcessCommand::Restart => {
            stop_pid(state.relay.pid, &state.process_spec_for_relay(), true)?;
            state.relay.pid = None;
            require_manager(&state)?;
            start_relay(&mut state)?;
        }
    }
    write_state(&state)?;
    println!("Updated relay.");
    Ok(())
}

pub(crate) fn configure_trust(root: &Path, mode: TrustMode) -> Result<()> {
    let mut state = read_state(root)?;
    apply_trust_mode_to_state(&state, mode)?;
    state.trust = mode;
    write_state(&state)?;
    println!("Trust mode set to {:?}.", mode);
    Ok(())
}

fn start_relay(state: &mut LabState) -> Result<()> {
    check_cancelled(&state.root)?;
    let relay_home = state.relay.home.clone();
    let relay_log = state.relay.log.clone();
    fs::create_dir_all(&relay_home)
        .with_context(|| format!("failed to create relay home {}", relay_home.display()))?;
    let tcp_port = state.relay.tcp_port;
    let udp_port = state.relay.udp_port;
    let stdout = open_log(&relay_log)?;
    let log_offset = stdout.metadata()?.len();
    let stderr = stdout
        .try_clone()
        .with_context(|| format!("failed to clone {}", relay_log.display()))?;
    let mut command = Command::new(&state.fungi_bin);
    command
        .env("HOME", &relay_home)
        .arg("daemon")
        .arg("relay-server")
        .arg("--public-ip")
        .arg("127.0.0.1")
        .arg("--tcp-listen-port")
        .arg(tcp_port.to_string())
        .arg("--udp-listen-port")
        .arg(udp_port.to_string())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    detach_process_group(&mut command);
    let mut child = command.spawn().context("failed to start local relay")?;
    state.relay.pid = Some(child.id());
    let ready = (|| {
        write_state(state)?;
        let peer_id = wait_relay_peer_id_from_log(&relay_log, log_offset, STARTUP_TIMEOUT)?;
        check_cancelled(&state.root)?;
        if !state.relay.peer_id.is_empty() && peer_id != state.relay.peer_id {
            bail!("relay identity changed; stop and clean this lab before starting again");
        }
        if child.try_wait()?.is_some() {
            bail!(
                "relay exited during startup; inspect {} (TCP {tcp_port}, UDP {udp_port})",
                relay_log.display()
            );
        }
        Ok(peer_id)
    })();
    let peer_id = match ready {
        Ok(peer_id) => peer_id,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
    };
    let tcp_addr = format!("/ip4/127.0.0.1/tcp/{tcp_port}/p2p/{peer_id}");
    let udp_addr = format!("/ip4/127.0.0.1/udp/{udp_port}/quic-v1/p2p/{peer_id}");

    state.relay = RelayState {
        pid: Some(child.id()),
        home: relay_home,
        log: relay_log,
        peer_id,
        tcp_port,
        udp_port,
        tcp_addr,
        udp_addr,
    };
    write_state(state)
}

fn check_cancelled(root: &Path) -> Result<()> {
    if root.join("cancel").exists() {
        bail!("lab startup cancelled");
    }
    Ok(())
}

fn require_manager(state: &LabState) -> Result<()> {
    if !process_is_running(state.manager_pid, &state.process_spec_for_manager()) {
        bail!("lab manager is stopped; use `fungi-lab start` first");
    }
    Ok(())
}

fn open_log(path: &Path) -> Result<File> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "\n--- lab process start at {} ---", epoch_secs())?;
    Ok(file)
}

fn start_node(state: &mut LabState, node: NodeName) -> Result<NodeState> {
    check_cancelled(&state.root)?;
    let (name, dir) = match node {
        NodeName::A => ("a", state.node_a.dir.clone()),
        NodeName::B => ("b", state.node_b.dir.clone()),
    };
    fs::create_dir_all(&dir)
        .with_context(|| format!("failed to create node-{name} directory {}", dir.display()))?;
    run_cli_status(&state.fungi_bin, &state.repo, &dir, ["init"], None)?;

    let rpc_port = reserve_tcp_port()?;
    let tcp_port = reserve_tcp_port()?;
    let udp_port = reserve_udp_port()?;
    write_node_config(
        &dir,
        rpc_port,
        tcp_port,
        udp_port,
        &[state.relay.tcp_addr.clone(), state.relay.udp_addr.clone()],
    )?;

    let log = state.root.join(format!("node-{name}.log"));
    let stdout = open_log(&log)?;
    let stderr = stdout
        .try_clone()
        .with_context(|| format!("failed to clone {}", log.display()))?;
    let mut command = Command::new(&state.fungi_bin);
    command
        .arg("--fungi-dir")
        .arg(&dir)
        .arg("daemon")
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    detach_process_group(&mut command);
    let mut child = command
        .spawn()
        .with_context(|| format!("failed to start node-{name}"))?;
    state.node_mut(node).pid = Some(child.id());
    let ready = (|| {
        write_state(state)?;
        let peer_id = wait_peer_id(&state.fungi_bin, &state.repo, &dir, STARTUP_TIMEOUT)?;
        check_cancelled(&state.root)?;
        Ok(peer_id)
    })();
    let peer_id = match ready {
        Ok(peer_id) => peer_id,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
    };

    Ok(NodeState {
        name: name.to_string(),
        pid: Some(child.id()),
        dir,
        log,
        peer_id,
        rpc_port,
        tcp_port,
        udp_port,
    })
}

fn add_lab_devices(state: &LabState) -> Result<()> {
    check_cancelled(&state.root)?;
    let a_relay = circuit_addr(&state.relay.tcp_addr, &state.node_a.peer_id);
    let b_relay = circuit_addr(&state.relay.tcp_addr, &state.node_b.peer_id);
    run_cli_status(
        &state.fungi_bin,
        &state.repo,
        &state.node_a.dir,
        [
            "device",
            "add",
            "b",
            &state.node_b.peer_id,
            "--addr",
            &b_relay,
        ],
        None,
    )?;
    check_cancelled(&state.root)?;
    run_cli_status(
        &state.fungi_bin,
        &state.repo,
        &state.node_b.dir,
        [
            "device",
            "add",
            "a",
            &state.node_a.peer_id,
            "--addr",
            &a_relay,
        ],
        None,
    )?;
    Ok(())
}

fn apply_trust_mode_to_state(state: &LabState, mode: TrustMode) -> Result<()> {
    set_trust(
        state,
        NodeName::A,
        &state.node_b.peer_id,
        matches!(mode, TrustMode::Both | TrustMode::ATrustsB),
    )?;
    set_trust(
        state,
        NodeName::B,
        &state.node_a.peer_id,
        matches!(mode, TrustMode::Both | TrustMode::BTrustsA),
    )?;
    Ok(())
}

fn set_trust(state: &LabState, node: NodeName, peer_id: &str, trusted: bool) -> Result<()> {
    check_cancelled(&state.root)?;
    let dir = state.node(node).dir.clone();
    if trusted {
        println!(
            "Node {:?} ({}) grants service-management access to {peer_id} until revoked.",
            node,
            state.node(node).peer_id
        );
        println!(
            "{}",
            run_cli_capture(
                &state.fungi_bin,
                &state.repo,
                &dir,
                ["security", "show"],
                None
            )?
        );
        println!(
            "Rollback: fungi-lab --lab-dir {} trust none",
            shell_quote_path(&state.root)
        );
    }
    let command = if trusted { "trust" } else { "untrust" };
    run_cli_status(
        &state.fungi_bin,
        &state.repo,
        &dir,
        ["device", command, peer_id],
        if trusted { Some("y\n") } else { None },
    )?;
    Ok(())
}

pub(crate) fn stop_lab(state: &LabState) -> Result<()> {
    stop_lab_processes(state, false)
}

fn stop_lab_processes(state: &LabState, from_manager: bool) -> Result<()> {
    let mut errors = Vec::new();
    collect_stop_error(
        &mut errors,
        stop_pid(
            state.node_a.pid,
            &state.process_spec_for_node(NodeName::A),
            true,
        ),
    );
    collect_stop_error(
        &mut errors,
        stop_pid(
            state.node_b.pid,
            &state.process_spec_for_node(NodeName::B),
            true,
        ),
    );
    collect_stop_error(
        &mut errors,
        stop_pid(state.relay.pid, &state.process_spec_for_relay(), true),
    );
    if !from_manager {
        collect_stop_error(
            &mut errors,
            stop_pid(state.manager_pid, &state.process_spec_for_manager(), true),
        );
    }
    if !errors.is_empty() {
        bail!("failed to stop all lab processes:\n{}", errors.join("\n"));
    }
    Ok(())
}

fn collect_stop_error(errors: &mut Vec<String>, result: Result<()>) {
    if let Err(error) = result {
        errors.push(format!("- {error:#}"));
    }
}

fn ensure_safe_removal_target(path: &Path, label: &str, protected: &[&Path]) -> Result<()> {
    if !path.is_absolute() {
        bail!(
            "refusing to remove {label}: path is not absolute: {}",
            path.display()
        );
    }
    let resolved = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    if resolved.parent().is_none() {
        bail!("refusing to remove {label}: path is a filesystem root");
    }
    if resolved.components().count() < 3 {
        bail!(
            "refusing to remove {label}: path is too broad: {}",
            resolved.display()
        );
    }
    if let Some(home) = std::env::var_os("HOME") {
        let home = PathBuf::from(home);
        let home = home.canonicalize().unwrap_or(home);
        if resolved == home {
            bail!("refusing to remove {label}: path is the user home directory");
        }
    }
    for protected in protected {
        let protected = protected
            .canonicalize()
            .unwrap_or_else(|_| (*protected).to_path_buf());
        if protected.starts_with(&resolved) {
            bail!(
                "refusing to remove {label} {}: it contains protected path {}",
                resolved.display(),
                protected.display()
            );
        }
    }
    Ok(())
}

fn cleanup_partial_state(root: &Path) -> Result<()> {
    if !root.join(STATE_FILE).exists() {
        return Ok(());
    }
    let mut state = read_state(root)?;
    stop_lab_processes(&state, false)?;
    state.ready = false;
    state.manager_pid = None;
    state.relay.pid = None;
    state.node_a.pid = None;
    state.node_b.pid = None;
    write_state(&state)
}

fn with_cleanup_result(error: anyhow::Error, cleanup: Result<()>) -> anyhow::Error {
    match cleanup {
        Ok(()) if format!("{error:#}").contains("startup rollback completed") => error,
        Ok(()) => anyhow!("{error:#}\nstartup rollback completed"),
        Err(cleanup_error) => anyhow!("{error:#}\nstartup rollback also failed: {cleanup_error:#}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Opt-in real-process checks reuse the existing debug binary and build cache.
    // Run with cargo test -p fungi-lab real_ -- --ignored --test-threads=1.
    fn real_args(root: &Path) -> ManagerArgs {
        let repo = find_repo_root().unwrap();
        let fungi_bin = repo.join("target/debug/fungi");
        assert!(fungi_bin.is_file(), "build fungi first");
        ManagerArgs {
            repo,
            fungi_bin,
            root: root.to_path_buf(),
            trust: TrustMode::None,
        }
    }

    fn assert_stopped(state: &LabState) {
        let system = sysinfo::System::new_all();
        for spec in [
            state.process_spec_for_relay(),
            state.process_spec_for_node(NodeName::A),
            state.process_spec_for_node(NodeName::B),
        ] {
            for pid in system.processes().keys() {
                assert!(
                    !crate::process::process_matches(&system, pid.as_u32(), &spec),
                    "{} still running at {pid}",
                    spec.label
                );
            }
        }
    }

    #[test]
    #[ignore = "requires built fungi and local sockets"]
    fn real_startup_failures_roll_back_relay_and_first_node() {
        for node in ["a", "b"] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path();
            let _lock = crate::util::lock_lab(root, true).unwrap();
            let parent = root.join(format!("nodes/{node}"));
            fs::create_dir_all(&parent).unwrap();
            fs::write(parent.join("fungi"), "fault: node directory is a file").unwrap();
            let error = run_manager(real_args(root)).unwrap_err();
            assert!(
                format!("{error:#}").contains("startup rollback completed"),
                "{error:#}"
            );
            let state = read_state(root).unwrap();
            assert!(!state.ready);
            assert_stopped(&state);
        }
    }

    #[test]
    #[ignore = "requires built fungi/fungi-lab and local sockets"]
    fn real_startup_timeout_cancels_manager_and_reaps_children() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let _lock = crate::util::lock_lab(root, true).unwrap();
        let args = real_args(root);
        let log = root.join(MANAGER_LOG);
        let output = open_log(&log).unwrap();
        let mut manager = Command::new(args.repo.join("target/debug/fungi-lab"))
            .arg("--lab-dir")
            .arg(root)
            .arg("manager")
            .arg("--repo")
            .arg(&args.repo)
            .arg("--fungi-bin")
            .arg(&args.fungi_bin)
            .arg("--trust")
            .arg("none")
            .stdout(output.try_clone().unwrap())
            .stderr(output)
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !read_state(root).is_ok_and(|state| state.relay.pid.is_some()) {
            assert!(
                std::time::Instant::now() < deadline,
                "manager did not spawn relay"
            );
            thread::sleep(Duration::from_millis(10));
        }
        let error = await_startup(root, &mut manager, &log, Duration::ZERO).unwrap_err();
        assert!(
            format!("{error:#}").contains("startup rollback completed"),
            "{error:#}"
        );
        assert!(manager.try_wait().unwrap().is_some());
        assert_stopped(&read_state(root).unwrap());
    }

    #[test]
    #[ignore = "requires built fungi and local sockets"]
    fn real_expired_manager_stops_children_and_updates_state() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let lock = crate::util::lock_lab(root, true).unwrap();
        // Create a recoverable partial lab, then restart its relay for expiry.
        fs::create_dir_all(root.join("nodes/a")).unwrap();
        fs::write(root.join("nodes/a/fungi"), "injected failure").unwrap();
        run_manager(real_args(root)).unwrap_err();
        let mut state = read_state(root).unwrap();
        start_relay(&mut state).unwrap();
        fs::remove_file(root.join("nodes/a/fungi")).unwrap();
        state.node_a = start_node(&mut state, NodeName::A).unwrap();
        state.node_b = start_node(&mut state, NodeName::B).unwrap();
        let running = state.clone();
        assert!(process_is_running(
            running.relay.pid,
            &running.process_spec_for_relay()
        ));
        state.expires_at_epoch_secs = 0;
        write_state(&state).unwrap();
        drop(lock);
        supervise(state).unwrap();
        assert_stopped(&running);
        let stopped = read_state(root).unwrap();
        assert!(!stopped.ready);
        assert_eq!(stopped.relay.pid, None);
        assert_eq!(stopped.node_a.pid, None);
        assert_eq!(stopped.node_b.pid, None);
        assert_eq!(stopped.manager_pid, None);
    }

    #[test]
    fn cleanup_rejects_broad_and_protected_paths() {
        assert!(ensure_safe_removal_target(Path::new("/"), "test", &[]).is_err());
        assert!(ensure_safe_removal_target(Path::new("/tmp"), "test", &[]).is_err());

        let temp = tempfile::tempdir().unwrap();
        let protected = temp.path().join("repo");
        fs::create_dir_all(&protected).unwrap();
        assert!(ensure_safe_removal_target(temp.path(), "test", &[protected.as_path()]).is_err());

        let sibling = temp.path().join("lab");
        fs::create_dir_all(&sibling).unwrap();
        assert!(ensure_safe_removal_target(&sibling, "test", &[protected.as_path()]).is_ok());
    }

    #[test]
    fn completed_rollback_is_reported_once() {
        let error = with_cleanup_result(
            anyhow!("startup failed\nstartup rollback completed"),
            Ok(()),
        );
        assert_eq!(
            format!("{error:#}")
                .matches("startup rollback completed")
                .count(),
            1
        );
    }
}
