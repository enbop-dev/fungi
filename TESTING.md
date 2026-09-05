# Fungi Testing Guide

## Pick the right test

| If you are testing... | Put the test here | Run with |
|---|---|---|
| Pure logic with no I/O | `#[cfg(test)] mod tests` in the same file | `cargo test --lib -p <crate>` |
| Daemon API behavior or multiple components working together | `crates/daemon/tests/` | `cargo test -p fungi-daemon --test <name>` |
| The real CLI talking to real processes over gRPC | `crates/tests/src/bin/` | `cargo run --package fungi-tests --bin <name>` |

Start with the smallest test that proves the behavior you care about. Move to integration or CLI tests only when the behavior crosses process or API boundaries.

## Use `test_support` for daemon tests

`fungi_daemon::test_support` should be the default for tests that need a running `FungiDaemon`. It gives you temp dirs, random ports, and cleanup automatically, so you do not need to hand-roll test setup.

```rust
use fungi_daemon::test_support::{TestDaemon, TestDaemonBuilder, spawn_connected_pair};

// Single isolated daemon
let d = TestDaemon::spawn().await?;
let pid: PeerId   = d.peer_id();
let addr: Multiaddr = d.tcp_multiaddr(); // /ip4/127.0.0.1/tcp/<port>/p2p/<peer>

// Deterministic PeerId
let d = TestDaemon::spawn_with_keypair(Keypair::generate_ed25519()).await?;

// Custom setup
let server = TestDaemon::spawn().await?;
let client = TestDaemonBuilder::new()
    .with_allowed_peer(server.peer_id())
    .build().await?;

// Connected pair
let (client, server) = spawn_connected_pair().await?;
client.connect_to(&server).await?;
client.wait_connected(server.peer_id(), Duration::from_secs(5)).await?;
```

## Running tests

```bash
cargo test --lib                   # all unit tests
cargo test -p fungi-daemon         # daemon unit + integration tests
cargo test                         # everything

# CLI smoke test (requires built binary)
cargo build --bin fungi
cargo run --package fungi-tests --bin test-relay-config-cli
```

## Local CLI lab

Use `fungi-lab` for an interactive, real-process A/B environment. It starts one local relay and two daemons, disables community relays, and saves both devices. Neither node trusts the other by default.

```bash
cargo build -p fungi -p fungi-lab
./target/debug/fungi-lab start

eval "$(./target/debug/fungi-lab env)"
"$FUNGI_BIN" -f "$FUNGI_A_DIR" service list
"$FUNGI_BIN" -f "$FUNGI_B_DIR" device trusted

./target/debug/fungi-lab node stop b
./target/debug/fungi-lab node start b
./target/debug/fungi-lab stop
```

`--lab-dir PATH` (or `FUNGI_LAB_DIR`) selects the same lab for every command;
the default is `target/local-lab`. This is a data directory, not a checkout.
It contains `state.json`, logs, `relay-home/`, and `nodes/{a,b}/fungi/`.
Fungi's sibling user directories also stay inside each node's directory.
Reuse the source checkout and Rust build cache for separate labs.
`start --fungi-bin PATH` can select another built Fungi binary.

`start` refuses to replace a running manager. `stop` retains data and logs;
`start` after `stop` reuses identities and resets trust to the requested mode.
`node restart a` and `relay restart` append a timestamped log boundary; the relay
keeps its identity and ports. The manager stops the lab after two hours.
Use `clean` to stop and delete a lab whose evidence is no longer needed.
Commands that change a lab are serialized; a concurrent command reports that it is busy.

For service-management tests, explicitly use `start --trust b-trusts-a` or
`trust b-trusts-a` (B grants A access). `a-trusts-b`, `both`, and `none` are also
supported. These grants persist until `trust none`, a subsequent start resetting
trust, or cleanup. Only use them for the local test nodes; inspect `fungi security show`
on the granting node to see its host-path exposure.

State version 2 intentionally rejects older state and arbitrary external node
directories. Stop and clean old labs with the old binary before upgrading, or
choose a fresh `--lab-dir`. Corrupt state or an unrecognized directory is retained
with an error; there is no force-delete or migration mode. Port conflicts report
failure and preserve logs; automatic port retries are not implemented.

Lab state/CLI regression checks run with `cargo test -p fungi-lab`. After building
both binaries, explicitly run the local-process fault and expiry checks:

```bash
cargo test -p fungi-lab real_ -- --ignored --test-threads=1
```

These checks use temporary lab data with the existing debug Fungi binary. They
cover failures after relay/node startup, startup cancellation, and expiry cleanup.
They do not launch Docker services or require a two-hour wait.
