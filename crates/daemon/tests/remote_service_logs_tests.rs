use std::{fs, time::Duration};

use anyhow::Result;
use fungi_daemon::test_support::spawn_connected_pair;
use libp2p::swarm::dial_opts::DialOpts;

#[tokio::test]
async fn trusted_device_reads_bounded_remote_service_logs() -> Result<()> {
    let (client, server) = spawn_connected_pair().await?;
    let server_peer_id = server.peer_id();
    let server_addr = server.tcp_multiaddr();
    client
        .swarm_control()
        .invoke_swarm(move |swarm| {
            swarm.dial(
                DialOpts::peer_id(server_peer_id)
                    .addresses(vec![server_addr])
                    .build(),
            )
        })
        .await??;
    client
        .wait_connected(server.peer_id(), Duration::from_secs(5))
        .await?;

    let component = server.fungi_dir().join("demo.wasm");
    fs::write(&component, b"test component bytes")?;
    let manifest = format!(
        r#"fungi: service/v1
id: remote-log-demo
run:
  provider: wasmtime
  source:
    file: {}
publish:
  main:
    tcp:
      port: 18080
"#,
        component.display()
    );
    server
        .daemon()
        .pull_service_from_manifest_yaml(manifest, Some(server.fungi_dir().to_path_buf()))
        .await?;

    let runtime_log = server
        .fungi_dir()
        .join("runtime/wasmtime/remote-log-demo/runtime.log");
    let text = (0..2_500)
        .map(|line| format!("remote log line {line:03}\n"))
        .collect::<String>();
    fs::write(&runtime_log, text)?;

    let default_logs = client
        .daemon()
        .remote_get_service_logs(server.peer_id(), "remote-log-demo".to_string(), None)
        .await?;
    assert_eq!(default_logs.text.lines().count(), 200);
    assert!(!default_logs.text.contains("remote log line 2299"));
    assert!(default_logs.text.contains("remote log line 2300"));
    assert!(default_logs.text.contains("remote log line 2499"));

    let tail_logs = client
        .daemon()
        .remote_get_service_logs(server.peer_id(), "remote-log-demo".to_string(), Some(3))
        .await?;
    assert_eq!(
        tail_logs.text,
        "remote log line 2497\nremote log line 2498\nremote log line 2499\n"
    );

    let capped_logs = client
        .daemon()
        .remote_get_service_logs(
            server.peer_id(),
            "remote-log-demo".to_string(),
            Some(usize::MAX),
        )
        .await?;
    assert_eq!(capped_logs.text.lines().count(), 2_000);
    assert!(capped_logs.text.contains("remote log line 500"));
    assert!(capped_logs.text.contains("remote log line 2499"));

    fs::write(&runtime_log, "\0".repeat(512 * 1024))?;
    let escaped_logs = client
        .daemon()
        .remote_get_service_logs(server.peer_id(), "remote-log-demo".to_string(), Some(1))
        .await?;
    assert!(
        escaped_logs
            .text
            .starts_with("[fungi] remote log output truncated\n")
    );
    assert!(escaped_logs.text.ends_with('\0'));
    assert!(escaped_logs.text.len() < 512 * 1024);

    Ok(())
}
