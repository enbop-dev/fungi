use fungi_daemon_grpc::{
    Request,
    fungi_daemon_grpc::{PingPeerRequest, ping_peer_event},
};

use crate::commands::CommonArgs;

use super::{
    client::get_rpc_client,
    shared::{
        PeerInput, fatal, fatal_grpc, print_target_peer, resolve_peer_input, shorten_peer_id,
        simplify_multiaddr_peer_ids, summarize_ping_error_message,
    },
};

fn ping_count_exceeded(count: u32, tick_seq: u64) -> bool {
    count > 0 && tick_seq > u64::from(count)
}

pub async fn execute_ping(
    args: CommonArgs,
    peer: PeerInput,
    interval_ms: u32,
    count: u32,
    verbose: bool,
) {
    let mut client = match get_rpc_client(&args).await {
        Some(c) => c,
        None => fatal("Cannot connect to Fungi daemon. Is it running?"),
    };

    let resolved = match resolve_peer_input(&args, &peer) {
        Ok(peer) => peer,
        Err(error) => fatal(error),
    };
    print_target_peer(&resolved);

    let req = PingPeerRequest {
        peer_id: resolved.peer_id.clone(),
        interval_ms,
        count,
    };

    let response = match client.ping_peer(Request::new(req)).await {
        Ok(resp) => resp,
        Err(e) => fatal_grpc(e),
    };

    let mut stream = response.into_inner();
    let peer_label = if verbose {
        resolved.peer_id.clone()
    } else {
        shorten_peer_id(&resolved.peer_id)
    };
    if count == 0 {
        println!(
            "Ping stream peer={} interval={}ms (Ctrl+C to stop)",
            peer_label, interval_ms
        );
    } else {
        println!(
            "Ping peer={} count={} interval={}ms",
            peer_label, count, interval_ms
        );
    }

    if !verbose {
        println!(
            "{:<6} {:<6} {:<8} {:<5} {:<10} ADDR/MSG",
            "TICK", "CONN", "DIR", "RLY", "RTT"
        );
    }

    loop {
        let event = match stream.message().await {
            Ok(Some(event)) => event,
            Ok(None) => break,
            Err(error) => fatal_grpc(error),
        };
        // Older daemons ignore the count field and keep streaming. Waiting for
        // the next tick preserves every result from the requested final round.
        if ping_count_exceeded(count, event.tick_seq) {
            break;
        }
        match event.event {
            Some(ping_peer_event::Event::Connecting(_)) => {
                if verbose {
                    println!("[tick={}] connecting...", event.tick_seq);
                } else {
                    println!(
                        "{:<6} {:<6} {:<8} {:<5} {:<10} connecting",
                        event.tick_seq, "-", "-", "-", "-"
                    );
                }
            }
            Some(ping_peer_event::Event::Connected(_)) => {
                if verbose {
                    println!("[tick={}] connected", event.tick_seq);
                } else {
                    println!(
                        "{:<6} {:<6} {:<8} {:<5} {:<10} connected",
                        event.tick_seq, "-", "-", "-", "-"
                    );
                }
            }
            Some(ping_peer_event::Event::Idle(_)) => {
                if verbose {
                    println!("[tick={}] no active connections", event.tick_seq);
                } else {
                    println!(
                        "{:<6} {:<6} {:<8} {:<5} {:<10} no active connections",
                        event.tick_seq, "-", "-", "-", "-"
                    );
                }
            }
            Some(ping_peer_event::Event::Result(result)) => {
                if verbose {
                    println!(
                        "[tick={}] conn={} dir={} addr={} rtt={}ms",
                        event.tick_seq,
                        result.connection_id,
                        result.direction,
                        result.remote_addr,
                        result.rtt_ms
                    );
                } else {
                    let relay = if result.remote_addr.contains("/p2p-circuit") {
                        "yes"
                    } else {
                        "no"
                    };
                    println!(
                        "{:<6} {:<6} {:<8} {:<5} {:<10} {}",
                        event.tick_seq,
                        result.connection_id,
                        result.direction,
                        relay,
                        format!("{}ms", result.rtt_ms),
                        simplify_multiaddr_peer_ids(&result.remote_addr)
                    );
                }
            }
            Some(ping_peer_event::Event::Error(error)) => {
                if verbose {
                    if error.connection_id.is_empty() {
                        println!("[tick={}] error={}", event.tick_seq, error.message);
                    } else {
                        println!(
                            "[tick={}] conn={} dir={} addr={} error={}",
                            event.tick_seq,
                            error.connection_id,
                            error.direction,
                            error.remote_addr,
                            error.message
                        );
                    }
                } else {
                    let relay = if error.remote_addr.contains("/p2p-circuit") {
                        "yes"
                    } else {
                        "no"
                    };
                    println!(
                        "{:<6} {:<6} {:<8} {:<5} {:<10} {}",
                        event.tick_seq,
                        if error.connection_id.is_empty() {
                            "-"
                        } else {
                            &error.connection_id
                        },
                        error.direction,
                        relay,
                        "error",
                        if error.remote_addr.is_empty() {
                            summarize_ping_error_message(&error.message, verbose)
                        } else {
                            format!(
                                "{} | {}",
                                simplify_multiaddr_peer_ids(&error.remote_addr),
                                error.message
                            )
                        }
                    );
                }
            }
            _ => {
                if verbose {
                    println!("[tick={}] unknown event", event.tick_seq);
                } else {
                    println!(
                        "{:<6} {:<6} {:<8} {:<5} {:<10} unknown event",
                        event.tick_seq, "-", "-", "-", "-"
                    );
                }
            }
        }
    }

    if count > 0 {
        println!("Ping completed: {count} rounds");
    }
}

#[cfg(test)]
mod tests {
    use super::ping_count_exceeded;

    #[test]
    fn finite_client_accepts_every_requested_round() {
        assert!(!ping_count_exceeded(4, 4));
        assert!(ping_count_exceeded(4, 5));
    }

    #[test]
    fn watch_mode_never_stops_for_a_tick_count() {
        assert!(!ping_count_exceeded(0, u64::MAX));
    }
}
