use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use fungi_config::{devices::DeviceInfo, direct_addresses::DirectAddressCache};
use fungi_swarm::{ConnectionDirection, PeerAddressSource, State, SwarmControl};
use libp2p::Multiaddr;
use parking_lot::Mutex;
use tokio::task::JoinHandle;

use crate::controls::mdns::MdnsControl;

const DIRECT_ADDRESS_CACHE_SYNC_INTERVAL: Duration = Duration::from_secs(30);

struct ConnectivityInner {
    swarm: SwarmControl,
    mdns: MdnsControl,
    direct_address_cache: Arc<Mutex<DirectAddressCache>>,
}

/// Shared connectivity capabilities backed by the daemon's single libp2p swarm.
#[derive(Clone)]
pub struct Connectivity {
    inner: Arc<ConnectivityInner>,
}

impl Connectivity {
    pub(crate) fn new(
        swarm: SwarmControl,
        mdns: MdnsControl,
        direct_address_cache: DirectAddressCache,
    ) -> Self {
        Self {
            inner: Arc::new(ConnectivityInner {
                swarm,
                mdns,
                direct_address_cache: Arc::new(Mutex::new(direct_address_cache)),
            }),
        }
    }

    pub fn swarm_control(&self) -> &SwarmControl {
        &self.inner.swarm
    }

    pub fn mdns_control(&self) -> &MdnsControl {
        &self.inner.mdns
    }

    pub(crate) fn record_device_addresses(&self, device_info: &DeviceInfo) {
        for address in &device_info.multiaddrs {
            match address.parse::<Multiaddr>() {
                Ok(multiaddr) => {
                    self.inner.swarm.state().record_peer_address(
                        device_info.peer_id,
                        multiaddr,
                        PeerAddressSource::DeviceConfig,
                    );
                }
                Err(error) => log::debug!(
                    "Ignoring invalid device multiaddr for peer {}: {} ({})",
                    device_info.peer_id,
                    address,
                    error
                ),
            }
        }
    }

    pub(crate) fn spawn_direct_address_cache_sync(&self) -> JoinHandle<()> {
        let swarm = self.inner.swarm.clone();
        let direct_address_cache = self.inner.direct_address_cache.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(DIRECT_ADDRESS_CACHE_SYNC_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut last_synced_pairs = BTreeSet::<(String, String)>::new();

            loop {
                interval.tick().await;

                let grouped = collect_direct_connection_addresses(swarm.state());
                let grouped = new_direct_address_successes(grouped, &mut last_synced_pairs);
                if grouped.is_empty() {
                    continue;
                }

                let mut current = direct_address_cache.lock().clone();
                let mut updated_any = false;
                for (peer_id, addresses) in grouped {
                    match current.record_successful_addresses(peer_id, addresses) {
                        Ok(updated) => {
                            current = updated;
                            updated_any = true;
                        }
                        Err(error) => {
                            log::warn!("Failed to save cached direct address: {error}");
                        }
                    }
                }

                if updated_any {
                    *direct_address_cache.lock() = current;
                }
            }
        })
    }
}

fn collect_direct_connection_addresses(state: &State) -> BTreeMap<String, Vec<String>> {
    let mut grouped = BTreeMap::<String, Vec<String>>::new();
    for peer_id in state.connected_peer_ids() {
        for connection in state.get_connections_by_peer_id(&peer_id) {
            if !matches!(connection.direction, ConnectionDirection::Outbound)
                || connection.is_relay()
            {
                continue;
            }

            grouped
                .entry(peer_id.to_string())
                .or_default()
                .push(connection.remote_addr.to_string());
        }
    }

    normalize_direct_address_groups(grouped)
}

fn new_direct_address_successes(
    grouped: BTreeMap<String, Vec<String>>,
    last_synced_pairs: &mut BTreeSet<(String, String)>,
) -> BTreeMap<String, Vec<String>> {
    let current_pairs = direct_address_pairs(&grouped);
    let mut new_pairs = BTreeMap::<String, Vec<String>>::new();

    for (peer_id, address) in current_pairs.difference(last_synced_pairs) {
        new_pairs
            .entry(peer_id.clone())
            .or_default()
            .push(address.clone());
    }

    *last_synced_pairs = current_pairs;
    new_pairs
}

fn direct_address_pairs(grouped: &BTreeMap<String, Vec<String>>) -> BTreeSet<(String, String)> {
    grouped
        .iter()
        .flat_map(|(peer_id, addresses)| {
            addresses
                .iter()
                .map(|address| (peer_id.clone(), address.clone()))
        })
        .collect()
}

fn normalize_direct_address_groups(
    mut grouped: BTreeMap<String, Vec<String>>,
) -> BTreeMap<String, Vec<String>> {
    grouped.retain(|_, addresses| {
        addresses.retain(|address| !address.trim().is_empty());
        for address in addresses.iter_mut() {
            *address = address.trim().to_string();
        }
        addresses.sort();
        addresses.dedup();
        !addresses.is_empty()
    });
    grouped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_direct_address_successes_only_returns_new_pairs() {
        let mut last_synced_pairs = BTreeSet::new();

        let first = new_direct_address_successes(
            BTreeMap::from([
                (
                    "peer-a".to_string(),
                    vec!["/ip4/192.168.1.7/tcp/4001".to_string()],
                ),
                (
                    "peer-b".to_string(),
                    vec!["/ip4/192.168.1.8/tcp/4001".to_string()],
                ),
            ]),
            &mut last_synced_pairs,
        );
        assert_eq!(first.len(), 2);

        let second = new_direct_address_successes(
            BTreeMap::from([
                (
                    "peer-a".to_string(),
                    vec!["/ip4/192.168.1.7/tcp/4001".to_string()],
                ),
                (
                    "peer-b".to_string(),
                    vec![
                        "/ip4/192.168.1.8/tcp/4001".to_string(),
                        "/ip4/192.168.1.9/tcp/4001".to_string(),
                    ],
                ),
            ]),
            &mut last_synced_pairs,
        );
        assert_eq!(
            second,
            BTreeMap::from([(
                "peer-b".to_string(),
                vec!["/ip4/192.168.1.9/tcp/4001".to_string()]
            )])
        );

        let third = new_direct_address_successes(
            BTreeMap::from([
                (
                    "peer-a".to_string(),
                    vec!["/ip4/192.168.1.7/tcp/4001".to_string()],
                ),
                (
                    "peer-b".to_string(),
                    vec![
                        "/ip4/192.168.1.8/tcp/4001".to_string(),
                        "/ip4/192.168.1.9/tcp/4001".to_string(),
                    ],
                ),
            ]),
            &mut last_synced_pairs,
        );
        assert!(third.is_empty());

        let empty = new_direct_address_successes(BTreeMap::new(), &mut last_synced_pairs);
        assert!(empty.is_empty());

        let after_disconnect = new_direct_address_successes(
            BTreeMap::from([(
                "peer-a".to_string(),
                vec!["/ip4/192.168.1.7/tcp/4001".to_string()],
            )]),
            &mut last_synced_pairs,
        );
        assert_eq!(
            after_disconnect,
            BTreeMap::from([(
                "peer-a".to_string(),
                vec!["/ip4/192.168.1.7/tcp/4001".to_string()]
            )])
        );
    }
}
