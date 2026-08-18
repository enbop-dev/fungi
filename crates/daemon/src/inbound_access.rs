use std::{collections::HashSet, sync::Arc};

use anyhow::Result;
use fungi_config::trusted_devices::TrustedDevicesConfig;
use libp2p::PeerId;
use parking_lot::{Mutex, RwLock};

struct InboundAccessPolicyInner {
    trusted_peers: Arc<Mutex<TrustedDevicesConfig>>,
    incoming_allow_list: Arc<RwLock<HashSet<PeerId>>>,
}

/// Controls which peers may use this daemon's inbound protocols.
///
/// This policy is intentionally independent from the actively managed device directory: a peer
/// can be authorized without being a device this daemon controls, and vice versa.
#[derive(Clone)]
pub struct InboundAccessPolicy {
    inner: Arc<InboundAccessPolicyInner>,
}

impl InboundAccessPolicy {
    pub(crate) fn new(
        trusted_peers: TrustedDevicesConfig,
        incoming_allow_list: Arc<RwLock<HashSet<PeerId>>>,
    ) -> Self {
        Self {
            inner: Arc::new(InboundAccessPolicyInner {
                trusted_peers: Arc::new(Mutex::new(trusted_peers)),
                incoming_allow_list,
            }),
        }
    }

    pub fn authorize(&self, peer_id: PeerId) -> Result<()> {
        let mut trusted_peers = self.inner.trusted_peers.lock();
        *trusted_peers = trusted_peers.add_trusted_device(&peer_id)?;
        drop(trusted_peers);
        self.inner.incoming_allow_list.write().insert(peer_id);
        Ok(())
    }

    pub fn revoke(&self, peer_id: PeerId) -> Result<()> {
        let mut trusted_peers = self.inner.trusted_peers.lock();
        *trusted_peers = trusted_peers.remove_trusted_device(&peer_id)?;
        drop(trusted_peers);
        self.inner.incoming_allow_list.write().remove(&peer_id);
        Ok(())
    }

    pub fn is_authorized(&self, peer_id: PeerId) -> bool {
        self.inner.incoming_allow_list.read().contains(&peer_id)
    }

    pub fn authorized_peers(&self) -> Vec<PeerId> {
        let mut peers = self
            .inner
            .incoming_allow_list
            .read()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        peers.sort();
        peers
    }

    pub(crate) fn trusted_peers_config(&self) -> Arc<Mutex<TrustedDevicesConfig>> {
        self.inner.trusted_peers.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Barrier, thread};

    use tempfile::TempDir;

    use super::*;

    fn policy(config: TrustedDevicesConfig) -> InboundAccessPolicy {
        InboundAccessPolicy::new(config, Arc::new(RwLock::new(HashSet::new())))
    }

    #[test]
    fn authorization_updates_persisted_and_runtime_policy() {
        let temp_dir = TempDir::new().unwrap();
        let policy = policy(TrustedDevicesConfig::apply_from_dir(temp_dir.path()).unwrap());
        let peer_id = PeerId::random();

        policy.authorize(peer_id).unwrap();

        assert!(policy.is_authorized(peer_id));
        let reloaded = TrustedDevicesConfig::apply_from_dir(temp_dir.path()).unwrap();
        assert_eq!(reloaded.trusted_devices, vec![peer_id]);

        policy.revoke(peer_id).unwrap();

        assert!(!policy.is_authorized(peer_id));
        let reloaded = TrustedDevicesConfig::apply_from_dir(temp_dir.path()).unwrap();
        assert!(reloaded.trusted_devices.is_empty());
    }

    #[test]
    fn concurrent_authorizations_do_not_overwrite_each_other() {
        let policy = policy(TrustedDevicesConfig::in_memory(Vec::new()));
        let first_peer = PeerId::random();
        let second_peer = PeerId::random();
        let barrier = Arc::new(Barrier::new(3));

        let first_task = {
            let policy = policy.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                policy.authorize(first_peer).unwrap();
            })
        };
        let second_task = {
            let policy = policy.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                policy.authorize(second_peer).unwrap();
            })
        };

        barrier.wait();
        first_task.join().unwrap();
        second_task.join().unwrap();

        assert_eq!(policy.authorized_peers().len(), 2);
        let trusted_peers = policy.trusted_peers_config();
        assert_eq!(trusted_peers.lock().trusted_devices.len(), 2);
    }

    #[test]
    fn failed_persistence_does_not_change_runtime_policy() {
        let temp_dir = TempDir::new().unwrap();
        let config = TrustedDevicesConfig::apply_from_dir(temp_dir.path()).unwrap();
        drop(temp_dir);
        let policy = policy(config);
        let peer_id = PeerId::random();

        assert!(policy.authorize(peer_id).is_err());
        assert!(!policy.is_authorized(peer_id));
    }
}
