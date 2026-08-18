use std::sync::Arc;

use anyhow::{Result, bail};
use fungi_config::devices::{DeviceInfo, DevicesConfig};
use libp2p::PeerId;
use parking_lot::Mutex;

use crate::{Connectivity, DeviceServices, Services};

struct DevicesInner {
    local_device: DeviceInfo,
    config: Arc<Mutex<DevicesConfig>>,
    connectivity: Connectivity,
    services: Services,
}

pub(crate) struct DevicesInit {
    pub local_device: DeviceInfo,
    pub config: DevicesConfig,
    pub connectivity: Connectivity,
    pub services: Services,
}

/// Directory and shared capabilities for devices actively managed by this daemon.
///
/// `DevicesConfig` remains the single directory state. Handles are cheap identity views created
/// on demand, rather than entries in a second in-memory map.
#[derive(Clone)]
pub struct Devices {
    inner: Arc<DevicesInner>,
}

impl Devices {
    pub(crate) fn new(init: DevicesInit) -> Self {
        Self {
            inner: Arc::new(DevicesInner {
                local_device: init.local_device,
                config: Arc::new(Mutex::new(init.config)),
                connectivity: init.connectivity,
                services: init.services,
            }),
        }
    }

    /// Compatibility view for callers that still read `DevicesConfig` directly.
    /// New code should prefer the domain methods on `Devices` and `DeviceHandle`.
    pub fn lock(&self) -> parking_lot::MutexGuard<'_, DevicesConfig> {
        self.inner.config.lock()
    }

    /// Returns this daemon's device handle.
    pub fn local(&self) -> DeviceHandle {
        self.peer(self.inner.local_device.peer_id)
    }

    /// Returns a handle only when the peer is local or belongs to the saved device directory.
    pub fn get(&self, peer_id: PeerId) -> Option<DeviceHandle> {
        if peer_id == self.inner.local_device.peer_id
            || self.inner.config.lock().get_device_info(&peer_id).is_some()
        {
            Some(self.peer(peer_id))
        } else {
            None
        }
    }

    /// Creates an addressable handle for any peer.
    ///
    /// This is used for persisted service-access records that can outlive directory membership.
    /// Call [`Devices::get`] when saved membership is required.
    pub fn peer(&self, peer_id: PeerId) -> DeviceHandle {
        DeviceHandle {
            peer_id,
            devices: self.clone(),
        }
    }

    /// Lists local and saved remote devices using the common handle shape.
    pub fn list(&self) -> Vec<DeviceHandle> {
        std::iter::once(self.local()).chain(self.saved()).collect()
    }

    /// Lists only devices persisted in `devices.toml`.
    pub fn saved(&self) -> Vec<DeviceHandle> {
        self.saved_device_infos()
            .into_iter()
            .map(|device| self.peer(device.peer_id))
            .collect()
    }

    pub fn saved_device_infos(&self) -> Vec<DeviceInfo> {
        self.inner.config.lock().get_all_devices().clone()
    }

    pub fn add_or_update(&self, device_info: DeviceInfo) -> Result<()> {
        {
            let mut config = self.inner.config.lock();
            let updated = config.add_or_update_device(device_info.clone())?;
            *config = updated;
        }
        self.record_addresses(&device_info);
        Ok(())
    }

    /// Removes active-management state without changing inbound authorization.
    pub async fn remove(&self, peer_id: PeerId) -> Result<()> {
        if peer_id == self.inner.local_device.peer_id {
            bail!("cannot remove the local device");
        }

        {
            let mut config = self.inner.config.lock();
            let updated = config.remove_device(&peer_id)?;
            *config = updated;
        }

        let _ = self.inner.services.remove_snapshot(peer_id)?;
        Ok(())
    }

    pub(crate) fn config(&self) -> Arc<Mutex<DevicesConfig>> {
        self.inner.config.clone()
    }

    fn record_addresses(&self, device_info: &DeviceInfo) {
        self.inner.connectivity.record_device_addresses(device_info);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeviceKind {
    Local,
    Remote,
}

/// Stable identity and entry point for one local or remote device.
#[derive(Clone)]
pub struct DeviceHandle {
    peer_id: PeerId,
    devices: Devices,
}

impl DeviceHandle {
    pub fn id(&self) -> PeerId {
        self.peer_id
    }

    pub fn kind(&self) -> DeviceKind {
        if self.is_local() {
            DeviceKind::Local
        } else {
            DeviceKind::Remote
        }
    }

    pub fn is_local(&self) -> bool {
        self.peer_id == self.devices.inner.local_device.peer_id
    }

    /// Resolves current device metadata without storing it in the handle.
    pub fn info(&self) -> Option<DeviceInfo> {
        if self.is_local() {
            Some(self.devices.inner.local_device.clone())
        } else {
            self.devices
                .inner
                .config
                .lock()
                .get_device_info(&self.peer_id)
                .cloned()
        }
    }

    pub fn services(&self) -> DeviceServices {
        self.devices.inner.services.for_device(self.peer_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestDaemon, spawn_connected_pair};

    #[tokio::test]
    async fn local_and_remote_devices_share_the_handle_shape() {
        let daemon = TestDaemon::spawn().await.unwrap();
        let devices = daemon.daemon().devices();
        let local = devices.local();

        assert_eq!(local.id(), daemon.peer_id());
        assert_eq!(local.kind(), DeviceKind::Local);
        assert!(local.info().is_some());
        assert!(local.services().snapshot().unwrap().is_none());

        let remote_id = PeerId::random();
        let remote = devices.peer(remote_id);
        assert_eq!(remote.kind(), DeviceKind::Remote);
        assert_eq!(remote.services().service("ssh").name(), "ssh");
        assert!(remote.info().is_none());
        assert!(devices.get(remote_id).is_none());
    }

    #[tokio::test]
    async fn handles_reflect_directory_updates_without_a_second_state_map() {
        let daemon = TestDaemon::spawn().await.unwrap();
        let devices = daemon.daemon().devices();
        let remote_id = PeerId::random();
        let remote = devices.peer(remote_id);

        let mut info = DeviceInfo::new_unknown(remote_id);
        info.name = Some("workstation".to_string());
        devices.add_or_update(info).unwrap();

        assert_eq!(
            remote.info().and_then(|info| info.name).as_deref(),
            Some("workstation")
        );
        assert_eq!(devices.saved().len(), 1);
        assert_eq!(devices.list().len(), 2);

        devices.remove(remote_id).await.unwrap();
        assert!(remote.info().is_none());
        assert!(devices.get(remote_id).is_none());
    }

    #[tokio::test]
    async fn removing_managed_device_does_not_implicitly_revoke_inbound_authorization() {
        let (client, server) = spawn_connected_pair().await.unwrap();
        let server_id = server.peer_id();
        let mut server_info = DeviceInfo::new_unknown(server_id);
        server_info.name = Some("server".to_string());
        client
            .daemon()
            .devices()
            .add_or_update(server_info)
            .unwrap();

        client.daemon().devices().remove(server_id).await.unwrap();

        assert!(
            client
                .daemon()
                .list_trusted_devices()
                .iter()
                .any(|device| device.peer_id == server_id)
        );
    }
}
