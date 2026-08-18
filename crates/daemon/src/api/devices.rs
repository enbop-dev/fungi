use anyhow::Result;
use fungi_config::devices::DeviceInfo;
use libp2p::PeerId;

use crate::FungiControl;

impl FungiControl {
    pub async fn mdns_get_local_devices(&self) -> Result<Vec<DeviceInfo>> {
        let local_devices = self
            .mdns_control()
            .get_all_devices()
            .values()
            .cloned()
            .collect();
        Ok(local_devices)
    }

    pub fn devices_get_all(&self) -> Vec<DeviceInfo> {
        self.devices().saved_device_infos()
    }

    pub fn devices_add_or_update(&self, device_info: DeviceInfo) -> Result<()> {
        self.devices().add_or_update(device_info)
    }

    pub fn devices_get_peer(&self, peer_id: PeerId) -> Option<DeviceInfo> {
        self.devices().get(peer_id).and_then(|device| device.info())
    }

    pub async fn devices_remove(&self, peer_id: PeerId) -> Result<()> {
        self.devices().remove(peer_id).await?;
        self.service_access().forget_device(peer_id).await?;
        // Preserve the current CLI behavior while authorization remains a separate daemon-level
        // domain. `Devices::remove` itself intentionally does not imply this policy decision.
        self.untrust_device(peer_id)?;
        Ok(())
    }

    pub fn list_trusted_devices(&self) -> Vec<DeviceInfo> {
        let trusted_device_ids = self.inbound_access().authorized_peers();
        let devices_config_guard = self.devices_config();
        let devices_config = devices_config_guard.lock();

        trusted_device_ids
            .into_iter()
            .map(
                |peer_id| match devices_config.get_device_info(&peer_id).cloned() {
                    Some(device_info) => device_info,
                    None => DeviceInfo::new_unknown(peer_id),
                },
            )
            .collect()
    }
}
