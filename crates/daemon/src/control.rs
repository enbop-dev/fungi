use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use fungi_config::{FungiConfig, devices::DevicesConfig, trusted_devices::TrustedDevicesConfig};
use fungi_swarm::SwarmControl;
use parking_lot::Mutex;
use tokio::task::JoinHandle;

use crate::{
    Connectivity, InboundAccessPolicy, Settings,
    controls::{
        NodeCapabilitiesControl, ServiceControlProtocolControl, ServiceDiscoveryControl,
        TcpTunnelingControl, mdns::MdnsControl,
    },
    devices::{DeviceHandle, Devices},
    runtime::RuntimeControl,
    service_access_manager::{ServiceAccessManager, restore_saved_service_accesses},
    services::{ServiceHandle, ServiceKey, Services},
};

/// Cloneable entry point for all daemon application APIs.
///
/// `FungiControl` owns only shared handles. The unique daemon tasks and their lifecycle remain
/// owned by [`crate::FungiDaemon`]. The low-level fields below are transitional and will move into
/// their corresponding domain handles as the refactor progresses.
#[derive(Clone)]
pub struct FungiControl {
    settings: Settings,
    devices: Devices,
    services: Services,
    service_access: ServiceAccessManager,
    inbound_access: InboundAccessPolicy,
    connectivity: Connectivity,
}

pub(crate) struct FungiControlInit {
    pub settings: Settings,
    pub devices: Devices,
    pub services: Services,
    pub service_access: ServiceAccessManager,
    pub inbound_access: InboundAccessPolicy,
    pub connectivity: Connectivity,
}

impl FungiControl {
    pub(crate) fn new(init: FungiControlInit) -> Self {
        Self {
            settings: init.settings,
            devices: init.devices,
            services: init.services,
            service_access: init.service_access,
            inbound_access: init.inbound_access,
            connectivity: init.connectivity,
        }
    }

    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// Compatibility handle for callers that still need to inspect the complete config.
    pub fn config(&self) -> Arc<Mutex<FungiConfig>> {
        self.settings.config_handle()
    }

    pub fn devices(&self) -> &Devices {
        &self.devices
    }

    pub fn device(&self, device_id: libp2p::PeerId) -> Result<DeviceHandle> {
        self.devices
            .get(device_id)
            .ok_or_else(|| anyhow::anyhow!("device is not managed: {device_id}"))
    }

    pub fn services(&self) -> &Services {
        &self.services
    }

    pub fn service(&self, key: ServiceKey) -> ServiceHandle {
        self.services
            .for_device(key.device_id)
            .service(key.service_name)
    }

    pub fn connectivity(&self) -> &Connectivity {
        &self.connectivity
    }

    pub fn inbound_access(&self) -> &InboundAccessPolicy {
        &self.inbound_access
    }

    pub fn devices_config(&self) -> Arc<Mutex<DevicesConfig>> {
        self.devices.config()
    }

    pub fn trusted_devices(&self) -> Arc<Mutex<TrustedDevicesConfig>> {
        self.inbound_access.trusted_peers_config()
    }

    pub fn swarm_control(&self) -> &SwarmControl {
        self.connectivity.swarm_control()
    }

    pub fn tcp_tunneling_control(&self) -> &TcpTunnelingControl {
        self.services.tcp_tunneling()
    }

    pub(crate) fn service_access_manager(&self) -> &ServiceAccessManager {
        &self.service_access
    }

    pub fn runtime_control(&self) -> &RuntimeControl {
        self.services.runtime()
    }

    pub fn service_discovery_control(&self) -> &ServiceDiscoveryControl {
        self.services.service_discovery()
    }

    /// Low-level compatibility accessor used by protocol integration tests.
    pub fn node_capabilities_control(&self) -> &NodeCapabilitiesControl {
        self.devices.node_capabilities()
    }

    pub fn service_control_protocol_control(&self) -> &ServiceControlProtocolControl {
        self.services.service_control()
    }

    pub fn mdns_control(&self) -> &MdnsControl {
        self.connectivity.mdns_control()
    }

    /// Starts the one-shot restoration of saved remote service accesses.
    ///
    /// The task owns only the two domain handles it needs, not the whole control facade.
    pub fn spawn_saved_service_access_restore(&self) -> JoinHandle<()> {
        let service_access = self.service_access.clone();
        let services = self.services.clone();
        tokio::spawn(async move {
            log::info!("Restoring saved service access in the background...");
            restore_saved_service_accesses(service_access, services).await;
            log::info!("Finished restoring saved service access");
        })
    }

    pub fn config_fungi_dir(&self) -> Result<PathBuf> {
        self.settings.fungi_dir()
    }
}
