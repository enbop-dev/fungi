use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use fungi_config::{FungiConfig, devices::DevicesConfig, trusted_devices::TrustedDevicesConfig};
use fungi_swarm::SwarmControl;
use parking_lot::Mutex;
use tokio::task::JoinHandle;

use crate::{
    controls::{
        DockerControl, NodeCapabilitiesControl, ServiceControlProtocolControl,
        ServiceDiscoveryControl, TcpTunnelingControl, mdns::MdnsControl,
    },
    devices::Devices,
    runtime::RuntimeControl,
    service_access_manager::{ServiceAccessManager, restore_saved_service_accesses},
    services::Services,
};

/// Cloneable entry point for all daemon application APIs.
///
/// `FungiControl` owns only shared handles. The unique daemon tasks and their lifecycle remain
/// owned by [`crate::FungiDaemon`]. The low-level fields below are transitional and will move into
/// their corresponding domain handles as the refactor progresses.
#[derive(Clone)]
pub struct FungiControl {
    config: Arc<Mutex<FungiConfig>>,
    devices: Devices,
    services: Services,
    service_access: ServiceAccessManager,
    trusted_devices_config: Arc<Mutex<TrustedDevicesConfig>>,
    swarm_control: SwarmControl,
    mdns_control: MdnsControl,
    docker_control: Option<DockerControl>,
    node_capabilities_control: NodeCapabilitiesControl,
}

pub(crate) struct FungiControlInit {
    pub config: Arc<Mutex<FungiConfig>>,
    pub devices: Devices,
    pub services: Services,
    pub service_access: ServiceAccessManager,
    pub trusted_devices_config: Arc<Mutex<TrustedDevicesConfig>>,
    pub swarm_control: SwarmControl,
    pub mdns_control: MdnsControl,
    pub docker_control: Option<DockerControl>,
    pub node_capabilities_control: NodeCapabilitiesControl,
}

impl FungiControl {
    pub(crate) fn new(init: FungiControlInit) -> Self {
        Self {
            config: init.config,
            devices: init.devices,
            services: init.services,
            service_access: init.service_access,
            trusted_devices_config: init.trusted_devices_config,
            swarm_control: init.swarm_control,
            mdns_control: init.mdns_control,
            docker_control: init.docker_control,
            node_capabilities_control: init.node_capabilities_control,
        }
    }

    pub fn config(&self) -> Arc<Mutex<FungiConfig>> {
        self.config.clone()
    }

    pub fn devices(&self) -> &Devices {
        &self.devices
    }

    pub fn services(&self) -> &Services {
        &self.services
    }

    pub fn devices_config(&self) -> Arc<Mutex<DevicesConfig>> {
        self.devices.config()
    }

    pub fn trusted_devices(&self) -> Arc<Mutex<TrustedDevicesConfig>> {
        self.trusted_devices_config.clone()
    }

    pub fn swarm_control(&self) -> &SwarmControl {
        &self.swarm_control
    }

    pub fn docker_control(&self) -> Option<&DockerControl> {
        self.docker_control.as_ref()
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

    pub fn node_capabilities_control(&self) -> &NodeCapabilitiesControl {
        &self.node_capabilities_control
    }

    pub fn service_control_protocol_control(&self) -> &ServiceControlProtocolControl {
        self.services.service_control()
    }

    pub fn mdns_control(&self) -> &MdnsControl {
        &self.mdns_control
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
        self.config
            .lock()
            .config_file_path()
            .parent()
            .map(std::path::Path::to_path_buf)
            .ok_or_else(|| anyhow::anyhow!("config file has no parent directory"))
    }
}
