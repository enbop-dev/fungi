use std::path::PathBuf;

use anyhow::Result;
use tokio::task::JoinHandle;

use crate::{
    Connectivity, InboundAccessPolicy, Settings,
    devices::{DeviceHandle, Devices},
    service_accesses::{ServiceAccesses, restore_saved_service_accesses},
    services::{ServiceHandle, ServiceKey, Services},
};

/// Cloneable entry point for all daemon application APIs.
///
/// `FungiControl` owns only shared domain handles. The unique daemon tasks and their lifecycle
/// remain owned by [`crate::FungiDaemon`].
#[derive(Clone)]
pub struct FungiControl {
    settings: Settings,
    devices: Devices,
    services: Services,
    service_access: ServiceAccesses,
    inbound_access: InboundAccessPolicy,
    connectivity: Connectivity,
}

pub(crate) struct FungiControlInit {
    pub settings: Settings,
    pub devices: Devices,
    pub services: Services,
    pub service_access: ServiceAccesses,
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

    pub fn service_access(&self) -> &ServiceAccesses {
        &self.service_access
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
