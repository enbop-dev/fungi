use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use fungi_config::{EffectiveRelayAddress, FungiConfig, runtime::Runtime as RuntimeConfig};
use libp2p::Multiaddr;
use parking_lot::Mutex;

struct SettingsInner {
    config: Arc<Mutex<FungiConfig>>,
}

/// Shared daemon settings and their persisted updates.
#[derive(Clone)]
pub struct Settings {
    inner: Arc<SettingsInner>,
}

impl Settings {
    pub(crate) fn new(config: FungiConfig) -> Self {
        Self {
            inner: Arc::new(SettingsInner {
                config: Arc::new(Mutex::new(config)),
            }),
        }
    }

    pub fn snapshot(&self) -> FungiConfig {
        self.inner.config.lock().clone()
    }

    pub fn hostname(&self) -> Option<String> {
        self.inner.config.lock().get_hostname()
    }

    pub fn config_file_path(&self) -> PathBuf {
        self.inner.config.lock().config_file_path().to_path_buf()
    }

    pub fn fungi_dir(&self) -> Result<PathBuf> {
        self.inner
            .config
            .lock()
            .config_file_path()
            .parent()
            .map(std::path::Path::to_path_buf)
            .ok_or_else(|| anyhow::anyhow!("config file has no parent directory"))
    }

    pub fn runtime(&self) -> RuntimeConfig {
        self.inner.config.lock().get_runtime_config()
    }

    pub fn relay_enabled(&self) -> bool {
        self.inner.config.lock().network.relay_enabled
    }

    pub fn use_community_relays(&self) -> bool {
        self.inner.config.lock().network.use_community_relays
    }

    pub fn custom_relay_addresses(&self) -> Vec<Multiaddr> {
        self.inner
            .config
            .lock()
            .network
            .custom_relay_addresses
            .clone()
    }

    pub fn effective_relay_addresses(&self) -> Vec<EffectiveRelayAddress> {
        self.inner
            .config
            .lock()
            .network
            .effective_relay_addresses(&fungi_swarm::get_default_relay_addrs())
    }

    pub fn set_relay_enabled(&self, enabled: bool) -> Result<()> {
        self.update(|config| config.set_relay_enabled(enabled))?;
        Ok(())
    }

    pub fn set_use_community_relays(&self, enabled: bool) -> Result<()> {
        self.update(|config| config.set_use_community_relays(enabled))?;
        Ok(())
    }

    pub fn add_custom_relay_address(&self, address: Multiaddr) -> Result<()> {
        self.update(|config| config.add_custom_relay_address(address))?;
        Ok(())
    }

    pub fn remove_custom_relay_address(&self, address: &Multiaddr) -> Result<()> {
        self.update(|config| config.remove_custom_relay_address(address))?;
        Ok(())
    }

    pub(crate) fn add_runtime_allowed_host_path(&self, path: PathBuf) -> Result<FungiConfig> {
        self.update(|config| config.add_runtime_allowed_host_path(path))
    }

    pub(crate) fn remove_runtime_allowed_host_path(
        &self,
        path: &std::path::Path,
    ) -> Result<FungiConfig> {
        self.update(|config| config.remove_runtime_allowed_host_path(path))
    }

    pub(crate) fn config_handle(&self) -> Arc<Mutex<FungiConfig>> {
        self.inner.config.clone()
    }

    fn update(
        &self,
        update: impl FnOnce(&FungiConfig) -> Result<FungiConfig>,
    ) -> Result<FungiConfig> {
        let mut config = self.inner.config.lock();
        let updated = update(&config)?;
        *config = updated.clone();
        Ok(updated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_updates_share_one_serialized_config_state() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let settings = Settings::new(FungiConfig::apply_from_dir(temp_dir.path()).unwrap());
        let first = Multiaddr::empty().with(libp2p::multiaddr::Protocol::Memory(1));
        let second = Multiaddr::empty().with(libp2p::multiaddr::Protocol::Memory(2));

        let first_task = {
            let settings = settings.clone();
            std::thread::spawn(move || settings.add_custom_relay_address(first).unwrap())
        };
        let second_task = {
            let settings = settings.clone();
            std::thread::spawn(move || settings.add_custom_relay_address(second).unwrap())
        };
        first_task.join().unwrap();
        second_task.join().unwrap();

        assert_eq!(settings.custom_relay_addresses().len(), 2);
    }
}
