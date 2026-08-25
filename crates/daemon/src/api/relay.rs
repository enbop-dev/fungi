use anyhow::Result;
use fungi_config::EffectiveRelayAddress;
use libp2p::Multiaddr;

use crate::FungiControl;

impl FungiControl {
    pub fn relay_enabled(&self) -> bool {
        self.settings().relay_enabled()
    }

    pub fn use_community_relays(&self) -> bool {
        self.settings().use_community_relays()
    }

    pub fn custom_relay_addresses(&self) -> Vec<Multiaddr> {
        self.settings().custom_relay_addresses()
    }

    pub fn effective_relay_addresses(&self) -> Vec<EffectiveRelayAddress> {
        self.settings().effective_relay_addresses()
    }

    pub fn set_relay_enabled(&self, enabled: bool) -> Result<()> {
        self.settings().set_relay_enabled(enabled)
    }

    pub fn set_use_community_relays(&self, enabled: bool) -> Result<()> {
        self.settings().set_use_community_relays(enabled)
    }

    pub fn add_custom_relay_address(&self, address: Multiaddr) -> Result<()> {
        self.settings().add_custom_relay_address(address)
    }

    pub fn remove_custom_relay_address(&self, address: Multiaddr) -> Result<()> {
        self.settings().remove_custom_relay_address(&address)
    }
}
