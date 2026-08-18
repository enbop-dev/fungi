use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    net::TcpListener as StdTcpListener,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::{Result, bail};
use fungi_config::{
    local_preferences::{LocalPortSource, LocalPreferenceCache, LocalServicePreference},
    tcp_tunneling::ForwardingRule,
};
use futures::future::join_all;
use libp2p::PeerId;
use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    DeviceService, DeviceServiceEndpoint, DeviceServiceSnapshot, ServiceAccess,
    ServiceAccessEndpoint, Services, controls::TcpTunnelingControl,
};

const STARTUP_SERVICE_REFRESH_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Default)]
struct ServiceAccessRuntimeState {
    /// Session-only overrides. Persistent preferences remain intact so a daemon restart restores
    /// an explicitly detached access, matching the existing CLI behavior.
    detached_services: BTreeSet<(String, String)>,
}

struct ServiceAccessManagerInner {
    fungi_dir: PathBuf,
    tcp_tunneling: TcpTunnelingControl,
    /// Serializes preference read/modify/write sequences and listener replacement.
    operations: AsyncMutex<()>,
    runtime_state: Mutex<ServiceAccessRuntimeState>,
}

/// Owns local service-access preferences and their active forwarding listeners.
#[derive(Clone)]
pub(crate) struct ServiceAccessManager {
    inner: Arc<ServiceAccessManagerInner>,
}

impl ServiceAccessManager {
    pub(crate) fn new(fungi_dir: PathBuf, tcp_tunneling: TcpTunnelingControl) -> Self {
        Self {
            inner: Arc::new(ServiceAccessManagerInner {
                fungi_dir,
                tcp_tunneling,
                operations: AsyncMutex::new(()),
                runtime_state: Mutex::new(ServiceAccessRuntimeState::default()),
            }),
        }
    }

    pub(crate) fn forwarding_rules(&self) -> Vec<(String, ForwardingRule)> {
        self.inner.tcp_tunneling.get_forwarding_rules()
    }

    pub(crate) async fn attach(
        &self,
        peer_id: PeerId,
        service: DeviceService,
        entry: Option<String>,
        local_port: Option<u16>,
    ) -> Result<ServiceAccess> {
        if service.endpoints.is_empty() {
            bail!(
                "remote service exposes no named TCP endpoints: {}",
                service.name
            );
        }

        if local_port.is_some() && service.endpoints.len() > 1 && entry.is_none() {
            bail!("choose a service entry before assigning a fixed local port");
        }

        let _operation = self.inner.operations.lock().await;
        let peer_id_string = peer_id.to_string();
        let mut local_preferences = self.local_preferences()?;
        let mut active_rules = self.forwarding_rules();
        let mut reserved_local_ports = local_preferences
            .records
            .iter()
            .map(|record| record.local_port)
            .chain(active_rules.iter().map(|(_, rule)| rule.local_port))
            .collect::<BTreeSet<_>>();
        let mut enabled_endpoints = Vec::new();
        let endpoints = service
            .endpoints
            .into_iter()
            .filter(|endpoint| {
                entry
                    .as_deref()
                    .map(|entry| endpoint.name == entry)
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>();

        if endpoints.is_empty() {
            let entry = entry.unwrap_or_else(|| "default".to_string());
            bail!("remote service entry not found: {entry}");
        }

        for endpoint in endpoints {
            let existing_record = local_preferences
                .find_record(&peer_id_string, &service.name, &endpoint.name)
                .cloned();
            let existing_active_rule = find_active_rule(
                &active_rules,
                &peer_id_string,
                &service.name,
                &endpoint.name,
            );

            if let Some(record) = &existing_record {
                reserved_local_ports.remove(&record.local_port);
            }
            if let Some((_, rule)) = &existing_active_rule {
                reserved_local_ports.remove(&rule.local_port);
            }

            let selected_local_port = match (local_port, existing_record.as_ref()) {
                (Some(local_port), _) => local_port,
                (None, Some(record)) => record.local_port,
                (None, None) => allocate_free_local_port(&reserved_local_ports)?,
            };
            let local_port_source = if local_port.is_some() {
                LocalPortSource::User
            } else {
                existing_record
                    .as_ref()
                    .map(|record| record.local_port_source)
                    .unwrap_or_default()
            };

            let active_rule_matches = existing_active_rule.as_ref().is_some_and(|(_, rule)| {
                rule.local_host == "127.0.0.1"
                    && rule.local_port == selected_local_port
                    && rule.remote_protocol.as_deref() == Some(endpoint.protocol.as_str())
            });
            let active_rule_owns_selected_port =
                existing_active_rule.as_ref().is_some_and(|(_, rule)| {
                    rule.local_host == "127.0.0.1" && rule.local_port == selected_local_port
                });

            if !active_rule_matches && !active_rule_owns_selected_port {
                ensure_local_port_available(selected_local_port, &reserved_local_ports)?;
            }

            let record = LocalServicePreference {
                remote_peer_id: peer_id_string.clone(),
                remote_service_name: service.name.clone(),
                remote_service_port_name: endpoint.name.clone(),
                local_host: "127.0.0.1".to_string(),
                local_port: selected_local_port,
                local_port_source,
            };
            let updated_local_preferences =
                local_preferences.with_upserted_record(record.clone())?;

            let mut removed_active_rule = None;
            let mut started_rule_id = None;
            if !active_rule_matches {
                if let Some((rule_id, rule)) = existing_active_rule {
                    self.remove_forwarding_rule(&rule_id)?;
                    removed_active_rule = Some(rule);
                }

                match self
                    .start_forwarding_rule(&record, endpoint.protocol.clone())
                    .await
                {
                    Ok(rule_id) => started_rule_id = Some(rule_id),
                    Err(error) => {
                        if let Some(rule) = removed_active_rule {
                            self.restore_forwarding_rule_if_attached(rule).await;
                        }
                        return Err(error);
                    }
                }
            }

            if let Err(error) = updated_local_preferences.save_to_file() {
                if let Some(rule_id) = started_rule_id
                    && let Err(rollback_error) = self.remove_forwarding_rule(&rule_id)
                {
                    log::warn!(
                        "Failed to roll back service access listener after save failure: {rollback_error}"
                    );
                }
                if let Some(rule) = removed_active_rule {
                    self.restore_forwarding_rule_if_attached(rule).await;
                }
                return Err(error);
            }

            local_preferences = updated_local_preferences;
            if !active_rule_matches {
                active_rules = self.forwarding_rules();
            }
            reserved_local_ports.insert(selected_local_port);
            enabled_endpoints.push(ServiceAccessEndpoint {
                name: endpoint.name,
                protocol: endpoint.protocol,
                local_host: record.local_host,
                local_port: record.local_port,
            });
        }

        enabled_endpoints.sort_by(|left, right| left.name.cmp(&right.name));
        self.clear_detached(&peer_id_string, &service.name);
        Ok(ServiceAccess {
            peer_id: peer_id_string,
            service_name: service.name,
            endpoints: enabled_endpoints,
        })
    }

    pub(crate) fn detach(&self, peer_id: PeerId, service_name: &str) -> Result<()> {
        let peer_id = peer_id.to_string();
        self.inner
            .runtime_state
            .lock()
            .detached_services
            .insert((peer_id.clone(), service_name.to_string()));
        self.remove_matching_rules(&peer_id, Some(service_name))
    }

    pub(crate) async fn forget_service(&self, peer_id: PeerId, service_name: &str) -> Result<()> {
        let _operation = self.inner.operations.lock().await;
        let peer_id = peer_id.to_string();
        self.inner
            .runtime_state
            .lock()
            .detached_services
            .insert((peer_id.clone(), service_name.to_string()));
        self.remove_matching_rules(&peer_id, Some(service_name))?;
        self.local_preferences()?
            .remove_service_records(&peer_id, service_name)?;
        Ok(())
    }

    pub(crate) async fn forget_device(&self, peer_id: PeerId) -> Result<()> {
        let _operation = self.inner.operations.lock().await;
        let peer_id = peer_id.to_string();
        let preferences = self.local_preferences()?;
        {
            let mut state = self.inner.runtime_state.lock();
            for record in &preferences.records {
                if record.remote_peer_id == peer_id {
                    state
                        .detached_services
                        .insert((peer_id.clone(), record.remote_service_name.clone()));
                }
            }
        }
        self.remove_matching_rules(&peer_id, None)?;
        preferences.remove_device_records(&peer_id)?;
        Ok(())
    }

    pub(crate) async fn saved_entries(
        &self,
        peer_id: PeerId,
        service_name: &str,
    ) -> Result<BTreeSet<String>> {
        let _operation = self.inner.operations.lock().await;
        let peer_id = peer_id.to_string();
        Ok(self
            .local_preferences()?
            .records
            .into_iter()
            .filter(|record| {
                record.remote_peer_id == peer_id && record.remote_service_name == service_name
            })
            .map(|record| record.remote_service_port_name)
            .collect())
    }

    pub(crate) async fn list(&self, peer_id: Option<PeerId>) -> Result<Vec<ServiceAccess>> {
        let _operation = self.inner.operations.lock().await;
        let peer_filter = peer_id.map(|peer_id| peer_id.to_string());
        let mut grouped = BTreeMap::<(String, String), Vec<ServiceAccessEndpoint>>::new();

        for record in self.local_preferences()?.records {
            if let Some(peer_filter) = &peer_filter
                && &record.remote_peer_id != peer_filter
            {
                continue;
            }

            grouped
                .entry((
                    record.remote_peer_id.clone(),
                    record.remote_service_name.clone(),
                ))
                .or_default()
                .push(ServiceAccessEndpoint {
                    name: record.remote_service_port_name,
                    protocol: String::new(),
                    local_host: record.local_host,
                    local_port: record.local_port,
                });
        }

        let mut services = grouped
            .into_iter()
            .map(|((peer_id, service_name), mut endpoints)| {
                endpoints.sort_by(|left, right| left.name.cmp(&right.name));
                ServiceAccess {
                    peer_id,
                    service_name,
                    endpoints,
                }
            })
            .collect::<Vec<_>>();
        services.sort_by(|left, right| {
            left.peer_id
                .cmp(&right.peer_id)
                .then(left.service_name.cmp(&right.service_name))
        });
        Ok(services)
    }

    pub(crate) async fn preference_records(&self) -> Result<Vec<LocalServicePreference>> {
        let _operation = self.inner.operations.lock().await;
        Ok(self.local_preferences()?.records)
    }

    async fn restore_from_cached_snapshots(&self, services: &Services) {
        let records = match self.preference_records().await {
            Ok(records) => records,
            Err(error) => {
                log::warn!("Failed to read local service access preferences: {error}");
                return;
            }
        };

        self.restore_records_from_cached_snapshots(&records, services)
            .await;
    }

    pub(crate) async fn restore_records_from_cached_snapshots(
        &self,
        records: &[LocalServicePreference],
        services: &Services,
    ) {
        for candidate in records {
            let peer_id = match candidate.remote_peer_id.parse::<PeerId>() {
                Ok(peer_id) => peer_id,
                Err(error) => {
                    log::warn!(
                        "Skipping service access restore for invalid peer id '{}': {error}",
                        candidate.remote_peer_id
                    );
                    continue;
                }
            };
            let snapshot = match services.for_device(peer_id).snapshot() {
                Ok(Some(snapshot)) => snapshot,
                Ok(None) => {
                    log::debug!(
                        "No cached device service snapshot for {}; skipping service access restore",
                        candidate.remote_peer_id
                    );
                    continue;
                }
                Err(error) => {
                    log::warn!(
                        "Failed to load cached device service snapshot for {}: {error}",
                        candidate.remote_peer_id
                    );
                    continue;
                }
            };

            let Some(endpoint) = snapshot
                .services
                .iter()
                .find(|service| service.name == candidate.remote_service_name)
                .and_then(|service| {
                    service
                        .endpoints
                        .iter()
                        .find(|endpoint| endpoint.name == candidate.remote_service_port_name)
                })
            else {
                log::warn!(
                    "Cached service metadata for {} does not include {} entry {}; skipping service access restore",
                    candidate.remote_peer_id,
                    candidate.remote_service_name,
                    candidate.remote_service_port_name
                );
                continue;
            };

            if let Err(error) = self.restore_current_record(candidate, endpoint).await {
                log::warn!(
                    "Failed to restore local listener for {}@{} entry {} on {}:{}: {error}",
                    candidate.remote_service_name,
                    candidate.remote_peer_id,
                    candidate.remote_service_port_name,
                    candidate.local_host,
                    candidate.local_port
                );
            }
        }
    }

    async fn restore_current_record(
        &self,
        candidate: &LocalServicePreference,
        endpoint: &DeviceServiceEndpoint,
    ) -> Result<()> {
        let _operation = self.inner.operations.lock().await;
        if self.is_detached(&candidate.remote_peer_id, &candidate.remote_service_name) {
            log::debug!(
                "Skipping detached service access restore for {}@{}",
                candidate.remote_service_name,
                candidate.remote_peer_id
            );
            return Ok(());
        }

        let preferences = self.local_preferences()?;
        let Some(current_record) = preferences
            .find_record(
                &candidate.remote_peer_id,
                &candidate.remote_service_name,
                &candidate.remote_service_port_name,
            )
            .cloned()
        else {
            log::debug!(
                "Skipping stale service access restore for {}@{} entry {}",
                candidate.remote_service_name,
                candidate.remote_peer_id,
                candidate.remote_service_port_name
            );
            return Ok(());
        };

        self.restore_forwarding_record(&current_record, endpoint)
            .await
    }

    async fn restore_forwarding_record(
        &self,
        record: &LocalServicePreference,
        endpoint: &DeviceServiceEndpoint,
    ) -> Result<()> {
        let existing_active_rule = find_active_rule(
            &self.forwarding_rules(),
            &record.remote_peer_id,
            &record.remote_service_name,
            &record.remote_service_port_name,
        );
        let active_rule_matches = existing_active_rule.as_ref().is_some_and(|(_, rule)| {
            rule.local_host == record.local_host
                && rule.local_port == record.local_port
                && rule.remote_protocol.as_deref() == Some(endpoint.protocol.as_str())
        });
        if active_rule_matches {
            return Ok(());
        }

        let mut removed_active_rule = None;
        if let Some((rule_id, rule)) = existing_active_rule {
            self.remove_forwarding_rule(&rule_id)?;
            removed_active_rule = Some(rule);
        }

        match self
            .start_forwarding_rule(record, endpoint.protocol.clone())
            .await
        {
            Ok(rule_id) => {
                // `detach` remains synchronous for Rust API compatibility and can race this
                // background operation on another executor thread. Re-check after insertion so
                // every ordering converges to a detached listener being absent.
                if self.is_detached(&record.remote_peer_id, &record.remote_service_name) {
                    let _ = self.remove_forwarding_rule(&rule_id);
                }
                Ok(())
            }
            Err(error) => {
                if let Some(rule) = removed_active_rule {
                    self.restore_forwarding_rule_if_attached(rule).await;
                }
                Err(error)
            }
        }
    }

    async fn start_forwarding_rule(
        &self,
        record: &LocalServicePreference,
        remote_protocol: String,
    ) -> Result<String> {
        let rule = ForwardingRule {
            local_host: record.local_host.clone(),
            local_port: record.local_port,
            remote_peer_id: record.remote_peer_id.clone(),
            remote_protocol: Some(remote_protocol),
            remote_port: None,
            remote_service_id: None,
            remote_service_name: Some(record.remote_service_name.clone()),
            remote_service_port_name: Some(record.remote_service_port_name.clone()),
        };
        ensure_service_access_rule(&rule)?;
        self.inner.tcp_tunneling.add_forwarding_rule(rule).await
    }

    async fn restore_forwarding_rule_if_attached(&self, rule: ForwardingRule) {
        let Some(service_name) = rule.remote_service_name.as_deref() else {
            return;
        };
        if self.is_detached(&rule.remote_peer_id, service_name) {
            return;
        }

        match self
            .inner
            .tcp_tunneling
            .add_forwarding_rule(rule.clone())
            .await
        {
            Ok(rule_id) => {
                if self.is_detached(&rule.remote_peer_id, service_name) {
                    let _ = self.remove_forwarding_rule(&rule_id);
                }
            }
            Err(error) => log::warn!(
                "Failed to restore previous service access listener after attach failure: {error}"
            ),
        }
    }

    fn remove_forwarding_rule(&self, rule_id: &str) -> Result<()> {
        self.inner.tcp_tunneling.remove_forwarding_rule(rule_id)
    }

    fn remove_matching_rules(&self, peer_id: &str, service_name: Option<&str>) -> Result<()> {
        let rules_to_remove = self
            .forwarding_rules()
            .into_iter()
            .filter(|(_, rule)| {
                rule.remote_peer_id == peer_id
                    && service_name
                        .is_none_or(|name| rule.remote_service_name.as_deref() == Some(name))
            })
            .map(|(rule_id, _)| rule_id)
            .collect::<Vec<_>>();

        for rule_id in rules_to_remove {
            self.remove_forwarding_rule(&rule_id)?;
        }
        Ok(())
    }

    fn local_preferences(&self) -> Result<LocalPreferenceCache> {
        LocalPreferenceCache::apply_from_dir(&self.inner.fungi_dir)
    }

    fn is_detached(&self, peer_id: &str, service_name: &str) -> bool {
        self.inner
            .runtime_state
            .lock()
            .detached_services
            .contains(&(peer_id.to_string(), service_name.to_string()))
    }

    fn clear_detached(&self, peer_id: &str, service_name: &str) {
        self.inner
            .runtime_state
            .lock()
            .detached_services
            .remove(&(peer_id.to_string(), service_name.to_string()));
    }
}

pub(crate) async fn restore_saved_service_accesses(
    service_access: ServiceAccessManager,
    services: Services,
) {
    service_access
        .restore_from_cached_snapshots(&services)
        .await;

    let records = match service_access.preference_records().await {
        Ok(records) => records,
        Err(error) => {
            log::warn!(
                "Failed to read local service access preferences before remote refresh: {error}"
            );
            return;
        }
    };
    let peer_ids = records
        .into_iter()
        .filter_map(|record| match record.remote_peer_id.parse::<PeerId>() {
            Ok(peer_id) => Some(peer_id),
            Err(error) => {
                log::warn!(
                    "Skipping service access refresh for invalid peer id '{}': {error}",
                    record.remote_peer_id
                );
                None
            }
        })
        .collect::<BTreeSet<_>>();

    for (peer_id, result) in refresh_devices(peer_ids, |peer_id| {
        let device_services = services.for_device(peer_id);
        async move { device_services.refresh().await }
    })
    .await
    {
        if let Err(error) = result {
            log::warn!(
                "Failed to refresh device service snapshot during startup restore for {peer_id}: {error}"
            );
        }
    }

    service_access
        .restore_from_cached_snapshots(&services)
        .await;
}

async fn refresh_devices<F, Fut>(
    peer_ids: impl IntoIterator<Item = PeerId>,
    refresh: F,
) -> Vec<(PeerId, Result<DeviceServiceSnapshot>)>
where
    F: Fn(PeerId) -> Fut,
    Fut: Future<Output = Result<DeviceServiceSnapshot>>,
{
    join_all(peer_ids.into_iter().map(|peer_id| {
        let refresh = &refresh;
        async move {
            let result = tokio::time::timeout(STARTUP_SERVICE_REFRESH_TIMEOUT, refresh(peer_id))
                .await
                .unwrap_or_else(|_| {
                    Err(anyhow::anyhow!(
                        "timed out after {} seconds",
                        STARTUP_SERVICE_REFRESH_TIMEOUT.as_secs()
                    ))
                });
            (peer_id, result)
        }
    }))
    .await
}

fn ensure_service_access_rule(rule: &ForwardingRule) -> Result<()> {
    if rule
        .remote_service_name
        .as_deref()
        .is_none_or(str::is_empty)
        || rule
            .remote_service_port_name
            .as_deref()
            .is_none_or(str::is_empty)
    {
        bail!("service access forwarding rules require remote service metadata");
    }
    Ok(())
}

fn find_active_rule(
    active_rules: &[(String, ForwardingRule)],
    remote_peer_id: &str,
    remote_service_name: &str,
    remote_service_port_name: &str,
) -> Option<(String, ForwardingRule)> {
    active_rules
        .iter()
        .find(|(_, rule)| {
            rule.remote_peer_id == remote_peer_id
                && rule.remote_service_name.as_deref() == Some(remote_service_name)
                && rule.remote_service_port_name.as_deref() == Some(remote_service_port_name)
        })
        .cloned()
}

fn ensure_local_port_available(port: u16, reserved_ports: &BTreeSet<u16>) -> Result<()> {
    if reserved_ports.contains(&port) {
        bail!("local port is already reserved by another service access: {port}");
    }
    StdTcpListener::bind(("127.0.0.1", port))
        .map(|_| ())
        .map_err(|error| anyhow::anyhow!("local port {port} is not available: {error}"))
}

fn allocate_free_local_port(reserved_ports: &BTreeSet<u16>) -> Result<u16> {
    for _ in 0..32 {
        let listener = StdTcpListener::bind(("127.0.0.1", 0))?;
        let port = listener.local_addr()?.port();
        if !reserved_ports.contains(&port) {
            return Ok(port);
        }
    }

    bail!("failed to allocate a free local TCP port for remote service access")
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use tokio::sync::Semaphore;

    use super::*;

    #[tokio::test]
    async fn startup_refresh_polls_all_devices_concurrently() {
        let device_count = 32;
        let started = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let peer_ids = (0..device_count)
            .map(|_| PeerId::random())
            .collect::<Vec<_>>();
        let refresh_started = started.clone();
        let refresh_release = release.clone();
        let refresh_task = tokio::spawn(refresh_devices(peer_ids, move |_| {
            let started = refresh_started.clone();
            let release = refresh_release.clone();
            async move {
                started.fetch_add(1, Ordering::SeqCst);
                let _permit = release.acquire().await.unwrap();
                Ok(DeviceServiceSnapshot {
                    peer_id: PeerId::random().to_string(),
                    services: Vec::new(),
                    updated_at: std::time::SystemTime::now(),
                })
            }
        }));

        tokio::time::timeout(Duration::from_secs(1), async {
            while started.load(Ordering::SeqCst) != device_count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all device refreshes should be polled without a concurrency cap");

        release.add_permits(device_count);
        let results = refresh_task.await.unwrap();
        assert_eq!(results.len(), device_count);
    }

    #[tokio::test(start_paused = true)]
    async fn startup_refresh_times_out_each_device_after_fifteen_seconds() {
        let results = refresh_devices([PeerId::random()], |_| {
            std::future::pending::<Result<DeviceServiceSnapshot>>()
        })
        .await;

        let error = results.into_iter().next().unwrap().1.unwrap_err();
        assert!(error.to_string().contains("timed out after 15 seconds"));
    }
}
