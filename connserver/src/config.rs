use std::{path::PathBuf, u64};

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Master {
    /// Address of the master server to connect to
    pub address: String,
    /// Capacity of the channel to/from the master server task
    /// Larger values reduce chance of backpressure, but increase memory usage
    pub channel_capacity: usize,
}

impl Default for Master {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:42001".to_string(),
            channel_capacity: 32_768,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Listener {
    /// Address for the connection server to listen on
    pub address: String,
}

impl Default for Listener {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:42000".to_string(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum LogLevel {
    /// No logging
    Off,
    /// Only errors
    Error,
    /// Errors and warnings
    Warn,
    /// Errors, warnings, and info
    Info,
    /// All logs
    Debug,
    /// All logs, including trace
    Trace,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct General {
    log_level: Option<LogLevel>,
}

impl Default for General {
    fn default() -> Self {
        Self {
            log_level: Some(LogLevel::Info),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Clients {
    pub channel_capacity: usize,
    pub stale_timeout_secs: u64,
    pub max_stale_clients: u64,
    pub stale_reap_interval_secs: u64,
    pub missed_packets_buffer_size: usize,
}

impl Default for Clients {
    fn default() -> Self {
        Self {
            channel_capacity: 8192,
            stale_timeout_secs: 60 * 5,
            max_stale_clients: u16::MAX as u64,
            stale_reap_interval_secs: 15,
            missed_packets_buffer_size: 0,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct Config {
    pub general: General,
    pub listener: Listener,
    pub master: Master,
    pub clients: Clients,
}

impl Config {
    pub fn load_or_new(filename: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let filename: PathBuf = filename.into();
        if filename.exists() {
            let config = toml::from_str(&std::fs::read_to_string(filename)?)?;
            Ok(config)
        } else {
            let config = Config::default();
            let toml = toml::to_string_pretty(&config)?;
            std::fs::write(filename, toml)?;
            Ok(config)
        }
    }

    pub fn fill_from_env(&mut self) -> anyhow::Result<()> {
        if let Ok(log_level) = std::env::var("VOLTLANE_LOG_LEVEL") {
            self.general.log_level = Some(match log_level.to_lowercase().as_str() {
                "off" => LogLevel::Off,
                "error" => LogLevel::Error,
                "warn" => LogLevel::Warn,
                "info" => LogLevel::Info,
                "debug" => LogLevel::Debug,
                "trace" => LogLevel::Trace,
                _ => return Err(anyhow::anyhow!("Invalid log level: {}", log_level)),
            });
        }
        if let Ok(address) = std::env::var("VOLTLANE_MASTER_ADDRESS") {
            self.master.address = address;
        }
        if let Ok(address) = std::env::var("VOLTLANE_LISTENER_ADDRESS") {
            self.listener.address = address;
        }
        if let Ok(channel_capacity) = std::env::var("VOLTLANE_MASTER_CHANNEL_CAPACITY") {
            self.master.channel_capacity = channel_capacity.parse()?;
        }
        if let Ok(address) = std::env::var("VOLTLANE_MASTER_ADDRESS") {
            self.master.address = address;
        }
        if let Ok(channel_capacity) = std::env::var("VOLTLANE_CLIENTS_CHANNEL_CAPACITY") {
            self.clients.channel_capacity = channel_capacity.parse()?;
        }
        if let Ok(stale_timeout_secs) = std::env::var("VOLTLANE_CLIENTS_STALE_TIMEOUT_SECS") {
            self.clients.stale_timeout_secs = stale_timeout_secs.parse()?;
        }
        if let Ok(max_stale_clients) = std::env::var("VOLTLANE_CLIENTS_MAX_STALE_CLIENTS") {
            self.clients.max_stale_clients = max_stale_clients.parse()?;
        }
        if let Ok(stale_reap_interval_secs) = std::env::var("VOLTLANE_CLIENTS_STALE_REAP_INTERVAL_SECS") {
            self.clients.stale_reap_interval_secs = stale_reap_interval_secs.parse()?;
        }
        if let Ok(missed_packets_buffer_size) = std::env::var("VOLTLANE_CLIENTS_MISSED_PACKETS_BUFFER_SIZE") {
            self.clients.missed_packets_buffer_size = missed_packets_buffer_size.parse()?;
        }
        Ok(())
    }
}
