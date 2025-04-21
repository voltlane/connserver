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
}
