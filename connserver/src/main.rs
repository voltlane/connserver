use connserver::ConnServer;
use log::{error, info};

mod args;
mod config;
mod connection;
mod connserver;
mod ids;
mod master;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::max())
        .format_timestamp(None)
        .init();

    let args = match args::Args::from_env() {
        Ok(val) => val,
        Err(e) => {
            error!("Error: {}. Try --help.", e);
            std::process::exit(1);
        }
    };

    let mut config = match config::Config::load_or_new("connserver.toml") {
        Ok(config) => config,
        Err(e) => {
            error!("Error loading config: {}", e);
            std::process::exit(1);
        }
    };

    if let Some(listen_addr) = args.listen_addr {
        info!("Overriding listener.address from commandline");
        config.listener.address = listen_addr;
    }
    if let Some(master_addr) = args.master_addr {
        info!("Overriding master.address from commandline");
        config.master.address = master_addr;
    }

    let master = match master::TcpMasterDuplex::connect(&config.master.address).await {
        Ok(master) => master,
        Err(e) => {
            error!("Error connecting to master: {}", e);
            std::process::exit(1);
        }
    };
    let mut server = ConnServer::new(config).await;
    if let Err(err) = server.run(master).await {
        error!("Error: {}", err);
        std::process::exit(1);
    }
}
