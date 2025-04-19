#[derive(Default, Debug)]
pub struct Args {
    pub listen_addr: Option<String>,
    pub master_addr: Option<String>,
}

pub fn print_usage() {
    println!(
        r#"USAGE:
        {} [OPTIONS]

OPTIONS:
        --help              -h  print this help message
        --listen <ip:port>      ip/hostname and port to listen on
        --master <ip:port>      ip/hostname and port of the master server
"#,
        std::env::args().next().unwrap()
    );
}
impl Args {
    pub fn from_env() -> Result<Args, String> {
        let args: Vec<String> = std::env::args().skip(1).collect();
        let mut result = Args::default();
        let mut i = 0;
        while i < args.len() {
            let arg = args.get(i).unwrap();
            match arg.as_str() {
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                "--listen" => {
                    i += 1;
                    if let Some(listen_addr) = args.get(i) {
                        result.listen_addr = Some(listen_addr.clone());
                    } else {
                        return Err(format!("Missing argument for \"{arg}\""));
                    }
                }
                "--master" => {
                    i += 1;
                    if let Some(master_addr) = args.get(i) {
                        result.master_addr = Some(master_addr.clone());
                    } else {
                        return Err(format!("Missing argument for \"{arg}\""));
                    }
                }
                _ => return Err(format!("Unknown argument \"{arg}\"")),
            }
            i += 1;
        }
        Ok(result)
    }
}
