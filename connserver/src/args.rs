#[derive(Default, Debug)]
pub struct Args {
}

pub fn print_usage() {
    println!(r#"USAGE:
        {} [OPTIONS]

OPTIONS:
        --help      -h  print this help message
"#, std::env::args().next().unwrap());
}
impl Args {
    pub fn from_env() -> Result<Args, String> {
        let args = std::env::args().skip(1);
        let mut result = Args::default();
        for arg in args {
            match arg.as_str() {
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                _ => return Err(format!("Unknown argument \"{arg}\"")),
            }
        }
        Ok(result)
    }
}
