use simple_config::Config;
use umbilical_linux::UMBConfig;

#[tokio::main]
async fn main() {
    let mut config = UMBConfig::new();
    config.parse_file("umbilical.conf").expect("could not parse config file");
    config.parse_cli().expect("could not parse cli");

    umbilical_linux::run(config).await;
}
