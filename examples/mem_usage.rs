extern crate analytics_rs;
extern crate sysinfo;
#[macro_use]
extern crate json;
extern crate env_logger;
#[macro_use]
extern crate log;

use analytics_rs::keenio::KeenClient;
use analytics_rs::keenio::ProjectSettings;
use log::LevelFilter;
use std::env;
use std::thread;
use std::time::Duration;
use sysinfo::SystemExt;

fn main() {
    let mut builder = env_logger::Builder::new();
    builder.filter(Some("analytics_rs"), LevelFilter::Trace);
    builder.init();

    let settings = match (
        env::var("KEEN_PROJECT_ID"),
        env::var("KEEN_READ_KEY"),
        env::var("KEEN_WRITE_KEY"),
    ) {
        (Ok(project_id), Ok(read_key), Ok(write_key)) => {
            ProjectSettings::new(&project_id, &read_key, &write_key)
        }
        _ => {
            panic!("KEEN_PROJECT_ID, KEEN_READ_KEY and KEEN_WRITE_KEY have to be defined as environment variable");
        }
    };

    let mut client = KeenClient::new(settings);
    client.start();

    let mut system = sysinfo::System::new();

    loop {
        system.refresh_all();
        let memory_used: f64 =
            system.get_used_memory() as f64 / system.get_total_memory() as f64 * 100.0;
        let json = object!{
            "memory_used" => memory_used,
        };
        if let Err(e) = client.add_event("memory_usage", &json.to_string()) {
            error!("Event can't be added: {}", e);
        }

        thread::sleep(Duration::from_millis(5000));
    }
    client.stop();
}
