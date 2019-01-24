extern crate analytics_rs;
extern crate env_logger;
extern crate sysinfo;
#[macro_use]
extern crate log;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;

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
    builder.filter(Some("mem_usage"), LevelFilter::Trace);
    builder.init();

    let settings = match (env::var("KEEN_PROJECT_ID"), env::var("KEEN_API_KEY")) {
        (Ok(project_id), Ok(write_key)) => ProjectSettings::new(None, &project_id, &write_key),
        _ => {
            panic!("KEEN_PROJECT_ID and KEEN_API_KEY have to be defined as environment variable");
        }
    };

    let mut client = KeenClient::new(settings, Some(Duration::from_secs(10)));
    client.start();

    let mut system = sysinfo::System::new();

    loop {
        system.refresh_all();
        let memory_used: f64 =
            system.get_used_memory() as f64 / system.get_total_memory() as f64 * 100.0;

        let system_info = SystemInfo {
            mem_used: memory_used,
        };

        if let Err(e) =
            client.add_event("system_info", &serde_json::to_value(&system_info).unwrap())
        {
            error!("Event can't be added: {}", e);
        }

        thread::sleep(Duration::from_millis(2000));
    }
    client.stop();
}

#[derive(Serialize, Deserialize)]
struct SystemInfo {
    mem_used: f64,
}
