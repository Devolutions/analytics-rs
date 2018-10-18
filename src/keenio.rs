use chrono::{SecondsFormat, Utc};
use curl;
use curl::easy::{Easy, List};
use serde_json;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::{channel, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

pub struct ProjectSettings {
    project_id: String,
    api_key: String,
}

impl ProjectSettings {
    pub fn new(project_id: &str, api_key: &str) -> Self {
        ProjectSettings {
            project_id: project_id.to_owned(),
            api_key: api_key.to_owned(),
        }
    }
}

pub struct KeenClient {
    settings: Arc<ProjectSettings>,
    send_interval: Arc<Option<Duration>>,
    sender: Option<Sender<Event>>,
    thread_handle: Option<JoinHandle<()>>,
}

impl KeenClient {
    pub fn new(settings: ProjectSettings, send_interval: Option<Duration>) -> Self {
        KeenClient {
            settings: Arc::new(settings),
            sender: None,
            thread_handle: None,
            send_interval: Arc::new(send_interval),
        }
    }

    pub fn start(&mut self) {
        let (sender, receiver) = channel();
        self.sender = Some(sender);

        let settings = self.settings.clone();
        let send_interval = self.send_interval.clone();

        self.thread_handle = Some(thread::spawn(move || {
            send_events_thread(receiver, settings, send_interval);
        }));
    }

    pub fn stop(&mut self) {
        {
            // We drop the sender. The receiver will fail and thread will close.
            self.sender.take();
        }

        // Wait the end of the thread
        let _ = self.thread_handle.take().unwrap().join();
    }

    pub fn add_event(&self, collection: &str, json: &serde_json::Value) -> Result<(), String> {
        self.add_event_with_param(collection, json, false)
    }

    pub fn add_event_with_geo_enrichment(&self, collection: &str, json: &serde_json::Value) -> Result<(), String> {
        self.add_event_with_param(collection, json, true)
    }

    fn add_event_with_param(&self, collection: &str, json: &serde_json::Value, add_ip_geo: bool) -> Result<(), String> {
        if let Some(ref sender) = self.sender {

            // Add a timestamp
            let mut json_clone = json.clone();
            if let Some(object) = json_clone.as_object_mut() {
                let mut keen_info = KeenInfo::new(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true));
                if add_ip_geo {
                    keen_info.add_addon(KeenAddons::build_ip_geo_addons("ip_address", "ip_geo_info", true));
                    object.insert("ip_address".to_string(), json!("${keen.ip}"));
                }

                object.insert(
                    "keen".to_string(),
                    serde_json::to_value(&keen_info).unwrap(),
                );
            }

            let event = Event {
                collection: collection.to_owned(),
                json: json_clone,
            };
            sender.send(event).map_err(|e| e.to_string())
        } else {
            Err("Thread is not running. Function \"start\" has to be called first".to_string())
        }
    }

}

fn send_events_thread(
    receiver: Receiver<Event>,
    settings: Arc<ProjectSettings>,
    send_interval: Arc<Option<Duration>>,
) {
    let mut send_events = false;
    let mut events = Vec::new();
    let mut now = SystemTime::now();

    loop {
        match send_interval.as_ref() {
            Some(interval) => {
                // Calculate next timeout before sending events
                let elapsed = now.elapsed().unwrap_or_else(|_| *interval);
                let timeout = if *interval > elapsed {
                    *interval - elapsed
                } else {
                    Duration::from_millis(0)
                };

                match receiver.recv_timeout(timeout) {
                    Ok(event) => events.push(event),
                    Err(RecvTimeoutError::Timeout) => {
                        now = SystemTime::now();
                        send_events = true;
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
            None => match receiver.recv() {
                Ok(event) => {
                    events.push(event);
                    send_events = true;
                }
                Err(_) => break,
            },
        }

        if send_events {
            trace!("Sending events: {} events to send", events.len());
            for event in &events {
                match send_event(&settings, &event) {
                    Ok(_) => {
                        trace!("Event sent: {:?}", event);
                    }
                    Err(e) => {
                        error!("The event can't be sent: {}", e);
                    }
                }
            }
            events.clear();
            send_events = false;
        }
    }

    debug!("Thread stopped: {} events not sent", events.len());
}

fn send_event(settings: &ProjectSettings, event: &Event) -> Result<(), curl::Error> {
    // Prepare curl request
    let mut easy = Easy::new();
    let url = format!(
        "https://api.keen.io/3.0/projects/{}/events/{}?api_key={}",
        settings.project_id, event.collection, settings.api_key
    );
    easy.url(&url)?;
    easy.post(true)?;

    // Set content-type
    let mut list = List::new();
    list.append("Content-Type: application/json")?;
    easy.http_headers(list)?;

    // Set body
    easy.post_fields_copy(event.json.to_string().as_ref())?;

    // Send request
    easy.perform()?;
    Ok(())
}

#[derive(Debug)]
struct Event {
    collection: String,
    json: serde_json::Value,
}

#[derive(Serialize, Deserialize)]
struct KeenInfo {
    timestamp: String,
    addons: Vec<KeenAddons>,
}

impl KeenInfo {
    fn new(timestamp: String) -> Self {
        KeenInfo {
            timestamp,
            addons: Vec::new(),
        }
    }

    fn add_addon(&mut self, addon: KeenAddons) {
        self.addons.push(addon);
    }
}
#[derive(Serialize, Deserialize)]
struct KeenAddons {
    name: String,
    input: KeenInput,
    output: String,
}

impl KeenAddons {
    fn build_ip_geo_addons(input_field_name: &str, output_field_name: &str, remove_ip_property: bool) -> Self {
        KeenAddons {
            name: "keen:ip_to_geo".to_string(),
            input: KeenInput {
                ip: input_field_name.to_string(),
                remove_ip_property,
            },
            output: output_field_name.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct KeenInput {
    ip: String,
    remove_ip_property: bool,
}