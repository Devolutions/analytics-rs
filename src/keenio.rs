use chrono::{SecondsFormat, Utc};
use curl;
use curl::easy::{Easy, List};
use serde_json;
use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::{channel, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

const MAX_EVENTS_BY_REQUEST: u32 = 5000;

#[derive(Clone)]
pub struct ProjectSettings {
    custom_domain_url: Option<String>,
    project_id: String,
    api_key: String,
}

impl ProjectSettings {
    pub fn new(custom_domain_url: Option<String>, project_id: &str, api_key: &str) -> Self {
        ProjectSettings {
            custom_domain_url,
            project_id: project_id.to_owned(),
            api_key: api_key.to_owned(),
        }
    }
}

#[derive(Clone)]
pub struct KeenClient {
    settings: ProjectSettings,
    send_interval: Option<Duration>,
    // Keep the sender/receiver in a Mutex because the KeenClient struct has to be sync in DenRouter
    sender: Arc<Mutex<Option<Sender<Event>>>>,          // Use to send events to the thread
    receiver_sync: Arc<Mutex<Option<Receiver<()>>>>,    // Use to wait the end of a task in the thread
    thread_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl KeenClient {
    pub fn new(settings: ProjectSettings, send_interval: Option<Duration>) -> Self {
        KeenClient {
            settings: settings,
            sender: Arc::new(Mutex::new(None)),
            receiver_sync: Arc::new(Mutex::new(None)),
            thread_handle: Arc::new(Mutex::new(None)),
            send_interval: send_interval,
        }
    }

    pub fn start(&mut self) {
        let (sender_event, receiver_event) = channel();
        let (sender_sync, receiver_sync) = channel();

        let mut sender_event_opt = self.sender.lock().unwrap();
        let mut receiver_sync_opt = self.receiver_sync.lock().unwrap();
        if sender_event_opt.is_none() && receiver_sync_opt.is_none() {
            *sender_event_opt = Some(sender_event);
            *receiver_sync_opt = Some(receiver_sync);

            let settings = self.settings.clone();
            let send_interval = self.send_interval.clone();

            self.thread_handle = Arc::new(Mutex::new(Some(thread::spawn(move || {
                send_events_thread(receiver_event, sender_sync, settings, send_interval);
            }))));
        }
    }

    pub fn stop(&mut self) {
        {
            // We drop the sender. The receiver will fail and thread will close.
            self.sender.lock().unwrap().take();
        }

        // Wait the end of the thread
        if let Some(handle) = self.thread_handle.lock().unwrap().take() {
            let _ = handle.join();
        }
    }

    pub fn flush(&mut self, wait: bool) -> Result<(), String> {
        // Send the event FLUSH
        let sender = self.sender.lock().unwrap();

        if let Some(ref sender) = *sender {
            sender.send(Event::Flush(wait)).map_err(|e| e.to_string())?;
            if wait {
                let receiver_sync_opt = self.receiver_sync.lock().unwrap();
                if let Some(ref receiver_sync) = *receiver_sync_opt {
                    receiver_sync.recv().map_err(|e| e.to_string())?;
                }
            }
            Ok(())
        } else {
            Err("Thread is not running. Function \"start\" has to be called first".to_string())
        }
    }

    pub fn add_event(&self, collection: &str, json: &serde_json::Value) -> Result<(), String> {
        self.add_event_with_param(collection, json, false)
    }

    pub fn add_event_with_geo_enrichment(
        &self,
        collection: &str,
        json: &serde_json::Value,
    ) -> Result<(), String> {
        self.add_event_with_param(collection, json, true)
    }

    fn add_event_with_param(
        &self,
        collection: &str,
        json: &serde_json::Value,
        add_ip_geo: bool,
    ) -> Result<(), String> {
        // Add a timestamp
        let mut json_clone = json.clone();
        if let Some(object) = json_clone.as_object_mut() {
            let mut keen_info =
                KeenInfo::new(Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true));
            if add_ip_geo {
                keen_info.add_addon(KeenAddons::build_ip_geo_addons(
                    "ip_address",
                    "ip_geo_info",
                    true,
                ));
                object.insert("ip_address".to_string(), json!("${keen.ip}"));
            }

            object.insert(
                "keen".to_string(),
                serde_json::to_value(&keen_info).unwrap(),
            );
        }

        let event = Event::KeenEvent(collection.to_owned(), json_clone);

        // Send the event
        let sender = self.sender.lock().unwrap();
        if let Some(ref sender) = *sender {
            sender.send(event).map_err(|e| e.to_string())
        } else {
            Err("Thread is not running. Function \"start\" has to be called first".to_string())
        }
    }
}

fn send_events_thread(
    receiver: Receiver<Event>,
    sender_sync: Sender<()>,
    settings: ProjectSettings,
    send_interval: Option<Duration>,
) {
    let mut send_events = false;
    let mut notify_caller = false;
    let mut events_qty = 0u32;
    let mut events = HashMap::new();
    let mut stop_thread = false;
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
                    Ok(Event::KeenEvent(collection, json)) => {
                        events_qty += 1;
                        let collection = events.entry(collection).or_insert(Vec::new());
                        collection.push(json);
                    }
                    Ok(Event::Flush(notify)) => {
                        send_events = true;
                        notify_caller = notify;
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        send_events = true;
                    }
                    Err(_) => {
                        stop_thread = true;
                    }
                }
            }
            None => match receiver.recv() {
                Ok(Event::KeenEvent(collection, json)) => {
                    let collection = events.entry(collection).or_insert(Vec::new());
                    collection.push(json);
                    send_events = true;
                }
                Ok(Event::Flush(notify)) => {
                    send_events = true;
                    notify_caller = notify;
                }
                Err(_) => {
                    stop_thread = true;
                },
            },
        }

        if send_events || events_qty >= MAX_EVENTS_BY_REQUEST || stop_thread {
            now = SystemTime::now();
            if !events.is_empty() {
                trace!("Sending events: {} events to send!", events_qty);
                let body = serde_json::to_string(&events).unwrap();
                match post_to_keen(&settings, &body) {
                    Ok(_) => {
                        trace!("Events sent: {}", body);
                    }
                    Err(e) => {
                        error!("Events can't be sent: {}", e);
                    }
                }
                events.clear();
            }
            send_events = false;
            events_qty = 0;
        }

        // Notify the caller that the flush is done
        if notify_caller {
            let _ = sender_sync.send(());
        }

        if stop_thread {
            break;
        }
    }
}

fn post_to_keen(settings: &ProjectSettings, body: &str) -> Result<(), curl::Error> {
    // Prepare curl request
    let mut easy = Easy::new();

    // Don't validate the certificate since curl request will fail if mbedtlsis used
    // and installed certificates are not provided to mbedtls (wayk windows has that problem).
    let _ = easy.ssl_verify_host(false);
    let _ = easy.ssl_verify_peer(false);


    let domain_url = settings.custom_domain_url.as_ref().map_or("https://api.keen.io".to_string(), |url| url.to_string());
    let url = format!(
        "{}/3.0/projects/{}/events?api_key={}", domain_url, settings.project_id, settings.api_key
    );
    easy.url(&url)?;
    easy.post(true)?;

    // Set content-type
    let mut list = List::new();
    list.append("Content-Type: application/json")?;
    easy.http_headers(list)?;

    // Set body
    easy.post_fields_copy(body.as_ref())?;

    // Send request
    easy.perform()?;
    Ok(())
}

#[derive(Debug)]
enum Event {
    KeenEvent(String, serde_json::Value),
    Flush(bool),
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
    fn build_ip_geo_addons(
        input_field_name: &str,
        output_field_name: &str,
        remove_ip_property: bool,
    ) -> Self {
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
