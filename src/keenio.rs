use curl;
use curl::easy::{Easy, List};
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

pub struct ProjectSettings {
    project_id: String,
    _read_key: String,
    write_key: String,
}

impl ProjectSettings {
    pub fn new(project_id: &str, read_key: &str, write_key: &str) -> Self {
        ProjectSettings {
            project_id: project_id.to_owned(),
            _read_key: read_key.to_owned(),
            write_key: write_key.to_owned(),
        }
    }
}

pub struct KeenClient {
    settings: Arc<ProjectSettings>,
    sender: Option<Sender<Event>>,
    thread_handle: Option<JoinHandle<()>>,
}

impl KeenClient {
    pub fn new(settings: ProjectSettings) -> Self {
        KeenClient {
            settings: Arc::new(settings),
            sender: None,
            thread_handle: None,
        }
    }

    pub fn start(&mut self) {
        let (sender, receiver) = channel();
        self.sender = Some(sender);

        let settings_t = self.settings.clone();

        self.thread_handle = Some(thread::spawn(move || loop {
            match receiver.recv() {
                Ok(event) => match send_event(&settings_t, &event) {
                    Ok(_) => {
                        trace!("Event sent: {:?}", event);
                    }
                    Err(e) => {
                        error!("The event can't be sent: {}", e);
                    }
                },
                Err(_) => {
                    break;
                }
            }
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

    pub fn add_event(&self, collection: &str, json: &str) -> Result<(), String> {
        if let Some(ref sender) = self.sender {
            let event = Event {
                collection: collection.to_owned(),
                data: json.to_owned(),
            };
            sender.send(event).map_err(|e| e.to_string())
        } else {
            Err("Thread is not running. Function \"start\" has to be called first".to_string())
        }
    }
}

fn send_event(settings: &ProjectSettings, event: &Event) -> Result<(), curl::Error> {
    // Prepare curl request
    let mut easy = Easy::new();
    let url = format!(
        "https://api.keen.io/3.0/projects/{}/events/{}?api_key={}",
        settings.project_id, event.collection, settings.write_key
    );
    easy.url(&url)?;
    easy.post(true)?;

    // Set content-type
    let mut list = List::new();
    list.append("Content-Type: application/json")?;
    easy.http_headers(list)?;

    // Set body
    easy.post_fields_copy(event.data.as_ref())?;

    // Send request
    easy.perform()?;
    Ok(())
}

#[derive(Debug)]
struct Event {
    collection: String,
    data: String,
}
