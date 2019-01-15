use std::ffi::CStr;
use std::os::raw::{c_int, c_char};
use keenio::{KeenClient, ProjectSettings};

#[no_mangle]
pub extern "C" fn Keen_New(c_project_key: *const c_char, c_api_key: *const c_char) -> *mut KeenClient {

    let project_key_opt = unsafe {
        CStr::from_ptr(c_project_key).to_str().ok()
    };

    let api_key_opt = unsafe {
        CStr::from_ptr(c_api_key).to_str().ok()
    };

    if let (Some(project_key), Some(api_key)) = (project_key_opt, api_key_opt) {
        let setting = ProjectSettings::new(project_key, api_key);
        Box::into_raw(Box::new(KeenClient::new(setting, None))) as *mut KeenClient
    }
    else {
        0 as *mut KeenClient
    }
}

#[no_mangle]
pub extern "C" fn Keen_Start(keen_handle: *mut KeenClient) {
    let keen = unsafe { &mut *keen_handle };
    keen.start();
}

#[no_mangle]
pub extern "C" fn Keen_Stop(keen_handle: *mut KeenClient) {
    let keen = unsafe { &mut *keen_handle };
    keen.stop();
}

#[no_mangle]
pub extern "C" fn Keen_AddEvent(keen_handle: *mut KeenClient, c_collection: *const c_char, c_event: *const c_char) -> c_int {
    let keen = unsafe { &mut *keen_handle };

    let collection_opt = unsafe {
        CStr::from_ptr(c_collection).to_str().ok()
    };

    let event_opt = unsafe {
        CStr::from_ptr(c_event).to_str().ok()
    };

    if let (Some(collection), Some(event)) = (collection_opt, event_opt) {
        match serde_json::from_str(event) {
            Ok(json_event) => {
                match keen.add_event(collection, &json_event) {
                    Ok(_) => return 1,
                    Err(e) => {
                        error!("Event can't be added: {}", e);
                        return -1;
                    }
                }
            }
            Err(e) => {
                error!("Can't build a json from string \"{}\". Error: {}", event, e);
            }
        }
    }

    return -1;
}

