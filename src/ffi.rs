use std::ffi::CStr;
use std::os::raw::{c_int, c_char, c_ulonglong};
use keenio::{Error, KeenClient, ProjectSettings};
use std::time::Duration;
use std::ptr;

#[no_mangle]
pub extern "C" fn Keen_New(c_custom_domain_url: *const c_char, c_project_key: *const c_char, c_api_key: *const c_char, send_interval: c_ulonglong) -> *mut KeenClient {

    let custom_domain_url_opt = if c_custom_domain_url != ptr::null() {
        unsafe {CStr::from_ptr(c_custom_domain_url).to_str().ok()}
    }
    else {
        None
    };

    let project_key_opt = unsafe {
        CStr::from_ptr(c_project_key).to_str().ok()
    };

    let api_key_opt = unsafe {
        CStr::from_ptr(c_api_key).to_str().ok()
    };

    if let (Some(project_key), Some(api_key)) = (project_key_opt, api_key_opt) {
        let setting = ProjectSettings::new(custom_domain_url_opt.map_or(None, |domain_name| Some(domain_name.to_string())), project_key, api_key);
        Box::into_raw(Box::new(KeenClient::new(setting, Some(Duration::from_millis(send_interval))))) as *mut KeenClient
    }
    else {
        0 as *mut KeenClient
    }
}

#[no_mangle]
pub extern "C" fn Keen_Free(keen_handle: *mut KeenClient) {
    // Will be deleted when the keen will go out of scope
    let _keen = unsafe { Box::from_raw(keen_handle) };
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
pub extern "C" fn Keen_Flush(keen_handle: *mut KeenClient, wait: c_int) -> c_int {
    let keen = unsafe { &mut *keen_handle };
    let wait_flush = wait != 0;
    if keen.flush(wait_flush).is_ok() {
        1
    }
    else {
        -1
    }
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
                    Err(Error::NotStarted) => {
                        trace!("Events can't be sent: {}", Error::NotStarted);
                        return -1;
                    },
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

