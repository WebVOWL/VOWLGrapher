use std::time::SystemTime;

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
use std::time::{Duration, UNIX_EPOCH};

use time::OffsetDateTime;
use time::macros::format_description;

/// Returns a formatted timetamp corresponding to "now".
pub fn get_timestamp() -> String {
    // [year]-[month repr:short]-[day] [hour]:[minute]:[second]
    let fmt_desc = format_description!("[hour]:[minute]:[second]\n");

    #[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
    let t: OffsetDateTime = SystemTime::now().into();

    #[cfg(all(target_family = "wasm", target_os = "unknown"))]
    let t: OffsetDateTime = {
        match web_sys::window() {
            Some(window) => match window.performance() {
                Some(performance) => perf_to_system(performance.now()).into(),
                None => return "Failed to get performance".to_string(),
            },
            None => return "Failed to get window".to_string(),
        }
    };

    match t.format(&fmt_desc) {
        Ok(f) => f,
        Err(e) => e.to_string(),
    }
}

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
fn perf_to_system(amt: f64) -> SystemTime {
    let secs = (amt as u64) / 1_000;
    let nanos = (((amt as u64) % 1_000) as u32) * 1_000_000;
    UNIX_EPOCH + Duration::new(secs, nanos)
}
