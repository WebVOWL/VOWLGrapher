use std::time::SystemTime;

use time::OffsetDateTime;
use time::macros::format_description;

/// Returns a formatted timetamp corresponding to "now".
pub fn get_timestamp() -> String {
    // [year]-[month repr:short]-[day] [hour]:[minute]:[second]
    let fmt_desc = format_description!("[hour]:[minute]:[second]\n");
    let t: OffsetDateTime = SystemTime::now().into();
    match t.format(&fmt_desc) {
        Ok(e) => e,
        Err(e) => e.to_string(),
    }
}
