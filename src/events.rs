use grapher::prelude::{EVENT_DISPATCHER, GUIEvent};

pub struct EventHandler;

impl EventHandler {
    pub fn handle_event() {
        while let Ok(event) = EVENT_DISPATCHER.gui_read_chan.recv() {
            match event {
                GUIEvent::ShowMetadata(idx) => {
                    // call relevant signal
                }
                GUIEvent::HideMetadata() => {
                    // call relevant signal
                }
            }
        }
    }
}
