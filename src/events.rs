use crate::errors::{ClientErrorKind, ErrorLogContext};
use grapher::prelude::{EVENT_DISPATCHER, GUIEvent};
use leptos::prelude::{RwSignal, Update, expect_context};

#[derive(Clone, Copy)]
pub struct EventContext {
    pub show_metadata: RwSignal<Option<usize>>,
}

impl Default for EventContext {
    fn default() -> Self {
        Self {
            show_metadata: RwSignal::new(None),
        }
    }
}

impl EventContext {
    pub fn new() -> Self {
        Self::default()
    }
}

pub struct EventHandler;

impl EventHandler {
    pub async fn handle_event() {
        let EventContext { show_metadata } = expect_context::<EventContext>();
        loop {
            match EVENT_DISPATCHER.gui_read_chan.recv().await {
                Ok(event) => match event {
                    GUIEvent::ShowMetadata(idx) => {
                        show_metadata.update(|v| *v = idx);
                    }
                },
                Err(e) => {
                    let error_context = expect_context::<ErrorLogContext>();
                    error_context.push(ClientErrorKind::EventHandlingError(e.to_string()).into());
                }
            }
        }
    }
}
