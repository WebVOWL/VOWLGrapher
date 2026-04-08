//! Utility code used by the entire VOWL-R workspace

mod datatypes;
mod error_handler;
mod layout;
mod time;

pub mod prelude {
    //! Export all types of the crate.
    pub use crate::datatypes::DataType;
    pub use crate::error_handler::{ErrorRecord, ErrorSeverity, ErrorType, VOWLRError};
    pub use crate::layout::TableHTML;
    pub use crate::time::get_timestamp;

    #[cfg(feature = "ssr")]
    pub use crate::user_session::manage_user_id;
}

#[cfg(feature = "ssr")]
mod user_session {
    use actix_session::Session;
    use leptos::server_fn::ServerFnError;
    use leptos_actix::extract;
    use uuid::Uuid;

    /// # Errors
    /// Throws an error if fails to get session
    pub async fn manage_user_id() -> Result<String, ServerFnError> {
        let user_session = extract::<Session>()
            .await
            .map_err(|e| ServerFnError::new(format!("Failed to extract session: {e}")))?;

        if let Ok(Some(user_id)) = user_session.get::<String>("user_id") {
            return Ok(user_id);
        }

        let new_user_id = Uuid::new_v4().to_string();

        user_session
            .insert("user_id", &new_user_id)
            .map_err(|e| ServerFnError::new(format!("Failed to save session: {e}")))?;

        Ok(new_user_id)
    }
}
