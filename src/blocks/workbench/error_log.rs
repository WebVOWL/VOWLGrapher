use super::WorkbenchMenuItems;
use crate::{components::table::Table, errors::ErrorLogContext};
use leptos::either::Either;
use leptos::prelude::*;

pub fn ErrorLog() -> impl IntoView {
    let error_context = expect_context::<ErrorLogContext>();

    view! {
        {move || {
            if error_context.is_empty() {
                Either::Left(
                    view! {
                        <p class="font-sans text-xl antialiased font-normal leading-normal text-blue-gray-900">
                            "No errors"
                        </p>
                    },
                )
            } else {
                Either::Right(

                    view! {
                        <div class="min-w-250 md:min-w-[80vw]">
                            <Table items=error_context.records />
                        </div>
                    },
                )
            }
        }}
    }
}

#[component]
pub fn ErrorMenu() -> impl IntoView {
    view! {
        <WorkbenchMenuItems title="Error Log">
            <ErrorLog />
        </WorkbenchMenuItems>
    }
}
