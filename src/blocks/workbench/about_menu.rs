use super::WorkbenchMenuItems;
use crate::components::icon::Icon;
use leptos::prelude::*;

pub fn VOWLGrapherDesc() -> impl IntoView {
    let description = include_str!("../../../public/about_description.txt");
    view! {
        <p>{description}</p>
        <div class="flex gap-4">
            <a
                class="text-2xl transition"
                href="https://github.com/WebVOWL/VOWLGrapher"
            >
                <Icon icon=icondata::AiGithubFilled />
            </a>
            <a
                class="text-2xl transition"
                href="mailto:cs-25-dat-7-03@student.aau.dk"
            >
                <Icon icon=icondata::IoMail />
            // TODO - Insert the email we can be contacted on.
            // FIXME: Insert valid email that will last at least a year from now
            </a>
        </div>
    }
}

pub fn Version() -> impl IntoView {
    let version = env!("CARGO_PKG_VERSION");
    view! {
        <p>
            <b>{format!("Version {version}")}</b>
        </p>
    }
}

#[component]
pub fn AboutMenu() -> impl IntoView {
    view! {
        <WorkbenchMenuItems title="About">
            <Version />
            <VOWLGrapherDesc />
        </WorkbenchMenuItems>
    }
}
