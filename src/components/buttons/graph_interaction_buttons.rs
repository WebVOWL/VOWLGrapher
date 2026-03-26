use crate::components::icon::Icon;
use crate::errors::ClientErrorKind;
use crate::errors::ErrorLogContext;
use grapher::prelude::EVENT_DISPATCHER;
use grapher::prelude::RenderEvent;
use leptos::prelude::*;

#[component]
pub fn PauseButton() -> impl IntoView {
    let paused = RwSignal::new(false);

    let toggle_pause = move |_| {
        let currently_paused = paused.get();
        if currently_paused {
            if let Err(e) = EVENT_DISPATCHER.rend_write_chan.send(RenderEvent::Resumed) {
                let error_context = expect_context::<ErrorLogContext>();
                error_context.push(ClientErrorKind::EventHandlingError(e.to_string()).into());
            }
            paused.set(false);
        } else {
            if let Err(e) = EVENT_DISPATCHER.rend_write_chan.send(RenderEvent::Paused) {
                let error_context = expect_context::<ErrorLogContext>();
                error_context.push(ClientErrorKind::EventHandlingError(e.to_string()).into());
            }
            paused.set(true);
        }
    };

    view! {
        <button
            class="flex justify-center items-center text-black bg-white border border-black transition-colors cursor-pointer w-[35px] h-[35px] hover:bg-[#dd9900]"
            title=move || {
                if paused.get() {
                    "Resume the graph simulation"
                } else {
                    "Pause the graph simulation"
                }
            }
            on:click=toggle_pause
        >
            {move || {
                if paused.get() {
                    view! { <Icon icon=icondata::AiPlayCircleOutlined /> }
                } else {
                    view! { <Icon icon=icondata::AiPauseCircleOutlined /> }
                }
            }}
        </button>
    }
}

#[component]
pub fn ResetButton() -> impl IntoView {
    view! {
        <button
            class="flex justify-center items-center text-black bg-white border border-black transition-colors cursor-pointer w-[35px] h-[35px] hover:bg-[#dd9900]"
            title="Reset the graph position"
        >
            <Icon icon=icondata::AiReloadOutlined />
        </button>
    }
}

#[component]
pub fn ZoomInButton() -> impl IntoView {
    let zoom_in = move |_| {
        if let Err(e) = EVENT_DISPATCHER
            .rend_write_chan
            .send(RenderEvent::Zoomed(20.0))
        {
            let error_context = expect_context::<ErrorLogContext>();
            error_context.push(ClientErrorKind::EventHandlingError(e.to_string()).into());
        }
    };

    view! {
        <button
            class="flex justify-center items-center text-black bg-white border border-black transition-colors cursor-pointer w-[35px] h-[35px] hover:bg-[#dd9900]"
            title="Zoom in on the graph"
            on:click=zoom_in
        >
            <Icon icon=icondata::AiZoomInOutlined />
        </button>
    }
}

#[component]
pub fn ZoomOutButton() -> impl IntoView {
    let zoom_out = move |_| {
        if let Err(e) = EVENT_DISPATCHER
            .rend_write_chan
            .send(RenderEvent::Zoomed(-20.0))
        {
            let error_context = expect_context::<ErrorLogContext>();
            error_context.push(ClientErrorKind::EventHandlingError(e.to_string()).into());
        }
    };

    view! {
        <button
            class="flex justify-center items-center text-black bg-white border border-black transition-colors cursor-pointer w-[35px] h-[35px] hover:bg-[#dd9900]"
            title="Zoom out on the graph"
            on:click=zoom_out
        >
            <Icon icon=icondata::AiZoomOutOutlined />
        </button>
    }
}

#[component]
pub fn CenterGraphButton() -> impl IntoView {
    let center = move |_| {
        if let Err(e) = EVENT_DISPATCHER
            .rend_write_chan
            .send(RenderEvent::CenterGraph)
        {
            let error_context = expect_context::<ErrorLogContext>();
            error_context.push(ClientErrorKind::EventHandlingError(e.to_string()).into());
        }
    };

    view! {
        <button
            class="flex justify-center items-center text-black bg-white border border-black transition-colors cursor-pointer w-[35px] h-[35px] hover:bg-[#dd9900]"
            title="Center the graph"
            on:click=center
        >
            <Icon icon=icondata::MdiImageFilterCenterFocus />
        </button>
    }
}

#[component]
pub fn GraphInteractionButtons(#[prop(into)] is_sidebar_open: RwSignal<bool>) -> impl IntoView {
    view! {
        <div
            class="flex absolute gap-1 duration-500 ease-in-out bottom-[1%] transition-[right]"
            class=("right-[23%]", move || is_sidebar_open.get())
            class=("right-[1%]", move || !is_sidebar_open.get())
        >
            <div class="flex flex-col gap-1 justify-end">
                <ZoomInButton />
                <ZoomOutButton />
            </div>
            <div class="flex flex-col gap-1">
                <CenterGraphButton />
                <PauseButton />
                <ResetButton />
            </div>
        </div>
    }
}
