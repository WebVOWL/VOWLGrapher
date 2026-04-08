use super::WorkbenchMenuItems;
use crate::components::user_input::range_select::Slider;
use crate::errors::{ClientErrorKind, ErrorLogContext};
use grapher::prelude::*;
use leptos::prelude::*;

#[component]
pub fn SimulatorSettings() -> impl IntoView {
    let repel_force: RwSignal<f64> = RwSignal::new(RepelForce::default().0.into());
    let spring_stiffness = RwSignal::new(SpringStiffness::default().0.into());
    let spring_neutral_length = RwSignal::new(SpringNeutralLength::default().0.into());
    let gravity_force = RwSignal::new(GravityForce::default().0.into());
    let delta_time = RwSignal::new(DeltaTime::default().0.into());
    let damping = RwSignal::new(Damping::default().0.into());
    let quadtree_theta = RwSignal::new(QuadTreeTheta::default().0.into());
    let freeze_thresh = RwSignal::new(FreezeThreshold::default().0.into());

    #[expect(
        clippy::cast_possible_truncation,
        reason = "These values should never be big enough to be truncated"
    )]
    Effect::new(move |_| {
        let messages = [
            SimulatorEvent::RepelForceUpdated(repel_force.get() as f32),
            SimulatorEvent::SpringStiffnessUpdated(spring_stiffness.get() as f32),
            SimulatorEvent::SpringNeutralLengthUpdated(spring_neutral_length.get() as f32),
            SimulatorEvent::GravityForceUpdated(gravity_force.get() as f32),
            SimulatorEvent::DeltaTimeUpdated(delta_time.get() as f32),
            SimulatorEvent::DampingUpdated(damping.get() as f32),
            SimulatorEvent::QuadTreeThetaUpdated(quadtree_theta.get() as f32),
            SimulatorEvent::FreezeThresholdUpdated(freeze_thresh.get() as f32),
        ];
        for msg in messages {
            if let Err(e) = EVENT_DISPATCHER.sim_write_chan.send(msg) {
                let error_context = expect_context::<ErrorLogContext>();
                error_context.push(ClientErrorKind::EventHandlingError(e.to_string()).into());
            }
        }
    });

    view! {
        <fieldset>
            <legend>"Graph Simulation"</legend>
            <div class="flex flex-col content-around m-4 size-fit">
  <Slider
                        label="Node Distance"
                        value=repel_force
                        min=(-10e8).to_string()
                        max=(-10e6).to_string()
                        step=(-10e3).to_string()
                    ></Slider>
                {// </ToolTip<f64>>

 <Slider
                        label="Edge Stiffness"
                        value=spring_stiffness
                        min="50.0"
                        max="200.0"
                        step="10.0"
                    ></Slider>
              {  // </ToolTip<f64>>
              }
                <Slider
                    label="Edge Length"
                    value=spring_neutral_length
                    min="20.0"
                    max="200.0"
                    step="10.0"
                ></Slider>

                <Slider
                    label="Center Gravity Strength"
                    value=gravity_force
                    min="5.0"
                    max="40.0"
                    step="5.0"
                ></Slider>

                <Slider
                    label="Simulation Speed"
                    value=delta_time
                    min="0.001"
                    max="0.01"
                    step="0.0005"
                ></Slider>

                <Slider
                    label="Damping"
                    value=damping
                    min="0.1"
                    max="0.9"
                    step="0.1"
                ></Slider>

                <Slider
                    label="Simulation Accuracy"
                    value=quadtree_theta
                    min="0.1"
                    max="1.0"
                    step="0.1"
                ></Slider>

                <Slider
                    label="Freeze Threshold"
                    value=freeze_thresh
                    min="5.0"
                    max="40.0"
                    step="5.0"
                ></Slider>
            </div>
        </fieldset>
    }
}

#[component]
pub fn OptionsMenu() -> impl IntoView {
    view! {
        <WorkbenchMenuItems title="Settings">
            <SimulatorSettings />
        </WorkbenchMenuItems>
    }
}
