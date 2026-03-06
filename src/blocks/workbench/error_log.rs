use super::WorkbenchMenuItems;
use crate::components::table::Table;
use leptos::prelude::*;
use vowlr_util::prelude::{ErrorRecord, VOWLRError};

#[derive(Debug, Copy, Clone)]
pub struct ErrorLogContext {
    pub records: RwSignal<Vec<ErrorRecord>>,
}

impl ErrorLogContext {
    pub fn new(records: Vec<ErrorRecord>) -> Self {
        Self {
            records: RwSignal::new(records),
        }
    }

    /// Appends an element to the back of a collection.
    ///
    /// # Panics
    /// Panics if you update the value of the signal of `self` before this function returns.
    pub fn push(&self, record: ErrorRecord) {
        self.records.update(|records| records.push(record));
    }

    /// Extends a collection with the contents of an iterator.
    ///
    /// # Panics
    /// Panics if you update the value of the signal of `self` before this function returns.
    pub fn extend(&self, records: Vec<ErrorRecord>) {
        self.records.update(|records_| records_.extend(records));
    }

    /// Clears the collection, removing all values.
    ///
    /// Note that this method has no effect on the allocated capacity of the vector.
    ///
    /// # Panics
    /// Panics if you update the value of the signal of `self` before this function returns.
    pub fn clear(&self) {
        // self.records.update(|records| records.clear());
        self.records.update(std::vec::Vec::clear);
    }

    /// Returns the number of elements in the collection, also referred to as its 'length'
    ///
    /// # Panics
    /// Panics if you try to access the signal of `self` when it has been disposed.
    pub fn len(&self) -> usize {
        self.records.read().len()
    }
}

impl Default for ErrorLogContext {
    fn default() -> Self {
        Self {
            records: RwSignal::new(Vec::new()),
        }
    }
}

impl From<VOWLRError> for ErrorLogContext {
    fn from(value: VOWLRError) -> Self {
        Self::new(value.records)
    }
}

pub fn ErrorLog() -> impl IntoView {
    let error_context = expect_context::<ErrorLogContext>();
    view! {
        <div class="min-w-250 md:min-w-[80vw]">
            <Table items=error_context.records />
        </div>
    }
    // view! {
    //     <table>
    //         <TableContent rows=error_context scroll_container="html" />
    //     </table>
    // }

    // view! {
    //     {move || {
    //         let records = error_context.records.read();
    //         view! {
    //             <div class="overflow-y-auto p-2 mt-2 bg-red-50 rounded border border-red-200 max-h-130">
    //                 {if records.is_empty() {
    //                     view! { <p class="text-xs text-gray-600">"No errors"</p> }
    //                         .into_any()
    //                 } else {
    //                     view! {
    //                         <ul class="space-y-1 text-xs text-red-700">
    //                             {records
    //                                 .iter()
    //                                 .map(|record| {
    //                                     view! {
    //                                         <li class="font-mono whitespace-pre-wrap">"• " {record.to_string()}</li>
    //                                     }
    //                                 })
    //                                 .collect_view()}

    //                         </ul>
    //                     }
    //                         .into_any()
    //                 }}
    //             </div>
    //         }
    //             .into_any()
    //     }}
    // }
}

#[component]
pub fn ErrorMenu() -> impl IntoView {
    view! {
        <WorkbenchMenuItems title="Error Log">
            <ErrorLog />
        </WorkbenchMenuItems>
    }
}
