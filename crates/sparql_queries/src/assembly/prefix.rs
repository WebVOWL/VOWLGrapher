use leptos::either::Either;
use leptos::prelude::*;

// TODO: Remove when automatic prefix fetching is implemented.
pub const DEFAULT_PREFIXES: [&str; 9] = [
    "example: <http://www.example.com/iri#>",
    "vowlgrapher: <https://purl.org/vowlgrapher>",
    "owl: <http://www.w3.org/2002/07/owl#>",
    "rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
    "rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
    "xsd: <http://www.w3.org/2001/XMLSchema#>",
    "xml: <http://www.w3.org/XML/1998/namespace>",
    "dc: <http://purl.org/dc/elements/1.1/>",
    "dcterms: <http://purl.org/dc/terms/>",
];

/// Returns the [`DEFAULT_PREFIXES`] formatted into a HTML table.
#[component]
pub fn DefaultPrefixTable() -> impl IntoView {
    let prefixes = move || {
        DEFAULT_PREFIXES
            .iter()
            .filter(|full_prefix| {
                !(full_prefix.contains("example:") || full_prefix.contains("vowlgrapher:"))
            })
            .map(|full_prefix| {
                if let Some((prefix, iri)) = full_prefix.split_once(':') {
                    Either::Left(view! {
                        <tr>
                            <td>{prefix}</td>
                            <td>{iri.trim()}</td>
                        </tr>
                    })
                } else {
                    Either::Right(())
                }
            })
            .collect_view()
    };

    view! {
        <table class="w-full text-left border-collapse text-[10px]">
            <thead class="bg-gray-100 border-b border-gray-200">
                <tr>
                    <th class="p-1 font-semibold">"Prefix"</th>
                    <th class="p-1 font-semibold">"Namespace IRI"</th>
                </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-100">
                {prefixes()}
            </tbody>
        </table>
    }
}
