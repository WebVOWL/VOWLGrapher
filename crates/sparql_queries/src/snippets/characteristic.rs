use grapher::prelude::Characteristic;

use crate::snippets::SparqlSnippet;

impl SparqlSnippet for Characteristic {
    fn snippet(self) -> &'static str {
        match self {
            Characteristic::TransitiveProperty => {
                r#"{
                ?id a owl:TransitiveProperty
                BIND(owl:TransitiveProperty AS ?nodeType)
            }"#
            }
            Characteristic::FunctionalProperty => {
                r#"{
                ?id a owl:FunctionalProperty
                BIND(owl:FunctionalProperty AS ?nodeType)
            }"#
            }
            Characteristic::InverseFunctionalProperty => {
                r#"{
                ?id a owl:InverseFunctionalProperty
                BIND(owl:InverseFunctionalProperty AS ?nodeType)
            }"#
            }
            Characteristic::ReflexiveProperty => {
                r#"{
                ?id a owl:ReflexiveProperty
                BIND(owl:ReflexiveProperty AS ?nodeType)
            }"#
            }
            Characteristic::IrreflexiveProperty => {
                r#"{
                ?id a owl:IrreflexiveProperty
                BIND(owl:IrreflexiveProperty AS ?nodeType)
            }"#
            }
            Characteristic::SymmetricProperty => {
                r#"{
                ?id a owl:SymmetricProperty
                BIND(owl:SymmetricProperty AS ?nodeType)
            }"#
            }
            Characteristic::AsymmetricProperty => {
                r#"{
                ?id a owl:AsymmetricProperty
                BIND(owl:AsymmetricProperty AS ?nodeType)
            }"#
            }
            Characteristic::HasKey => {
                r#"{
                ?id a owl:hasKey
                BIND(owl:hasKey AS ?nodeType)
            }"#
            }
        }
    }
}
