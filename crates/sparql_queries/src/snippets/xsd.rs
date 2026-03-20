use grapher::prelude::XSDNode;

use crate::snippets::SparqlSnippet;

impl SparqlSnippet for XSDNode {
    fn snippet(self) -> &'static str {
        match self {
            XSDNode::Int => {
                r#"{
                ?id a xsd:int .
                BIND(xsd:int AS ?nodeType)
                }"#
            }
            XSDNode::Integer => {
                r#"{
                ?id a xsd:integer .
                BIND(xsd:integer AS ?nodeType)
                }"#
            }
            XSDNode::NegativeInteger => {
                r#"{
                ?id a xsd:negativeInteger .
                BIND(xsd:negativeInteger AS ?nodeType)
                }"#
            }
            XSDNode::NonNegativeInteger => {
                r#"{
                ?id a xsd:nonNegativeInteger .
                BIND(xsd:nonNegativeInteger AS ?nodeType)
                }"#
            }
            XSDNode::NonPositiveInteger => {
                r#"{
                ?id a xsd:nonPositiveInteger .
                BIND(xsd:nonPositiveInteger AS ?nodeType)
                }"#
            }
            XSDNode::PositiveInteger => {
                r#"{
                ?id a xsd:positiveInteger .
                BIND(xsd:positiveInteger AS ?nodeType)
                }"#
            }
            XSDNode::UnsignedInt => {
                r#"{
                ?id a xsd:unsignedInt .
                BIND(xsd:unsignedInt AS ?nodeType)
                }"#
            }
            XSDNode::UnsignedLong => {
                r#"{
                ?id a xsd:unsignedLong .
                BIND(xsd:unsignedLong AS ?nodeType)
                }"#
            }
            XSDNode::UnsignedShort => {
                r#"{
                ?id a xsd:unsignedShort .
                BIND(xsd:unsignedShort AS ?nodeType)
                }"#
            }
            XSDNode::Decimal => {
                r#"{
                ?id a xsd:decimal .
                BIND(xsd:decimal AS ?nodeType)
                }"#
            }
            XSDNode::Float => {
                r#"{
                ?id a xsd:float .
                BIND(xsd:float AS ?nodeType)
                }"#
            }
            XSDNode::Double => {
                r#"{
                ?id a xsd:double .
                BIND(xsd:double AS ?nodeType)
                }"#
            }
            XSDNode::Short => {
                r#"{
                ?id a xsd:short .
                BIND(xsd:short AS ?nodeType)
                }"#
            }
            XSDNode::Long => {
                r#"{
                ?id a xsd:long .
                BIND(xsd:long AS ?nodeType)
                }"#
            }
            XSDNode::Date => {
                r#"{
                ?id a xsd:date .
                BIND(xsd:date AS ?nodeType)
                }"#
            }
            XSDNode::DataTime => {
                r#"{
                ?id a xsd:dateTime .
                BIND(xsd:dateTime AS ?nodeType)
                }"#
            }
            XSDNode::DateTimeStamp => {
                r#"{
                ?id a xsd:dateTimeStamp .
                BIND(xsd:dateTimeStamp AS ?nodeType)
                }"#
            }
            XSDNode::Duration => {
                r#"{
                ?id a xsd:duration .
                BIND(xsd:duration AS ?nodeType)
                }"#
            }
            XSDNode::GDay => {
                r#"{
                ?id a xsd:gDay .
                BIND(xsd:gDay AS ?nodeType)
                }"#
            }
            XSDNode::GMonth => {
                r#"{
                ?id a xsd:gMonth .
                BIND(xsd:gMonth AS ?nodeType)
                }"#
            }
            XSDNode::GMonthDay => {
                r#"{
                ?id a xsd:gMonthDay .
                BIND(xsd:gMonthDay AS ?nodeType)
                }"#
            }
            XSDNode::GYear => {
                r#"{
                ?id a xsd:gYear .
                BIND(xsd:gYear AS ?nodeType)
                }"#
            }
            XSDNode::GYearMonth => {
                r#"{
                ?id a xsd:gYearMonth .
                BIND(xsd:gYearMonth AS ?nodeType)
                }"#
            }
            XSDNode::Time => {
                r#"{
                ?id a xsd:time .
                BIND(xsd:time AS ?nodeType)
                }"#
            }
            XSDNode::AnyURI => {
                r#"{
                ?id a xsd:anyURI .
                BIND(xsd:anyURI AS ?nodeType)
                }"#
            }
            XSDNode::ID => {
                r#"{
                ?id a xsd:ID .
                BIND(xsd:ID AS ?nodeType)
                }"#
            }
            XSDNode::Idref => {
                r#"{
                ?id a xsd:IDREF .
                BIND(xsd:IDREF AS ?nodeType)
                }"#
            }
            XSDNode::Language => {
                r#"{
                ?id a xsd:language .
                BIND(xsd:language AS ?nodeType)
                }"#
            }
            XSDNode::Nmtoken => {
                r#"{
                ?id a xsd:NMTOKEN .
                BIND(xsd:NMTOKEN AS ?nodeType)
                }"#
            }
            XSDNode::Name => {
                r#"{
                ?id a xsd:Name .
                BIND(xsd:Name AS ?nodeType)
                }"#
            }
            XSDNode::NCName => {
                r#"{
                ?id a xsd:NCName .
                BIND(xsd:NCName AS ?nodeType)
                }"#
            }
            XSDNode::QName => {
                r#"{
                ?id a xsd:QName .
                BIND(xsd:QName AS ?nodeType)
                }"#
            }
            XSDNode::String => {
                r#"{
                ?id a xsd:string .
                BIND(xsd:string AS ?nodeType)
                }"#
            }
            XSDNode::Token => {
                r#"{
                ?id a xsd:token .
                BIND(xsd:token AS ?nodeType)
                }"#
            }
            XSDNode::NormalizedString => {
                r#"{
                ?id a xsd:normalizedString .
                BIND(xsd:normalizedString AS ?nodeType)
                }"#
            }
            XSDNode::Notation => {
                r#"{
                ?id a xsd:NOTATION .
                BIND(xsd:NOTATION AS ?nodeType)
                }"#
            }
            XSDNode::AnySimpleType => {
                r#"{
                ?id a xsd:anySimpleType .
                BIND(xsd:anySimpleType AS ?nodeType)
                }"#
            }
            XSDNode::Base64Binary => {
                r#"{
                ?id a xsd:base64Binary .
                BIND(xsd:base64Binary AS ?nodeType)
                }"#
            }
            XSDNode::Boolean => {
                r#"{
                ?id a xsd:boolean .
                BIND(xsd:boolean AS ?nodeType)
                }"#
            }
            XSDNode::Entity => {
                r#"{
                ?id a xsd:ENTITY .
                BIND(xsd:ENTITY AS ?nodeType)
                }"#
            }
            XSDNode::UnsignedByte => {
                r#"{
                ?id a xsd:unsignedByte .
                BIND(xsd:unsignedByte AS ?nodeType)
                }"#
            }
            XSDNode::Byte => {
                r#"{
                ?id a xsd:byte .
                BIND(xsd:byte AS ?nodeType)
                }"#
            }
            XSDNode::HexBinary => {
                r#"{
                ?id a xsd:hexBinary .
                BIND(xsd:hexBinary AS ?nodeType)
                }"#
            }
        }
    }
}
