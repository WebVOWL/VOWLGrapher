pub const COMMENT: &str = r#"{
            # Find comments for elements.
            ?id rdfs:comment ?target .
            BIND(rdfs:comment as ?nodeType)
            }"#;

pub const IS_DEFINED_BY: &str = r#"{
            # Find isDefinedBy for elements.
            ?id rdfs:isDefinedBy ?target .
            BIND(rdfs:isDefinedBy as ?nodeType)
            }"#;

pub const SEE_ALSO: &str = r#"{
            # Find seeAlso for elements.
            ?id rdfs:seeAlso ?target .
            BIND(rdfs:seeAlso as ?nodeType)
            }"#;

pub const VERSION_INFO: &str = r#"{
            # Find versionInfo for elements.
            ?id owl:versionInfo ?target .
            BIND(owl:versionInfo as ?nodeType)
            }"#;

pub const VERSION_IRI: &str = r#"{
            # Find versionIRI for elements.
            ?id owl:versionIRI ?target .
            BIND(owl:versionIRI as ?nodeType)
            }"#;

pub const PRIOR_VERSION: &str = r#"{
            # Find priorVersion for elements.
            ?id owl:priorVersion ?target .
            BIND(owl:priorVersion as ?nodeType)
            }"#;

pub const INCOMPATIBLE_WITH: &str = r#"{
            # Find incompatibleWith for elements.
            ?id owl:incompatibleWith ?target .
            BIND(owl:incompatibleWith as ?nodeType)
            }"#;

pub const BACKWARD_COMPATIBLE_WITH: &str = r#"{
            # Find backwardCompatibleWith for elements.
            ?id owl:backwardCompatibleWith ?target .
            BIND(owl:backwardCompatibleWith as ?nodeType)
            }"#;
