pub mod dcmi;

pub const COMMENT: &str = r"{
            ?id rdfs:comment ?target .
            BIND(rdfs:comment as ?nodeType)
            }";

pub const IS_DEFINED_BY: &str = r"{
            ?id rdfs:isDefinedBy ?target .
            BIND(rdfs:isDefinedBy as ?nodeType)
            }";

pub const SEE_ALSO: &str = r"{
            ?id rdfs:seeAlso ?target .
            BIND(rdfs:seeAlso as ?nodeType)
            }";

pub const VERSION_INFO: &str = r"{
            ?id owl:versionInfo ?target .
            BIND(owl:versionInfo as ?nodeType)
            }";

pub const VERSION_IRI: &str = r"{
            ?id owl:versionIRI ?target .
            BIND(owl:versionIRI as ?nodeType)
            }";

pub const PRIOR_VERSION: &str = r"{
            ?id owl:priorVersion ?target .
            BIND(owl:priorVersion as ?nodeType)
            }";

pub const INCOMPATIBLE_WITH: &str = r"{
            ?id owl:incompatibleWith ?target .
            BIND(owl:incompatibleWith as ?nodeType)
            }";

pub const BACKWARD_COMPATIBLE_WITH: &str = r"{
            ?id owl:backwardCompatibleWith ?target .
            BIND(owl:backwardCompatibleWith as ?nodeType)
            }";

pub const AXIOM: &str = r"{
            ?id a owl:Axiom .
            BIND(owl:Axiom as ?nodeType)
            }";

pub const ANNOTATED_SOURCE: &str = r"{
            ?id owl:annotatedSource ?target .
            BIND(owl:annotatedSource as ?nodeType)
            }";

pub const ANNOTATED_PROPERTY: &str = r"{
            ?id owl:annotatedProperty ?target .
            BIND(owl:annotatedProperty as ?nodeType)
            }";

pub const ANNOTATED_TARGET: &str = r"{
            ?id owl:annotatedTarget ?target .
            BIND(owl:annotatedTarget as ?nodeType)
            }";
