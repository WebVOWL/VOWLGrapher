pub const TYPE: &str = r"{
        ?s a ?o
    }";

pub const ONTOLOGY: &str = r"{
        ?ont a owl:Ontology
    }";

pub const OWL_DEPRECATED: &str = r"{
        ?s owl:deprecated ?o
    }";

pub const LABEL: &str = r"{
        ?s rdfs:label ?o
    }";

pub const DOMAIN_RANGES: &str = r"{
        ?s rdfs:domain ?o
        ?s rdfs:range ?o
    }";
