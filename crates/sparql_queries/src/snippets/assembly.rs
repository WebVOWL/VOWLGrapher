pub const TYPE: &str = r"{
        ?id a ?target
        BIND(rdf:type as ?nodeType)
    }";

pub const ONTOLOGY: &str = r"{
        ?base a owl:Ontology
    }";

pub const LABEL: &str = r"{
        ?nodeType a rdfs:label .
        ?id ?nodeType ?target
    }";

pub const DOMAIN: &str = r"{
        ?id rdfs:domain ?t0
    }";

pub const RANGE: &str = r"{
        ?id rdfs:range ?t2
    }";
