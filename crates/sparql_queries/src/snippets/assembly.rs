pub const TYPE: &str = r"{
        ?s a ?o
        BIND(rdf:type as ?p)
    }";

pub const ONTOLOGY: &str = r"{
        ?base a owl:Ontology
    }";

pub const LABEL: &str = r"{
        ?p a rdfs:label .
        ?s ?p ?o
    }";

pub const DOMAIN: &str = r"{
        ?p a rdfs:domain .
        ?t1 ?p ?t0
    }";

pub const RANGE: &str = r"{
        ?p a rdfs:range .
        ?t1 ?p ?t2
    }";
