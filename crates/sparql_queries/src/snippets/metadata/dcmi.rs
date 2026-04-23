pub mod dc {
    //! SPARQL snippets for the [Dublin Core](https://www.dublincore.org/specifications/dublin-core/dcmi-terms/) vocabulary.
    //!
    //! The original fifteen-element Dublin Core namespace.
    pub const CONTRIBUTOR: &str = r"{
            ?id dc:contributor ?target .
            BIND(dc:contributor as ?nodeType)
            }";
    pub const COVERAGE: &str = r"{
            ?id dc:coverage ?target .
            BIND(dc:coverage as ?nodeType)
            }";
    pub const CREATOR: &str = r"{
            ?id dc:creator ?target .
            BIND(dc:creator as ?nodeType)
            }";
    pub const DATE: &str = r"{
            ?id dc:date ?target .
            BIND(dc:date as ?nodeType)
            }";
    pub const DESCRIPTION: &str = r"{
            ?id dc:description ?target .
            BIND(dc:description as ?nodeType)
            }";
    pub const FORMAT: &str = r"{
            ?id dc:format ?target .
            BIND(dc:format as ?nodeType)
            }";
    pub const IDENTIFIER: &str = r"{
            ?id dc:identifier ?target .
            BIND(dc:identifier as ?nodeType)
            }";
    pub const LANGUAGE: &str = r"{
            ?id dc:language ?target .
            BIND(dc:language as ?nodeType)
            }";
    pub const PUBLISHER: &str = r"{
            ?id dc:publisher ?target .
            BIND(dc:publisher as ?nodeType)
            }";
    pub const RELATION: &str = r"{
            ?id dc:relation ?target .
            BIND(dc:relation as ?nodeType)
            }";
    pub const RIGHTS: &str = r"{
            ?id dc:rights ?target .
            BIND(dc:rights as ?nodeType)
            }";
    pub const SOURCE: &str = r"{
            ?id dc:source ?target .
            BIND(dc:source as ?nodeType)
            }";
    pub const SUBJECT: &str = r"{
            ?id dc:subject ?target .
            BIND(dc:subject as ?nodeType)
            }";
    pub const TITLE: &str = r"{
            ?id dc:title ?target .
            BIND(dc:title as ?nodeType)
            }";
    pub const TYPE: &str = r"{
            ?id dc:type ?target .
            BIND(dc:type as ?nodeType)
            }";
}

pub mod dcterms {
    // TODO: Add snippets for remaining
    //! Provides SPARQL snippets for the [Dublin Core terms](https://www.dublincore.org/specifications/dublin-core/dcmi-terms/) vocabulary.
    //!
    //! Extends the Dublin Core namespace. However, Dublin Core is duplicated in this vocabulary.
    //! As a result, there exists both a [dc:date](http://purl.org/dc/elements/1.1/date) with no formal range and a
    //! corresponding [dcterms:date](http://purl.org/dc/terms/date) with a formal range of "literal".
    //! While these distinctions are significant for creators of RDF applications, most users can safely treat the
    //! fifteen parallel properties as equivalent.
    pub const CONTRIBUTOR: &str = r"{
            ?id dcterms:contributor ?target .
            BIND(dcterms:contributor as ?nodeType)
            }";
    pub const COVERAGE: &str = r"{
            ?id dcterms:coverage ?target .
            BIND(dcterms:coverage as ?nodeType)
            }";
    pub const CREATOR: &str = r"{
            ?id dcterms:creator ?target .
            BIND(dcterms:creator as ?nodeType)
            }";
    pub const DATE: &str = r"{
            ?id dcterms:date ?target .
            BIND(dcterms:date as ?nodeType)
            }";
    pub const DESCRIPTION: &str = r"{
            ?id dcterms:description ?target .
            BIND(dcterms:description as ?nodeType)
            }";
    pub const FORMAT: &str = r"{
            ?id dcterms:format ?target .
            BIND(dcterms:format as ?nodeType)
            }";
    pub const IDENTIFIER: &str = r"{
            ?id dcterms:identifier ?target .
            BIND(dcterms:identifier as ?nodeType)
            }";
    pub const LANGUAGE: &str = r"{
            ?id dcterms:language ?target .
            BIND(dcterms:language as ?nodeType)
            }";
    pub const PUBLISHER: &str = r"{
            ?id dcterms:publisher ?target .
            BIND(dcterms:publisher as ?nodeType)
            }";
    pub const RELATION: &str = r"{
            ?id dcterms:relation ?target .
            BIND(dcterms:relation as ?nodeType)
            }";
    pub const RIGHTS: &str = r"{
            ?id dcterms:rights ?target .
            BIND(dcterms:rights as ?nodeType)
            }";
    pub const SOURCE: &str = r"{
            ?id dcterms:source ?target .
            BIND(dcterms:source as ?nodeType)
            }";
    pub const SUBJECT: &str = r"{
            ?id dcterms:subject ?target .
            BIND(dcterms:subject as ?nodeType)
            }";
    pub const TITLE: &str = r"{
            ?id dcterms:title ?target .
            BIND(dcterms:title as ?nodeType)
            }";
    pub const TYPE: &str = r"{
            ?id dcterms:type ?target .
            BIND(dcterms:type as ?nodeType)
            }";
}
