//! [RDF](https://www.w3.org/TR/rdf11-concepts/) vocabulary.
use oxrdf::NamedNodeRef;

/// The class of containers of alternatives.
pub const ALT: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#Alt");
/// The class of unordered containers.
pub const BAG: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#Bag");
// /// The class of language-tagged string literal values with a base direction.
// #[cfg(feature = "rdf-12")]
// pub const DIR_LANG_STRING: NamedNodeRef<'_> =
//     NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#dirLangString");
/// The first item in the subject RDF list.
pub const FIRST: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#first");
/// The class of HTML literal values.
pub const HTML: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#HTML");
// /// The datatype of RDF literals storing JSON content.
// #[cfg(feature = "rdf-12")]
// pub const JSON: NamedNodeRef<'_> =
//     NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON");
pub const LANG_STRING: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString");
/// The class of RDF lists.
pub const LIST: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#List");
/// The empty list.
pub const NIL: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil");
/// The object of the subject RDF statement.
pub const OBJECT: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#object");
/// The predicate of the subject RDF statement.
pub const PREDICATE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate");
/// The class of RDF properties.
pub const PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#Property");
// /// Associate a resource (reifier) with a triple (proposition).
// #[cfg(feature = "rdf-12")]
// pub const REIFIES: NamedNodeRef<'_> =
//     NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies");
/// The rest of the subject RDF list after the first item.
pub const REST: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest");
/// The class of ordered containers.
pub const SEQ: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq");
/// The class of RDF statements.
pub const STATEMENT: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement");
/// The subject of the subject RDF statement.
pub const SUBJECT: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#subject");
/// The subject is an instance of a class.
pub const TYPE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
/// Idiomatic property used for structured values.
pub const VALUE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#value");
/// The class of XML literal values.
pub const XML_LITERAL: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral");
/// The class of plain (i.e. untyped) literal values, as used in RIF and OWL 2.
pub const PLAIN_LITERAL: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#PlainLiteral");
/// A class representing a compound literal.
pub const COMPOUND_LITERAL: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#CompoundLiteral ");
/// The base direction component of a [`COMPOUND_LITERAL`].
pub const DIRECTION: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#direction");
