//! [OWL2](https://www.w3.org/TR/owl2-syntax/) vocabulary.
use oxrdf::NamedNodeRef;

/// The class of collections of pairwise different individuals.
pub const ALL_DIFFERENT: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#AllDifferent");
/// The class of collections of pairwise disjoint classes.
pub const ALL_DISJOINT_CLASSES: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#AllDisjointClasses");
/// The class of collections of pairwise disjoint properties.
pub const ALL_DISJOINT_PROPERTIES: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#AllDisjointProperties");
/// The class of annotated annotations for which the RDF serialization consists of an annotated subject, predicate and object.
pub const ANNOTATION: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#Annotation");
/// The class of annotation properties.
pub const ANNOTATION_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#AnnotationProperty");
/// The class of asymmetric properties.
pub const ASYMMETRIC_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#AsymmetricProperty");
/// The class of annotated axioms for which the RDF serialization consists of an annotated subject, predicate and object.
pub const AXIOM: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#Axiom");
/// The class of OWL classes.
pub const CLASS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#Class");
/// NOTE: The use of the IRI owl:DataRange has been deprecated as of OWL 2. The IRI rdfs:Datatype SHOULD be used instead.
///
/// The class of OWL data ranges, which are special kinds of datatypes.
pub const DATA_RANGE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#DataRange");
/// The class of data properties.
pub const DATATYPE_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#DatatypeProperty");
/// The class of deprecated classes.
pub const DEPRECATED_CLASS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#DeprecatedClass");
/// The class of deprecated properties.
pub const DEPRECATED_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#DeprecatedProperty");
/// The class of functional properties.
pub const FUNCTIONAL_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#FunctionalProperty");
/// The class of inverse-functional properties.
pub const INVERSE_FUNCTIONAL_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#InverseFunctionalProperty");
/// The class of irreflexive properties.
pub const IRREFLEXIVE_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#IrreflexiveProperty");
/// The class of named individuals.
pub const NAMED_INDIVIDUAL: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#NamedIndividual");
/// The class of negative property assertions.
pub const NEGATIVE_PROPERTY_ASSERTION: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#NegativePropertyAssertion");
/// This is the empty class.
pub const NOTHING: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#Nothing");
/// The class of object properties.
pub const OBJECT_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#ObjectProperty");
/// The class of ontologies.
pub const ONTOLOGY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#Ontology");
/// The class of ontology properties.
pub const ONTOLOGY_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#OntologyProperty");
/// The class of reflexive properties.
pub const REFLEXIVE_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#ReflexiveProperty");
/// The class of property restrictions.
pub const RESTRICTION: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#Restriction");
/// The class of symmetric properties.
pub const SYMMETRIC_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#SymmetricProperty");
/// The class of transitive properties.
pub const TRANSITIVE_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#TransitiveProperty");
/// The class of OWL individuals.
pub const THING: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#Thing");
/// The property that determines the class that a universal property restriction refers to.
pub const ALL_VALUES_FROM: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#allValuesFrom");
/// The property that determines the predicate of an annotated axiom or annotated annotation.
pub const ANNOTATED_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#annotatedProperty");
/// The property that determines the subject of an annotated axiom or annotated annotation.
pub const ANNOTATED_SOURCE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#annotatedSource");
/// The property that determines the object of an annotated axiom or annotated annotation.
pub const ANNOTATED_TARGET: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#annotatedTarget");
/// The property that determines the predicate of a negative property assertion.
pub const ASSERTION_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#assertionProperty");
/// The annotation property that indicates that a given ontology is backward compatible with another ontology.
pub const BACKWARD_COMPATIBLE_WITH: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#backwardCompatibleWith");
/// The data property that does not relate any individual to any data value.
pub const BOTTOM_DATA_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#bottomDataProperty");
/// The object property that does not relate any two individuals.
pub const BOTTOM_OBJECT_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#bottomObjectProperty");
/// The property that determines the cardinality of an exact cardinality restriction.
pub const CARDINALITY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#cardinality");
/// The property that determines that a given class is the complement of another class.
pub const COMPLEMENT_OF: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#complementOf");
/// The property that determines that a given data range is the complement of another data range with respect to the data domain.
pub const DATATYPE_COMPLEMENT_OF: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#datatypeComplementOf");
/// The annotation property that indicates that a given entity has been deprecated.
pub const DEPRECATED: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#deprecated");
/// The property that determines that two given individuals are different.
pub const DIFFERENT_FROM: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#differentFrom");
/// The property that determines that a given class is equivalent to the disjoint union of a collection of other classes.
pub const DISJOINT_UNION_OF: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#disjointUnionOf");
/// The property that determines that two given classes are disjoint.
pub const DISJOINT_WITH: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#disjointWith");
/// The property that determines the collection of pairwise different individuals in a owl:AllDifferent axiom.
pub const DISTINCT_MEMBERS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#distinctMembers");
/// The property that determines that two given classes are equivalent, and that is used to specify datatype definitions.
pub const EQUIVALENT_CLASS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#equivalentClass");
/// The property that determines that two given properties are equivalent.
pub const EQUIVALENT_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#equivalentProperty");
/// The property that determines the collection of properties that jointly build a key.
pub const HAS_KEY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#hasKey");
/// The property that determines the property that a self restriction refers to.
pub const HAS_SELF: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#hasSelf");
/// The property that determines the individual that a has-value restriction refers to.
pub const HAS_VALUE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#hasValue");
/// The property that is used for importing other ontologies into a given ontology.
pub const IMPORTS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#imports");
/// The annotation property that indicates that a given ontology is incompatible with another ontology.
pub const INCOMPATIBLE_WITH: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#incompatibleWith");
/// The property that determines the collection of classes or data ranges that build an intersection.
pub const INTERSECTION_OF: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#intersectionOf");
/// The property that determines that two given properties are inverse.
pub const INVERSE_OF: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#inverseOf");
/// The property that determines the cardinality of a maximum cardinality restriction.
pub const MAX_CARDINALITY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#maxCardinality");
/// The property that determines the cardinality of a maximum qualified cardinality restriction.
pub const MAX_QUALIFIED_CARDINALITY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#maxQualifiedCardinality");
/// The property that determines the collection of members in either a owl:AllDifferent, owl:AllDisjointClasses or owl:AllDisjointProperties axiom.
pub const MEMBERS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#members");
/// The property that determines the cardinality of a minimum cardinality restriction.
pub const MIN_CARDINALITY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#minCardinality");
/// The property that determines the cardinality of a minimum qualified cardinality restriction.
pub const MIN_QUALIFIED_CARDINALITY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#minQualifiedCardinality");
/// The property that determines the class that a qualified object cardinality restriction refers to.
pub const ON_CLASS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#onClass");
/// The property that determines the data range that a qualified data cardinality restriction refers to.
pub const ON_DATARANGE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#onDataRange");
/// The property that determines the datatype that a datatype restriction refers to.
pub const ON_DATATYPE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#onDatatype");
/// The property that determines the collection of individuals or data values that build an enumeration.
pub const ONE_OF: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#oneOf");
/// The property that determines the n-tuple of properties that a property restriction on an n-ary data range refers to.
pub const ON_PROPERTIES: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#onProperties");
/// The property that determines the property that a property restriction refers to.
pub const ON_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#onProperty");
/// The annotation property that indicates the predecessor ontology of a given ontology.
pub const PRIOR_VERSION: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#priorVersion");
/// The property that determines the n-tuple of properties that build a sub property chain of a given property.
pub const PROPERTY_CHAIN_AXIOM: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#propertyChainAxiom");
/// The property that determines that two given properties are disjoint.
pub const PROPERTY_DISJOINT_WITH: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#propertyDisjointWith");
/// The property that determines the cardinality of an exact qualified cardinality restriction.
pub const QUALIFIED_CARDINALITY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#qualifiedCardinality");
/// The property that determines that two given individuals are equal.
pub const SAME_AS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#sameAs");
/// The property that determines the class that an existential property restriction refers to.
pub const SOME_VALUES_FROM: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#someValuesFrom");
/// The property that determines the subject of a negative property assertion.
pub const SOURCE_INDIVIDUAL: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#sourceIndividual");
/// The property that determines the object of a negative object property assertion.
pub const TARGET_INDIVIDUAL: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#targetIndividual");
/// The property that determines the value of a negative data property assertion.
pub const TARGET_VALUE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#targetValue");
/// The data property that relates every individual to every data value.
pub const TOP_DATA_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#topDataProperty");
/// The object property that relates every two individuals.
pub const TOP_OBJECT_PROPERTY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#topObjectProperty");
/// The property that determines the collection of classes or data ranges that build a union.
pub const UNION_OF: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#unionOf");
/// The annotation property that provides version information for an ontology or another OWL construct.
pub const VERSION_INFO: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#versionInfo");
/// The property that identifies the version IRI of an ontology.
pub const VERSION_IRI: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owlversionIRI#");
/// The property that determines the collection of facet-value pairs that define a datatype restriction.
pub const WITH_RESTRICTIONS: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#withRestrictions");
/// The set of all real numbers.
pub const REAL: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#real");
/// The set of all rational numbers. It is a subset of the value space of owl:real, and it contains the
/// value space of xsd:decimal (and thus of all xsd: numeric datatypes listed in the [OWL2 vocabulary](https://www.w3.org/TR/owl2-syntax/#Real_Numbers.2C_Decimal_Numbers.2C_and_Integers)).
pub const RATIONAL: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2002/07/owl#rational");
