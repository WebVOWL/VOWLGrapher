//! [DCMI Metadata Terms](https://www.dublincore.org/specifications/dublin-core/dcmi-terms/) vocabularies.

pub mod dc {
    //! [Dublin Core](https://www.dublincore.org/specifications/dublin-core/dcmi-terms/) vocabulary.
    //!
    //! The original fifteen-element Dublin Core namespace.
    use oxrdf::NamedNodeRef;

    /// An entity responsible for making contributions to the resource.
    ///
    /// The guidelines for using names of persons or organizations as creators also apply to contributors.
    /// Typically, the name of a Contributor should be used to indicate the entity.
    pub const CONTRIBUTOR: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/elements/1.1/contributor");
    /// The spatial or temporal topic of the resource, spatial applicability of the resource, or
    /// jurisdiction under which the resource is relevant.
    ///
    /// Spatial topic and spatial applicability may be a named place or a location specified by its geographic coordinates.
    /// Temporal topic may be a named period, date, or date range. A jurisdiction may be a named administrative entity
    /// or a geographic place to which the resource applies. Recommended practice is to use a controlled vocabulary such as
    /// the Getty Thesaurus of Geographic Names [TGN]. Where appropriate, named places or time periods may be used in preference
    /// to numeric identifiers such as sets of coordinates or date ranges.
    pub const COVERAGE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/elements/1.1/coverage");
    /// An entity primarily responsible for making the resource.
    ///
    /// Examples of a Creator include a person, an organization, or a service.
    /// Typically, the name of a Creator should be used to indicate the entity.
    pub const CREATOR: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/elements/1.1/creator");
    /// A point or period of time associated with an event in the lifecycle of the resource.
    ///
    /// Date may be used to express temporal information at any level of granularity.
    /// Recommended practice is to express the date, date/time, or period of time according to ISO 8601-1 [ISO 8601-1] or
    /// a published profile of the ISO standard, such as the W3C Note on Date and Time Formats [W3CDTF] or
    /// the Extended Date/Time Format Specification [EDTF]. If the full date is unknown, month and year (YYYY-MM) or
    /// just year (YYYY) may be used. Date ranges may be specified using ISO 8601 period of time specification in which
    /// start and end dates are separated by a '/' (slash) character. Either the start or end date may be missing.
    pub const DATE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/elements/1.1/date");
    /// An account of the resource.
    ///
    /// Description may include but is not limited to: an abstract, a table of contents, a graphical
    /// representation, or a free-text account of the resource.
    pub const DESCRIPTION: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/elements/1.1/description");
    /// The file format, physical medium, or dimensions of the resource.
    ///
    /// Recommended practice is to use a controlled vocabulary where available. For example, for file formats
    /// one could use the list of Internet Media Types [MIME].
    pub const FORMAT: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/elements/1.1/format");
    /// An unambiguous reference to the resource within a given context.
    ///
    /// Recommended practice is to identify the resource by means of a string conforming to an identification system.
    pub const IDENTIFIER: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/elements/1.1/identifier");
    /// A language of the resource.
    ///
    /// Recommended practice is to use either a non-literal value representing a language
    /// from a controlled vocabulary such as ISO 639-2 or ISO 639-3, or a literal value consisting of
    /// an IETF Best Current Practice 47 [IETF-BCP47] language tag.
    pub const LANGUAGE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/elements/1.1/language");
    /// An entity responsible for making the resource available.
    ///
    /// Examples of a Publisher include a person, an organization, or a service.
    /// Typically, the name of a Publisher should be used to indicate the entity.
    pub const PUBLISHER: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/elements/1.1/publisher");
    /// A related resource.
    ///
    /// Recommended practice is to identify the related resource by means of a URI.
    /// If this is not possible or feasible, a string conforming to a formal identification system may be provided.
    pub const RELATION: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/elements/1.1/relation");
    /// Information about rights held in and over the resource.
    ///
    /// Typically, rights information includes a statement about various property rights associated with the resource,
    /// including intellectual property rights.
    pub const RIGHTS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/elements/1.1/rights");
    /// A related resource from which the described resource is derived.
    ///
    /// The described resource may be derived from the related resource in whole or in part.
    /// Recommended best practice is to identify the related resource by means of a string conforming to a
    /// formal identification system.
    pub const SOURCE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/elements/1.1/source");
    /// The topic of the resource.
    ///
    /// Typically, the subject will be represented using keywords, key phrases, or classification codes.
    /// Recommended best practice is to use a controlled vocabulary.
    pub const SUBJECT: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/elements/1.1/subject");
    /// A name given to the resource.
    pub const TITLE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/elements/1.1/title");
    /// The nature or genre of the resource.
    ///
    /// Recommended practice is to use a controlled vocabulary such as the DCMI Type Vocabulary [DCMI-TYPE].
    /// To describe the file format, physical medium, or dimensions of the resource, use the Format element.
    pub const TYPE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/elements/1.1/type");
}

pub mod dcterms {
    //! [Dublin Core terms](https://www.dublincore.org/specifications/dublin-core/dcmi-terms/) vocabulary.
    //!
    //! Extends the Dublin Core namespace. However, Dublin Core is duplicated in this vocabulary.
    //! As a result, there exists both a [dc:date](http://purl.org/dc/elements/1.1/date) with no formal range and a
    //! corresponding [dcterms:date](http://purl.org/dc/terms/date) with a formal range of "literal".
    //! While these distinctions are significant for creators of RDF applications, most users can safely treat the
    //! fifteen parallel properties as equivalent.
    use oxrdf::NamedNodeRef;
    /// A summary of the resource.
    pub const ABSTRACT: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/abstract");
    /// Information about who access the resource or an indication of its security status.
    ///
    /// Access Rights may include information regarding access or restrictions based on privacy, security, or other policies.
    pub const ACCESS_RIGHTS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/accessRights");
    /// The method by which items are added to a collection.
    ///
    /// Recommended practice is to use a value from
    /// the [Collection Description Accrual Method](https://dublincore.org/groups/collections/accrual-method/) vocabulary.
    pub const ACCRUAL_METHOD: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/accrualMethod");
    /// The frequency with which items are added to a collection.
    ///
    /// Recommended practice is to use a value from the [Collection Description Frequency](https://dublincore.org/groups/collections/frequency/) vocabulary.
    pub const ACCRUAL_PERIODICITY: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/accrualPeriodicity");
    /// The policy governing the addition of items to a collection.
    ///
    /// Recommended practice is to use a value from the [Collection Description Accrual Policy](https://dublincore.org/groups/collections/accrual-policy/) vocabulary.
    pub const ACCRUAL_POLICY: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/accrualPolicy");
    /// An alternative name for the resource.
    ///
    /// The distinction between titles and alternative titles is application-specific.
    pub const ALTERNATIVE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/alternative");
    /// A class of agents for whom the resource is intended or useful.
    ///
    /// Recommended practice is to use this property with non-literal values from a vocabulary of audience types.
    pub const AUDIENCE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/audience");
    /// Date that the resource became or will become available.
    ///
    /// Recommended practice is to describe the date, date/time, or period of time as recommended for the
    /// property Date, of which this is a subproperty.
    pub const DATE_AVAILABLE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/available");
    /// A bibliographic reference for the resource.
    ///
    /// Recommended practice is to include sufficient bibliographic detail to identify the resource as unambiguously as possible.
    pub const BIBLIOGRAPHIC_CITATION: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/bibliographicCitation");
    /// An established standard to which the described resource conforms.
    pub const CONFORMS_TO: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/conformsTo");
    /// An entity responsible for making contributions to the resource.
    ///
    /// The guidelines for using names of persons or organizations as creators apply to contributors.
    pub const CONTRIBUTOR: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/contributor");
    /// The spatial or temporal topic of the resource, spatial applicability of the resource, or jurisdiction under which the resource is relevant.
    ///
    /// Spatial topic and spatial applicability may be a named place or a location specified by its geographic coordinates.
    /// Temporal topic may be a named period, date, or date range. A jurisdiction may be a named administrative entity or
    /// a geographic place to which the resource applies. Recommended practice is to use a controlled vocabulary such as
    /// the Getty Thesaurus of Geographic Names [TGN]. Where appropriate, named places or time periods may be used in preference
    /// to numeric identifiers such as sets of coordinates or date ranges. Because coverage is so broadly defined, it is preferable
    /// to use the more specific subproperties Temporal Coverage and Spatial Coverage.
    pub const COVERAGE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/coverage");
    /// Date of creation of the resource.
    ///
    /// Recommended practice is to describe the date, date/time, or period of time as recommended for the
    /// property Date, of which this is a subproperty.
    pub const DATE_CREATED: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/created");
    /// An entity responsible for making the resource.
    ///
    /// Recommended practice is to identify the creator with a URI. If this is not possible or
    /// feasible, a literal value that identifies the creator may be provided.
    pub const CREATOR: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/creator");
    /// A point or period of time associated with an event in the lifecycle of the resource.
    ///
    /// Date may be used to express temporal information at any level of granularity.
    /// Recommended practice is to express the date, date/time, or period of time according to ISO 8601-1 [ISO 8601-1] or
    /// a published profile of the ISO standard, such as the W3C Note on Date and Time Formats [W3CDTF] or
    /// the Extended Date/Time Format Specification [EDTF]. If the full date is unknown, month and year (YYYY-MM) or
    /// just year (YYYY) may be used. Date ranges may be specified using ISO 8601 period of time specification in which
    /// start and end dates are separated by a '/' (slash) character. Either the start or end date may be missing.
    pub const DATE: NamedNodeRef<'_> = NamedNodeRef::new_unchecked("http://purl.org/dc/terms/date");
    /// Date of acceptance of the resource.
    ///
    /// Recommended practice is to describe the date, date/time, or period of time as recommended for the property
    /// Date, of which this is a subproperty. Examples of resources to which a date of acceptance may be relevant are a
    /// thesis (accepted by a university department) or an article (accepted by a journal).
    pub const DATE_ACCEPTED: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/dateAccepted");
    /// Date of copyright of the resource.
    ///
    /// Typically a year. Recommended practice is to describe the date, date/time, or period of time as recommended for the
    /// property Date, of which this is a subproperty.
    pub const DATE_COPYRIGHTED: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/dateCopyrighted");
    /// Date of submission of the resource.
    ///
    /// Recommended practice is to describe the date, date/time, or period of time as recommended for the property Date, of which
    /// this is a subproperty. Examples of resources to which a 'Date Submitted' may be relevant include a thesis
    /// (submitted to a university department) or an article (submitted to a journal).
    pub const DATE_SUBMITTED: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/dateSubmitted");
    /// An account of the resource.
    ///
    /// Description may include but is not limited to: an abstract, a table of contents, a graphical
    /// representation, or a free-text account of the resource.
    pub const DESCRIPTION: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/description");
    /// A class of agents, defined in terms of progression through an educational or training context, for which the described resource is intended.
    pub const AUDIENCE_EDUCATION_LEVEL: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/educationLevel");
    /// The size or duration of the resource.
    ///
    /// Recommended practice is to specify the file size in megabytes and duration in ISO 8601 format.
    pub const EXTENT: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/extent");
    /// The file format, physical medium, or dimensions of the resource.
    ///
    /// Recommended practice is to use a controlled vocabulary where available.
    /// For example, for file formats one could use the list of Internet Media Types [MIME]. Examples of dimensions include size and duration.
    pub const FORMAT: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/format");
    /// A related resource that is substantially the same as the pre-existing described resource, but in another format.
    ///
    /// This property is intended to be used with non-literal values. This property is an inverse property of Is Format Of.
    pub const HAS_FORMAT: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/hasFormat");
    /// A related resource that is included either physically or logically in the described resource.
    ///
    /// This property is intended to be used with non-literal values. This property is an inverse property of Is Part Of.
    pub const HAS_PART: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/hasPart");
    /// A related resource that is a version, edition, or adaptation of the described resource.
    ///
    /// Changes in version imply substantive changes in content rather than differences in format.
    /// This property is intended to be used with non-literal values.
    /// This property is an inverse property of Is Version Of.
    pub const HAS_VERSION: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/hasVersion");
    /// An unambiguous reference to the resource within a given context.
    ///
    /// Recommended practice is to identify the resource by means of a string conforming to an identification system.
    /// Examples include International Standard Book Number (ISBN), Digital Object Identifier (DOI), and Uniform Resource Name (URN).
    /// Persistent identifiers should be provided as HTTP URIs.
    pub const IDENTIFIER: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/identifier");
    /// A process, used to engender knowledge, attitudes and skills, that the described resource is designed to support.
    ///
    /// Instructional Method typically includes ways of presenting instructional materials or conducting instructional activities, patterns of
    /// learner-to-learner and learner-to-instructor interactions, and mechanisms by which group and individual levels of learning are measured.
    /// Instructional methods include all aspects of the instruction and learning processes from planning and implementation through evaluation and feedback.
    pub const INSTRUCTIONAL_METHOD: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/instructionalMethod");
    /// A pre-existing related resource that is substantially the same as the described resource, but in another format.
    ///
    /// This property is intended to be used with non-literal values.
    /// This property is an inverse property of Has Format.
    pub const IS_FORMAT_OF: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/isFormatOf");
    /// A related resource in which the described resource is physically or logically included.
    ///
    /// This property is intended to be used with non-literal values.
    /// This property is an inverse property of Has Part.
    pub const IS_PART_OF: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/isPartOf");
    /// A related resource that references, cites, or otherwise points to the described resource.
    ///
    /// This property is intended to be used with non-literal values.
    /// This property is an inverse property of References.
    pub const IS_REFERENCED_BY: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/isReferencedBy");
    /// A related resource that supplants, displaces, or supersedes the described resource.
    ///
    /// This property is intended to be used with non-literal values.
    /// This property is an inverse property of Replaces.
    pub const IS_REPLACED_BY: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/isReplacedBy");
    /// A related resource that requires the described resource to support its function, delivery, or coherence.
    ///
    /// This property is intended to be used with non-literal values.
    /// This property is an inverse property of Requires.
    pub const IS_REQUIRED_BY: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/isRequiredBy");
    /// Recommended practice is to describe the date, date/time, or period of time as
    /// recommended for the property Date, of which this is a subproperty.
    pub const DATE_ISSUED: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/issued");
    /// A related resource of which the described resource is a version, edition, or adaptation.
    ///
    /// Changes in version imply substantive changes in content rather than differences in format.
    /// This property is intended to be used with non-literal values.
    /// This property is an inverse property of Has Version.
    pub const IS_VERSION_OF: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/isVersionOf");
    /// A language of the resource.
    ///
    /// Recommended practice is to use either a non-literal value representing a language from a controlled vocabulary such as
    /// ISO 639-2 or ISO 639-3, or a literal value consisting of an IETF Best Current Practice 47 [IETF-BCP47] language tag.
    pub const LANGUAGE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/language");
    /// A legal document giving official permission to do something with the resource.
    ///
    /// Recommended practice is to identify the license document with a URI.
    /// If this is not possible or feasible, a literal value that identifies the license may be provided.
    pub const LICENSE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/license");
    /// An entity that mediates access to the resource.
    ///
    /// In an educational context, a mediator might be a parent, teacher, teaching assistant, or care-giver.
    pub const MEDIATOR: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/mediator");
    /// The material or physical carrier of the resource.
    pub const MEDIUM: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/medium");
    /// Date on which the resource was changed.
    ///
    /// Recommended practice is to describe the date, date/time, or period of time as
    /// recommended for the property Date, of which this is a subproperty.
    pub const DATE_MODIFIED: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/modified");
    /// A statement of any changes in ownership and custody of the resource since its creation that are
    /// significant for its authenticity, integrity, and interpretation.
    ///
    /// The statement may include a description of any changes successive custodians made to the resource.
    pub const PROVENANCE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/provenance");
    /// An entity responsible for making the resource available.
    pub const PUBLISHER: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/publisher");
    /// A related resource that is referenced, cited, or otherwise pointed to by the described resource.
    ///
    /// This property is intended to be used with non-literal values.
    /// This property is an inverse property of Is Referenced By.
    pub const REFERENCES: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/references");
    /// A related resource.
    ///
    /// Recommended practice is to identify the related resource by means of a URI.
    /// If this is not possible or feasible, a string conforming to a formal identification system may be provided.
    pub const RELATION: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/relation");
    /// A related resource that is supplanted, displaced, or superseded by the described resource.
    ///
    /// This property is intended to be used with non-literal values.
    /// This property is an inverse property of Is Replaced By.
    pub const REPLACES: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/replaces");
    /// A related resource that is required by the described resource to support its function, delivery, or coherence.
    ///
    /// This property is intended to be used with non-literal values.
    /// This property is an inverse property of Is Required By.
    pub const REQUIRES: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/requires");
    /// Information about rights held in and over the resource.
    ///
    /// Typically, rights information includes a statement about various property rights associated with the
    /// resource, including intellectual property rights. Recommended practice is to refer to a rights statement with a URI.
    /// If this is not possible or feasible, a literal value (name, label, or short text) may be provided.
    pub const RIGHTS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/rights");
    /// A person or organization owning or managing rights over the resource.
    ///
    /// Recommended practice is to refer to the rights holder with a URI.
    /// If this is not possible or feasible, a literal value that identifies the rights holder may be provided.
    pub const RIGHTS_HOLDER: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/rightsHolder");
    /// A related resource from which the described resource is derived.
    ///
    /// This property is intended to be used with non-literal values.
    /// The described resource may be derived from the related resource in whole or in part.
    /// Best practice is to identify the related resource by means of a URI or a string conforming to a formal identification system.
    pub const SOURCE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/source");
    /// Spatial characteristics of the resource.
    pub const SPATIAL_COVERAGE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/spatial");
    /// A topic of the resource.
    ///
    /// Recommended practice is to refer to the subject with a URI.
    /// If this is not possible or feasible, a literal value that identifies the subject may be provided.
    /// Both should preferably refer to a subject in a controlled vocabulary.
    pub const SUBJECT: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/subject");
    /// A list of subunits of the resource.
    pub const TABLE_OF_CONTENTS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/tableOfContents");
    /// Temporal characteristics of the resource.
    pub const TEMPORAL: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/temporal");
    /// A name given to the resource.
    pub const TITLE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/title");
    /// The nature or genre of the resource.
    ///
    /// Recommended practice is to use a controlled vocabulary such as the DCMI Type Vocabulary [DCMI-TYPE].
    /// To describe the file format, physical medium, or dimensions of the resource, use the property Format.
    pub const TYPE: NamedNodeRef<'_> = NamedNodeRef::new_unchecked("http://purl.org/dc/terms/type");
    /// Date (often a range) of validity of a resource.
    ///
    /// Recommended practice is to describe the date, date/time, or period of time as
    /// recommended for the property Date, of which this is a subproperty.
    pub const VALID: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/valid");
}
