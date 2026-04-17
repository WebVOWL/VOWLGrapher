//! [RDF compatible XSD datatypes](https://www.w3.org/TR/rdf11-concepts/#dfn-rdf-compatible-xsd-types).
use oxrdf::NamedNodeRef;

/// Absolute or relative URIs and IRIs.
pub const ANY_URI: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#anyURI");
/// Base64-encoded binary data.
pub const BASE_64_BINARY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#base64Binary");
/// true, false.
pub const BOOLEAN: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean");
/// 128…+127 (8 bit).
pub const BYTE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#byte");
/// Dates (yyyy-mm-dd) with or without timezone.
pub const DATE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#date");
/// Duration of time (days, hours, minutes, seconds only).
pub const DAY_TIME_DURATION: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#dayTimeDuration");
/// Date and time with or without timezone.
pub const DATE_TIME: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#dateTime");
/// Date and time with required timezone.
pub const DATE_TIME_STAMP: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#dateTimeStamp");
/// Arbitrary-precision decimal numbers.
pub const DECIMAL: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#decimal");
/// 64-bit floating point numbers incl. ±Inf, ±0, NaN.
pub const DOUBLE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#double");
/// Duration of time.
pub const DURATION: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#duration");
/// 32-bit floating point numbers incl. ±Inf, ±0, NaN.
pub const FLOAT: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#float");
/// Gregorian calendar day of the month.
pub const G_DAY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#gDay");
/// Gregorian calendar month.
pub const G_MONTH: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#gMonth");
/// Gregorian calendar month and day.
pub const G_MONTH_DAY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#gMonthDay");
/// Gregorian calendar year.
pub const G_YEAR: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#gYear");
/// Gregorian calendar year and month.
pub const G_YEAR_MONTH: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#gYearMonth");
/// Hex-encoded binary data.
pub const HEX_BINARY: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#hexBinary");
/// -2147483648…+2147483647 (32 bit).
pub const INT: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#int");
/// Arbitrary-size integer numbers.
pub const INTEGER: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#integer");
/// Language tags per [BCP47](http://tools.ietf.org/html/bcp47).
pub const LANGUAGE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#language");
/// -9223372036854775808…+9223372036854775807 (64 bit).
pub const LONG: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#long");
/// XML Names.
pub const NAME: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#Name");
#[expect(clippy::doc_markdown, reason = "NCName is the official name")]
/// XML NCName.
pub const NC_NAME: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#NCName");
/// Integer numbers <0.
pub const NEGATIVE_INTEGER: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#negativeInteger");
/// XML NMTOKENs.
pub const NMTOKEN: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#NMTOKEN");
/// Integer numbers ≥0.
pub const NON_NEGATIVE_INTEGER: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#nonNegativeInteger");
/// Integer numbers ≤0.
pub const NON_POSITIVE_INTEGER: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#nonPositiveInteger");
/// Whitespace-normalized strings.
pub const NORMALIZED_STRING: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#normalizedString");
/// Integer numbers >0.
pub const POSITIVE_INTEGER: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#positiveInteger");
/// Times (hh:mm:ss.sss…) with or without timezone.
pub const TIME: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#time");
/// -32768…+32767 (16 bit).
pub const SHORT: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#short");
/// Character strings (but not all Unicode character strings).
pub const STRING: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#string");
/// Tokenized strings.
pub const TOKEN: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#token");
/// 0…255 (8 bit).
pub const UNSIGNED_BYTE: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#unsignedByte");
/// 0…4294967295 (32 bit).
pub const UNSIGNED_INT: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#unsignedInt");
/// 0…18446744073709551615 (64 bit).
pub const UNSIGNED_LONG: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#unsignedLong");
/// 0…65535 (16 bit).
pub const UNSIGNED_SHORT: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#unsignedShort");
/// Duration of time (months and years only).
pub const YEAR_MONTH_DURATION: NamedNodeRef<'_> =
    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#yearMonthDuration");
