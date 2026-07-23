/// Semantic XSD/RDF datatype identity used by ontology validation.
///
/// RDF parsers must expand prefixed names before constructing a literal. The
/// runtime accepts canonical absolute datatype IRIs only.
package enum XSDDatatypeKind: Sendable, Hashable, CaseIterable {
    case owlReal
    case owlRational
    case rdfsLiteral
    case string
    case boolean
    case decimal
    case float
    case double
    case duration
    case dateTime
    case dateTimeStamp
    case time
    case date
    case anyURI
    case base64Binary
    case hexBinary
    case normalizedString
    case token
    case language
    case nmtoken
    case name
    case ncname
    case integer
    case nonPositiveInteger
    case negativeInteger
    case nonNegativeInteger
    case positiveInteger
    case long
    case int
    case short
    case byte
    case unsignedLong
    case unsignedInt
    case unsignedShort
    case unsignedByte
    case rdfLangString
    case rdfPlainLiteral
    case rdfXMLLiteral

    package static let xsdNamespace = "http://www.w3.org/2001/XMLSchema#"
    package static let rdfNamespace = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    package static let rdfsNamespace = "http://www.w3.org/2000/01/rdf-schema#"
    package static let owlNamespace = "http://www.w3.org/2002/07/owl#"

    package init?(iri: String) {
        let localName: Substring
        let namespace: Namespace
        if iri.hasPrefix(Self.xsdNamespace) {
            localName = iri.dropFirst(Self.xsdNamespace.count)
            namespace = .xsd
        } else if iri.hasPrefix(Self.rdfNamespace) {
            localName = iri.dropFirst(Self.rdfNamespace.count)
            namespace = .rdf
        } else if iri.hasPrefix(Self.rdfsNamespace) {
            localName = iri.dropFirst(Self.rdfsNamespace.count)
            namespace = .rdfs
        } else if iri.hasPrefix(Self.owlNamespace) {
            localName = iri.dropFirst(Self.owlNamespace.count)
            namespace = .owl
        } else {
            return nil
        }

        switch (namespace, localName) {
        case (.owl, "real"): self = .owlReal
        case (.owl, "rational"): self = .owlRational
        case (.rdfs, "Literal"): self = .rdfsLiteral
        case (.xsd, "string"): self = .string
        case (.xsd, "boolean"): self = .boolean
        case (.xsd, "decimal"): self = .decimal
        case (.xsd, "float"): self = .float
        case (.xsd, "double"): self = .double
        case (.xsd, "duration"): self = .duration
        case (.xsd, "dateTime"): self = .dateTime
        case (.xsd, "dateTimeStamp"): self = .dateTimeStamp
        case (.xsd, "time"): self = .time
        case (.xsd, "date"): self = .date
        case (.xsd, "anyURI"): self = .anyURI
        case (.xsd, "base64Binary"): self = .base64Binary
        case (.xsd, "hexBinary"): self = .hexBinary
        case (.xsd, "normalizedString"): self = .normalizedString
        case (.xsd, "token"): self = .token
        case (.xsd, "language"): self = .language
        case (.xsd, "NMTOKEN"): self = .nmtoken
        case (.xsd, "Name"): self = .name
        case (.xsd, "NCName"): self = .ncname
        case (.xsd, "integer"): self = .integer
        case (.xsd, "nonPositiveInteger"): self = .nonPositiveInteger
        case (.xsd, "negativeInteger"): self = .negativeInteger
        case (.xsd, "nonNegativeInteger"): self = .nonNegativeInteger
        case (.xsd, "positiveInteger"): self = .positiveInteger
        case (.xsd, "long"): self = .long
        case (.xsd, "int"): self = .int
        case (.xsd, "short"): self = .short
        case (.xsd, "byte"): self = .byte
        case (.xsd, "unsignedLong"): self = .unsignedLong
        case (.xsd, "unsignedInt"): self = .unsignedInt
        case (.xsd, "unsignedShort"): self = .unsignedShort
        case (.xsd, "unsignedByte"): self = .unsignedByte
        case (.rdf, "langString"): self = .rdfLangString
        case (.rdf, "PlainLiteral"): self = .rdfPlainLiteral
        case (.rdf, "XMLLiteral"): self = .rdfXMLLiteral
        default: return nil
        }
    }

    package var canonicalIRI: String {
        switch self {
        case .owlReal:
            return Self.owlNamespace + "real"
        case .owlRational:
            return Self.owlNamespace + "rational"
        case .rdfsLiteral:
            return Self.rdfsNamespace + "Literal"
        case .rdfLangString:
            return Self.rdfNamespace + "langString"
        case .rdfPlainLiteral:
            return Self.rdfNamespace + "PlainLiteral"
        case .rdfXMLLiteral:
            return Self.rdfNamespace + "XMLLiteral"
        default:
            return Self.xsdNamespace + localName
        }
    }

    package var localName: String {
        switch self {
        case .owlReal: "real"
        case .owlRational: "rational"
        case .rdfsLiteral: "Literal"
        case .string: "string"
        case .boolean: "boolean"
        case .decimal: "decimal"
        case .float: "float"
        case .double: "double"
        case .duration: "duration"
        case .dateTime: "dateTime"
        case .dateTimeStamp: "dateTimeStamp"
        case .time: "time"
        case .date: "date"
        case .anyURI: "anyURI"
        case .base64Binary: "base64Binary"
        case .hexBinary: "hexBinary"
        case .normalizedString: "normalizedString"
        case .token: "token"
        case .language: "language"
        case .nmtoken: "NMTOKEN"
        case .name: "Name"
        case .ncname: "NCName"
        case .integer: "integer"
        case .nonPositiveInteger: "nonPositiveInteger"
        case .negativeInteger: "negativeInteger"
        case .nonNegativeInteger: "nonNegativeInteger"
        case .positiveInteger: "positiveInteger"
        case .long: "long"
        case .int: "int"
        case .short: "short"
        case .byte: "byte"
        case .unsignedLong: "unsignedLong"
        case .unsignedInt: "unsignedInt"
        case .unsignedShort: "unsignedShort"
        case .unsignedByte: "unsignedByte"
        case .rdfLangString: "langString"
        case .rdfPlainLiteral: "PlainLiteral"
        case .rdfXMLLiteral: "XMLLiteral"
        }
    }

    package var isInteger: Bool {
        switch self {
        case .integer, .nonPositiveInteger, .negativeInteger,
             .nonNegativeInteger, .positiveInteger, .long, .int, .short,
             .byte, .unsignedLong, .unsignedInt, .unsignedShort, .unsignedByte:
            true
        default:
            false
        }
    }

    package var isNumeric: Bool {
        isInteger || self == .decimal || self == .float || self == .double
            || self == .owlReal || self == .owlRational
    }

    package var isStringFamily: Bool {
        switch self {
        case .string, .normalizedString, .token, .language, .nmtoken,
             .name, .ncname:
            return true
        default:
            return false
        }
    }

    private enum Namespace {
        case xsd
        case rdf
        case rdfs
        case owl
    }
}
