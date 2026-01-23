/// Literal.swift
/// Unified literal value representation for SQL and SPARQL
///
/// Reference:
/// - ISO/IEC 9075:2023 (SQL literals)
/// - W3C SPARQL 1.1/1.2 (RDF literals)
/// - W3C RDF-star (quoted triples)

import Foundation

/// Unified literal value representation
/// Combines SQL literals and SPARQL RDF terms
public enum Literal: Sendable, Equatable, Hashable {
    // MARK: - Common Literals

    /// NULL / UNDEF
    case null

    /// Boolean value
    case bool(Bool)

    /// Integer value (64-bit)
    case int(Int64)

    /// Floating-point value
    case double(Double)

    /// String value
    case string(String)

    /// Date value (date only, no time)
    case date(Date)

    /// Timestamp value (date + time)
    case timestamp(Date)

    /// Binary data
    case binary(Data)

    /// Array of literals
    case array([Literal])

    // MARK: - SPARQL/RDF-Specific Literals

    /// IRI (Internationalized Resource Identifier)
    /// Example: <http://example.org/resource>
    case iri(String)

    /// Blank node identifier
    /// Example: _:b1
    case blankNode(String)

    /// Typed literal with explicit datatype
    /// Example: "42"^^xsd:integer
    case typedLiteral(value: String, datatype: String)

    /// Language-tagged literal
    /// Example: "Hello"@en
    case langLiteral(value: String, language: String)
}

// MARK: - Convenience Initializers

extension Literal {
    /// Create a literal from any supported Swift type
    public init?(_ value: Any) {
        switch value {
        case let v as Bool:
            self = .bool(v)
        case let v as Int:
            self = .int(Int64(v))
        case let v as Int64:
            self = .int(v)
        case let v as Double:
            self = .double(v)
        case let v as Float:
            self = .double(Double(v))
        case let v as String:
            self = .string(v)
        case let v as Date:
            self = .timestamp(v)
        case let v as Data:
            self = .binary(v)
        case let v as [Any]:
            let literals = v.compactMap { Literal($0) }
            guard literals.count == v.count else { return nil }
            self = .array(literals)
        default:
            return nil
        }
    }
}

// MARK: - Type Accessors

extension Literal {
    /// Returns true if this literal is NULL
    public var isNull: Bool {
        if case .null = self { return true }
        return false
    }

    /// Returns the boolean value if this is a bool literal
    public var boolValue: Bool? {
        if case .bool(let v) = self { return v }
        return nil
    }

    /// Returns the integer value if this is an int literal
    public var intValue: Int64? {
        if case .int(let v) = self { return v }
        return nil
    }

    /// Returns the double value if this is a double literal
    public var doubleValue: Double? {
        if case .double(let v) = self { return v }
        return nil
    }

    /// Returns the string value if this is a string literal
    public var stringValue: String? {
        switch self {
        case .string(let v): return v
        case .typedLiteral(let v, _): return v
        case .langLiteral(let v, _): return v
        default: return nil
        }
    }

    /// Returns the IRI string if this is an IRI literal
    public var iriValue: String? {
        if case .iri(let v) = self { return v }
        return nil
    }
}

// MARK: - XSD Datatype Support

extension Literal {
    /// Standard XSD datatype IRIs
    public enum XSDDatatype: String, Sendable {
        case string = "http://www.w3.org/2001/XMLSchema#string"
        case boolean = "http://www.w3.org/2001/XMLSchema#boolean"
        case integer = "http://www.w3.org/2001/XMLSchema#integer"
        case decimal = "http://www.w3.org/2001/XMLSchema#decimal"
        case double = "http://www.w3.org/2001/XMLSchema#double"
        case float = "http://www.w3.org/2001/XMLSchema#float"
        case date = "http://www.w3.org/2001/XMLSchema#date"
        case dateTime = "http://www.w3.org/2001/XMLSchema#dateTime"
        case time = "http://www.w3.org/2001/XMLSchema#time"
        case duration = "http://www.w3.org/2001/XMLSchema#duration"
        case anyURI = "http://www.w3.org/2001/XMLSchema#anyURI"
        case base64Binary = "http://www.w3.org/2001/XMLSchema#base64Binary"
        case hexBinary = "http://www.w3.org/2001/XMLSchema#hexBinary"
    }

    /// Create a typed literal with XSD datatype
    public static func xsd(_ value: String, type: XSDDatatype) -> Literal {
        .typedLiteral(value: value, datatype: type.rawValue)
    }

    /// Returns the XSD datatype of this literal, if applicable
    public var xsdDatatype: XSDDatatype? {
        switch self {
        case .bool:
            return .boolean
        case .int:
            return .integer
        case .double:
            return .double
        case .string:
            return .string
        case .date:
            return .date
        case .timestamp:
            return .dateTime
        case .binary:
            return .base64Binary
        case .typedLiteral(_, let datatype):
            return XSDDatatype(rawValue: datatype)
        default:
            return nil
        }
    }
}

// MARK: - CustomStringConvertible

extension Literal: CustomStringConvertible {
    public var description: String {
        switch self {
        case .null:
            return "NULL"
        case .bool(let v):
            return v ? "true" : "false"
        case .int(let v):
            return String(v)
        case .double(let v):
            return String(v)
        case .string(let v):
            return "\"\(v)\""
        case .date(let v):
            let formatter = ISO8601DateFormatter()
            formatter.formatOptions = [.withFullDate]
            return formatter.string(from: v)
        case .timestamp(let v):
            return ISO8601DateFormatter().string(from: v)
        case .binary(let v):
            return "binary(\(v.count) bytes)"
        case .array(let v):
            return "[\(v.map(\.description).joined(separator: ", "))]"
        case .iri(let v):
            return "<\(v)>"
        case .blankNode(let v):
            return "_:\(v)"
        case .typedLiteral(let value, let datatype):
            return "\"\(value)\"^^<\(datatype)>"
        case .langLiteral(let value, let language):
            return "\"\(value)\"@\(language)"
        }
    }
}
