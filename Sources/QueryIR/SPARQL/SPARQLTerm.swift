/// SPARQLTerm.swift
/// SPARQL term types (RDF terms)
///
/// Reference:
/// - W3C SPARQL 1.1/1.2 Query Language
/// - W3C RDF 1.1 Concepts
/// - W3C RDF-star

import Foundation

// Note: Core SPARQLTerm enum is defined in DataSource.swift
// This file provides additional utilities and extensions.

// MARK: - SPARQLTerm Builders

extension SPARQLTerm {
    /// Create a variable term
    public static func `var`(_ name: String) -> SPARQLTerm {
        .variable(name.hasPrefix("?") ? String(name.dropFirst()) : name)
    }

    /// Create an IRI term
    public static func uri(_ iri: String) -> SPARQLTerm {
        .iri(iri)
    }

    /// Create a prefixed name term
    public static func prefixed(_ prefix: String, _ local: String) -> SPARQLTerm {
        .prefixedName(prefix: prefix, local: local)
    }

    /// Create a string literal term
    public static func string(_ value: String) -> SPARQLTerm {
        .literal(.string(value))
    }

    /// Create an integer literal term
    public static func int(_ value: Int64) -> SPARQLTerm {
        .literal(.int(value))
    }

    /// Create a double literal term
    public static func double(_ value: Double) -> SPARQLTerm {
        .literal(.double(value))
    }

    /// Create a boolean literal term
    public static func bool(_ value: Bool) -> SPARQLTerm {
        .literal(.bool(value))
    }

    /// Create a typed literal term
    public static func typed(_ value: String, datatype: String) -> SPARQLTerm {
        .literal(.typedLiteral(value: value, datatype: datatype))
    }

    /// Create a language-tagged literal term
    public static func lang(_ value: String, language: String) -> SPARQLTerm {
        .literal(.langLiteral(value: value, language: language))
    }

    /// Create a blank node term
    public static func blank(_ id: String) -> SPARQLTerm {
        .blankNode(id)
    }

    /// Create a quoted triple term (RDF-star)
    public static func quoted(
        subject: SPARQLTerm,
        predicate: SPARQLTerm,
        object: SPARQLTerm
    ) -> SPARQLTerm {
        .quotedTriple(subject: subject, predicate: predicate, object: object)
    }
}

// MARK: - SPARQLTerm Analysis

extension SPARQLTerm {
    /// Returns true if this is a variable
    public var isVariable: Bool {
        if case .variable = self { return true }
        return false
    }

    /// Returns true if this is a concrete term (not a variable)
    public var isConcrete: Bool {
        !isVariable
    }

    /// Returns true if this is an IRI
    public var isIRI: Bool {
        if case .iri = self { return true }
        if case .prefixedName = self { return true }
        return false
    }

    /// Returns true if this is a literal
    public var isLiteral: Bool {
        if case .literal = self { return true }
        return false
    }

    /// Returns true if this is a blank node
    public var isBlankNode: Bool {
        if case .blankNode = self { return true }
        return false
    }

    /// Returns true if this is a quoted triple (RDF-star)
    public var isQuotedTriple: Bool {
        if case .quotedTriple = self { return true }
        return false
    }

    /// Returns the variable name if this is a variable
    public var variableName: String? {
        if case .variable(let name) = self { return name }
        return nil
    }

    /// Returns the IRI string if this is an IRI or prefixed name
    public func resolveIRI(prefixes: [String: String] = [:]) -> String? {
        switch self {
        case .iri(let iri):
            return iri
        case .prefixedName(let prefix, let local):
            if let base = prefixes[prefix] {
                return base + local
            }
            return "\(prefix):\(local)"
        default:
            return nil
        }
    }

    /// Returns the literal value if this is a literal
    public var literalValue: Literal? {
        if case .literal(let lit) = self { return lit }
        return nil
    }
}

// MARK: - SPARQL Serialization

extension SPARQLTerm {
    /// Generate SPARQL syntax with proper escaping
    public func toSPARQL(prefixes: [String: String] = [:]) -> String {
        switch self {
        case .variable(let name):
            return "?\(name)"

        case .iri(let iri):
            // Try to abbreviate with prefix
            for (prefix, base) in prefixes {
                if iri.hasPrefix(base) {
                    let local = String(iri.dropFirst(base.count))
                    // Validate NCName for both prefix and local
                    if SPARQLEscape.ncNameOrNil(prefix) != nil,
                       local.isEmpty || SPARQLEscape.ncNameOrNil(local) != nil {
                        return "\(prefix):\(local)"
                    }
                }
            }
            // Fall back to full IRI with proper escaping
            return SPARQLEscape.iri(iri)

        case .prefixedName(let prefix, let local):
            // Validate NCName components
            if let validatedPrefix = SPARQLEscape.ncNameOrNil(prefix) {
                // Local part can be empty or valid
                let localPattern = "^[a-zA-Z0-9_.-]*$"
                if local.isEmpty || local.range(of: localPattern, options: .regularExpression) != nil {
                    return "\(validatedPrefix):\(local)"
                }
            }
            // Fall back to expanding to full IRI if we have the prefix mapping
            if let base = prefixes[prefix] {
                return SPARQLEscape.iri(base + local)
            }
            // Last resort: return as-is with warning potential
            return "\(prefix):\(local)"

        case .literal(let lit):
            return lit.toSPARQL()

        case .blankNode(let id):
            // Validate blank node ID
            if SPARQLEscape.ncNameOrNil(id) != nil {
                return "_:\(id)"
            }
            // Generate safe blank node ID
            return "_:b\(abs(id.hashValue))"

        case .quotedTriple(let s, let p, let o):
            return "<< \(s.toSPARQL(prefixes: prefixes)) \(p.toSPARQL(prefixes: prefixes)) \(o.toSPARQL(prefixes: prefixes)) >>"
        }
    }
}

extension Literal {
    /// Cached ISO8601DateFormatter for date serialization
    /// Note: ISO8601DateFormatter is not Sendable but the formatter is immutable after creation
    nonisolated(unsafe) private static let sparqlDateFormatter: ISO8601DateFormatter = {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withFullDate]
        return formatter
    }()

    /// Cached ISO8601DateFormatter for timestamp serialization
    nonisolated(unsafe) private static let sparqlTimestampFormatter = ISO8601DateFormatter()

    /// Generate SPARQL literal syntax
    public func toSPARQL() -> String {
        switch self {
        case .null:
            return "UNDEF"
        case .bool(let v):
            return v ? "true" : "false"
        case .int(let v):
            return String(v)
        case .double(let v):
            return String(v)
        case .string(let v):
            return SPARQLEscape.string(v)
        case .date(let v):
            return "\"\(Self.sparqlDateFormatter.string(from: v))\"^^<http://www.w3.org/2001/XMLSchema#date>"
        case .timestamp(let v):
            return "\"\(Self.sparqlTimestampFormatter.string(from: v))\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"
        case .binary(let v):
            return "\"\(v.base64EncodedString())\"^^<http://www.w3.org/2001/XMLSchema#base64Binary>"
        case .array(let v):
            return "(" + v.map { $0.toSPARQL() }.joined(separator: " ") + ")"
        case .iri(let v):
            return SPARQLEscape.iri(v)
        case .blankNode(let v):
            if SPARQLEscape.ncNameOrNil(v) != nil {
                return "_:\(v)"
            }
            return "_:b\(abs(v.hashValue))"
        case .typedLiteral(let value, let datatype):
            return "\(SPARQLEscape.string(value))^^<\(datatype)>"
        case .langLiteral(let value, let language):
            return "\(SPARQLEscape.string(value))@\(language)"
        }
    }
}

// MARK: - Common Prefixes

extension SPARQLTerm {
    /// Common RDF/RDFS/OWL prefixes
    public static let commonPrefixes: [String: String] = [
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "owl": "http://www.w3.org/2002/07/owl#",
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "foaf": "http://xmlns.com/foaf/0.1/",
        "dc": "http://purl.org/dc/elements/1.1/",
        "dcterms": "http://purl.org/dc/terms/",
        "schema": "http://schema.org/",
        "skos": "http://www.w3.org/2004/02/skos/core#"
    ]

    /// Create a prefixed term using common prefixes
    public static func common(_ prefix: String, _ local: String) -> SPARQLTerm {
        .prefixedName(prefix: prefix, local: local)
    }

    /// RDF type property
    public static var rdfType: SPARQLTerm {
        .prefixedName(prefix: "rdf", local: "type")
    }

    /// RDFS label property
    public static var rdfsLabel: SPARQLTerm {
        .prefixedName(prefix: "rdfs", local: "label")
    }

    /// RDFS comment property
    public static var rdfsComment: SPARQLTerm {
        .prefixedName(prefix: "rdfs", local: "comment")
    }

    /// RDFS subClassOf property
    public static var rdfsSubClassOf: SPARQLTerm {
        .prefixedName(prefix: "rdfs", local: "subClassOf")
    }

    /// RDFS subPropertyOf property
    public static var rdfsSubPropertyOf: SPARQLTerm {
        .prefixedName(prefix: "rdfs", local: "subPropertyOf")
    }

    /// OWL sameAs property
    public static var owlSameAs: SPARQLTerm {
        .prefixedName(prefix: "owl", local: "sameAs")
    }

    /// OWL Class
    public static var owlClass: SPARQLTerm {
        .prefixedName(prefix: "owl", local: "Class")
    }
}

// MARK: - Term Comparison

extension SPARQLTerm {
    /// Compare two terms for SPARQL ordering
    /// Returns: negative if self < other, zero if equal, positive if self > other
    public func compare(to other: SPARQLTerm, prefixes: [String: String] = [:]) -> Int {
        // SPARQL term ordering:
        // 1. Blank nodes
        // 2. IRIs
        // 3. Literals

        let selfRank = termRank
        let otherRank = other.termRank

        if selfRank != otherRank {
            return selfRank - otherRank
        }

        // Same type, compare values
        switch (self, other) {
        case (.blankNode(let a), .blankNode(let b)):
            return a.compare(b) == .orderedAscending ? -1 : (a == b ? 0 : 1)

        case (.iri(let a), .iri(let b)):
            return a.compare(b) == .orderedAscending ? -1 : (a == b ? 0 : 1)

        case (.prefixedName, .prefixedName):
            let a = resolveIRI(prefixes: prefixes) ?? ""
            let b = other.resolveIRI(prefixes: prefixes) ?? ""
            return a.compare(b) == .orderedAscending ? -1 : (a == b ? 0 : 1)

        case (.literal(let a), .literal(let b)):
            return compareLiterals(a, b)

        case (.variable(let a), .variable(let b)):
            return a.compare(b) == .orderedAscending ? -1 : (a == b ? 0 : 1)

        default:
            return 0
        }
    }

    private var termRank: Int {
        switch self {
        case .blankNode: return 1
        case .iri, .prefixedName: return 2
        case .literal: return 3
        case .variable: return 0
        case .quotedTriple: return 4
        }
    }

    private func compareLiterals(_ a: Literal, _ b: Literal) -> Int {
        // Simple string comparison for now
        let aStr = a.description
        let bStr = b.description
        return aStr.compare(bStr) == .orderedAscending ? -1 : (aStr == bStr ? 0 : 1)
    }
}

// MARK: - Expression Conversion

extension SPARQLTerm {
    /// Convert to an Expression
    public func toExpression() -> Expression {
        switch self {
        case .variable(let name):
            return .variable(Variable(name))
        case .literal(let lit):
            return .literal(lit)
        case .iri(let iri):
            return .literal(.iri(iri))
        case .prefixedName(let prefix, let local):
            // Convert to IRI literal
            return .literal(.iri("\(prefix):\(local)"))
        case .blankNode(let id):
            return .literal(.blankNode(id))
        case .quotedTriple(let s, let p, let o):
            return .triple(
                subject: s.toExpression(),
                predicate: p.toExpression(),
                object: o.toExpression()
            )
        }
    }
}
