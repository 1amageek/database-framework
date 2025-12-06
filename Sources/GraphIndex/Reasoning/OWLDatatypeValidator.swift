// OWLDatatypeValidator.swift
// GraphIndex - XSD datatype validation
//
// Validates OWL literals against XSD datatypes and facet constraints.
//
// Reference: W3C XML Schema Part 2: Datatypes
// https://www.w3.org/TR/xmlschema-2/

import Foundation
import Graph

/// XSD Datatype Validator
///
/// Validates OWL literals for:
/// - Lexical form validity (e.g., "abc" is not a valid integer)
/// - Facet constraint satisfaction (e.g., minInclusive, pattern)
/// - Data range membership
///
/// **Example**:
/// ```swift
/// let validator = OWLDatatypeValidator()
///
/// // Validate literal
/// let age = OWLLiteral.integer(25)
/// if let error = validator.validateLexicalForm(age) {
///     print("Invalid: \(error)")
/// }
///
/// // Validate against range with facets
/// let ageRange = OWLDataRange.datatypeRestriction(
///     datatype: "xsd:integer",
///     facets: [.minInclusive(0), .maxInclusive(150)]
/// )
/// if let error = validator.validate(age, against: ageRange) {
///     print("Out of range: \(error)")
/// }
/// ```
public struct OWLDatatypeValidator: Sendable {

    // MARK: - Error Types

    /// Validation error types
    public enum ValidationError: Error, Sendable, Equatable, CustomStringConvertible {
        /// Lexical form is invalid for the datatype
        case invalidLexicalForm(literal: String, datatype: String, reason: String)

        /// Facet constraint violated
        case facetViolation(literal: String, facet: String, constraint: String)

        /// Unknown or unsupported datatype
        case unknownDatatype(String)

        /// Literal type incompatible with data range
        case incompatibleTypes(literal: String, range: String)

        /// Empty data range (no possible values)
        case emptyDataRange(String)

        public var description: String {
            switch self {
            case .invalidLexicalForm(let lit, let dt, let reason):
                return "Invalid lexical form '\(lit)' for \(dt): \(reason)"
            case .facetViolation(let lit, let facet, let constraint):
                return "Facet violation: '\(lit)' does not satisfy \(facet) \(constraint)"
            case .unknownDatatype(let dt):
                return "Unknown datatype: \(dt)"
            case .incompatibleTypes(let lit, let range):
                return "Incompatible types: '\(lit)' cannot be in range \(range)"
            case .emptyDataRange(let range):
                return "Empty data range: \(range)"
            }
        }
    }

    // MARK: - Supported Datatypes

    /// Set of supported XSD datatypes
    public static let supportedDatatypes: Set<String> = [
        // Primitive types
        XSDDatatype.string.iri,
        XSDDatatype.boolean.iri,
        XSDDatatype.decimal.iri,
        XSDDatatype.float.iri,
        XSDDatatype.double.iri,
        XSDDatatype.duration.iri,
        XSDDatatype.dateTime.iri,
        XSDDatatype.time.iri,
        XSDDatatype.date.iri,
        XSDDatatype.anyURI.iri,
        XSDDatatype.base64Binary.iri,
        XSDDatatype.hexBinary.iri,

        // Derived string types
        XSDDatatype.normalizedString.iri,
        XSDDatatype.token.iri,
        XSDDatatype.language.iri,
        XSDDatatype.nmtoken.iri,
        XSDDatatype.name.iri,
        XSDDatatype.ncname.iri,

        // Derived numeric types
        XSDDatatype.integer.iri,
        XSDDatatype.nonPositiveInteger.iri,
        XSDDatatype.negativeInteger.iri,
        XSDDatatype.nonNegativeInteger.iri,
        XSDDatatype.positiveInteger.iri,
        XSDDatatype.long.iri,
        XSDDatatype.int.iri,
        XSDDatatype.short.iri,
        XSDDatatype.byte.iri,
        XSDDatatype.unsignedLong.iri,
        XSDDatatype.unsignedInt.iri,
        XSDDatatype.unsignedShort.iri,
        XSDDatatype.unsignedByte.iri,

        // RDF types
        "rdf:langString",
        "rdf:PlainLiteral"
    ]

    // MARK: - Initialization

    public init() {}

    // MARK: - Lexical Form Validation

    /// Validate the lexical form of a literal
    ///
    /// - Parameter literal: The literal to validate
    /// - Returns: ValidationError if invalid, nil if valid
    public func validateLexicalForm(_ literal: OWLLiteral) -> ValidationError? {
        let lexical = literal.lexicalForm
        let datatype = literal.datatype

        // Check if datatype is supported
        guard Self.supportedDatatypes.contains(datatype) else {
            return .unknownDatatype(datatype)
        }

        switch datatype {
        // Boolean
        case XSDDatatype.boolean.iri:
            let valid = ["true", "false", "1", "0"]
            if !valid.contains(lexical.lowercased()) {
                return .invalidLexicalForm(literal: lexical, datatype: datatype,
                    reason: "must be 'true', 'false', '1', or '0'")
            }

        // Integer types
        case XSDDatatype.integer.iri, XSDDatatype.long.iri, XSDDatatype.int.iri,
             XSDDatatype.short.iri, XSDDatatype.byte.iri:
            if Int64(lexical) == nil {
                return .invalidLexicalForm(literal: lexical, datatype: datatype,
                    reason: "not a valid integer")
            }

        case XSDDatatype.nonNegativeInteger.iri, XSDDatatype.unsignedLong.iri,
             XSDDatatype.unsignedInt.iri, XSDDatatype.unsignedShort.iri,
             XSDDatatype.unsignedByte.iri:
            guard let value = Int64(lexical), value >= 0 else {
                return .invalidLexicalForm(literal: lexical, datatype: datatype,
                    reason: "must be a non-negative integer")
            }

        case XSDDatatype.positiveInteger.iri:
            guard let value = Int64(lexical), value > 0 else {
                return .invalidLexicalForm(literal: lexical, datatype: datatype,
                    reason: "must be a positive integer")
            }

        case XSDDatatype.nonPositiveInteger.iri:
            guard let value = Int64(lexical), value <= 0 else {
                return .invalidLexicalForm(literal: lexical, datatype: datatype,
                    reason: "must be a non-positive integer")
            }

        case XSDDatatype.negativeInteger.iri:
            guard let value = Int64(lexical), value < 0 else {
                return .invalidLexicalForm(literal: lexical, datatype: datatype,
                    reason: "must be a negative integer")
            }

        // Decimal/Float/Double
        case XSDDatatype.decimal.iri:
            if Decimal(string: lexical) == nil {
                return .invalidLexicalForm(literal: lexical, datatype: datatype,
                    reason: "not a valid decimal")
            }

        case XSDDatatype.float.iri:
            if Float(lexical) == nil && !isSpecialFloatValue(lexical) {
                return .invalidLexicalForm(literal: lexical, datatype: datatype,
                    reason: "not a valid float")
            }

        case XSDDatatype.double.iri:
            if Double(lexical) == nil && !isSpecialFloatValue(lexical) {
                return .invalidLexicalForm(literal: lexical, datatype: datatype,
                    reason: "not a valid double")
            }

        // Date/Time types
        case XSDDatatype.dateTime.iri:
            if !isValidDateTime(lexical) {
                return .invalidLexicalForm(literal: lexical, datatype: datatype,
                    reason: "not a valid ISO 8601 dateTime")
            }

        case XSDDatatype.date.iri:
            if !isValidDate(lexical) {
                return .invalidLexicalForm(literal: lexical, datatype: datatype,
                    reason: "not a valid ISO 8601 date")
            }

        case XSDDatatype.time.iri:
            if !isValidTime(lexical) {
                return .invalidLexicalForm(literal: lexical, datatype: datatype,
                    reason: "not a valid ISO 8601 time")
            }

        // URI
        case XSDDatatype.anyURI.iri:
            if URL(string: lexical) == nil {
                return .invalidLexicalForm(literal: lexical, datatype: datatype,
                    reason: "not a valid URI")
            }

        // Binary types
        case XSDDatatype.base64Binary.iri:
            if !isValidBase64(lexical) {
                return .invalidLexicalForm(literal: lexical, datatype: datatype,
                    reason: "not valid Base64")
            }

        case XSDDatatype.hexBinary.iri:
            if !isValidHexBinary(lexical) {
                return .invalidLexicalForm(literal: lexical, datatype: datatype,
                    reason: "not valid hex binary")
            }

        // Language tag
        case "rdf:langString":
            if literal.language == nil || literal.language!.isEmpty {
                return .invalidLexicalForm(literal: lexical, datatype: datatype,
                    reason: "langString requires a language tag")
            }

        // String types - always valid lexically
        case XSDDatatype.string.iri, XSDDatatype.normalizedString.iri,
             XSDDatatype.token.iri, XSDDatatype.name.iri, XSDDatatype.ncname.iri,
             XSDDatatype.nmtoken.iri, XSDDatatype.language.iri,
             "rdf:PlainLiteral":
            break

        default:
            break
        }

        return nil
    }

    // MARK: - Data Range Validation

    /// Validate a literal against a data range
    ///
    /// - Parameters:
    ///   - literal: The literal to validate
    ///   - range: The data range to check against
    /// - Returns: ValidationError if invalid, nil if valid
    public func validate(_ literal: OWLLiteral, against range: OWLDataRange) -> ValidationError? {
        switch range {
        case .datatype(let dt):
            // Check datatype compatibility
            if literal.datatype != dt && !isSubtypeOf(literal.datatype, dt) {
                return .incompatibleTypes(literal: literal.description, range: dt)
            }
            return validateLexicalForm(literal)

        case .dataIntersectionOf(let ranges):
            // Must satisfy all ranges
            for r in ranges {
                if let error = validate(literal, against: r) {
                    return error
                }
            }
            return nil

        case .dataUnionOf(let ranges):
            // Must satisfy at least one range
            for r in ranges {
                if validate(literal, against: r) == nil {
                    return nil
                }
            }
            return .incompatibleTypes(literal: literal.description, range: range.description)

        case .dataComplementOf(let inner):
            // Must NOT satisfy the inner range
            if validate(literal, against: inner) == nil {
                return .incompatibleTypes(literal: literal.description, range: range.description)
            }
            return nil

        case .dataOneOf(let literals):
            // Must be one of the enumerated values
            if !literals.contains(literal) {
                return .incompatibleTypes(literal: literal.description, range: range.description)
            }
            return nil

        case .datatypeRestriction(let dt, let facets):
            // Check base datatype
            if literal.datatype != dt && !isSubtypeOf(literal.datatype, dt) {
                return .incompatibleTypes(literal: literal.description, range: dt)
            }
            // Check facets
            return validateFacets(literal, facets: facets)
        }
    }

    // MARK: - Facet Validation

    /// Validate a literal against facet restrictions
    ///
    /// - Parameters:
    ///   - literal: The literal to validate
    ///   - facets: The facet restrictions
    /// - Returns: ValidationError if invalid, nil if valid
    public func validateFacets(_ literal: OWLLiteral, facets: [FacetRestriction]) -> ValidationError? {
        for facet in facets {
            if let error = validateSingleFacet(literal, facet: facet) {
                return error
            }
        }
        return nil
    }

    private func validateSingleFacet(_ literal: OWLLiteral, facet: FacetRestriction) -> ValidationError? {
        let lexical = literal.lexicalForm

        switch facet.facet {
        case .minInclusive:
            guard let comparison = compare(literal, facet.value) else {
                return .facetViolation(literal: lexical, facet: "minInclusive",
                    constraint: "cannot compare with \(facet.value.lexicalForm)")
            }
            if comparison == .orderedAscending {
                return .facetViolation(literal: lexical, facet: "minInclusive",
                    constraint: ">= \(facet.value.lexicalForm)")
            }

        case .maxInclusive:
            guard let comparison = compare(literal, facet.value) else {
                return .facetViolation(literal: lexical, facet: "maxInclusive",
                    constraint: "cannot compare with \(facet.value.lexicalForm)")
            }
            if comparison == .orderedDescending {
                return .facetViolation(literal: lexical, facet: "maxInclusive",
                    constraint: "<= \(facet.value.lexicalForm)")
            }

        case .minExclusive:
            guard let comparison = compare(literal, facet.value) else {
                return .facetViolation(literal: lexical, facet: "minExclusive",
                    constraint: "cannot compare with \(facet.value.lexicalForm)")
            }
            if comparison != .orderedDescending {
                return .facetViolation(literal: lexical, facet: "minExclusive",
                    constraint: "> \(facet.value.lexicalForm)")
            }

        case .maxExclusive:
            guard let comparison = compare(literal, facet.value) else {
                return .facetViolation(literal: lexical, facet: "maxExclusive",
                    constraint: "cannot compare with \(facet.value.lexicalForm)")
            }
            if comparison != .orderedAscending {
                return .facetViolation(literal: lexical, facet: "maxExclusive",
                    constraint: "< \(facet.value.lexicalForm)")
            }

        case .length:
            guard let length = facet.value.intValue else { break }
            if lexical.count != length {
                return .facetViolation(literal: lexical, facet: "length",
                    constraint: "= \(length)")
            }

        case .minLength:
            guard let minLen = facet.value.intValue else { break }
            if lexical.count < minLen {
                return .facetViolation(literal: lexical, facet: "minLength",
                    constraint: ">= \(minLen)")
            }

        case .maxLength:
            guard let maxLen = facet.value.intValue else { break }
            if lexical.count > maxLen {
                return .facetViolation(literal: lexical, facet: "maxLength",
                    constraint: "<= \(maxLen)")
            }

        case .pattern:
            let pattern = facet.value.lexicalForm
            do {
                let regex = try NSRegularExpression(pattern: pattern, options: [])
                let range = NSRange(lexical.startIndex..., in: lexical)
                if regex.firstMatch(in: lexical, options: [], range: range) == nil {
                    return .facetViolation(literal: lexical, facet: "pattern",
                        constraint: "matches '\(pattern)'")
                }
            } catch {
                return .facetViolation(literal: lexical, facet: "pattern",
                    constraint: "invalid regex '\(pattern)'")
            }

        case .totalDigits:
            guard let maxDigits = facet.value.intValue else { break }
            let digitCount = lexical.filter { $0.isNumber }.count
            if digitCount > maxDigits {
                return .facetViolation(literal: lexical, facet: "totalDigits",
                    constraint: "<= \(maxDigits)")
            }

        case .fractionDigits:
            guard let maxFraction = facet.value.intValue else { break }
            if let dotIndex = lexical.firstIndex(of: ".") {
                let fractionPart = lexical[lexical.index(after: dotIndex)...]
                if fractionPart.count > maxFraction {
                    return .facetViolation(literal: lexical, facet: "fractionDigits",
                        constraint: "<= \(maxFraction)")
                }
            }

        case .whiteSpace:
            // Whitespace handling is about normalization, not validation
            break
        }

        return nil
    }

    // MARK: - Comparison

    /// Compare two literals for ordering
    ///
    /// - Parameters:
    ///   - lhs: First literal
    ///   - rhs: Second literal
    /// - Returns: ComparisonResult or nil if incomparable
    public func compare(_ lhs: OWLLiteral, _ rhs: OWLLiteral) -> ComparisonResult? {
        // Numeric comparison
        if let lhsDouble = lhs.doubleValue, let rhsDouble = rhs.doubleValue {
            if lhsDouble < rhsDouble { return .orderedAscending }
            if lhsDouble > rhsDouble { return .orderedDescending }
            return .orderedSame
        }

        // Date comparison
        if let lhsDate = lhs.dateValue, let rhsDate = rhs.dateValue {
            return lhsDate.compare(rhsDate)
        }

        // String comparison (lexicographic)
        if lhs.datatype == rhs.datatype {
            return lhs.lexicalForm.compare(rhs.lexicalForm)
        }

        return nil
    }

    // MARK: - Helper Methods

    private func isSpecialFloatValue(_ value: String) -> Bool {
        let special = ["INF", "-INF", "NaN", "+INF"]
        return special.contains(value)
    }

    private func isValidDateTime(_ value: String) -> Bool {
        let formatter = ISO8601DateFormatter()
        return formatter.date(from: value) != nil
    }

    private func isValidDate(_ value: String) -> Bool {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withFullDate]
        return formatter.date(from: value) != nil
    }

    private func isValidTime(_ value: String) -> Bool {
        let pattern = #"^\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?$"#
        return value.range(of: pattern, options: .regularExpression) != nil
    }

    private func isValidBase64(_ value: String) -> Bool {
        let base64Pattern = #"^[A-Za-z0-9+/]*={0,2}$"#
        return value.range(of: base64Pattern, options: .regularExpression) != nil &&
               value.count % 4 == 0
    }

    private func isValidHexBinary(_ value: String) -> Bool {
        let hexPattern = #"^[0-9A-Fa-f]*$"#
        return value.range(of: hexPattern, options: .regularExpression) != nil &&
               value.count % 2 == 0
    }

    private func isSubtypeOf(_ sub: String, _ sup: String) -> Bool {
        // XSD type hierarchy
        let hierarchy: [String: Set<String>] = [
            XSDDatatype.integer.iri: [XSDDatatype.decimal.iri],
            XSDDatatype.long.iri: [XSDDatatype.integer.iri],
            XSDDatatype.int.iri: [XSDDatatype.long.iri],
            XSDDatatype.short.iri: [XSDDatatype.int.iri],
            XSDDatatype.byte.iri: [XSDDatatype.short.iri],
            XSDDatatype.nonNegativeInteger.iri: [XSDDatatype.integer.iri],
            XSDDatatype.positiveInteger.iri: [XSDDatatype.nonNegativeInteger.iri],
            XSDDatatype.unsignedLong.iri: [XSDDatatype.nonNegativeInteger.iri],
            XSDDatatype.unsignedInt.iri: [XSDDatatype.unsignedLong.iri],
            XSDDatatype.unsignedShort.iri: [XSDDatatype.unsignedInt.iri],
            XSDDatatype.unsignedByte.iri: [XSDDatatype.unsignedShort.iri],
            XSDDatatype.nonPositiveInteger.iri: [XSDDatatype.integer.iri],
            XSDDatatype.negativeInteger.iri: [XSDDatatype.nonPositiveInteger.iri],
            XSDDatatype.normalizedString.iri: [XSDDatatype.string.iri],
            XSDDatatype.token.iri: [XSDDatatype.normalizedString.iri],
            XSDDatatype.language.iri: [XSDDatatype.token.iri],
            XSDDatatype.nmtoken.iri: [XSDDatatype.token.iri],
            XSDDatatype.name.iri: [XSDDatatype.token.iri],
            XSDDatatype.ncname.iri: [XSDDatatype.name.iri],
        ]

        // Check direct or transitive subtype
        var current = sub
        var visited = Set<String>()
        while let supers = hierarchy[current], !supers.isEmpty {
            if supers.contains(sup) {
                return true
            }
            visited.insert(current)
            // Get first super that hasn't been visited
            if let next = supers.first(where: { !visited.contains($0) }) {
                current = next
            } else {
                break
            }
        }
        return false
    }
}
