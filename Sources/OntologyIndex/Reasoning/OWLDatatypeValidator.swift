import DatabaseTypes
import DatabaseKit

/// Compiles OWL data ranges and evaluates literals using XSD value semantics.
///
/// Invalid schemas, unsupported datatypes, invalid lexical forms, and resource
/// exhaustion are failures. They are never converted into non-membership.
public struct OWLDatatypeValidator: Sendable {
    public let profile: XSDDatatypeProfile
    public let limits: XSDValidationLimits

    private var parser: XSDValueParser {
        XSDValueParser(profile: profile, limits: limits)
    }

    public init(
        profile: XSDDatatypeProfile = .owl2,
        limits: XSDValidationLimits = .default
    ) {
        self.profile = profile
        self.limits = limits
    }

    public func validateLexicalForm(_ literal: RDFLiteral) throws {
        _ = try parser.parse(literal)
    }

    public func compile(
        _ range: OWLDataRange
    ) throws -> CompiledOWLDataRange {
        var state = CompilerState()
        return CompiledOWLDataRange(
            root: try compileNode(range, depth: 0, state: &state)
        )
    }

    public func contains(
        _ literal: RDFLiteral,
        in range: CompiledOWLDataRange
    ) throws -> DataRangeMembership {
        let value = try parser.parse(literal)
        return try contains(
            literal: literal,
            value: value,
            in: range.root
        )
    }

    public func membership(
        of literal: RDFLiteral,
        in range: OWLDataRange
    ) throws -> DataRangeMembership {
        try contains(literal, in: compile(range))
    }

    public func validateFacets(
        _ literal: RDFLiteral,
        facets: [FacetRestriction]
    ) throws -> DataRangeMembership {
        try membership(
            of: literal,
            in: .datatypeRestriction(
                datatype: literal.datatypeIRI.rawValue,
                facets: facets
            )
        )
    }

    public func compare(
        _ lhs: RDFLiteral,
        _ rhs: RDFLiteral
    ) throws -> XSDOrder {
        let lhsValue = try parser.parse(lhs)
        let rhsValue = try parser.parse(rhs)
        return try comparison(lhsValue, rhsValue)
    }

    public func isIdenticalValue(
        _ lhs: RDFLiteral,
        _ rhs: RDFLiteral
    ) throws -> Bool {
        let left = try parser.parse(lhs)
        let right = try parser.parse(rhs)
        return try resolveIdentity(left.isIdentical(to: right))
    }

    private func compileNode(
        _ range: OWLDataRange,
        depth: Int,
        state: inout CompilerState
    ) throws -> CompiledOWLDataRange.Node {
        guard depth <= limits.maxDataRangeDepth else {
            throw XSDValidationFailure.resourceLimitExceeded(
                resource: "dataRangeDepth",
                limit: limits.maxDataRangeDepth,
                actual: depth
            )
        }
        state.nodeCount += 1
        guard state.nodeCount <= limits.maxDataRangeNodes else {
            throw XSDValidationFailure.resourceLimitExceeded(
                resource: "dataRangeNodes",
                limit: limits.maxDataRangeNodes,
                actual: state.nodeCount
            )
        }

        switch range {
        case .datatype(let iri):
            return .datatype(try datatypeKind(iri))

        case .dataIntersectionOf(let ranges):
            guard ranges.count >= 2 else {
                throw invalidDataRangeCardinality(
                    constructor: "DataIntersectionOf",
                    minimum: 2,
                    actual: ranges.count
                )
            }
            var nodes: [CompiledOWLDataRange.Node] = []
            nodes.reserveCapacity(ranges.count)
            for child in ranges {
                nodes.append(try compileNode(
                    child,
                    depth: depth + 1,
                    state: &state
                ))
            }
            return .intersection(nodes)

        case .dataUnionOf(let ranges):
            guard ranges.count >= 2 else {
                throw invalidDataRangeCardinality(
                    constructor: "DataUnionOf",
                    minimum: 2,
                    actual: ranges.count
                )
            }
            var nodes: [CompiledOWLDataRange.Node] = []
            nodes.reserveCapacity(ranges.count)
            for child in ranges {
                nodes.append(try compileNode(
                    child,
                    depth: depth + 1,
                    state: &state
                ))
            }
            return .union(nodes)

        case .dataComplementOf(let child):
            return .complement(try compileNode(
                child,
                depth: depth + 1,
                state: &state
            ))

        case .dataOneOf(let literals):
            guard !literals.isEmpty else {
                throw invalidDataRangeCardinality(
                    constructor: "DataOneOf",
                    minimum: 1,
                    actual: 0
                )
            }
            guard literals.count <= limits.maxDataOneOfLiterals else {
                throw XSDValidationFailure.resourceLimitExceeded(
                    resource: "dataOneOfLiterals",
                    limit: limits.maxDataOneOfLiterals,
                    actual: literals.count
                )
            }
            try validateDataOneOfPayload(literals)
            var values: [XSDParsedValue] = []
            values.reserveCapacity(literals.count)
            for literal in literals {
                values.append(try parser.parse(literal))
            }
            return .oneOf(values)

        case .datatypeRestriction(let iri, let facets):
            guard !facets.isEmpty else {
                throw invalidDataRangeCardinality(
                    constructor: "DatatypeRestriction",
                    minimum: 1,
                    actual: 0
                )
            }
            let kind = try datatypeKind(iri)
            state.facetCount += facets.count
            guard state.facetCount <= limits.maxFacetCount else {
                throw XSDValidationFailure.resourceLimitExceeded(
                    resource: "facetCount",
                    limit: limits.maxFacetCount,
                    actual: state.facetCount
                )
            }
            var compiled: [CompiledFacet] = []
            compiled.reserveCapacity(facets.count)
            for facet in facets {
                compiled.append(try compileFacet(facet, for: kind))
            }
            return .restriction(kind, compiled)
        }
    }

    private func compileFacet(
        _ restriction: FacetRestriction,
        for kind: XSDDatatypeKind
    ) throws -> CompiledFacet {
        guard facet(restriction.facet, appliesTo: kind) else {
            throw XSDValidationFailure.invalidRestriction(XSDDiagnostic(
                code: "inapplicableFacet",
                message: "\(restriction.facet.rawValue) does not apply to \(kind.canonicalIRI)"
            ))
        }

        switch restriction.facet {
        case .minInclusive:
            return .minInclusive(try compileBound(restriction.value, for: kind))
        case .maxInclusive:
            return .maxInclusive(try compileBound(restriction.value, for: kind))
        case .minExclusive:
            return .minExclusive(try compileBound(restriction.value, for: kind))
        case .maxExclusive:
            return .maxExclusive(try compileBound(restriction.value, for: kind))
        case .length:
            return .length(try compileNonNegativeInteger(restriction.value))
        case .minLength:
            return .minLength(try compileNonNegativeInteger(restriction.value))
        case .maxLength:
            return .maxLength(try compileNonNegativeInteger(restriction.value))
        case .totalDigits:
            let value = try compileNonNegativeInteger(restriction.value)
            guard value.sign > 0 else {
                throw XSDValidationFailure.invalidRestriction(XSDDiagnostic(
                    code: "totalDigits",
                    message: "totalDigits must be positive"
                ))
            }
            return .totalDigits(value)
        case .fractionDigits:
            return .fractionDigits(try compileNonNegativeInteger(restriction.value))
        case .pattern:
            let parsedPattern: XSDParsedValue
            do {
                parsedPattern = try parser.parse(restriction.value)
            } catch let failure {
                switch failure {
                case .invalidLexicalForm(_, _, let diagnostic):
                    throw XSDValidationFailure.invalidRestriction(diagnostic)
                default:
                    throw failure
                }
            }
            guard case .text(let pattern) = parsedPattern,
                  pattern.kind == .string else {
                throw XSDValidationFailure.invalidRestriction(XSDDiagnostic(
                    code: "patternDatatype",
                    message: "pattern must be an xsd:string literal"
                ))
            }
            do {
                return .pattern(try XSDRegularExpression(
                    pattern: restriction.value.lexicalForm,
                    limits: regularExpressionLimits
                ))
            } catch let error as XSDRegularExpression.Error {
                throw mapRegularExpressionError(error)
            }
        case .whiteSpace:
            throw XSDValidationFailure.invalidRestriction(XSDDiagnostic(
                code: "whiteSpaceFacet",
                message: "whiteSpace is a pre-lexical schema facet, not an OWL runtime restriction"
            ))
        case .langRange:
            let parsedRange: XSDParsedValue
            do {
                parsedRange = try parser.parse(restriction.value)
            } catch let failure {
                switch failure {
                case .invalidLexicalForm(_, _, let diagnostic):
                    throw XSDValidationFailure.invalidRestriction(diagnostic)
                default:
                    throw failure
                }
            }
            guard case .text(let rangeText) = parsedRange,
                  rangeText.kind == .string,
                  let range = RDFLanguageRange(
                    restriction.value.lexicalForm
                  ) else {
                throw XSDValidationFailure.invalidRestriction(XSDDiagnostic(
                    code: "langRange",
                    message: "rdf:langRange requires an xsd:string basic language range"
                ))
            }
            return .languageRange(range)
        }
    }

    private func validateDataOneOfPayload(
        _ literals: [RDFLiteral]
    ) throws {
        var total = 0
        for literal in literals {
            try addDataOneOfPayload(literal.lexicalForm, to: &total)
            try addDataOneOfPayload(
                literal.datatypeIRI.rawValue,
                to: &total
            )
            try addDataOneOfPayload(
                literal.languageTag?.rawValue,
                to: &total
            )
            try addDataOneOfPayload(
                literal.baseDirection?.rawValue,
                to: &total
            )
        }
    }

    private func addDataOneOfPayload(
        _ field: String?,
        to total: inout Int
    ) throws {
        guard let field else { return }
        let (next, overflow) = total.addingReportingOverflow(field.utf8.count)
        let actual = overflow ? Int.max : next
        guard !overflow,
              actual <= limits.maxDataOneOfPayloadUTF8Bytes else {
            throw XSDValidationFailure.resourceLimitExceeded(
                resource: "dataOneOfPayloadUTF8Bytes",
                limit: limits.maxDataOneOfPayloadUTF8Bytes,
                actual: actual
            )
        }
        total = actual
    }

    private func compileBound(
        _ literal: RDFLiteral,
        for kind: XSDDatatypeKind
    ) throws -> XSDParsedValue {
        let value: XSDParsedValue
        do {
            value = try parser.parse(literal)
        } catch let failure {
            switch failure {
            case .invalidLexicalForm(_, _, let diagnostic):
                throw XSDValidationFailure.invalidRestriction(diagnostic)
            default:
                throw failure
            }
        }
        guard valueBelongs(value, to: kind) else {
            throw XSDValidationFailure.invalidRestriction(XSDDiagnostic(
                code: "boundDatatype",
                message: "bound is outside \(kind.canonicalIRI)'s value space"
            ))
        }
        return value
    }

    private func compileNonNegativeInteger(
        _ literal: RDFLiteral
    ) throws -> XSDDecimalValue {
        let value: XSDParsedValue
        do {
            value = try parser.parse(literal)
        } catch let failure {
            switch failure {
            case .invalidLexicalForm(_, _, let diagnostic):
                throw XSDValidationFailure.invalidRestriction(diagnostic)
            default:
                throw failure
            }
        }
        guard case .decimal(_, let decimal) = value,
              decimal.fractionDigits == 0,
              decimal.sign >= 0 else {
            throw XSDValidationFailure.invalidRestriction(XSDDiagnostic(
                code: "facetInteger",
                message: "facet value must be a representable non-negative integer"
            ))
        }
        return decimal
    }

    private func contains(
        literal: RDFLiteral,
        value: XSDParsedValue,
        in node: CompiledOWLDataRange.Node
    ) throws -> DataRangeMembership {
        switch node {
        case .datatype(let kind):
            guard valueBelongs(value, to: kind) else {
                return .notMember(XSDDiagnostic(
                    code: "datatype",
                    message: "value is outside \(kind.canonicalIRI)"
                ))
            }
            return .member

        case .intersection(let children):
            for child in children {
                let result = try contains(literal: literal, value: value, in: child)
                if !result.isMember { return result }
            }
            return .member

        case .union(let children):
            for child in children {
                if try contains(literal: literal, value: value, in: child).isMember {
                    return .member
                }
            }
            return .notMember(XSDDiagnostic(
                code: "union",
                message: "value is outside every union member"
            ))

        case .complement(let child):
            let inner = try contains(literal: literal, value: value, in: child)
            if inner.isMember {
                return .notMember(XSDDiagnostic(
                    code: "complement",
                    message: "value belongs to the complemented range"
                ))
            }
            return .member

        case .oneOf(let allowed):
            for candidate in allowed {
                if try resolveIdentity(value.isIdentical(to: candidate)) {
                    return .member
                }
            }
            return .notMember(XSDDiagnostic(
                code: "oneOf",
                message: "value is not identical to an enumerated value"
            ))

        case .restriction(let kind, let facets):
            guard valueBelongs(value, to: kind) else {
                return .notMember(XSDDiagnostic(
                    code: "restrictionDatatype",
                    message: "value is outside \(kind.canonicalIRI)"
                ))
            }
            for facet in facets {
                let result = try evaluate(
                    facet,
                    literal: literal,
                    value: value
                )
                if !result.isMember { return result }
            }
            return .member
        }
    }

    private func evaluate(
        _ facet: CompiledFacet,
        literal: RDFLiteral,
        value: XSDParsedValue
    ) throws -> DataRangeMembership {
        switch facet {
        case .minInclusive(let bound):
            return try orderedMembership(
                value,
                bound,
                accepted: { $0 == .equal || $0 == .greater },
                code: "minInclusive"
            )
        case .maxInclusive(let bound):
            return try orderedMembership(
                value,
                bound,
                accepted: { $0 == .equal || $0 == .less },
                code: "maxInclusive"
            )
        case .minExclusive(let bound):
            return try orderedMembership(
                value,
                bound,
                accepted: { $0 == .greater },
                code: "minExclusive"
            )
        case .maxExclusive(let bound):
            return try orderedMembership(
                value,
                bound,
                accepted: { $0 == .less },
                code: "maxExclusive"
            )
        case .length(let required):
            return lengthMembership(
                value,
                required: required,
                accepted: { $0 == 0 },
                code: "length"
            )
        case .minLength(let required):
            return lengthMembership(
                value,
                required: required,
                accepted: { $0 <= 0 },
                code: "minLength"
            )
        case .maxLength(let required):
            return lengthMembership(
                value,
                required: required,
                accepted: { $0 >= 0 },
                code: "maxLength"
            )
        case .pattern(let expression):
            do {
                let input: Substring
                if case .text(let text) = value {
                    input = text.value
                } else {
                    input = literal.lexicalForm[...]
                }
                guard try expression.wholeMatch(input) else {
                    return .notMember(XSDDiagnostic(
                        code: "pattern",
                        message: "lexical form does not match the XSD pattern"
                    ))
                }
                return .member
            } catch let error as XSDRegularExpression.Error {
                throw mapRegularExpressionError(error)
            }
        case .totalDigits(let maximum):
            guard let decimal = value.decimalValue else {
                throw internalRestrictionFailure("totalDigits requires decimal")
            }
            return maximum.compare(toNonNegativeInt: decimal.totalDigits) >= 0
                ? .member
                : .notMember(XSDDiagnostic(
                    code: "totalDigits",
                    message: "value has \(decimal.totalDigits) total digits; maximum is \(maximum.source)"
                ))
        case .fractionDigits(let maximum):
            guard let decimal = value.decimalValue else {
                throw internalRestrictionFailure("fractionDigits requires decimal")
            }
            return maximum.compare(toNonNegativeInt: decimal.fractionDigits) >= 0
                ? .member
                : .notMember(XSDDiagnostic(
                    code: "fractionDigits",
                    message: "value has \(decimal.fractionDigits) fraction digits; maximum is \(maximum.source)"
                ))
        case .languageRange(let range):
            guard case .text(let text) = value,
                  text.kind == .rdfPlainLiteral else {
                throw internalRestrictionFailure(
                    "rdf:langRange requires rdf:PlainLiteral"
                )
            }
            return range.matches(text.language)
                ? .member
                : .notMember(XSDDiagnostic(
                    code: "langRange",
                    message: "language tag does not match the basic language range"
                ))
        }
    }

    private func orderedMembership(
        _ value: XSDParsedValue,
        _ bound: XSDParsedValue,
        accepted: (XSDOrder) -> Bool,
        code: String
    ) throws -> DataRangeMembership {
        let order = try comparison(value, bound)
        guard order != .unordered, accepted(order) else {
            return .notMember(XSDDiagnostic(
                code: code,
                message: order == .unordered
                    ? "values are unordered"
                    : "ordered facet is not satisfied"
            ))
        }
        return .member
    }

    private func lengthMembership(
        _ value: XSDParsedValue,
        required: XSDDecimalValue,
        accepted: (Int) -> Bool,
        code: String
    ) -> DataRangeMembership {
        guard let length = value.length else {
            return .notMember(XSDDiagnostic(
                code: code,
                message: "value has no XSD length measure"
            ))
        }
        return accepted(required.compare(toNonNegativeInt: length))
            ? .member
            : .notMember(XSDDiagnostic(
                code: code,
                message: "length facet is not satisfied"
            ))
    }

    private func comparison(
        _ lhs: XSDParsedValue,
        _ rhs: XSDParsedValue
    ) throws -> XSDOrder {
        switch lhs.compareForFacet(to: rhs) {
        case .success(let order):
            return order
        case .failure(.duration(.arithmeticLimit)):
            throw XSDValidationFailure.resourceLimitExceeded(
                resource: "durationArithmetic",
                limit: limits.maxDurationComponentDigits,
                actual: limits.maxDurationComponentDigits
            )
        case .failure(.duration(.componentLimit(let actual, let limit))):
            throw XSDValidationFailure.resourceLimitExceeded(
                resource: "durationComponentDigits",
                limit: limit,
                actual: actual
            )
        case .failure(.duration(.invalid)):
            throw internalRestrictionFailure("validated duration became invalid")
        case .failure(.rationalWork(let limit, let actual)):
            throw XSDValidationFailure.resourceLimitExceeded(
                resource: "rationalComparisonWork",
                limit: limit,
                actual: actual
            )
        case .failure(.xmlWork(let limit, let actual)):
            throw XSDValidationFailure.resourceLimitExceeded(
                resource: "xmlComparisonWork",
                limit: limit,
                actual: actual
            )
        }
    }

    private func resolveIdentity(
        _ result: Result<Bool, XSDValueComparisonFailure>
    ) throws -> Bool {
        switch result {
        case .success(let identical):
            return identical
        case .failure(.rationalWork(let limit, let actual)):
            throw XSDValidationFailure.resourceLimitExceeded(
                resource: "rationalComparisonWork",
                limit: limit,
                actual: actual
            )
        case .failure(.duration):
            throw internalRestrictionFailure(
                "duration identity unexpectedly required comparison work"
            )
        case .failure(.xmlWork(let limit, let actual)):
            throw XSDValidationFailure.resourceLimitExceeded(
                resource: "xmlComparisonWork",
                limit: limit,
                actual: actual
            )
        }
    }

    private func valueBelongs(
        _ value: XSDParsedValue,
        to expected: XSDDatatypeKind
    ) -> Bool {
        switch (value, expected) {
        case (_, .rdfsLiteral):
            return true
        case (.text(let text), .string):
            guard text.kind.isStringFamily else { return false }
            return XSDUnicodeRules.allXMLCharacters(text.value)
        case (.text(let text), .normalizedString):
            guard text.kind.isStringFamily else { return false }
            return XSDUnicodeRules.isNormalizedString(text.value)
        case (.text(let text), .token):
            guard text.kind.isStringFamily else { return false }
            return XSDUnicodeRules.isToken(text.value)
        case (.text(let text), .language):
            guard text.kind.isStringFamily else { return false }
            return XSDUnicodeRules.isLanguage(text.value)
        case (.text(let text), .nmtoken):
            guard text.kind.isStringFamily else { return false }
            return XSDUnicodeRules.isNMTOKEN(text.value)
        case (.text(let text), .name):
            guard text.kind.isStringFamily else { return false }
            return XSDUnicodeRules.isName(text.value, allowsColon: true)
        case (.text(let text), .ncname):
            guard text.kind.isStringFamily else { return false }
            return XSDUnicodeRules.isName(text.value, allowsColon: false)
        case (.text(let text), .anyURI):
            return text.kind == .anyURI
        case (.text(let text), .rdfLangString):
            return text.kind == .rdfLangString && text.language != nil
        case (.text(let text), .rdfPlainLiteral):
            return text.kind == .rdfPlainLiteral
                || text.kind == .rdfLangString
                || (text.kind.isStringFamily && text.language == nil)

        case (.xmlLiteral, .rdfXMLLiteral):
            return true

        case (.boolean, .boolean):
            return true

        case (.decimal, .decimal):
            return true
        case (.decimal, .owlRational), (.decimal, .owlReal):
            return true
        case (.rational, .owlRational), (.rational, .owlReal):
            return true
        case (.decimal(_, let decimal), let integer) where integer.isInteger:
            return decimal.fractionDigits == 0
                && integerValue(decimal, belongsTo: integer)

        case (.floating(let actual, _), .float):
            return actual == .float
        case (.floating(let actual, _), .double):
            return actual == .double

        case (.duration, .duration):
            return true

        case (.temporal(let actual, let temporal), .dateTime):
            return (actual == .dateTime || actual == .dateTimeStamp)
                && temporal.kind == .dateTime
        case (.temporal(let actual, let temporal), .dateTimeStamp):
            return (actual == .dateTime || actual == .dateTimeStamp)
                && temporal.kind == .dateTime
                && temporal.timezoneOffsetMinutes != nil
        case (.temporal(let actual, _), .date):
            return actual == .date
        case (.temporal(let actual, _), .time):
            return actual == .time

        case (.binary(let binary), .hexBinary):
            return binary.kind == .hexadecimal
        case (.binary(let binary), .base64Binary):
            return binary.kind == .base64
        default:
            return false
        }
    }

    private func invalidDataRangeCardinality(
        constructor: String,
        minimum: Int,
        actual: Int
    ) -> XSDValidationFailure {
        .invalidRestriction(XSDDiagnostic(
            code: "dataRangeCardinality",
            message: "\(constructor) requires at least \(minimum) operand(s); received \(actual)"
        ))
    }

    private func integerValue(
        _ value: XSDDecimalValue,
        belongsTo kind: XSDDatatypeKind
    ) -> Bool {
        switch kind {
        case .integer:
            return true
        case .nonPositiveInteger:
            return value.sign <= 0
        case .negativeInteger:
            return value.sign < 0
        case .nonNegativeInteger:
            return value.sign >= 0
        case .positiveInteger:
            return value.sign > 0
        case .long:
            return value.isWithin(minimum: "-9223372036854775808", maximum: "9223372036854775807")
        case .int:
            return value.isWithin(minimum: "-2147483648", maximum: "2147483647")
        case .short:
            return value.isWithin(minimum: "-32768", maximum: "32767")
        case .byte:
            return value.isWithin(minimum: "-128", maximum: "127")
        case .unsignedLong:
            return value.isWithin(minimum: "0", maximum: "18446744073709551615")
        case .unsignedInt:
            return value.isWithin(minimum: "0", maximum: "4294967295")
        case .unsignedShort:
            return value.isWithin(minimum: "0", maximum: "65535")
        case .unsignedByte:
            return value.isWithin(minimum: "0", maximum: "255")
        default:
            return false
        }
    }

    private func facet(
        _ facet: XSDFacet,
        appliesTo kind: XSDDatatypeKind
    ) -> Bool {
        switch profile {
        case .owl2, .owl2RDF11:
            switch facet {
            case .minInclusive, .maxInclusive, .minExclusive, .maxExclusive:
                return kind.isNumeric || kind == .dateTime || kind == .dateTimeStamp
            case .length, .minLength, .maxLength:
                return isStringLengthKind(kind) || isBinaryKind(kind)
            case .pattern:
                return isStringLengthKind(kind)
            case .totalDigits, .fractionDigits, .whiteSpace:
                return false
            case .langRange:
                return kind == .rdfPlainLiteral
            }
        case .extendedXSD11:
            switch facet {
            case .minInclusive, .maxInclusive, .minExclusive, .maxExclusive:
                return kind.isNumeric || kind == .duration
                    || kind == .dateTime || kind == .dateTimeStamp
                    || kind == .date || kind == .time
            case .length, .minLength, .maxLength:
                return isStringLengthKind(kind) || isBinaryKind(kind)
            case .pattern:
                return true
            case .totalDigits, .fractionDigits:
                return kind == .decimal || kind.isInteger
            case .whiteSpace:
                return false
            case .langRange:
                return kind == .rdfPlainLiteral
            }
        }
    }

    private func isStringLengthKind(_ kind: XSDDatatypeKind) -> Bool {
        switch kind {
        case .string, .normalizedString, .token, .language,
             .nmtoken, .name, .ncname, .anyURI, .rdfPlainLiteral:
            return true
        default:
            return false
        }
    }

    private func isBinaryKind(_ kind: XSDDatatypeKind) -> Bool {
        kind == .hexBinary || kind == .base64Binary
    }

    private func datatypeKind(_ iri: String) throws -> XSDDatatypeKind {
        guard let kind = XSDDatatypeKind(iri: iri), profile.supports(kind) else {
            throw XSDValidationFailure.unsupportedDatatype(iri)
        }
        return kind
    }

    private var regularExpressionLimits: XSDRegularExpression.Limits {
        XSDRegularExpression.Limits(
            patternUTF8Bytes: limits.maxPatternUTF8Bytes,
            patternScalars: limits.maxPatternScalars,
            nestingDepth: limits.maxRegexNestingDepth,
            astNodes: limits.maxRegexASTNodes,
            nfaStates: limits.maxRegexNFAStates,
            quantifier: limits.maxRegexQuantifier,
            activeTransitionWork: limits.maxRegexTransitionWork
        )
    }

    private func mapRegularExpressionError(
        _ error: XSDRegularExpression.Error
    ) -> XSDValidationFailure {
        switch error {
        case .invalidSyntax(let offset, let reason):
            return .invalidRestriction(XSDDiagnostic(
                code: "patternSyntax",
                message: "invalid XSD pattern at scalar offset \(offset): \(reason)"
            ))
        case .resourceLimit(let name, let limit, let actual):
            return .resourceLimitExceeded(
                resource: "regex.\(name)",
                limit: limit,
                actual: actual
            )
        }
    }

    private func internalRestrictionFailure(
        _ message: String
    ) -> XSDValidationFailure {
        .invalidRestriction(XSDDiagnostic(
            code: "compiledRestrictionInvariant",
            message: message
        ))
    }

    private struct CompilerState {
        var nodeCount = 0
        var facetCount = 0
    }
}
