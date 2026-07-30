import DatabaseTypes
import OntologyIndex

/// Applies SPARQL 1.1 value-comparison semantics to canonical RDF literals.
///
/// Parsing delegates to the bounded XSD value parser. Numeric comparison then
/// applies XPath numeric promotion without materializing digit arrays. Resource
/// exhaustion remains a thrown runtime failure; invalid or incomparable RDF
/// operands become a SPARQL type error.
struct SPARQLValueComparator: Sendable {
    private let parser: XSDValueParser

    init(limits: XSDValidationLimits = .default) {
        parser = XSDValueParser(profile: .extendedXSD11, limits: limits)
    }

    static func supportsLexicalValidation(datatype: String) -> Bool {
        guard let kind = XSDDatatypeKind(iri: datatype) else { return false }
        return kind.isNumeric
            || kind == .string
            || kind == .boolean
            || kind == .dateTime
            || kind == .dateTimeStamp
    }

    func validateLexicalForm(
        _ literal: RDFLiteral
    ) throws(XSDValidationFailure) -> Bool {
        guard Self.supportsLexicalValidation(
            datatype: literal.datatypeIRI.rawValue
        ) else {
            return true
        }
        switch try parse(literal) {
        case .success:
            return true
        case .typeError:
            return false
        }
    }

    func compare(
        _ lhs: RDFLiteral,
        _ rhs: RDFLiteral
    ) throws(XSDValidationFailure) -> SPARQLValueComparison {
        let lhsResult = try parse(lhs)
        let rhsResult = try parse(rhs)
        guard case .success(let lhsValue) = lhsResult,
              case .success(let rhsValue) = rhsResult else {
            return .typeError
        }

        if lhsValue.kind.isNumeric, rhsValue.kind.isNumeric {
            return compareNumeric(lhsValue, rhsValue)
        }

        switch (lhsValue, rhsValue) {
        case (.text(let lhsText), .text(let rhsText))
            where lhsText.kind == .string
                && rhsText.kind == .string
                && lhsText.language == nil
                && rhsText.language == nil:
            return compareCodePoints(lhsText.value, rhsText.value)

        case (.boolean(let lhsBoolean), .boolean(let rhsBoolean)):
            if lhsBoolean == rhsBoolean { return .equal }
            return lhsBoolean ? .greater : .less

        case (.temporal(let lhsKind, let lhsTemporal),
              .temporal(let rhsKind, let rhsTemporal))
            where isDateTime(kind: lhsKind) && isDateTime(kind: rhsKind):
            guard let order = lhsTemporal.compare(to: rhsTemporal) else {
                return .unordered
            }
            return order < 0 ? .less : order > 0 ? .greater : .equal

        default:
            return .typeError
        }
    }

    private enum ParseResult {
        case success(XSDParsedValue)
        case typeError
    }

    private func parse(
        _ literal: RDFLiteral
    ) throws(XSDValidationFailure) -> ParseResult {
        do throws(XSDValidationFailure) {
            return .success(try parser.parse(literal))
        } catch let failure {
            switch failure {
            case .invalidLexicalForm, .unsupportedDatatype:
                return .typeError
            case .resourceLimitExceeded, .invalidRestriction:
                throw failure
            }
        }
    }

    private func compareNumeric(
        _ lhs: XSDParsedValue,
        _ rhs: XSDParsedValue
    ) -> SPARQLValueComparison {
        let promotion = max(numericRank(lhs), numericRank(rhs))
        switch promotion {
        case 3:
            guard let left = doubleValue(lhs), let right = doubleValue(rhs) else {
                return .typeError
            }
            return compareFloating(left, right)
        case 2:
            guard let left = floatValue(lhs), let right = floatValue(rhs) else {
                return .typeError
            }
            return compareFloating(left, right)
        default:
            guard case .decimal(_, let left) = lhs,
                  case .decimal(_, let right) = rhs else {
                return .typeError
            }
            let order = left.compare(to: right)
            return order < 0 ? .less : order > 0 ? .greater : .equal
        }
    }

    private func numericRank(_ value: XSDParsedValue) -> Int {
        switch value {
        case .decimal(let kind, _):
            return kind == .decimal ? 1 : 0
        case .floating(let kind, _):
            return kind == .double ? 3 : 2
        default:
            return -1
        }
    }

    private func doubleValue(_ value: XSDParsedValue) -> Double? {
        switch value {
        case .decimal(_, let decimal):
            return Double(decimal.source)
        case .floating(_, let floating):
            return floating.doubleValue
        default:
            return nil
        }
    }

    private func floatValue(_ value: XSDParsedValue) -> Float? {
        switch value {
        case .decimal(_, let decimal):
            return Float(decimal.source)
        case .floating(_, let floating):
            return Float(floating.doubleValue)
        default:
            return nil
        }
    }

    private func compareFloating<T: BinaryFloatingPoint>(
        _ lhs: T,
        _ rhs: T
    ) -> SPARQLValueComparison {
        guard !lhs.isNaN, !rhs.isNaN else { return .unordered }
        if lhs < rhs { return .less }
        if lhs > rhs { return .greater }
        return .equal
    }

    private func compareCodePoints<Left: StringProtocol, Right: StringProtocol>(
        _ lhs: Left,
        _ rhs: Right
    ) -> SPARQLValueComparison {
        var left = lhs.unicodeScalars.makeIterator()
        var right = rhs.unicodeScalars.makeIterator()
        while true {
            switch (left.next(), right.next()) {
            case (.none, .none):
                return .equal
            case (.none, .some):
                return .less
            case (.some, .none):
                return .greater
            case (.some(let leftScalar), .some(let rightScalar)):
                if leftScalar.value < rightScalar.value { return .less }
                if leftScalar.value > rightScalar.value { return .greater }
            }
        }
    }

    private func isDateTime(kind: XSDDatatypeKind) -> Bool {
        kind == .dateTime || kind == .dateTimeStamp
    }
}
