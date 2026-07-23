/// Parsed XSD value used by data-range membership and facet evaluation.
package enum XSDParsedValue: Sendable {
    case text(XSDTextValue)
    case boolean(Bool)
    case decimal(kind: XSDDatatypeKind, value: XSDDecimalValue)
    case rational(XSDRationalValue)
    case floating(kind: XSDDatatypeKind, value: XSDFloatingPointValue)
    case duration(XSDDurationValue)
    case temporal(kind: XSDDatatypeKind, value: XSDTemporalValue)
    case binary(XSDBinaryValue)
    case xmlLiteral(XSDXMLLiteralValue)

    package var kind: XSDDatatypeKind {
        switch self {
        case .text(let value):
            value.kind
        case .decimal(let kind, _),
             .floating(let kind, _),
             .temporal(let kind, _):
            kind
        case .boolean:
            .boolean
        case .duration:
            .duration
        case .rational:
            .owlRational
        case .binary(let value):
            value.kind == .hexadecimal ? .hexBinary : .base64Binary
        case .xmlLiteral:
            .rdfXMLLiteral
        }
    }

    package var length: Int? {
        switch self {
        case .text(let value):
            value.length
        case .binary(let value):
            value.octetCount
        default:
            nil
        }
    }

    package var decimalValue: XSDDecimalValue? {
        if case .decimal(_, let value) = self { return value }
        return nil
    }

    package func compareForFacet(
        to other: XSDParsedValue
    ) -> Result<XSDOrder, XSDValueComparisonFailure> {
        switch (self, other) {
        case (.decimal(_, let lhs), .decimal(_, let rhs)):
            return .success(Self.order(lhs.compare(to: rhs)))

        case (.rational(let lhs), .rational(let rhs)):
            return Self.rationalOrder(lhs.compare(to: rhs))
        case (.rational(let lhs), .decimal(_, let rhs)):
            return Self.rationalOrder(lhs.compare(to: rhs))
        case (.decimal(_, let lhs), .rational(let rhs)):
            return Self.rationalOrder(rhs.compare(to: lhs)).map {
                Self.reversed($0)
            }

        case (.floating(let lhsKind, let lhs),
              .floating(let rhsKind, let rhs)) where lhsKind == rhsKind:
            if lhs.doubleValue.isNaN || rhs.doubleValue.isNaN {
                return .success(.unordered)
            }
            if lhs.doubleValue < rhs.doubleValue { return .success(.less) }
            if lhs.doubleValue > rhs.doubleValue { return .success(.greater) }
            return .success(.equal)

        case (.duration(let lhs), .duration(let rhs)):
            return lhs.compare(to: rhs).mapError {
                .duration($0)
            }

        case (.temporal(let lhsKind, let lhs),
              .temporal(let rhsKind, let rhs))
            where Self.temporalKindsAreComparable(lhsKind, rhsKind):
            guard let comparison = lhs.compare(to: rhs) else {
                return .success(.unordered)
            }
            return .success(Self.order(comparison))

        default:
            return .success(.unordered)
        }
    }

    package func isIdentical(
        to other: XSDParsedValue
    ) -> Result<Bool, XSDValueComparisonFailure> {
        switch (self, other) {
        case (.text(let lhs), .text(let rhs)):
            guard Self.textKindsShareValueSpace(lhs.kind, rhs.kind) else {
                return .success(false)
            }
            return .success(lhs.isIdentical(to: rhs))

        case (.boolean(let lhs), .boolean(let rhs)):
            return .success(lhs == rhs)

        case (.decimal(_, let lhs), .decimal(_, let rhs)):
            return .success(lhs.compare(to: rhs) == 0)

        case (.rational(let lhs), .rational(let rhs)):
            return Self.rationalIdentity(lhs.compare(to: rhs))
        case (.rational(let lhs), .decimal(_, let rhs)):
            return Self.rationalIdentity(lhs.compare(to: rhs))
        case (.decimal(_, let lhs), .rational(let rhs)):
            return Self.rationalIdentity(rhs.compare(to: lhs))

        case (.floating(let lhsKind, let lhs),
              .floating(let rhsKind, let rhs)) where lhsKind == rhsKind:
            switch (lhs, rhs) {
            case (.nan, .nan),
                 (.positiveInfinity, .positiveInfinity),
                 (.negativeInfinity, .negativeInfinity):
                return .success(true)
            case (.finite(let lhsValue), .finite(let rhsValue)):
                if lhsKind == .float {
                    return .success(
                        Float(lhsValue).bitPattern == Float(rhsValue).bitPattern
                    )
                }
                return .success(lhsValue.bitPattern == rhsValue.bitPattern)
            default:
                return .success(false)
            }

        case (.duration(let lhs), .duration(let rhs)):
            return .success(lhs.isIdentical(to: rhs))

        case (.temporal(let lhsKind, let lhs),
              .temporal(let rhsKind, let rhs))
            where Self.temporalKindsAreComparable(lhsKind, rhsKind):
            return .success(lhs.isIdentical(to: rhs))

        case (.binary(let lhs), .binary(let rhs)):
            return .success(lhs.isIdentical(to: rhs))

        case (.xmlLiteral(let lhs), .xmlLiteral(let rhs)):
            return lhs.isIdentical(to: rhs).mapError { failure in
                switch failure {
                case .workLimit(let limit, let actual):
                    return .xmlWork(limit: limit, actual: actual)
                }
            }

        default:
            return .success(false)
        }
    }

    private static func rationalOrder(
        _ result: Result<Int, XSDRationalValue.ComparisonFailure>
    ) -> Result<XSDOrder, XSDValueComparisonFailure> {
        result
            .map(Self.order)
            .mapError { failure in
                switch failure {
                case .workLimit(let limit, let actual):
                    return .rationalWork(limit: limit, actual: actual)
                }
            }
    }

    private static func rationalIdentity(
        _ result: Result<Int, XSDRationalValue.ComparisonFailure>
    ) -> Result<Bool, XSDValueComparisonFailure> {
        result
            .map { $0 == 0 }
            .mapError { failure in
                switch failure {
                case .workLimit(let limit, let actual):
                    return .rationalWork(limit: limit, actual: actual)
                }
            }
    }

    private static func reversed(_ order: XSDOrder) -> XSDOrder {
        switch order {
        case .less: .greater
        case .equal: .equal
        case .greater: .less
        case .unordered: .unordered
        }
    }

    private static func temporalKindsAreComparable(
        _ lhs: XSDDatatypeKind,
        _ rhs: XSDDatatypeKind
    ) -> Bool {
        if lhs == rhs { return true }
        switch (lhs, rhs) {
        case (.dateTime, .dateTimeStamp), (.dateTimeStamp, .dateTime):
            return true
        default:
            return false
        }
    }

    private static func textKindsShareValueSpace(
        _ lhs: XSDDatatypeKind,
        _ rhs: XSDDatatypeKind
    ) -> Bool {
        if lhs == rhs { return true }
        if isStringKind(lhs) && isStringKind(rhs) { return true }
        if lhs == .rdfPlainLiteral {
            return isStringKind(rhs) || rhs == .rdfLangString
        }
        if rhs == .rdfPlainLiteral {
            return isStringKind(lhs) || lhs == .rdfLangString
        }
        return false
    }

    private static func isStringKind(_ kind: XSDDatatypeKind) -> Bool {
        switch kind {
        case .string, .normalizedString, .token, .language,
             .nmtoken, .name, .ncname:
            return true
        default:
            return false
        }
    }

    private static func order(_ comparison: Int) -> XSDOrder {
        if comparison < 0 { return .less }
        if comparison > 0 { return .greater }
        return .equal
    }
}
