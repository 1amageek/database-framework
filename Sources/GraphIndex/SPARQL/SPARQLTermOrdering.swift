import Core
import DatabaseValue
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

enum SPARQLTermOrdering {
    static func compare(
        _ left: FieldValue,
        _ right: FieldValue
    ) throws -> ComparisonResult {
        if case .rdfTerm(let leftTerm) = left,
           case .rdfTerm(let rightTerm) = right {
            return try compare(leftTerm, rightTerm)
        }
        if let leftNumeric = SPARQLNumericValue(left),
           let rightNumeric = SPARQLNumericValue(right),
           let comparison = leftNumeric.compare(to: rightNumeric) {
            return comparison
        }
        if left == right { return .orderedSame }
        return left < right ? .orderedAscending : .orderedDescending
    }

    static func compare(
        _ left: DatabaseRDFTerm,
        _ right: DatabaseRDFTerm
    ) throws -> ComparisonResult {
        let leftRank = rank(left)
        let rightRank = rank(right)
        if leftRank != rightRank {
            return leftRank < rightRank ? .orderedAscending : .orderedDescending
        }

        switch (left, right) {
        case (.blankNode(let lhs), .blankNode(let rhs)),
             (.iri(let lhs), .iri(let rhs)):
            return compareStrings(lhs, rhs)

        case (.literal(let lhs), .literal(let rhs)):
            switch try SPARQLValueComparator().compare(lhs, rhs) {
            case .less: return .orderedAscending
            case .equal: return .orderedSame
            case .greater: return .orderedDescending
            case .unordered, .typeError:
                if lhs.annotation != rhs.annotation {
                    return lhs.annotation < rhs.annotation
                        ? .orderedAscending
                        : .orderedDescending
                }
                return compareStrings(lhs.lexicalForm, rhs.lexicalForm)
            }

        case (
            .tripleTerm(let leftSubject, let leftPredicate, let leftObject),
            .tripleTerm(let rightSubject, let rightPredicate, let rightObject)
        ):
            let subject = try compare(leftSubject, rightSubject)
            if subject != .orderedSame { return subject }
            let predicate = try compare(leftPredicate, rightPredicate)
            if predicate != .orderedSame { return predicate }
            return try compare(leftObject, rightObject)

        default:
            throw SPARQLExpressionEvaluationError.runtimeInvariant(
                "equal RDF term ranks had different cases"
            )
        }
    }

    private static func rank(_ term: DatabaseRDFTerm) -> UInt8 {
        switch term {
        case .blankNode: return 0
        case .iri: return 1
        case .literal: return 2
        case .tripleTerm: return 3
        }
    }

    private static func compareStrings(
        _ left: String,
        _ right: String
    ) -> ComparisonResult {
        if left < right { return .orderedAscending }
        if left > right { return .orderedDescending }
        return .orderedSame
    }
}
