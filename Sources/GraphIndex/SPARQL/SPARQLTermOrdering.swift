import DatabaseKit
import DatabaseTypes
import OntologyIndex

enum SPARQLTermOrdering {
    static func compare(
        _ left: FieldValue,
        _ right: FieldValue
    ) throws(SPARQLExpressionEvaluationError) -> SPARQLComparisonOrder {
        if case .rdfTerm(let leftTerm) = left,
           case .rdfTerm(let rightTerm) = right {
            return try compare(leftTerm, rightTerm)
        }
        if let leftNumeric = SPARQLNumericValue(left),
           let rightNumeric = SPARQLNumericValue(right),
           let comparison = leftNumeric.compare(to: rightNumeric) {
            return comparison
        }
        if left == right { return .same }
        return left < right ? .ascending : .descending
    }

    static func compare(
        _ left: RDFTerm,
        _ right: RDFTerm
    ) throws(SPARQLExpressionEvaluationError) -> SPARQLComparisonOrder {
        let leftRank = rank(left)
        let rightRank = rank(right)
        if leftRank != rightRank {
            return leftRank < rightRank ? .ascending : .descending
        }

        switch (left, right) {
        case (.blankNode(let lhs), .blankNode(let rhs)):
            return compareStrings(lhs.rawValue, rhs.rawValue)
        case (.iri(let lhs), .iri(let rhs)):
            return compareStrings(lhs.rawValue, rhs.rawValue)

        case (.literal(let lhs), .literal(let rhs)):
            let comparison: SPARQLValueComparison
            do throws(XSDValidationFailure) {
                comparison = try SPARQLValueComparator().compare(lhs, rhs)
            } catch let failure {
                throw mapValidationFailure(failure)
            }
            switch comparison {
            case .less: return .ascending
            case .equal: return .same
            case .greater: return .descending
            case .unordered, .typeError:
                if lhs.annotation != rhs.annotation {
                    return lhs.annotation < rhs.annotation
                        ? .ascending
                        : .descending
                }
                return compareStrings(lhs.lexicalForm, rhs.lexicalForm)
            }

        case (
            .tripleTerm(let leftSubject, let leftPredicate, let leftObject),
            .tripleTerm(let rightSubject, let rightPredicate, let rightObject)
        ):
            let subject = try compare(leftSubject.term, rightSubject.term)
            if subject != .same { return subject }
            let predicate = try compare(
                leftPredicate.term,
                rightPredicate.term
            )
            if predicate != .same { return predicate }
            return try compare(leftObject, rightObject)

        default:
            throw SPARQLExpressionEvaluationError.runtimeInvariant(
                "equal RDF term ranks had different cases"
            )
        }
    }

    private static func rank(_ term: RDFTerm) -> UInt8 {
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
    ) -> SPARQLComparisonOrder {
        if left < right { return .ascending }
        if left > right { return .descending }
        return .same
    }

    private static func mapValidationFailure(
        _ failure: XSDValidationFailure
    ) -> SPARQLExpressionEvaluationError {
        switch failure {
        case .resourceLimitExceeded(let resource, let limit, let actual):
            return .resourceLimitExceeded(
                stage: resource,
                required: UInt64(max(0, actual)),
                maximum: UInt64(max(0, limit))
            )
        case .invalidLexicalForm, .unsupportedDatatype:
            return .typeError(failure.description)
        case .invalidRestriction:
            return .runtimeInvariant(failure.description)
        }
    }
}
