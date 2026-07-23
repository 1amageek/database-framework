import DatabaseEngine
import DatabaseValue

/// Performs endpoint-pattern compatibility checks without allocating a binding
/// Dictionary or a retained scratch collection. Repeated variables are checked
/// by rescanning the two endpoint trees, trading bounded work for zero retained
/// allocation on the final result-limit path.
enum SPARQLPropertyPathEndpointCompatibility {
    static func matches(
        subject: ExecutionTerm,
        start: DatabaseRDFTerm,
        object: ExecutionTerm,
        end: DatabaseRDFTerm,
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        try termMatches(
            subject,
            start,
            rootSubject: subject,
            rootStart: start,
            rootObject: object,
            rootEnd: end,
            workMeter: workMeter
        ) && termMatches(
            object,
            end,
            rootSubject: subject,
            rootStart: start,
            rootObject: object,
            rootEnd: end,
            workMeter: workMeter
        )
    }

    private static func termMatches(
        _ pattern: ExecutionTerm,
        _ term: DatabaseRDFTerm,
        rootSubject: ExecutionTerm,
        rootStart: DatabaseRDFTerm,
        rootObject: ExecutionTerm,
        rootEnd: DatabaseRDFTerm,
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        try workMeter.consume(at: .pathExpansion)
        switch pattern {
        case .variable(let variable):
            return try occurrencesMatch(
                variable: variable,
                expected: term,
                pattern: rootSubject,
                term: rootStart,
                workMeter: workMeter
            ) && occurrencesMatch(
                variable: variable,
                expected: term,
                pattern: rootObject,
                term: rootEnd,
                workMeter: workMeter
            )
        case .value(let value):
            guard case .rdfTerm(let expected) = value else { return false }
            return try rdfTermsEqual(
                expected,
                term,
                workMeter: workMeter
            )
        case .wildcard:
            return true
        case .tripleTerm(let subject, let predicate, let object):
            guard case .tripleTerm(
                let storedSubject,
                let storedPredicate,
                let storedObject
            ) = term else {
                return false
            }
            return try termMatches(
                subject,
                storedSubject,
                rootSubject: rootSubject,
                rootStart: rootStart,
                rootObject: rootObject,
                rootEnd: rootEnd,
                workMeter: workMeter
            ) && termMatches(
                predicate,
                storedPredicate,
                rootSubject: rootSubject,
                rootStart: rootStart,
                rootObject: rootObject,
                rootEnd: rootEnd,
                workMeter: workMeter
            ) && termMatches(
                object,
                storedObject,
                rootSubject: rootSubject,
                rootStart: rootStart,
                rootObject: rootObject,
                rootEnd: rootEnd,
                workMeter: workMeter
            )
        }
    }

    private static func occurrencesMatch(
        variable: String,
        expected: DatabaseRDFTerm,
        pattern: ExecutionTerm,
        term: DatabaseRDFTerm,
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        try workMeter.consume(at: .deduplication)
        switch pattern {
        case .variable(let candidate):
            guard candidate == variable else { return true }
            return try rdfTermsEqual(
                expected,
                term,
                workMeter: workMeter
            )
        case .value, .wildcard:
            return true
        case .tripleTerm(let subject, let predicate, let object):
            guard case .tripleTerm(
                let storedSubject,
                let storedPredicate,
                let storedObject
            ) = term else {
                return false
            }
            return try occurrencesMatch(
                variable: variable,
                expected: expected,
                pattern: subject,
                term: storedSubject,
                workMeter: workMeter
            ) && occurrencesMatch(
                variable: variable,
                expected: expected,
                pattern: predicate,
                term: storedPredicate,
                workMeter: workMeter
            ) && occurrencesMatch(
                variable: variable,
                expected: expected,
                pattern: object,
                term: storedObject,
                workMeter: workMeter
            )
        }
    }

    private static func rdfTermsEqual(
        _ lhs: DatabaseRDFTerm,
        _ rhs: DatabaseRDFTerm,
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        try workMeter.consume(at: .deduplication)
        switch (lhs, rhs) {
        case (.iri(let lhs), .iri(let rhs)),
             (.blankNode(let lhs), .blankNode(let rhs)):
            return lhs == rhs
        case (.literal(let lhs), .literal(let rhs)):
            return lhs == rhs
        case (
            .tripleTerm(let lhsSubject, let lhsPredicate, let lhsObject),
            .tripleTerm(let rhsSubject, let rhsPredicate, let rhsObject)
        ):
            return try rdfTermsEqual(
                lhsSubject,
                rhsSubject,
                workMeter: workMeter
            ) && rdfTermsEqual(
                lhsPredicate,
                rhsPredicate,
                workMeter: workMeter
            ) && rdfTermsEqual(
                lhsObject,
                rhsObject,
                workMeter: workMeter
            )
        default:
            return false
        }
    }
}
