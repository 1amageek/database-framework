import QueryIR

struct SPARQLReifiedTripleExpansionPlan: Sendable, Equatable {
    let supplementalTripleCount: Int
    let totalTripleCount: Int

    static func basicGraphPattern(
        _ pattern: borrowing QueryIR.BasicGraphPattern
    ) throws -> SPARQLReifiedTripleExpansionPlan {
        var terms: [QueryIR.SPARQLTerm] = []
        terms.reserveCapacity(64)
        var supplementalTripleCount = 0
        var sourceTripleCount = 0

        for element in pattern.elements {
            switch element {
            case .triple(let triple):
                let (nextCount, overflow) = sourceTripleCount
                    .addingReportingOverflow(1)
                guard !overflow else { throw overflowError() }
                sourceTripleCount = nextCount
                terms.append(triple.object)
                terms.append(triple.predicate)
                terms.append(triple.subject)
            case .propertyPath(let pathPattern):
                terms.append(pathPattern.object)
                terms.append(pathPattern.subject)
            }
            try consumeTerms(
                &terms,
                supplementalTripleCount: &supplementalTripleCount
            )
        }

        let (totalTripleCount, overflow) = sourceTripleCount
            .addingReportingOverflow(supplementalTripleCount)
        guard !overflow else { throw overflowError() }
        return SPARQLReifiedTripleExpansionPlan(
            supplementalTripleCount: supplementalTripleCount,
            totalTripleCount: totalTripleCount
        )
    }

    private static func consumeTerms(
        _ terms: inout [QueryIR.SPARQLTerm],
        supplementalTripleCount: inout Int
    ) throws {
        while let term = terms.popLast() {
            switch term {
            case .tripleTerm(let subject, let predicate, let object):
                terms.append(object)
                terms.append(predicate)
                terms.append(subject)
            case .reifiedTriple(
                let subject,
                let predicate,
                let object,
                let reifier
            ):
                let (nextCount, overflow) = supplementalTripleCount
                    .addingReportingOverflow(1)
                guard !overflow else { throw overflowError() }
                supplementalTripleCount = nextCount
                terms.append(reifier)
                terms.append(object)
                terms.append(predicate)
                terms.append(subject)
            case .variable, .iri, .literal, .blankNode:
                break
            }
        }
    }

    private static func overflowError() -> SPARQLSemanticValidationError {
        .structural(
            .resourceLimitExceeded(
                resource: .reifiedTripleExpansions,
                actual: UInt64.max,
                maximum: UInt64(Int.max)
            )
        )
    }
}
