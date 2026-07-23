import DatabaseEngine

/// Linear ownership for request-scoped property-path match storage.
enum SPARQLPropertyPathMatches: ~Copyable, Sendable {
    case empty
    case unique(DatabaseRetainedBuffer<SPARQLPropertyPathMatch>)

    var count: Int {
        switch self {
        case .empty:
            return 0
        case .unique(let storage):
            return storage.count
        }
    }

    var isEmpty: Bool { count == 0 }

    borrowing func withSpan<Result, Failure: Error>(
        _ body: (Span<SPARQLPropertyPathMatch>) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        switch self {
        case .empty:
            return try body([SPARQLPropertyPathMatch]().span)
        case .unique(let storage):
            return try storage.withSpan(body)
        }
    }

    borrowing func withElement<Result, Failure: Error>(
        at index: Int,
        _ body: (
            borrowing SPARQLPropertyPathMatch
        ) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        switch self {
        case .empty:
            preconditionFailure(
                "Cannot borrow a property-path match from an empty relation"
            )
        case .unique(let storage):
            return try storage.withElement(at: index, body)
        }
    }

    consuming func inverted() -> SPARQLPropertyPathMatches {
        switch consume self {
        case .empty:
            return .empty
        case .unique(let storage):
            return .unique(
                storage.transformingElements { match in
                    match = SPARQLPropertyPathMatch(
                        start: match.end,
                        end: match.start
                    )
                }
            )
        }
    }

    borrowing func withElement<Result, Failure: Error>(
        at index: Int,
        _ body: (
            borrowing SPARQLPropertyPathMatch
        ) async throws(Failure) -> Result
    ) async throws(Failure) -> Result {
        switch self {
        case .empty:
            preconditionFailure(
                "Cannot borrow a property-path match from an empty relation"
            )
        case .unique(let storage):
            return try await storage.withElement(at: index, body)
        }
    }
}
