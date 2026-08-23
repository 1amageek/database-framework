import DatabaseKit

/// Request-accounted immutable output from one typed fusion source.
///
/// The retained result owns its intermediate-memory reservation for exactly
/// as long as the source rows can be consumed by later fusion stages.
public struct FusionQueryResult<Item: Persistable>:
    Sendable {
    public typealias Element = ScoredResult<Item>

    private let storage: DatabaseSharedRetainedArray<Element>
    package let workMeter: DatabaseWorkMeter
    package let ordering: FusionQueryResultOrdering

    package init(
        storage: DatabaseSharedRetainedArray<Element>,
        workMeter: DatabaseWorkMeter,
        ordering: FusionQueryResultOrdering
    ) {
        self.storage = storage
        self.workMeter = workMeter
        self.ordering = ordering
    }

    public var count: Int { storage.count }
    public var isEmpty: Bool { storage.isEmpty }

    /// Promotes one completed standalone source result to an output Array.
    /// Fusion stages retain the request-accounted representation internally
    /// and only the final public boundary performs this ownership transition.
    public consuming func promoteToOutput() -> [Element] {
        storage.promoteToOutput()
    }

    package var retainedElements: DatabaseSharedRetainedArray<Element> {
        storage
    }

    package func validateWorkMeter(
        _ expected: DatabaseWorkMeter
    ) throws {
        guard workMeter === expected else {
            throw FusionQueryError.workMeterMismatch
        }
    }
}

package enum FusionQueryResultOrdering: Sendable {
    /// Scores descend and equal scores use canonical identifier order.
    case scoreDescendingCanonicalIdentity

    package var isRankOrdered: Bool {
        switch self {
        case .scoreDescendingCanonicalIdentity:
            return true
        }
    }
}
