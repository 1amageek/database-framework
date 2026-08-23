import DatabaseKit

/// Request-accounted immutable outputs produced by one fusion stage.
public struct FusionStageResult<Item: Persistable>:
    Sendable
{
    public typealias Element = FusionQueryResult<Item>

    private let storage: DatabaseSharedRetainedArray<Element>
    package let workMeter: DatabaseWorkMeter

    package init(
        storage: DatabaseSharedRetainedArray<Element>,
        workMeter: DatabaseWorkMeter
    ) {
        self.storage = storage
        self.workMeter = workMeter
    }

    public var count: Int { storage.count }
    public var isEmpty: Bool { storage.isEmpty }

    /// Promotes a standalone stage result to its nested public output shape.
    public consuming func promoteToOutput() -> [[ScoredResult<Item>]] {
        storage.promoteToOutput().map { result in
            result.promoteToOutput()
        }
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
