import DatabaseKit

package struct FusionStageResultBuilder<Item: Persistable>: ~Copyable {
    private var storage: DatabaseRetainedArrayBuilder<
        FusionQueryResult<Item>
    >
    private let workMeter: DatabaseWorkMeter

    package init(
        execution: ReadExecutionContext,
        expectedCount: Int
    ) throws {
        self.workMeter = execution.workMeter
        self.storage = try DatabaseRetainedArrayBuilder(
            workMeter: execution.workMeter,
            stage: .indexScan,
            layout: try CanonicalRelationalFootprintMeter.retainedArrayLayout(
                for: FusionQueryResult<Item>.self
            ),
            expectedCount: expectedCount
        )
    }

    package mutating func append(
        _ result: consuming FusionQueryResult<Item>
    ) throws {
        try result.validateWorkMeter(workMeter)
        let admission = try storage.prepareAppend(
            footprint: DatabaseIntermediateFootprint(),
            at: .indexScan
        )
        storage.append(result, using: admission)
    }

    package consuming func finish() throws -> FusionStageResult<Item> {
        FusionStageResult(
            storage: try storage.finish().moveToSharedOwnership(
                at: .indexScan
            ),
            workMeter: workMeter
        )
    }
}
