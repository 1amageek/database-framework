/// Admitted builder for logical-source rows returned through the runtime
/// extension boundary.
public struct DatabaseRetainedQueryRowsBuilder: ~Copyable {
    private var storage: DatabaseRetainedArrayBuilder<QueryRow>
    private let workMeter: DatabaseWorkMeter
    private let stage: DatabaseWorkStage

    public init(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        expectedCount: Int = 0
    ) throws {
        self.storage = try DatabaseRetainedArrayBuilder(
            workMeter: workMeter,
            stage: stage,
            layout: try DatabaseRetainedArrayLayout.forElement(QueryRow.self),
            expectedCount: expectedCount
        )
        self.workMeter = workMeter
        self.stage = stage
    }

    public var count: Int { storage.count }
    public var isEmpty: Bool { storage.isEmpty }

    public mutating func append(_ row: consuming QueryRow) throws {
        try workMeter.consume(at: stage)
        let footprint = try CanonicalRelationalFootprintMeter.footprint(
            of: row,
            workMeter: workMeter,
            stage: stage
        )
        let admission = try storage.prepareAppend(
            footprint: footprint,
            at: stage
        )
        storage.append(consume row, using: consume admission)
    }

    public consuming func finish() -> DatabaseRetainedQueryRows {
        DatabaseRetainedQueryRows(storage: storage.finish())
    }
}
