import DatabaseKit

/// Admitted builder for rows and their explicit shared schema.
public struct DatabaseRetainedQueryRowsBuilder: ~Copyable {
    private var storage: DatabaseRetainedArrayBuilder<ExecutionQueryRow>
    public let columns: [QueryColumn]
    package let workMeter: DatabaseWorkMeter
    package let payloadLeases: [DatabaseQueryPayloadLease]
    package let schemaLeases: [DatabaseIntermediateReservation]
    private let stage: DatabaseWorkStage

    public init(
        columns: [QueryColumn],
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        expectedCount: Int = 0
    ) throws {
        try self.init(columns: columns, workMeter: workMeter, stage: stage,
                      expectedCount: expectedCount, payloadLeases: [], schemaLeases: [])
    }

    /// Existing schema claims are reused only when the producing owner has
    /// already admitted these exact immutable columns on the same meter.
    package init(
        columns: [QueryColumn],
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        expectedCount: Int = 0,
        payloadLeases: [DatabaseQueryPayloadLease],
        schemaLeases: [DatabaseIntermediateReservation]
    ) throws {
        try QueryColumn.validate(columns)
        guard payloadLeases.allSatisfy({ $0.workMeter === workMeter }),
              schemaLeases.allSatisfy({ $0.workMeter === workMeter }) else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        let retainedSchemaLeases: [DatabaseIntermediateReservation]
        if schemaLeases.isEmpty {
            var footprint = try DatabaseIntermediateFootprint(
                bytes: UInt64(MemoryLayout<QueryColumn>.stride)
            ).multiplied(by: UInt64(columns.count))
            footprint = try footprint.adding(DatabaseIntermediateFootprint(
                bytes: UInt64(MemoryLayout<[QueryColumn]>.stride) + 64
            ))
            for column in columns {
                footprint = try footprint.adding(DatabaseIntermediateFootprint(bytes: UInt64(column.name.utf8.count)))
            }
            try DatabaseByteProcessingMeter.consume(byteCount: footprint.bytes, workMeter: workMeter, stage: stage)
            retainedSchemaLeases = [try workMeter.reserveIntermediate(bytes: footprint.bytes, at: stage)]
        } else {
            retainedSchemaLeases = schemaLeases
        }
        self.storage = try DatabaseRetainedArrayBuilder(
            workMeter: workMeter, stage: stage,
            layout: try DatabaseRetainedArrayLayout.forElement(ExecutionQueryRow.self), expectedCount: expectedCount
        )
        self.columns = columns
        self.workMeter = workMeter
        self.payloadLeases = payloadLeases
        self.schemaLeases = retainedSchemaLeases
        self.stage = stage
    }

    public var count: Int { storage.count }
    public var isEmpty: Bool { storage.isEmpty }

    public mutating func append(_ row: consuming ExecutionQueryRow) throws {
        try row.validate(columns: columns)
        try workMeter.consume(at: stage)
        let footprint = try CanonicalRelationalFootprintMeter.footprint(of: row, workMeter: workMeter, stage: stage)
        let admission = try storage.prepareAppend(footprint: footprint, at: stage)
        storage.append(consume row, using: consume admission)
    }

    /// Prospective row storage is admitted before construction and validated
    /// against the same schema before the builder accepts the result.
    package mutating func append<Failure: Error>(
        footprint: DatabaseIntermediateFootprint,
        make: () throws(Failure) -> ExecutionQueryRow
    ) throws {
        try workMeter.consume(at: stage)
        let columns = columns
        try storage.append(footprint: footprint, at: stage) {
            let row = try make()
            try row.validate(columns: columns)
            return row
        }
    }

    public consuming func finish() -> DatabaseRetainedQueryRows {
        DatabaseRetainedQueryRows(storage: storage.finish(), columns: columns,
                                  payloadLeases: payloadLeases, schemaLeases: schemaLeases)
    }
}
