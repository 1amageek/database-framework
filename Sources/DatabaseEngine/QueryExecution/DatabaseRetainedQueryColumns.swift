import DatabaseKit

/// Bound output metadata whose descriptor admission survives its producing scope.
@_spi(DatabaseExecution)
@_spi(PolymorphicRuntime)
public struct DatabaseRetainedQueryColumns: Sendable {
    public let columns: [QueryColumn]
    package let payloadLeases: [DatabaseQueryPayloadLease]

    package init(columns: [QueryColumn], payloadLeases: [DatabaseQueryPayloadLease]) {
        self.columns = columns
        self.payloadLeases = payloadLeases
    }
}

extension DatabaseRetainedQueryRowsBuilder {
    @_spi(DatabaseExecution)
    public init(columns: DatabaseRetainedQueryColumns, workMeter: DatabaseWorkMeter,
                stage: DatabaseWorkStage, expectedCount: Int = 0) throws {
        try self.init(columns: columns.columns, workMeter: workMeter, stage: stage,
            expectedCount: expectedCount, payloadLeases: columns.payloadLeases, schemaLeases: [])
    }
}
