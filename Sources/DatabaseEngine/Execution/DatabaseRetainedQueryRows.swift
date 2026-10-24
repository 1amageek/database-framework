import DatabaseKit

/// Linear row, schema and source-payload ownership until final output transfer.
public struct DatabaseRetainedQueryRows: ~Copyable, Sendable {
    private enum Storage: ~Copyable, Sendable {
        case unique(DatabaseRetainedBuffer<ExecutionQueryRow>)
        case shared(DatabaseSharedRetainedQueryRows.View)
    }

    private var storage: Storage
    public let columns: [QueryColumn]
    package let payloadLeases: [DatabaseQueryPayloadLease]
    package let schemaLeases: [DatabaseIntermediateReservation]

    /// The producing owner has admitted schema/payload and validated rows.
    package init(
        storage: consuming DatabaseRetainedBuffer<ExecutionQueryRow>,
        columns: [QueryColumn],
        payloadLeases: [DatabaseQueryPayloadLease] = [],
        schemaLeases: [DatabaseIntermediateReservation] = []
    ) {
        self.storage = .unique(consume storage)
        self.columns = columns
        self.payloadLeases = payloadLeases
        self.schemaLeases = schemaLeases
    }

    package init(sharedStorage: DatabaseSharedRetainedQueryRows.View) {
        self.storage = .shared(sharedStorage)
        self.columns = sharedStorage.columns
        self.payloadLeases = sharedStorage.payloadLeases
        self.schemaLeases = sharedStorage.schemaLeases
    }

    public var count: Int {
        switch storage {
        case .unique(let storage): return storage.count
        case .shared(let storage): return storage.count
        }
    }
    public var isEmpty: Bool { count == 0 }
    package var workMeter: DatabaseWorkMeter {
        switch storage {
        case .unique(let storage): return storage.workMeter
        case .shared(let storage): return storage.workMeter
        }
    }

    package borrowing func withElement<Failure: Error>(
        at index: Int,
        _ body: (borrowing ExecutionQueryRow) throws(Failure) -> Void
    ) throws(Failure) {
        defer {
            withExtendedLifetime(payloadLeases) {}
            withExtendedLifetime(schemaLeases) {}
        }
        switch storage {
        case .unique(let storage): try storage.withElement(at: index, body)
        case .shared(let storage): try storage.withElement(at: index, body)
        }
    }

    package borrowing func withElement<Failure: Error>(
        at index: Int,
        _ body: (borrowing ExecutionQueryRow) async throws(Failure) -> Void
    ) async throws(Failure) {
        defer {
            withExtendedLifetime(payloadLeases) {}
            withExtendedLifetime(schemaLeases) {}
        }
        switch storage {
        case .unique(let storage): try await storage.withElement(at: index, body)
        case .shared(let storage): try await storage.withElement(at: index, body)
        }
    }

    package borrowing func withSpan<Result, Failure: Error>(
        _ body: (Span<ExecutionQueryRow>) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        defer {
            withExtendedLifetime(payloadLeases) {}
            withExtendedLifetime(schemaLeases) {}
        }
        switch storage {
        case .unique(let storage): return try storage.withSpan(body)
        case .shared(let storage): return try storage.withSpan(body)
        }
    }

    public consuming func promoteToOutput() -> [ExecutionQueryRow] {
        let retainedPayload = payloadLeases
        let retainedSchema = schemaLeases
        defer {
            withExtendedLifetime(retainedPayload) {}
            withExtendedLifetime(retainedSchema) {}
        }
        switch consume storage {
        case .unique(let storage): return storage.promoteToOutput()
        case .shared(let storage): return storage.owner.materializePage(storage.range)
        }
    }

    package consuming func moveToSharedOwnership(
        at stage: DatabaseWorkStage
    ) throws -> DatabaseSharedRetainedQueryRows {
        switch consume storage {
        case .unique(let storage):
            return DatabaseSharedRetainedQueryRows(
                storage: try storage.moveToSharedOwnership(at: stage), columns: columns,
                payloadLeases: payloadLeases, schemaLeases: schemaLeases
            )
        case .shared(let storage):
            if storage.range == storage.owner.startIndex..<storage.owner.endIndex {
                return storage.owner
            }
            var rebuilt = try DatabaseRetainedQueryRowsBuilder(
                columns: columns, workMeter: storage.workMeter, stage: stage,
                expectedCount: storage.count, payloadLeases: payloadLeases, schemaLeases: schemaLeases
            )
            for index in 0..<storage.count {
                try storage.withElement(at: index) { try rebuilt.append(copy $0) }
            }
            return try rebuilt.finish().moveToSharedOwnership(at: stage)
        }
    }

    package consuming func moveToSharedView(
        at stage: DatabaseWorkStage
    ) throws -> DatabaseSharedRetainedQueryRows.View {
        switch consume storage {
        case .unique(let storage):
            let shared = DatabaseSharedRetainedQueryRows(
                storage: try storage.moveToSharedOwnership(at: stage), columns: columns,
                payloadLeases: payloadLeases, schemaLeases: schemaLeases
            )
            return shared.boundedView(shared.startIndex..<shared.endIndex)
        case .shared(let storage): return storage
        }
    }
}
