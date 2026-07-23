import DatabaseEngine
import DatabaseValue
import Graph
import GraphIndex
import StorageKit
import Synchronization

final class TrackingRDFGraphMutationStore: RDFGraphMutationStore, Sendable {
    enum ClearBehavior: Sendable {
        case delegate
        case delegateThenFail(RDFGraphStoreError)
    }

    private struct State: Sendable {
        var scanReadModes: [RDFDatasetReadMode] = []
        var scanRetainedIntermediateRows: [UInt64] = []
        var insertRetainedIntermediateRows: [UInt64] = []
    }

    private let base: CanonicalRDFGraphStore
    private let clearBehavior: ClearBehavior
    private let state = Mutex(State())

    init(
        base: CanonicalRDFGraphStore = CanonicalRDFGraphStore(),
        clearBehavior: ClearBehavior = .delegate
    ) {
        self.base = base
        self.clearBehavior = clearBehavior
    }

    var scanReadModes: [RDFDatasetReadMode] {
        state.withLock { $0.scanReadModes }
    }

    var insertRetainedIntermediateRows: [UInt64] {
        state.withLock { $0.insertRetainedIntermediateRows }
    }

    var scanRetainedIntermediateRows: [UInt64] {
        state.withLock { $0.scanRetainedIntermediateRows }
    }

    func scan(
        subject: DatabaseRDFTerm?,
        predicate: DatabaseRDFTerm?,
        object: DatabaseRDFTerm?,
        graphScope: RDFGraphScanScope,
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFDatasetScanResult {
        state.withLock { state in
            state.scanReadModes.append(readMode)
        }
        let result = try await base.scan(
            subject: subject,
            predicate: predicate,
            object: object,
            graphScope: graphScope,
            limit: limit,
            readMode: readMode,
            transaction: transaction,
            workMeter: workMeter
        )
        state.withLock { state in
            state.scanRetainedIntermediateRows.append(
                workMeter.retainedIntermediateRows
            )
        }
        return result
    }

    func namedGraphs(
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> [RDFGraphName] {
        try await base.namedGraphs(
            limit: limit,
            readMode: readMode,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    func containsGraph(
        _ graph: RDFGraphName,
        readMode: RDFDatasetReadMode,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        try await base.containsGraph(
            graph,
            readMode: readMode,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    func createGraph(
        _ graph: RDFGraphName,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws {
        try await base.createGraph(
            graph,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    func insert(
        _ quad: RDFQuad,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFGraphInsertResult {
        state.withLock { state in
            state.insertRetainedIntermediateRows.append(
                workMeter.retainedIntermediateRows
            )
        }
        return try await base.insert(
            quad,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    func delete(
        _ quad: RDFQuad,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        try await base.delete(
            quad,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    func clear(
        _ scope: RDFGraphMutationScope,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> UInt64 {
        let deleted = try await base.clear(
            scope,
            transaction: transaction,
            workMeter: workMeter
        )
        switch clearBehavior {
        case .delegate:
            return deleted
        case .delegateThenFail(let error):
            throw error
        }
    }

    func drop(
        _ scope: RDFGraphMutationScope,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> UInt64 {
        try await base.drop(
            scope,
            transaction: transaction,
            workMeter: workMeter
        )
    }
}
