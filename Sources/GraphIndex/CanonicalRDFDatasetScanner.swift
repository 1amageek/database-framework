import DatabaseEngine
import DatabaseTypes
import DatabaseKit
import StorageKit

/// One logical RDF dataset composed from the authoritative mutable graph store
/// and schema-derived RDF projections.
public struct CanonicalRDFDatasetScanner: RDFDatasetScanner {
    private let authoritativeStore: CanonicalRDFGraphStore
    private let indexedScanner: IndexedRDFDatasetScanner
    private let projectedScanner: IndexedRDFDatasetScanner

    public init(
        authoritativeStore: CanonicalRDFGraphStore,
        projectedSources: [RDFDatasetSource]
    ) {
        self.authoritativeStore = authoritativeStore
        self.indexedScanner = IndexedRDFDatasetScanner(
            sources: [authoritativeStore.datasetSource] + projectedSources
        )
        self.projectedScanner = IndexedRDFDatasetScanner(
            sources: projectedSources
        )
    }

    public func scan(
        subject: RDFTerm?,
        predicate: RDFTerm?,
        object: RDFTerm?,
        graphTarget: RDFGraphScanTarget,
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFDatasetScanResult {
        try await indexedScanner.scan(
            subject: subject,
            predicate: predicate,
            object: object,
            graphTarget: graphTarget,
            limit: limit,
            readMode: readMode,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    public func namedGraphs(
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFDatasetNamedGraphs {
        if let limit, limit <= 0 {
            return .empty(workMeter: workMeter)
        }

        let authoritative = try await authoritativeStore.namedGraphs(
            limit: nil,
            readMode: readMode,
            transaction: transaction,
            workMeter: workMeter
        )
        let projected = try await projectedScanner.namedGraphs(
            limit: nil,
            readMode: readMode,
            transaction: transaction,
            workMeter: workMeter
        )

        var graphs = RDFDatasetNamedGraphBuilder(workMeter: workMeter)
        for index in 0..<authoritative.count {
            try authoritative.withGraph(at: index) { graph in
                try graphs.append(graph)
            }
        }
        for index in 0..<projected.count {
            try projected.withGraph(at: index) { graph in
                try graphs.append(graph)
            }
        }
        return try graphs.finish(limit: limit)
    }

    public func containsNamedGraph(
        _ graph: RDFGraphName,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        if try await authoritativeStore.containsNamedGraph(
            graph,
            readMode: readMode,
            transaction: transaction,
            workMeter: workMeter
        ) {
            return true
        }
        return try await projectedScanner.containsNamedGraph(
            graph,
            readMode: readMode,
            transaction: transaction,
            workMeter: workMeter
        )
    }
}
