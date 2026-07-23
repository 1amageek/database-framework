import DatabaseEngine
import DatabaseValue
import Graph
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
        subject: DatabaseRDFTerm?,
        predicate: DatabaseRDFTerm?,
        object: DatabaseRDFTerm?,
        graphScope: RDFGraphScanScope,
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFDatasetScanResult {
        try await indexedScanner.scan(
            subject: subject,
            predicate: predicate,
            object: object,
            graphScope: graphScope,
            limit: limit,
            readMode: readMode,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    public func namedGraphs(
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> [RDFGraphName] {
        if let limit, limit <= 0 { return [] }

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

        var seen = Set<RDFGraphName>()
        var graphs: [RDFGraphName] = []
        graphs.reserveCapacity(authoritative.count + projected.count)
        for graph in authoritative {
            try workMeter.consume(at: .deduplication)
            if seen.insert(graph).inserted {
                graphs.append(graph)
            }
        }
        for graph in projected {
            try workMeter.consume(at: .deduplication)
            if seen.insert(graph).inserted {
                graphs.append(graph)
            }
        }
        try workMeter.consume(UInt64(graphs.count), at: .sortInput)
        var ordered = try graphs.sorted { lhs, rhs in
            try workMeter.consume(2, at: .sortComparison)
            return lhs < rhs
        }
        if let limit, ordered.count > limit {
            ordered.removeLast(ordered.count - limit)
        }
        return ordered
    }

    public func containsNamedGraph(
        _ graph: RDFGraphName,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionAccess,
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
