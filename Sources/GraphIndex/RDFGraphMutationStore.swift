import DatabaseEngine
import DatabaseKit
import StorageKit

/// Authoritative mutable RDF dataset. Implementations own graph-store state;
/// schema-derived RDF projections remain separate read-only dataset sources.
public protocol RDFGraphMutationStore: RDFDatasetScanner {
    func containsGraph(
        _ graph: RDFGraphName,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool

    func createGraph(
        _ graph: RDFGraphName,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws

    @discardableResult
    func insert(
        _ quad: RDFQuad,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFGraphInsertResult

    @discardableResult
    func delete(
        _ quad: RDFQuad,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool

    @discardableResult
    func clear(
        _ scope: RDFGraphMutationScope,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> UInt64

    @discardableResult
    func drop(
        _ scope: RDFGraphMutationScope,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> UInt64
}

extension RDFGraphMutationStore {
    public func containsNamedGraph(
        _ graph: RDFGraphName,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        try await containsGraph(
            graph,
            readMode: readMode,
            transaction: transaction,
            workMeter: workMeter
        )
    }
}
