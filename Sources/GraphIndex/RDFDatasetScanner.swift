import DatabaseTypes
import DatabaseEngine
import DatabaseKit
import StorageKit

/// Reads canonical RDF quads without exposing their physical index layout.
public protocol RDFDatasetScanner: Sendable {
    func scan(
        subject: RDFTerm?,
        predicate: RDFTerm?,
        object: RDFTerm?,
        graphTarget: RDFGraphScanTarget,
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFDatasetScanResult

    /// Enumerates named graphs, including graph-store catalog entries that do
    /// not currently contain a quad.
    func namedGraphs(
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFNamedGraphResult

    /// Tests logical graph existence without inferring it from a non-empty
    /// triple scan. Authoritative stores can therefore preserve empty graphs.
    func containsNamedGraph(
        _ graph: RDFGraphName,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool
}
