import DatabaseValue
import DatabaseEngine
import Graph
import StorageKit

/// Reads canonical RDF quads without exposing their physical index layout.
public protocol RDFDatasetScanner: Sendable {
    func scan(
        subject: DatabaseRDFTerm?,
        predicate: DatabaseRDFTerm?,
        object: DatabaseRDFTerm?,
        graphScope: RDFGraphScanScope,
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFDatasetScanResult

    /// Enumerates named graphs, including graph-store catalog entries that do
    /// not currently contain a quad.
    func namedGraphs(
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> [RDFGraphName]

    /// Tests logical graph existence without inferring it from a non-empty
    /// triple scan. Authoritative stores can therefore preserve empty graphs.
    func containsNamedGraph(
        _ graph: RDFGraphName,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool
}
