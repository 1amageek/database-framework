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
    ) async throws -> RDFDatasetNamedGraphs

    /// Tests logical graph existence without inferring it from a non-empty
    /// triple scan. Authoritative stores can therefore preserve empty graphs.
    func containsNamedGraph(
        _ graph: RDFGraphName,
        readMode: RDFDatasetReadMode,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool
}

/// Reads one retained scan result and rejects an implementation that returns
/// ownership charged to a different request.
package func scanRetained(
    using scanner: any RDFDatasetScanner,
    subject: RDFTerm?,
    predicate: RDFTerm?,
    object: RDFTerm?,
    graphTarget: RDFGraphScanTarget,
    limit: Int?,
    readMode: RDFDatasetReadMode,
    transaction: any TransactionReadAccess,
    workMeter: DatabaseWorkMeter
) async throws -> RDFDatasetScanResult {
    let result = try await scanner.scan(
        subject: subject,
        predicate: predicate,
        object: object,
        graphTarget: graphTarget,
        limit: limit,
        readMode: readMode,
        transaction: transaction,
        workMeter: workMeter
    )
    guard result.workMeter === workMeter else {
        throw DatabaseIntermediateReservationError.workMeterMismatch
    }
    return result
}

/// Reads retained named-graph discovery output on the requesting meter.
package func namedGraphsRetained(
    using scanner: any RDFDatasetScanner,
    limit: Int?,
    readMode: RDFDatasetReadMode,
    transaction: any TransactionReadAccess,
    workMeter: DatabaseWorkMeter
) async throws -> RDFDatasetNamedGraphs {
    let result = try await scanner.namedGraphs(
        limit: limit,
        readMode: readMode,
        transaction: transaction,
        workMeter: workMeter
    )
    guard result.workMeter === workMeter else {
        throw DatabaseIntermediateReservationError.workMeterMismatch
    }
    return result
}
