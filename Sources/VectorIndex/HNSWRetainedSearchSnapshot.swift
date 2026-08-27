import DatabaseEngine
import DatabaseTypes
import StorageKit
import SwiftHNSW

/// One HNSW search snapshot owned either by the process cache or by the
/// originating request for the complete search lifetime.
struct HNSWRetainedSearchSnapshot: Sendable {
    private let snapshot: HNSWGraphCache.Snapshot?
    private let graphData: ByteString?
    private let primaryKeyReservation: DatabaseIntermediateReservation?
    private let restoreReservation: DatabaseIntermediateReservation?

    static let empty = HNSWRetainedSearchSnapshot(
        snapshot: nil,
        graphData: nil,
        primaryKeyReservation: nil,
        restoreReservation: nil
    )

    init(cached snapshot: HNSWGraphCache.Snapshot) {
        self.init(
            snapshot: snapshot,
            graphData: nil,
            primaryKeyReservation: nil,
            restoreReservation: nil
        )
    }

    init(
        requestOwned snapshot: HNSWGraphCache.Snapshot,
        graphData: ByteString,
        primaryKeyReservation: DatabaseIntermediateReservation,
        restoreReservation: DatabaseIntermediateReservation
    ) {
        self.init(
            snapshot: snapshot,
            graphData: graphData,
            primaryKeyReservation: primaryKeyReservation,
            restoreReservation: restoreReservation
        )
    }

    private init(
        snapshot: HNSWGraphCache.Snapshot?,
        graphData: ByteString?,
        primaryKeyReservation: DatabaseIntermediateReservation?,
        restoreReservation: DatabaseIntermediateReservation?
    ) {
        self.snapshot = snapshot
        self.graphData = graphData
        self.primaryKeyReservation = primaryKeyReservation
        self.restoreReservation = restoreReservation
    }

    func search(
        queryVector: Vector,
        k: Int,
        efSearch: Int,
        workMeter: DatabaseWorkMeter? = nil
    ) throws -> [SearchResult] {
        try validateWorkMeter(workMeter)
        guard let snapshot else { return [] }
        let results = try snapshot.search(
            queryVector: queryVector,
            k: k,
            efSearch: efSearch,
            workMeter: workMeter
        )
        withExtendedLifetime(self) {}
        return results
    }

    func insertMatch(
        label: UInt64,
        distance: Double,
        into output: inout VectorSearchAccumulator
    ) throws -> Bool {
        guard let packedPrimaryKey = snapshot?.primaryKeysByLabel[label] else {
            return false
        }
        try output.insert(
            packedPrimaryKey: packedPrimaryKey,
            distance: distance
        )
        withExtendedLifetime(self) {}
        return true
    }

    /// Materializes a key only for the legacy maintainer output boundary.
    func materializedPrimaryKey(label: UInt64) throws -> Tuple? {
        guard let packedPrimaryKey = snapshot?.primaryKeysByLabel[label] else {
            return nil
        }
        let primaryKey = try Tuple(packed: packedPrimaryKey)
        withExtendedLifetime(self) {}
        return primaryKey
    }

    private func validateWorkMeter(
        _ workMeter: DatabaseWorkMeter?
    ) throws {
        guard let ownerMeter = primaryKeyReservation?.workMeter
                ?? restoreReservation?.workMeter else {
            return
        }
        guard let workMeter else { return }
        guard ownerMeter === workMeter else {
            throw DatabaseReadSessionError.workMeterMismatch
        }
    }
}
