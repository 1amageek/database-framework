import DatabaseEngine
import DatabaseTypes
import SwiftHNSW

/// HNSW query storage coupled to any normalization allocation it owns.
struct HNSWRetainedGraphVector: Sendable {
    private let vector: Vector
    private let reservation: DatabaseIntermediateReservation?

    init(
        vector: consuming Vector,
        reservation: DatabaseIntermediateReservation?
    ) {
        self.vector = vector
        self.reservation = reservation
    }

    func search(
        in snapshot: HNSWRetainedSearchSnapshot,
        k: Int,
        efSearch: Int,
        workMeter: DatabaseWorkMeter?
    ) throws -> [SearchResult] {
        if let reservation {
            guard let workMeter,
                  reservation.workMeter === workMeter else {
                throw DatabaseReadSessionError.workMeterMismatch
            }
        }
        let result = try snapshot.search(
            queryVector: vector,
            k: k,
            efSearch: efSearch,
            workMeter: workMeter
        )
        withExtendedLifetime(self) {}
        return result
    }

    func unmeteredOutput() -> Vector {
        precondition(
            reservation == nil,
            "A metered HNSW query vector cannot escape its request owner"
        )
        return vector
    }
}
