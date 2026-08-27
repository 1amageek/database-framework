import DatabaseKit
import StorageKit

/// Canonical admission for owned collections crossing an execution-stage
/// boundary.
///
/// Producers may release their private reservation when returning an Array.
/// Before the consumer allocates a second representation it uses this meter to
/// keep the returned owner charged for the complete overlap lifetime.
package enum DatabaseIntermediateCollectionMeter {
    package static func reserveTuples(
        _ tuples: [Tuple],
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateReservation {
        var footprint = try arrayFootprint(
            count: tuples.count,
            element: Tuple.self
        )
        for tuple in tuples {
            footprint = try footprint.adding(
                DatabaseIntermediateFootprint(
                    rows: 1,
                    bytes: try DatabaseIntermediateFootprint(
                        bytes: UInt64(tuple.packedByteCount)
                    ).adding(
                        DatabaseIntermediateFootprint(bytes: 32)
                    ).bytes
                )
            )
        }
        return try workMeter.reserveIntermediate(
            rows: footprint.rows,
            bytes: footprint.bytes,
            at: stage
        )
    }

    package static func arrayFootprint<Element>(
        count: Int,
        element: Element.Type
    ) throws -> DatabaseIntermediateFootprint {
        guard count >= 0 else {
            preconditionFailure("Array counts cannot be negative")
        }
        return try DatabaseIntermediateFootprint(
            bytes: UInt64(MemoryLayout<[Element]>.stride)
        ).adding(
            try DatabaseIntermediateFootprint(
                bytes: UInt64(max(1, MemoryLayout<Element>.stride))
            ).multiplied(by: UInt64(count))
        )
    }
}
