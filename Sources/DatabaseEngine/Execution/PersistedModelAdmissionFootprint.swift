import DatabaseKit

/// Bounds the transient hash tables used to validate persisted field identity.
/// Field-name payloads remain shared with the decoded fields; this footprint
/// admits only the two table containers and their deterministic slot storage.
package enum PersistedModelAdmissionFootprint {
    package static func validationScratchByteCount(
        fieldCount: Int
    ) throws -> UInt64 {
        let names = try DatabaseRetainedHashTableLayout.validated(
            containerByteCount: UInt64(MemoryLayout<Set<String>>.stride),
            elementCapacitySlotByteCount: UInt64(
                max(1, MemoryLayout<String>.stride + 16)
            )
        )
        let numbers = try DatabaseRetainedHashTableLayout.validated(
            containerByteCount: UInt64(MemoryLayout<Set<UInt32>>.stride),
            elementCapacitySlotByteCount: UInt64(
                max(1, MemoryLayout<UInt32>.stride + 16)
            )
        )
        let nameGrowth = try names.growth(from: 0, toFit: fieldCount)
        let numberGrowth = try numbers.growth(from: 0, toFit: fieldCount)
        return try DatabaseIntermediateFootprint(
            bytes: names.containerByteCount
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: nameGrowth.additionalByteCount
            )
        ).adding(
            DatabaseIntermediateFootprint(bytes: numbers.containerByteCount)
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: numberGrowth.additionalByteCount
            )
        ).bytes
    }
}
