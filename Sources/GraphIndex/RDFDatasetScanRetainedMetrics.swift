import DatabaseEngine
import DatabaseTypes
import DatabaseKit

/// Conservative retained-memory accounting for one unique dataset scan row.
///
/// Every accepted row is retained simultaneously by one deduplication Set and
/// the result Array, so its deterministic row claim is always two.
struct RDFDatasetScanRetainedMetrics: Sendable, Equatable {
    let rowCount: UInt64
    let retainedByteCount: UInt64

    // Fixed v1 admission constants. They intentionally do not depend on
    // platform pointer width, Swift ABI layout, or allocator capacity.
    private static let quadValueBaseline: UInt64 = 64
    private static let tripleValueBaseline: UInt64 = 48
    private static let resultOwnerOverhead: UInt64 = 64
    private static let setEntryOverhead: UInt64 = 32
    private static let geometricCapacityMultiplier: UInt64 = 2
    private static let coveringValueOwnerOverhead: UInt64 = 16
    private static let storedFieldArrayOverhead: UInt64 = 64
    private static let storedFieldSlotByteCount: UInt64 = 16
    private static let stringStorageOverhead: UInt64 = 16

    /// Reuses admitted caller-owned traversal storage across a physical scan.
    static func measure(
        _ quad: RDFQuad,
        mergesNamedGraphs: Bool,
        coveringValueByteCount: Int = 0,
        storedFieldNames: [String] = []
    ) throws -> RDFDatasetScanRetainedMetrics {
        let setValueByteCount = mergesNamedGraphs
            ? tripleValueBaseline
            : quadValueBaseline
        let arrayStorageByteCount = try checkedMultiply(
            quadValueBaseline,
            geometricCapacityMultiplier
        )
        let setStorageByteCount = try checkedMultiply(
            setValueByteCount,
            geometricCapacityMultiplier
        )
        var retainedByteCount = try checkedAdd(
            arrayStorageByteCount,
            setStorageByteCount
        )
        retainedByteCount = try checkedAdd(
            retainedByteCount,
            setEntryOverhead
        )
        retainedByteCount = try checkedAdd(
            retainedByteCount,
            resultOwnerOverhead
        )

        var termByteCount = try RDFTermRetainedFootprint.measure(
            quad.object
        ).bytes
        termByteCount = try checkedAdd(
            termByteCount,
            RDFTermRetainedFootprint.measure(quad.predicate.term).bytes
        )
        termByteCount = try checkedAdd(
            termByteCount,
            RDFTermRetainedFootprint.measure(quad.subject.term).bytes
        )
        if !mergesNamedGraphs, let graph = quad.graph {
            termByteCount = try checkedAdd(
                termByteCount,
                RDFTermRetainedFootprint.measure(graph.term).bytes
            )
        }

        // Set and Array values share Swift value payloads today. Counting the
        // term payload for both owners remains conservative if that ownership
        // representation changes.
        retainedByteCount = try checkedAdd(
            retainedByteCount,
            checkedMultiply(termByteCount, 2)
        )

        if coveringValueByteCount > 0 {
            let coveringValueFootprint = try checkedAdd(
                coveringValueOwnerOverhead,
                UInt64(coveringValueByteCount)
            )
            retainedByteCount = try checkedAdd(
                retainedByteCount,
                checkedMultiply(coveringValueFootprint, 2)
            )
        }

        if !storedFieldNames.isEmpty {
            var storedFieldFootprint = try checkedAdd(
                storedFieldArrayOverhead,
                checkedMultiply(
                    UInt64(storedFieldNames.count),
                    storedFieldSlotByteCount
                )
            )
            for fieldName in storedFieldNames {
                storedFieldFootprint = try addRetainedString(
                    fieldName,
                    to: storedFieldFootprint
                )
            }
            retainedByteCount = try checkedAdd(
                retainedByteCount,
                checkedMultiply(storedFieldFootprint, 2)
            )
        }

        return RDFDatasetScanRetainedMetrics(
            rowCount: 2,
            retainedByteCount: retainedByteCount
        )
    }

    private static func addRetainedString(
        _ value: String,
        to byteCount: UInt64
    ) throws(RDFDatasetScannerError) -> UInt64 {
        try checkedAdd(
            checkedAdd(byteCount, stringStorageOverhead),
            UInt64(value.utf8.count)
        )
    }

    static func checkedAdd(
        _ left: UInt64,
        _ right: UInt64
    ) throws(RDFDatasetScannerError) -> UInt64 {
        let (result, overflow) = left.addingReportingOverflow(right)
        guard !overflow else {
            throw .retainedByteCountOverflow(
                operation: .addition,
                left: left,
                right: right
            )
        }
        return result
    }

    static func checkedMultiply(
        _ left: UInt64,
        _ right: UInt64
    ) throws(RDFDatasetScannerError) -> UInt64 {
        let (result, overflow) = left.multipliedReportingOverflow(by: right)
        guard !overflow else {
            throw .retainedByteCountOverflow(
                operation: .multiplication,
                left: left,
                right: right
            )
        }
        return result
    }
}
