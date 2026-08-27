import DatabaseEngine
import DatabaseKit
import DatabaseTypes

/// Conservative retained-memory accounting for one unique dataset scan row.
///
/// Every accepted row is retained simultaneously by one deduplication Set and
/// the result Array, so its deterministic row claim is always two.
struct RDFDatasetScanRetainedMetrics: Sendable, Equatable {
    let rowCount: UInt64
    let retainedByteCount: UInt64
    let transientRowCount: UInt64
    let transientByteCount: UInt64

    var admittedRowCount: UInt64 {
        get throws {
            try Self.checkedAdd(rowCount, transientRowCount)
        }
    }

    var admittedByteCount: UInt64 {
        get throws {
            try Self.checkedAdd(retainedByteCount, transientByteCount)
        }
    }

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
    private static let mergedBlankNodeByteCount: UInt64 = 65

    /// Measures one prospective retained row from canonical physical bytes.
    /// No RDF term, String, Set entry, or result row exists at this point.
    static func preflight(
        _ proof: RDFQuadIndexReadPreflight,
        mergesNamedGraphs: Bool,
        coveringValueByteCount: Int = 0,
        includedFieldNames: [String] = []
    ) throws -> RDFDatasetScanRetainedMetrics {
        let replacement = mergesNamedGraphs
            ? mergedBlankNodeByteCount
            : nil
        var termByteCount = try RDFTermRetainedFootprint.measure(
            proof.object,
            replacingBlankNodeByteCount: replacement
        ).bytes
        termByteCount = try checkedAdd(
            termByteCount,
            RDFTermRetainedFootprint.measure(
                proof.predicate,
                replacingBlankNodeByteCount: replacement
            ).bytes
        )
        termByteCount = try checkedAdd(
            termByteCount,
            RDFTermRetainedFootprint.measure(
                proof.subject,
                replacingBlankNodeByteCount: replacement
            ).bytes
        )
        if !mergesNamedGraphs, let graph = proof.graph {
            termByteCount = try checkedAdd(
                termByteCount,
                RDFTermRetainedFootprint.measure(graph).bytes
            )
        }
        let transientByteCount = try mergesNamedGraphs
            ? decodedSourceByteCount(proof)
            : 0
        return try make(
            termByteCount: termByteCount,
            transientByteCount: transientByteCount,
            mergesNamedGraphs: mergesNamedGraphs,
            coveringValueByteCount: coveringValueByteCount,
            includedFieldNames: includedFieldNames
        )
    }

    /// Reuses admitted caller-owned traversal storage across a physical scan.
    static func measure(
        _ quad: RDFQuad,
        mergesNamedGraphs: Bool,
        coveringValueByteCount: Int = 0,
        includedFieldNames: [String] = []
    ) throws -> RDFDatasetScanRetainedMetrics {
        var termByteCount = try RDFTermRetainedFootprint.measure(
            quad.object,
            replacingBlankNodeByteCount: mergesNamedGraphs
                ? mergedBlankNodeByteCount
                : nil
        ).bytes
        termByteCount = try checkedAdd(
            termByteCount,
            RDFTermRetainedFootprint.measure(
                quad.predicate.term,
                replacingBlankNodeByteCount: mergesNamedGraphs
                    ? mergedBlankNodeByteCount
                    : nil
            ).bytes
        )
        termByteCount = try checkedAdd(
            termByteCount,
            RDFTermRetainedFootprint.measure(
                quad.subject.term,
                replacingBlankNodeByteCount: mergesNamedGraphs
                    ? mergedBlankNodeByteCount
                    : nil
            ).bytes
        )
        if !mergesNamedGraphs, let graph = quad.graph {
            termByteCount = try checkedAdd(
                termByteCount,
                RDFTermRetainedFootprint.measure(graph.term).bytes
            )
        }

        let transientByteCount = try mergesNamedGraphs
            ? decodedSourceByteCount(quad)
            : 0

        return try make(
            termByteCount: termByteCount,
            transientByteCount: transientByteCount,
            mergesNamedGraphs: mergesNamedGraphs,
            coveringValueByteCount: coveringValueByteCount,
            includedFieldNames: includedFieldNames
        )
    }

    private static func make(
        termByteCount: UInt64,
        transientByteCount: UInt64,
        mergesNamedGraphs: Bool,
        coveringValueByteCount: Int,
        includedFieldNames: [String]
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

        if !includedFieldNames.isEmpty {
            var storedFieldFootprint = try checkedAdd(
                storedFieldArrayOverhead,
                checkedMultiply(
                    UInt64(includedFieldNames.count),
                    storedFieldSlotByteCount
                )
            )
            for fieldName in includedFieldNames {
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
            retainedByteCount: retainedByteCount,
            transientRowCount: mergesNamedGraphs ? 1 : 0,
            transientByteCount: transientByteCount
        )
    }

    private static func decodedSourceByteCount(
        _ proof: RDFQuadIndexReadPreflight
    ) throws -> UInt64 {
        var bytes = quadValueBaseline
        bytes = try checkedAdd(
            bytes,
            RDFTermRetainedFootprint.measure(proof.object).bytes
        )
        bytes = try checkedAdd(
            bytes,
            RDFTermRetainedFootprint.measure(proof.predicate).bytes
        )
        bytes = try checkedAdd(
            bytes,
            RDFTermRetainedFootprint.measure(proof.subject).bytes
        )
        if let graph = proof.graph {
            bytes = try checkedAdd(
                bytes,
                RDFTermRetainedFootprint.measure(graph).bytes
            )
        }
        return bytes
    }

    private static func decodedSourceByteCount(
        _ quad: borrowing RDFQuad
    ) throws -> UInt64 {
        var bytes = quadValueBaseline
        bytes = try checkedAdd(
            bytes,
            RDFTermRetainedFootprint.measure(quad.object).bytes
        )
        bytes = try checkedAdd(
            bytes,
            RDFTermRetainedFootprint.measure(quad.predicate.term).bytes
        )
        bytes = try checkedAdd(
            bytes,
            RDFTermRetainedFootprint.measure(quad.subject.term).bytes
        )
        if let graph = quad.graph {
            bytes = try checkedAdd(
                bytes,
                RDFTermRetainedFootprint.measure(graph.term).bytes
            )
        }
        return bytes
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
