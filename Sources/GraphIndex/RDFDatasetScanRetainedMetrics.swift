import DatabaseEngine
import DatabaseValue
import Graph

/// Conservative retained-memory accounting for one unique dataset scan row.
///
/// Every accepted row is retained simultaneously by one deduplication Set and
/// the result Array, so its deterministic row claim is always two.
struct RDFDatasetScanRetainedMetrics: Sendable, Equatable {
    let rowCount: UInt64
    let retainedByteCount: UInt64

    // Fixed v1 admission constants. They intentionally do not depend on
    // platform pointer width, Swift ABI layout, or allocator capacity.
    static let initialWorklistCapacity: UInt64 = 4
    static let worklistContainerByteCount: UInt64 = 128
    static let worklistSlotByteCount: UInt64 = 32

    private static let quadValueBaseline: UInt64 = 64
    private static let tripleValueBaseline: UInt64 = 48
    private static let resultOwnerOverhead: UInt64 = 64
    private static let setEntryOverhead: UInt64 = 32
    private static let geometricCapacityMultiplier: UInt64 = 2
    private static let termNodeOverhead: UInt64 = 32
    private static let stringStorageOverhead: UInt64 = 16
    private static let literalPayloadOverhead: UInt64 = 24

    /// Reuses admitted caller-owned traversal storage across a physical scan.
    static func measure(
        _ quad: RDFQuad,
        mergesNamedGraphs: Bool,
        worklist: inout [DatabaseRDFTerm],
        modeledWorklistCapacity: inout UInt64,
        scratchReservation: inout DatabaseIntermediateReservation?,
        workMeter: DatabaseWorkMeter
    ) throws -> RDFDatasetScanRetainedMetrics {
        worklist.removeAll(keepingCapacity: true)
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

        try append(
            quad.object,
            to: &worklist,
            modeledCapacity: &modeledWorklistCapacity,
            scratchReservation: &scratchReservation,
            workMeter: workMeter
        )
        try append(
            quad.predicate,
            to: &worklist,
            modeledCapacity: &modeledWorklistCapacity,
            scratchReservation: &scratchReservation,
            workMeter: workMeter
        )
        try append(
            quad.subject,
            to: &worklist,
            modeledCapacity: &modeledWorklistCapacity,
            scratchReservation: &scratchReservation,
            workMeter: workMeter
        )
        if !mergesNamedGraphs, let graph = quad.graph {
            try append(
                graph,
                to: &worklist,
                modeledCapacity: &modeledWorklistCapacity,
                scratchReservation: &scratchReservation,
                workMeter: workMeter
            )
        }

        var termByteCount: UInt64 = 0
        while let term = worklist.popLast() {
            termByteCount = try checkedAdd(
                termByteCount,
                termNodeOverhead
            )
            switch term {
            case .iri(let value), .blankNode(let value):
                termByteCount = try addRetainedString(
                    value,
                    to: termByteCount
                )

            case .literal(let literal):
                termByteCount = try checkedAdd(
                    termByteCount,
                    literalPayloadOverhead
                )
                termByteCount = try addRetainedString(
                    literal.lexicalForm,
                    to: termByteCount
                )
                switch literal.annotation {
                case .typed(let datatype):
                    termByteCount = try addRetainedString(
                        datatype.rawValue,
                        to: termByteCount
                    )
                case .languageTagged(let language),
                     .directionalLanguageTagged(let language, _):
                    termByteCount = try addRetainedString(
                        language.rawValue,
                        to: termByteCount
                    )
                }

            case .tripleTerm(let subject, let predicate, let object):
                try append(
                    object,
                    to: &worklist,
                    modeledCapacity: &modeledWorklistCapacity,
                    scratchReservation: &scratchReservation,
                    workMeter: workMeter
                )
                try append(
                    predicate,
                    to: &worklist,
                    modeledCapacity: &modeledWorklistCapacity,
                    scratchReservation: &scratchReservation,
                    workMeter: workMeter
                )
                try append(
                    subject,
                    to: &worklist,
                    modeledCapacity: &modeledWorklistCapacity,
                    scratchReservation: &scratchReservation,
                    workMeter: workMeter
                )
            }
        }

        // Set and Array values share Swift value payloads today. Counting the
        // term payload for both owners remains conservative if that ownership
        // representation changes.
        retainedByteCount = try checkedAdd(
            retainedByteCount,
            checkedMultiply(termByteCount, 2)
        )
        return RDFDatasetScanRetainedMetrics(
            rowCount: 2,
            retainedByteCount: retainedByteCount
        )
    }

    private static func append(
        _ term: DatabaseRDFTerm,
        to worklist: inout [DatabaseRDFTerm],
        modeledCapacity: inout UInt64,
        scratchReservation: inout DatabaseIntermediateReservation?,
        workMeter: DatabaseWorkMeter
    ) throws {
        if UInt64(worklist.count) == modeledCapacity {
            let newCapacity = modeledCapacity == 0
                ? initialWorklistCapacity
                : try checkedMultiply(
                    modeledCapacity,
                    geometricCapacityMultiplier
                )
            guard let platformCapacity = Int(exactly: newCapacity) else {
                throw RDFDatasetScannerError.retainedWorklistCapacityExceeded(
                    required: newCapacity,
                    maximum: UInt64(Int.max)
                )
            }
            let additionalSlots = newCapacity - modeledCapacity
            let additionalSlotBytes = try checkedMultiply(
                additionalSlots,
                worklistSlotByteCount
            )
            let additionalBytes = modeledCapacity == 0
                ? try checkedAdd(
                    worklistContainerByteCount,
                    additionalSlotBytes
                )
                : additionalSlotBytes
            if let scratchReservation {
                try scratchReservation.reserveAdditional(
                    rows: additionalSlots,
                    bytes: additionalBytes,
                    at: .deduplication
                )
            } else {
                scratchReservation = try workMeter.reserveIntermediate(
                    rows: additionalSlots,
                    bytes: additionalBytes,
                    at: .deduplication
                )
            }
            worklist.reserveCapacity(platformCapacity)
            modeledCapacity = newCapacity
        }
        worklist.append(term)
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
