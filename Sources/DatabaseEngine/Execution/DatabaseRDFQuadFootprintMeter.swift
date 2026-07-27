import DatabaseTypes
import DatabaseKit

/// Iteratively measures retained RDF graph payload before allocation.
final class DatabaseRDFQuadFootprintMeter {
    private static let scratchContainerByteCount: UInt64 = 64
    private static let scratchSlotByteCount: UInt64 = 48
    private static let initialScratchCapacity = 4
    private static let quadPayloadByteCount: UInt64 = 64
    private static let termNodeByteCount: UInt64 = 32
    private static let stringStorageByteCount: UInt64 = 16
    private static let literalPayloadByteCount: UInt64 = 24

    private let workMeter: DatabaseWorkMeter
    private let stage: DatabaseWorkStage
    private var scratchReservation: DatabaseIntermediateReservation?
    private var worklist: [RDFTerm]
    private var accountedCapacity: Int

    private init(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        scratchReservation: DatabaseIntermediateReservation
    ) {
        self.workMeter = workMeter
        self.stage = stage
        self.scratchReservation = scratchReservation
        self.worklist = []
        self.accountedCapacity = 0
    }

    static func make(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseRDFQuadFootprintMeter {
        DatabaseRDFQuadFootprintMeter(
            workMeter: workMeter,
            stage: stage,
            scratchReservation: try workMeter.reserveIntermediate(
                bytes: scratchContainerByteCount,
                at: stage
            )
        )
    }

    func footprint(
        of quad: borrowing RDFQuad
    ) throws -> DatabaseIntermediateFootprint {
        precondition(worklist.isEmpty)
        defer { worklist.removeAll(keepingCapacity: true) }
        try append(quad.object)
        try append(quad.predicate.term)
        try append(quad.subject.term)
        if let graph = quad.graph {
            try append(graph.term)
        }

        var bytes = Self.quadPayloadByteCount
        while let term = worklist.popLast() {
            bytes = try checkedAdd(bytes, Self.termNodeByteCount)
            switch term {
            case .iri(let value):
                bytes = try addString(value.rawValue, to: bytes)
            case .blankNode(let value):
                bytes = try addString(value.rawValue, to: bytes)
            case .literal(let literal):
                bytes = try checkedAdd(
                    bytes,
                    Self.literalPayloadByteCount
                )
                bytes = try addString(literal.lexicalForm, to: bytes)
                switch literal.annotation {
                case .typed(let datatype):
                    bytes = try addString(datatype.rawValue, to: bytes)
                case .languageTagged(let language),
                     .directionalLanguageTagged(let language, _):
                    bytes = try addString(language.rawValue, to: bytes)
                }
            case .tripleTerm(let subject, let predicate, let object):
                try append(object)
                try append(predicate.term)
                try append(subject.term)
            }
        }
        return DatabaseIntermediateFootprint(rows: 1, bytes: bytes)
    }

    func shutdown() {
        worklist.removeAll(keepingCapacity: false)
        accountedCapacity = 0
        scratchReservation?.release()
        scratchReservation = nil
    }

    deinit {
        shutdown()
    }

    private func append(_ term: RDFTerm) throws {
        if worklist.count == accountedCapacity {
            let requiredCapacity: Int
            if accountedCapacity == 0 {
                requiredCapacity = Self.initialScratchCapacity
            } else {
                let (doubled, overflow) = accountedCapacity
                    .multipliedReportingOverflow(by: 2)
                guard !overflow else { throw limitError() }
                requiredCapacity = doubled
            }
            let additionalSlots = UInt64(
                requiredCapacity - accountedCapacity
            )
            let additionalBytes = try checkedMultiply(
                additionalSlots,
                Self.scratchSlotByteCount
            )
            guard let scratchReservation else {
                preconditionFailure(
                    "RDF quad footprint meter used after shutdown"
                )
            }
            try scratchReservation.reserveAdditional(
                rows: additionalSlots,
                bytes: additionalBytes,
                at: stage
            )
            worklist.reserveCapacity(requiredCapacity)
            accountedCapacity = requiredCapacity
        }
        worklist.append(term)
    }

    private func addString(
        _ value: String,
        to bytes: UInt64
    ) throws -> UInt64 {
        try checkedAdd(
            try checkedAdd(bytes, Self.stringStorageByteCount),
            UInt64(value.utf8.count)
        )
    }

    private func checkedAdd(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = left.addingReportingOverflow(right)
        guard !overflow else { throw limitError() }
        return result
    }

    private func checkedMultiply(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = left.multipliedReportingOverflow(by: right)
        guard !overflow else { throw limitError() }
        return result
    }

    private func limitError() -> DatabaseWorkLimitError {
        DatabaseWorkLimitError.maximumIntermediateBytes(
            stage: stage,
            consumed: workMeter.retainedIntermediateBytes,
            requested: UInt64.max,
            maximum: workMeter.budget.maximumIntermediateBytes
        )
    }
}
