import DatabaseEngine
import DatabaseValue

/// Computes canonical retained-memory charges for property-path matches.
///
/// RDF-star traversal is iterative. Its scratch Array grows only after the
/// request meter admits the next canonical capacity, so deeply nested terms
/// fail with a typed work-limit error instead of consuming untracked memory.
final class SPARQLPropertyPathMatchFootprintMeter {
    private enum WorkItem {
        case rdf(DatabaseRDFTerm)
    }

    // Fixed v1 admission units. They intentionally do not depend on the host
    // Swift ABI, allocator capacity, or pointer width.
    private static let scratchContainerByteCount: UInt64 = 64
    private static let scratchSlotByteCount: UInt64 = 32
    private static let initialScratchCapacity = 4
    private static let matchValueByteCount: UInt64 = 64
    private static let rdfTermNodeByteCount: UInt64 = 32
    private static let rdfLiteralByteCount: UInt64 = 24
    private static let stringStorageByteCount: UInt64 = 16

    private let stage: DatabaseWorkStage
    private var scratchReservation: DatabaseIntermediateReservation?
    private var worklist: [WorkItem]
    private var accountedCapacity: Int

    private init(
        stage: DatabaseWorkStage,
        scratchReservation: DatabaseIntermediateReservation
    ) {
        self.stage = stage
        self.scratchReservation = scratchReservation
        self.worklist = []
        self.accountedCapacity = 0
    }

    static func make(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> SPARQLPropertyPathMatchFootprintMeter {
        let reservation = try workMeter.reserveIntermediate(
            bytes: scratchContainerByteCount,
            at: stage
        )
        return SPARQLPropertyPathMatchFootprintMeter(
            stage: stage,
            scratchReservation: reservation
        )
    }

    static func retainedArrayLayout() throws
        -> DatabaseRetainedArrayLayout {
        try DatabaseRetainedArrayLayout.validated(
            containerByteCount: 64,
            elementCapacitySlotByteCount: 64,
            sharedOwnerByteCount: 64,
            appendAdmissionByteCount: 64
        )
    }

    func footprint(
        of match: borrowing SPARQLPropertyPathMatch
    ) throws -> DatabaseIntermediateFootprint {
        try footprint(start: match.start, end: match.end)
    }

    /// Measures both endpoints before a match value is constructed.
    func footprint(
        start: borrowing DatabaseRDFTerm,
        end: borrowing DatabaseRDFTerm
    ) throws -> DatabaseIntermediateFootprint {
        precondition(worklist.isEmpty)
        defer { worklist.removeAll(keepingCapacity: true) }

        var footprint = DatabaseIntermediateFootprint(
            rows: 1,
            bytes: Self.matchValueByteCount
        )
        try append(.rdf(copy end))
        try append(.rdf(copy start))

        while let item = worklist.popLast() {
            switch item {
            case .rdf(let term):
                footprint = try footprint.adding(
                    DatabaseIntermediateFootprint(
                        bytes: Self.rdfTermNodeByteCount
                    )
                )
                switch term {
                case .iri(let value), .blankNode(let value):
                    footprint = try footprint.adding(
                        try Self.stringFootprint(value)
                    )

                case .literal(let literal):
                    footprint = try footprint.adding(
                        DatabaseIntermediateFootprint(
                            bytes: Self.rdfLiteralByteCount
                        )
                    ).adding(
                        try Self.stringFootprint(literal.lexicalForm)
                    )
                    switch literal.annotation {
                    case .typed(let datatype):
                        footprint = try footprint.adding(
                            try Self.stringFootprint(datatype.rawValue)
                        )
                    case .languageTagged(let language),
                         .directionalLanguageTagged(let language, _):
                        footprint = try footprint.adding(
                            try Self.stringFootprint(language.rawValue)
                        )
                    }

                case .tripleTerm(let subject, let predicate, let object):
                    try append(.rdf(object))
                    try append(.rdf(predicate))
                    try append(.rdf(subject))
                }
            }
        }
        return footprint
    }

    /// Releases all scratch admission before a downstream owner is promoted.
    func shutdown() {
        worklist.removeAll(keepingCapacity: false)
        accountedCapacity = 0
        scratchReservation?.release()
        scratchReservation = nil
    }

    deinit {
        shutdown()
    }

    private func append(_ item: consuming WorkItem) throws {
        if worklist.count == accountedCapacity {
            let requiredCapacity: Int
            if accountedCapacity == 0 {
                requiredCapacity = Self.initialScratchCapacity
            } else {
                let (doubled, overflow) = accountedCapacity
                    .multipliedReportingOverflow(by: 2)
                guard !overflow else {
                    throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                        currentCapacity: accountedCapacity
                    )
                }
                requiredCapacity = doubled
            }

            let additionalSlots = UInt64(
                requiredCapacity - accountedCapacity
            )
            let additionalBytes = try DatabaseIntermediateFootprint(
                bytes: Self.scratchSlotByteCount
            ).multiplied(by: additionalSlots).bytes
            guard let scratchReservation else {
                preconditionFailure(
                    "Property-path footprint meter used after shutdown"
                )
            }
            try scratchReservation.reserveAdditional(
                bytes: additionalBytes,
                at: stage
            )
            worklist.reserveCapacity(requiredCapacity)
            accountedCapacity = requiredCapacity
        }
        worklist.append(item)
    }

    private static func stringFootprint(
        _ value: String
    ) throws -> DatabaseIntermediateFootprint {
        try DatabaseIntermediateFootprint(
            bytes: stringStorageByteCount
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: UInt64(value.utf8.count)
            )
        )
    }
}
