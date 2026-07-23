import DatabaseEngine
import DatabaseValue

/// Computes the exact final binding footprint directly from the seed,
/// endpoint patterns, and RDF terms before a VariableBinding Dictionary is
/// allocated. Repeated variables are matched once and contribute one entry.
final class SPARQLPropertyPathBindingFootprintMeter {
    private struct VariableAssignment {
        let variable: String
        let term: DatabaseRDFTerm
    }

    private enum WorkItem {
        case match(ExecutionTerm, DatabaseRDFTerm)
        case exact(DatabaseRDFTerm, DatabaseRDFTerm)
        case retain(DatabaseRDFTerm)
    }

    private static let scratchContainerByteCount: UInt64 = 128
    private static let scratchSlotByteCount: UInt64 = 96
    private static let assignmentSlotByteCount: UInt64 = 128
    private static let initialScratchCapacity = 4
    private static let bindingDictionaryByteCount: UInt64 = 64
    private static let bindingEntrySlotByteCount: UInt64 = 64
    private static let stringStorageByteCount: UInt64 = 16
    private static let rdfTermNodeByteCount: UInt64 = 32
    private static let rdfLiteralByteCount: UInt64 = 24
    private static let geometricCapacityMultiplier: UInt64 = 2

    private let workMeter: DatabaseWorkMeter
    private let stage: DatabaseWorkStage
    private let bindingFootprintMeter: SPARQLBindingFootprintMeter
    private var scratchReservation: DatabaseIntermediateReservation?
    private var worklist: [WorkItem]
    private var assignments: [VariableAssignment]
    private var accountedCapacity: Int
    private var accountedAssignmentCapacity: Int

    private init(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        bindingFootprintMeter: SPARQLBindingFootprintMeter,
        scratchReservation: DatabaseIntermediateReservation
    ) {
        self.workMeter = workMeter
        self.stage = stage
        self.bindingFootprintMeter = bindingFootprintMeter
        self.scratchReservation = scratchReservation
        self.worklist = []
        self.assignments = []
        self.accountedCapacity = 0
        self.accountedAssignmentCapacity = 0
    }

    static func make(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> SPARQLPropertyPathBindingFootprintMeter {
        let reservation = try workMeter.reserveIntermediate(
            bytes: scratchContainerByteCount,
            at: stage
        )
        let bindingFootprintMeter = try SPARQLBindingFootprintMeter.make(
            workMeter: workMeter,
            stage: stage
        )
        return SPARQLPropertyPathBindingFootprintMeter(
            workMeter: workMeter,
            stage: stage,
            bindingFootprintMeter: bindingFootprintMeter,
            scratchReservation: reservation
        )
    }

    func footprint(
        retaining seed: borrowing VariableBinding,
        subject: ExecutionTerm,
        start: DatabaseRDFTerm,
        object: ExecutionTerm,
        end: DatabaseRDFTerm
    ) throws -> SPARQLPropertyPathBindingFootprint {
        precondition(worklist.isEmpty)
        precondition(assignments.isEmpty)
        defer {
            worklist.removeAll(keepingCapacity: true)
            assignments.removeAll(keepingCapacity: true)
        }

        try append(.match(object, end))
        try append(.match(subject, start))
        var entryCount = 0
        var retainedBytes: UInt64 = 0

        while let item = worklist.popLast() {
            switch item {
            case .match(let pattern, let term):
                switch pattern {
                case .variable(let variable):
                    if let seedValue = seed[variable] {
                        guard try bindingFootprintMeter.valuesEqual(
                            seedValue,
                            .rdfTerm(term)
                        ) else {
                            return .incompatible
                        }
                        continue
                    }
                    var existingTerm: DatabaseRDFTerm?
                    for assignment in assignments {
                        try workMeter.consume(at: stage)
                        if assignment.variable == variable {
                            existingTerm = assignment.term
                            break
                        }
                    }
                    if let existingTerm {
                        try append(.exact(existingTerm, term))
                        continue
                    }
                    try appendAssignment(
                        VariableAssignment(variable: variable, term: term)
                    )
                    entryCount = try Self.checkedIncrement(entryCount)
                    retainedBytes = try Self.addString(
                        variable,
                        to: retainedBytes
                    )
                    try append(.retain(term))
                case .value(let value):
                    guard case .rdfTerm(let expected) = value else {
                        return .incompatible
                    }
                    try append(.exact(expected, term))
                case .wildcard:
                    break
                case .tripleTerm(
                    let subject,
                    let predicate,
                    let object
                ):
                    guard case .tripleTerm(
                        let storedSubject,
                        let storedPredicate,
                        let storedObject
                    ) = term else {
                        return .incompatible
                    }
                    try append(.match(object, storedObject))
                    try append(.match(predicate, storedPredicate))
                    try append(.match(subject, storedSubject))
                }

            case .exact(let expected, let actual):
                switch (expected, actual) {
                case (.iri(let lhs), .iri(let rhs)),
                     (.blankNode(let lhs), .blankNode(let rhs)):
                    guard lhs == rhs else { return .incompatible }
                case (.literal(let lhs), .literal(let rhs)):
                    guard lhs == rhs else { return .incompatible }
                case (
                    .tripleTerm(let lhsSubject, let lhsPredicate, let lhsObject),
                    .tripleTerm(let rhsSubject, let rhsPredicate, let rhsObject)
                ):
                    try append(.exact(lhsObject, rhsObject))
                    try append(.exact(lhsPredicate, rhsPredicate))
                    try append(.exact(lhsSubject, rhsSubject))
                default:
                    return .incompatible
                }

            case .retain(let term):
                retainedBytes = try Self.checkedAdd(
                    retainedBytes,
                    Self.rdfTermNodeByteCount
                )
                switch term {
                case .iri(let value), .blankNode(let value):
                    retainedBytes = try Self.addString(
                        value,
                        to: retainedBytes
                    )
                case .literal(let literal):
                    retainedBytes = try Self.checkedAdd(
                        retainedBytes,
                        Self.rdfLiteralByteCount
                    )
                    retainedBytes = try Self.addString(
                        literal.lexicalForm,
                        to: retainedBytes
                    )
                    switch literal.annotation {
                    case .typed(let datatype):
                        retainedBytes = try Self.addString(
                            datatype.rawValue,
                            to: retainedBytes
                        )
                    case .languageTagged(let language),
                         .directionalLanguageTagged(let language, _):
                        retainedBytes = try Self.addString(
                            language.rawValue,
                            to: retainedBytes
                        )
                    }
                case .tripleTerm(let subject, let predicate, let object):
                    try append(.retain(object))
                    try append(.retain(predicate))
                    try append(.retain(subject))
                }
            }
        }

        let entryCapacity = try Self.checkedMultiply(
            UInt64(entryCount),
            Self.geometricCapacityMultiplier
        )
        let entryBytes = try Self.checkedMultiply(
            entryCapacity,
            Self.bindingEntrySlotByteCount
        )
        let bytes = try Self.checkedAdd(
            try Self.checkedAdd(
                Self.bindingDictionaryByteCount,
                entryBytes
            ),
            retainedBytes
        )
        let additional = DatabaseIntermediateFootprint(
            rows: 1,
            bytes: bytes
        )
        return .compatible(
            try bindingFootprintMeter.footprint(
                retaining: seed,
                addingDisjointBinding: additional,
                additionalEntryCount: entryCount
            )
        )
    }

    func shutdown() {
        worklist.removeAll(keepingCapacity: false)
        assignments.removeAll(keepingCapacity: false)
        accountedCapacity = 0
        accountedAssignmentCapacity = 0
        bindingFootprintMeter.shutdown()
        scratchReservation?.release()
        scratchReservation = nil
    }

    deinit { shutdown() }

    private func append(_ item: WorkItem) throws {
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
            let additionalBytes = try Self.checkedMultiply(
                additionalSlots,
                Self.scratchSlotByteCount
            )
            guard let scratchReservation else {
                preconditionFailure(
                    "Property-path binding meter used after shutdown"
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

    private func appendAssignment(
        _ assignment: consuming VariableAssignment
    ) throws {
        if assignments.count == accountedAssignmentCapacity {
            let requiredCapacity: Int
            if accountedAssignmentCapacity == 0 {
                requiredCapacity = Self.initialScratchCapacity
            } else {
                let (doubled, overflow) = accountedAssignmentCapacity
                    .multipliedReportingOverflow(by: 2)
                guard !overflow else {
                    throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                        currentCapacity: accountedAssignmentCapacity
                    )
                }
                requiredCapacity = doubled
            }
            let additionalSlots = UInt64(
                requiredCapacity - accountedAssignmentCapacity
            )
            let additionalBytes = try Self.checkedMultiply(
                additionalSlots,
                Self.assignmentSlotByteCount
            )
            guard let scratchReservation else {
                preconditionFailure(
                    "Property-path binding meter used after shutdown"
                )
            }
            try scratchReservation.reserveAdditional(
                bytes: additionalBytes,
                at: stage
            )
            assignments.reserveCapacity(requiredCapacity)
            accountedAssignmentCapacity = requiredCapacity
        }
        assignments.append(assignment)
    }

    private static func checkedIncrement(_ value: Int) throws -> Int {
        let (result, overflow) = value.addingReportingOverflow(1)
        guard !overflow else {
            throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                currentCapacity: value
            )
        }
        return result
    }

    private static func addString(
        _ value: String,
        to byteCount: UInt64
    ) throws -> UInt64 {
        try checkedAdd(
            byteCount,
            try checkedAdd(
                stringStorageByteCount,
                UInt64(value.utf8.count)
            )
        )
    }

    private static func checkedAdd(
        _ lhs: UInt64,
        _ rhs: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = lhs.addingReportingOverflow(rhs)
        guard !overflow else {
            throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                currentCapacity: Int.max
            )
        }
        return result
    }

    private static func checkedMultiply(
        _ lhs: UInt64,
        _ rhs: UInt64
    ) throws -> UInt64 {
        let (result, overflow) = lhs.multipliedReportingOverflow(by: rhs)
        guard !overflow else {
            throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                currentCapacity: Int.max
            )
        }
        return result
    }
}
