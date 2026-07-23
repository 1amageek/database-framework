import Core
import DatabaseEngine
import DatabaseValue

/// Reusable, request-admitted retained-footprint traversal for SPARQL rows.
///
/// The traversal is iterative and keeps only depth-proportional borrowed value
/// headers in its worklist. Its scratch capacity is admitted before allocation.
final class SPARQLBindingFootprintMeter {
    private enum WorkItem {
        case field(FieldValue)
        case fieldArray([FieldValue], nextIndex: Int)
        case rdf(DatabaseRDFTerm)
        case fieldComparison(FieldValue, FieldValue)
        case fieldArrayComparison(
            [FieldValue],
            [FieldValue],
            nextIndex: Int
        )
        case rdfComparison(DatabaseRDFTerm, DatabaseRDFTerm)
    }

    // Fixed v1 admission constants. They are protocol/runtime budget units and
    // intentionally do not depend on the host Swift ABI or pointer width.
    private static let scratchContainerByteCount: UInt64 = 64
    private static let scratchSlotByteCount: UInt64 = 64
    private static let initialScratchCapacity = 4
    private static let bindingDictionaryByteCount: UInt64 = 64
    private static let bindingEntrySlotByteCount: UInt64 = 64
    private static let stringStorageByteCount: UInt64 = 16
    private static let arrayStorageByteCount: UInt64 = 64
    private static let arrayElementSlotByteCount: UInt64 = 32
    private static let bytesStorageOwnerByteCount: UInt64 = 16
    private static let rdfTermNodeByteCount: UInt64 = 32
    private static let rdfLiteralByteCount: UInt64 = 24
    private static let geometricCapacityMultiplier: UInt64 = 2

    static func retainedArrayLayout() throws -> DatabaseRetainedArrayLayout {
        try DatabaseRetainedArrayLayout.validated(
            containerByteCount: 64,
            elementCapacitySlotByteCount: 64,
            sharedOwnerByteCount: 64,
            appendAdmissionByteCount: 64
        )
    }

    private let workMeter: DatabaseWorkMeter
    private let stage: DatabaseWorkStage
    private var scratchReservation: DatabaseIntermediateReservation?
    private var worklist: [WorkItem]
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

    /// Admits the meter owner before allocating it.
    static func make(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> SPARQLBindingFootprintMeter {
        let reservation = try workMeter.reserveIntermediate(
            bytes: scratchContainerByteCount,
            at: stage
        )
        return SPARQLBindingFootprintMeter(
            workMeter: workMeter,
            stage: stage,
            scratchReservation: reservation
        )
    }

    /// Computes the exact v1 admission footprint before the row is retained.
    func footprint(
        of binding: borrowing VariableBinding
    ) throws -> DatabaseIntermediateFootprint {
        precondition(worklist.isEmpty)
        defer { worklist.removeAll(keepingCapacity: true) }
        var footprint = try Self.bindingFootprint(
            entryCount: binding.count
        )

        try binding.withBindings { values in
            for (variable, value) in values {
                try addEntry(
                    variable: variable,
                    value: value,
                    to: &footprint
                )
            }
        }
        return footprint
    }

    /// Computes retained payload ownership for one standalone value without
    /// inventing a temporary binding dictionary.
    func footprint(
        of value: borrowing FieldValue
    ) throws -> DatabaseIntermediateFootprint {
        precondition(worklist.isEmpty)
        defer { worklist.removeAll(keepingCapacity: true) }
        try append(.field(copy value))
        return try drainWorklist(into: DatabaseIntermediateFootprint())
    }

    /// Combines an existing seed with a disjoint prospective binding without
    /// materializing either a temporary Dictionary or a merged row. Both
    /// inputs include their own Dictionary header; the result replaces those
    /// headers with the exact header for the final entry count.
    func footprint(
        retaining seed: borrowing VariableBinding,
        addingDisjointBinding additional: DatabaseIntermediateFootprint,
        additionalEntryCount: Int
    ) throws -> DatabaseIntermediateFootprint {
        guard additional.rows == 1, additionalEntryCount >= 0 else {
            throw SPARQLPropertyPathExecutionError
                .invalidBindingFootprintComposition
        }
        let seedFootprint = try footprint(of: seed)
        let seedHeader = try Self.bindingFootprint(entryCount: seed.count)
        let additionalHeader = try Self.bindingFootprint(
            entryCount: additionalEntryCount
        )
        guard seedFootprint.rows == 1,
              seedFootprint.bytes >= seedHeader.bytes,
              additional.bytes >= additionalHeader.bytes else {
            throw SPARQLPropertyPathExecutionError
                .invalidBindingFootprintComposition
        }

        let finalEntryCount = try Self.checkedCountAddition(
            seed.count,
            additionalEntryCount
        )
        let finalHeader = try Self.bindingFootprint(
            entryCount: finalEntryCount
        )
        let payloads = try DatabaseIntermediateFootprint(
            bytes: seedFootprint.bytes - seedHeader.bytes
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: additional.bytes - additionalHeader.bytes
            )
        )
        return try finalHeader.adding(payloads)
    }

    /// Checks a join without materializing a merged Dictionary, then computes
    /// the canonical footprint of the exact left-biased merged row.
    func footprint(
        merging left: borrowing VariableBinding,
        with right: borrowing VariableBinding
    ) throws -> SPARQLBindingMergeFootprint {
        precondition(worklist.isEmpty)
        defer { worklist.removeAll(keepingCapacity: true) }

        return try left.withBindings { leftValues in
            try right.withBindings { rightValues in
                var additionalEntryCount = 0
                for (variable, rightValue) in rightValues {
                    if let leftValue = leftValues[variable] {
                        guard try valuesEqual(leftValue, rightValue) else {
                            return .incompatible
                        }
                    } else {
                        additionalEntryCount = try Self.checkedCountAddition(
                            additionalEntryCount,
                            1
                        )
                    }
                }

                let finalEntryCount = try Self.checkedCountAddition(
                    leftValues.count,
                    additionalEntryCount
                )
                var footprint = try Self.bindingFootprint(
                    entryCount: finalEntryCount
                )
                for (variable, value) in leftValues {
                    try addEntry(
                        variable: variable,
                        value: value,
                        to: &footprint
                    )
                }
                for (variable, value) in rightValues
                where leftValues[variable] == nil {
                    try addEntry(
                        variable: variable,
                        value: value,
                        to: &footprint
                    )
                }
                return .compatible(footprint)
            }
        }
    }

    /// Checks one VALUES row and computes its left-biased merged footprint
    /// before copying the seed Dictionary or constructing the result row.
    func footprint(
        extending seed: borrowing VariableBinding,
        with table: borrowing SPARQLValuesTable,
        row: Int
    ) throws -> SPARQLBindingMergeFootprint {
        precondition(row >= 0 && row < table.rowCount)
        precondition(worklist.isEmpty)
        defer { worklist.removeAll(keepingCapacity: true) }

        return try seed.withBindings { seedValues in
            var additionalEntryCount = 0
            for column in table.variables.indices {
                guard let value = table.value(row: row, column: column) else {
                    continue
                }
                let variable = table.variables[column]
                if let seedValue = seedValues[variable] {
                    guard try valuesEqual(seedValue, value) else {
                        return .incompatible
                    }
                } else {
                    additionalEntryCount = try Self.checkedCountAddition(
                        additionalEntryCount,
                        1
                    )
                }
            }

            let finalEntryCount = try Self.checkedCountAddition(
                seedValues.count,
                additionalEntryCount
            )
            var footprint = try Self.bindingFootprint(
                entryCount: finalEntryCount
            )
            for (variable, value) in seedValues {
                try addEntry(
                    variable: variable,
                    value: value,
                    to: &footprint
                )
            }
            for column in table.variables.indices {
                guard let value = table.value(row: row, column: column) else {
                    continue
                }
                let variable = table.variables[column]
                guard seedValues[variable] == nil else { continue }
                try addEntry(
                    variable: variable,
                    value: value,
                    to: &footprint
                )
            }
            return .compatible(footprint)
        }
    }

    /// Computes one BIND-style extension before the result Dictionary is
    /// copied. An equal existing binding is compatible and keeps the seed's
    /// left-biased representation; a different existing binding conflicts.
    func footprint(
        extending seed: borrowing VariableBinding,
        variable: String,
        value: FieldValue
    ) throws -> SPARQLBindingMergeFootprint {
        precondition(worklist.isEmpty)
        defer { worklist.removeAll(keepingCapacity: true) }

        return try seed.withBindings { seedValues in
            if let existing = seedValues[variable] {
                guard try valuesEqual(existing, value) else {
                    return .incompatible
                }
                var footprint = try Self.bindingFootprint(
                    entryCount: seedValues.count
                )
                for (seedVariable, seedValue) in seedValues {
                    try addEntry(
                        variable: seedVariable,
                        value: seedValue,
                        to: &footprint
                    )
                }
                return .compatible(footprint)
            }

            let finalEntryCount = try Self.checkedCountAddition(
                seedValues.count,
                1
            )
            var footprint = try Self.bindingFootprint(
                entryCount: finalEntryCount
            )
            for (seedVariable, seedValue) in seedValues {
                try addEntry(
                    variable: seedVariable,
                    value: seedValue,
                    to: &footprint
                )
            }
            try addEntry(
                variable: variable,
                value: value,
                to: &footprint
            )
            return .compatible(footprint)
        }
    }

    /// Computes the exact retained footprint of a projected row before its
    /// Dictionary is allocated. Projection variables are compiler-validated
    /// as unique, so one linear scan cannot count a binding twice.
    func footprint(
        projecting seed: borrowing VariableBinding,
        variables: [String]
    ) throws -> DatabaseIntermediateFootprint {
        precondition(worklist.isEmpty)
        defer { worklist.removeAll(keepingCapacity: true) }

        return try seed.withBindings { seedValues in
            var projectedEntryCount = 0
            for variable in variables where seedValues[variable] != nil {
                projectedEntryCount = try Self.checkedCountAddition(
                    projectedEntryCount,
                    1
                )
            }

            var footprint = try Self.bindingFootprint(
                entryCount: projectedEntryCount
            )
            for variable in variables {
                guard let value = seedValues[variable] else { continue }
                try addEntry(
                    variable: variable,
                    value: value,
                    to: &footprint
                )
            }
            return footprint
        }
    }

    /// Releases traversal scratch before a downstream owner is promoted.
    func shutdown() {
        worklist.removeAll(keepingCapacity: false)
        accountedCapacity = 0
        scratchReservation?.release()
        scratchReservation = nil
    }

    deinit {
        shutdown()
    }

    private func drainWorklist(
        into initial: DatabaseIntermediateFootprint
    ) throws -> DatabaseIntermediateFootprint {
        var footprint = initial
        while let item = worklist.popLast() {
            switch item {
            case .field(let value):
                switch value {
                case .int64, .uint64, .double, .bool, .null:
                    break
                case .string(let value):
                    footprint = try footprint.adding(
                        try Self.stringFootprint(value)
                    )
                case .data(let value):
                    footprint = try footprint.adding(
                        DatabaseIntermediateFootprint(
                            bytes: try Self.checkedAdd(
                                Self.bytesStorageOwnerByteCount,
                                Self.retainedStorageByteCount(of: value)
                            )
                        )
                    )
                case .rdfTerm(let term):
                    try append(.rdf(term))
                case .array(let values):
                    let slots = try DatabaseIntermediateFootprint(
                        bytes: Self.arrayElementSlotByteCount
                    ).multiplied(
                        by: try Self.checkedCapacity(for: values.count)
                    )
                    footprint = try footprint.adding(
                        DatabaseIntermediateFootprint(
                            bytes: Self.arrayStorageByteCount
                        )
                    ).adding(slots)
                    if !values.isEmpty {
                        try append(.fieldArray(values, nextIndex: 0))
                    }
                }

            case .fieldArray(let values, let nextIndex):
                let followingIndex = nextIndex + 1
                if followingIndex < values.count {
                    try append(
                        .fieldArray(values, nextIndex: followingIndex)
                    )
                }
                try append(.field(values[nextIndex]))

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

            case .fieldComparison, .fieldArrayComparison, .rdfComparison:
                preconditionFailure(
                    "Comparison work item reached footprint traversal"
                )
            }
        }
        return footprint
    }

    private func addEntry(
        variable: String,
        value: FieldValue,
        to footprint: inout DatabaseIntermediateFootprint
    ) throws {
        footprint = try footprint.adding(
            try Self.stringFootprint(variable)
        )
        try append(.field(value))
        footprint = try drainWorklist(into: footprint)
    }

    /// Iterative equality matching `FieldValue.==` without recursive Array or
    /// RDF-star descent.
    func valuesEqual(
        _ left: FieldValue,
        _ right: FieldValue
    ) throws -> Bool {
        precondition(worklist.isEmpty)
        try append(.fieldComparison(left, right))
        while let item = worklist.popLast() {
            switch item {
            case .fieldComparison(let left, let right):
                switch (left, right) {
                case (.array(let leftValues), .array(let rightValues)):
                    guard leftValues.count == rightValues.count else {
                        worklist.removeAll(keepingCapacity: true)
                        return false
                    }
                    if !leftValues.isEmpty {
                        try append(
                            .fieldArrayComparison(
                                leftValues,
                                rightValues,
                                nextIndex: 0
                            )
                        )
                    }
                case (.rdfTerm(let leftTerm), .rdfTerm(let rightTerm)):
                    try append(.rdfComparison(leftTerm, rightTerm))
                default:
                    guard left == right else {
                        worklist.removeAll(keepingCapacity: true)
                        return false
                    }
                }

            case .fieldArrayComparison(
                let leftValues,
                let rightValues,
                let nextIndex
            ):
                let followingIndex = nextIndex + 1
                if followingIndex < leftValues.count {
                    try append(
                        .fieldArrayComparison(
                            leftValues,
                            rightValues,
                            nextIndex: followingIndex
                        )
                    )
                }
                try append(
                    .fieldComparison(
                        leftValues[nextIndex],
                        rightValues[nextIndex]
                    )
                )

            case .rdfComparison(let left, let right):
                switch (left, right) {
                case (.iri(let leftValue), .iri(let rightValue)),
                     (.blankNode(let leftValue), .blankNode(let rightValue)):
                    guard leftValue == rightValue else {
                        worklist.removeAll(keepingCapacity: true)
                        return false
                    }
                case (.literal(let leftValue), .literal(let rightValue)):
                    guard leftValue == rightValue else {
                        worklist.removeAll(keepingCapacity: true)
                        return false
                    }
                case (
                    .tripleTerm(
                        let leftSubject,
                        let leftPredicate,
                        let leftObject
                    ),
                    .tripleTerm(
                        let rightSubject,
                        let rightPredicate,
                        let rightObject
                    )
                ):
                    try append(.rdfComparison(leftObject, rightObject))
                    try append(.rdfComparison(leftPredicate, rightPredicate))
                    try append(.rdfComparison(leftSubject, rightSubject))
                default:
                    worklist.removeAll(keepingCapacity: true)
                    return false
                }

            case .field, .fieldArray, .rdf:
                preconditionFailure(
                    "Footprint work item reached comparison traversal"
                )
            }
        }
        return true
    }

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
            let additionalBytes = try DatabaseIntermediateFootprint(
                bytes: Self.scratchSlotByteCount
            ).multiplied(by: additionalSlots).bytes
            guard let scratchReservation else {
                preconditionFailure(
                    "SPARQL footprint meter used after shutdown"
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

    private static func checkedCapacity(
        for count: Int
    ) throws -> UInt64 {
        guard count >= 0 else {
            throw DatabaseRetainedArrayLayoutError.invalidRequiredCount(count)
        }
        return try DatabaseIntermediateFootprint(
            bytes: UInt64(count)
        ).multiplied(by: geometricCapacityMultiplier).bytes
    }

    private static func bindingFootprint(
        entryCount: Int
    ) throws -> DatabaseIntermediateFootprint {
        let entryCapacity = try DatabaseIntermediateFootprint(
            bytes: bindingEntrySlotByteCount
        ).multiplied(
            by: try checkedCapacity(for: entryCount)
        )
        return try DatabaseIntermediateFootprint(
            rows: 1,
            bytes: bindingDictionaryByteCount
        ).adding(entryCapacity)
    }

    private static func checkedCountAddition(
        _ left: Int,
        _ right: Int
    ) throws -> Int {
        let (result, overflow) = left.addingReportingOverflow(right)
        guard !overflow, result >= 0 else {
            throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                currentCapacity: left
            )
        }
        return result
    }

    private static func stringFootprint(
        _ value: String
    ) throws -> DatabaseIntermediateFootprint {
        DatabaseIntermediateFootprint(
            bytes: try checkedAdd(
                stringStorageByteCount,
                UInt64(value.utf8.count)
            )
        )
    }

    private static func retainedStorageByteCount(
        of bytes: DatabaseBytes
    ) -> UInt64 {
        switch bytes.sharedStorage {
        case .array(let storage, _):
            UInt64(storage.count)
        case .allocation(let allocation, _):
            UInt64(allocation.count)
        case .owner(let owner, _):
            UInt64(owner.count)
        }
    }

    private static func checkedAdd(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        try DatabaseIntermediateFootprint(bytes: left)
            .adding(DatabaseIntermediateFootprint(bytes: right))
            .bytes
    }
}
