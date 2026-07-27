import DatabaseTypes
import DatabaseKit
import DatabaseEngine

/// Builds one SPARQL solution relation with admission before every retained
/// row construction and Array capacity change.
struct SPARQLRetainedBindingBuilder: ~Copyable {
    private var storage: DatabaseRetainedArrayBuilder<VariableBinding>
    private let footprintMeter: SPARQLBindingFootprintMeter

    private init(
        storage: consuming DatabaseRetainedArrayBuilder<VariableBinding>,
        footprintMeter: SPARQLBindingFootprintMeter
    ) {
        self.storage = storage
        self.footprintMeter = footprintMeter
    }

    static func make(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        expectedCount: Int = 0
    ) throws -> SPARQLRetainedBindingBuilder {
        let footprintMeter = try SPARQLBindingFootprintMeter.make(
            workMeter: workMeter,
            stage: stage
        )
        let storage = try DatabaseRetainedArrayBuilder<VariableBinding>(
            workMeter: workMeter,
            stage: stage,
            layout: try SPARQLBindingFootprintMeter.retainedArrayLayout(),
            expectedCount: expectedCount
        )
        return SPARQLRetainedBindingBuilder(
            storage: storage,
            footprintMeter: footprintMeter
        )
    }

    /// Reopens unique relation storage for appends without copying its Array
    /// buffer. Shared cache input is the only case that materializes a new
    /// unique header buffer, and every copied row is admitted first.
    static func resuming(
        _ bindings: consuming SPARQLRetainedBindings,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> SPARQLRetainedBindingBuilder {
        let footprintMeter = try SPARQLBindingFootprintMeter.make(
            workMeter: workMeter,
            stage: stage
        )
        switch consume bindings {
        case .empty:
            let storage = try DatabaseRetainedArrayBuilder<VariableBinding>(
                workMeter: workMeter,
                stage: stage,
                layout: try SPARQLBindingFootprintMeter
                    .retainedArrayLayout()
            )
            return SPARQLRetainedBindingBuilder(
                storage: storage,
                footprintMeter: footprintMeter
            )
        case .unique(let retained):
            return SPARQLRetainedBindingBuilder(
                storage: retained.resumeBuilding(at: stage),
                footprintMeter: footprintMeter
            )
        case .shared(let retained):
            let storage = try DatabaseRetainedArrayBuilder<VariableBinding>(
                workMeter: workMeter,
                stage: stage,
                layout: try SPARQLBindingFootprintMeter
                    .retainedArrayLayout(),
                expectedCount: retained.count
            )
            var builder = SPARQLRetainedBindingBuilder(
                storage: storage,
                footprintMeter: footprintMeter
            )
            for index in 0..<retained.count {
                try retained.withElement(at: index) { binding in
                    try builder.appendBorrowed(binding, at: stage)
                }
            }
            return consume builder
        case .sharedSlice(let retained, let visibleRange):
            let storage = try DatabaseRetainedArrayBuilder<VariableBinding>(
                workMeter: workMeter,
                stage: stage,
                layout: try SPARQLBindingFootprintMeter
                    .retainedArrayLayout(),
                expectedCount: visibleRange.count
            )
            var builder = SPARQLRetainedBindingBuilder(
                storage: storage,
                footprintMeter: footprintMeter
            )
            for index in visibleRange {
                try retained.withElement(at: index) { binding in
                    try builder.appendBorrowed(binding, at: stage)
                }
            }
            return consume builder
        }
    }

    var count: Int { storage.count }
    var isEmpty: Bool { storage.isEmpty }

    /// Measures and admits an existing row before retaining its value header.
    mutating func append(
        _ binding: consuming VariableBinding,
        at stage: DatabaseWorkStage? = nil
    ) throws {
        let footprint = try footprintMeter.footprint(of: binding)
        let admission = try storage.prepareAppend(
            footprint: footprint,
            at: stage
        )
        storage.append(binding, using: admission)
    }

    /// Retains one row borrowed from another live relation. The Dictionary
    /// storage remains copy-on-write shared; only the output row header is
    /// copied after admission.
    mutating func appendBorrowed(
        _ binding: borrowing VariableBinding,
        at stage: DatabaseWorkStage? = nil
    ) throws {
        let footprint = try footprintMeter.footprint(of: binding)
        let admission = try storage.prepareAppend(
            footprint: footprint,
            at: stage
        )
        let retainedBinding = copy binding
        storage.append(retainedBinding, using: admission)
    }

    /// Retains a bounded prefix of another relation without exposing its
    /// backing Array.
    mutating func appendBorrowed(
        contentsOf bindings: borrowing SPARQLRetainedBindings,
        limit: Int? = nil,
        at stage: DatabaseWorkStage? = nil
    ) throws {
        let appendCount = min(limit ?? bindings.count, bindings.count)
        for index in 0..<appendCount {
            try bindings.withElement(at: index) { binding in
                try appendBorrowed(binding, at: stage)
            }
        }
    }

    /// Admits a prospective row before its dictionary or deep payload is
    /// constructed. The caller must use the returned token for exactly that
    /// row and builder position.
    mutating func prepareAppend(
        footprint: DatabaseIntermediateFootprint,
        at stage: DatabaseWorkStage? = nil
    ) throws -> DatabaseRetainedArrayAppendAdmission<VariableBinding> {
        try storage.prepareAppend(footprint: footprint, at: stage)
    }

    /// Checks and admits a VALUES extension before the output row Dictionary
    /// is copied or mutated.
    mutating func prepareAppend(
        extending seed: borrowing VariableBinding,
        with table: borrowing SPARQLValuesTable,
        row: Int,
        at stage: DatabaseWorkStage? = nil
    ) throws -> SPARQLBindingAppendPreparation {
        switch try footprintMeter.footprint(
            extending: seed,
            with: table,
            row: row
        ) {
        case .incompatible:
            return .incompatible
        case .compatible(let footprint):
            return .admitted(
                try storage.prepareAppend(
                    footprint: footprint,
                    at: stage
                )
            )
        }
    }

    /// Checks and admits a join row before the merged output Dictionary is
    /// copied or mutated.
    mutating func prepareAppend(
        merging left: borrowing VariableBinding,
        with right: borrowing VariableBinding,
        at stage: DatabaseWorkStage? = nil
    ) throws -> SPARQLBindingAppendPreparation {
        switch try footprintMeter.footprint(
            merging: left,
            with: right
        ) {
        case .incompatible:
            return .incompatible
        case .compatible(let footprint):
            return .admitted(
                try storage.prepareAppend(
                    footprint: footprint,
                    at: stage
                )
            )
        }
    }

    /// Checks, admits, constructs, and appends one compatible merged row.
    /// No output Dictionary exists before both row and capacity admission.
    @discardableResult
    mutating func appendMerged(
        _ left: borrowing VariableBinding,
        with right: borrowing VariableBinding,
        at stage: DatabaseWorkStage? = nil
    ) throws -> Bool {
        switch try prepareAppend(
            merging: left,
            with: right,
            at: stage
        ) {
        case .incompatible:
            return false
        case .admitted(let admission):
            guard let merged = left.merged(with: right) else {
                throw SPARQLQueryError.executionFailed(
                    "prospective join compatibility disagrees with row construction"
                )
            }
            storage.append(merged, using: admission)
            return true
        }
    }

    /// Checks and admits a one-variable extension before the output row is
    /// copied or mutated.
    mutating func prepareAppend(
        extending seed: borrowing VariableBinding,
        variable: String,
        value: borrowing FieldValue,
        at stage: DatabaseWorkStage? = nil
    ) throws -> SPARQLBindingAppendPreparation {
        switch try footprintMeter.footprint(
            extending: seed,
            variable: variable,
            value: copy value
        ) {
        case .incompatible:
            return .incompatible
        case .compatible(let footprint):
            return .admitted(
                try storage.prepareAppend(
                    footprint: footprint,
                    at: stage
                )
            )
        }
    }

    /// Admits the projected row and output Array growth before projection
    /// allocates its Dictionary.
    mutating func prepareAppend(
        projecting seed: borrowing VariableBinding,
        variables: [String],
        at stage: DatabaseWorkStage? = nil
    ) throws -> DatabaseRetainedArrayAppendAdmission<VariableBinding> {
        let footprint = try footprintMeter.footprint(
            projecting: seed,
            variables: variables
        )
        return try storage.prepareAppend(
            footprint: footprint,
            at: stage
        )
    }

    /// Materializes one completed property-path match directly into the final
    /// query binding. Endpoint compatibility and the exact retained footprint
    /// are established before the Dictionary is allocated.
    mutating func appendCompatiblePropertyPathMatch(
        propertyPathMatch match: borrowing SPARQLPropertyPathMatch,
        retaining seed: borrowing VariableBinding,
        subject: ExecutionTerm,
        object: ExecutionTerm,
        footprintMeter: SPARQLPropertyPathBindingFootprintMeter,
        maximumResults: Int,
        at stage: DatabaseWorkStage? = nil
    ) throws {
        guard maximumResults >= 0 else {
            throw SPARQLPropertyPathMatchStorageError
                .invalidMaximumResults(maximumResults)
        }
        guard storage.count < maximumResults else {
            throw SPARQLQueryError.propertyPathResultLimitExceeded(
                maximum: maximumResults
            )
        }
        let footprint: DatabaseIntermediateFootprint
        switch try footprintMeter.footprint(
            retaining: seed,
            subject: subject,
            start: match.start,
            object: object,
            end: match.end
        ) {
        case .incompatible:
            throw SPARQLQueryError.executionFailed(
                "preflight-compatible property path disagrees with retained footprint construction"
            )
        case .compatible(let admittedFootprint):
            footprint = admittedFootprint
        }

        let admission = try storage.prepareAppend(
            footprint: footprint,
            at: stage
        )
        var binding = copy seed
        guard binding.match(
            subject,
            against: .rdfTerm(match.start)
        ), binding.match(
            object,
            against: .rdfTerm(match.end)
        ) else {
            throw SPARQLQueryError.executionFailed(
                "prospective property-path compatibility disagrees with row construction"
            )
        }
        storage.append(binding, using: admission)
    }

    mutating func append(
        _ binding: consuming VariableBinding,
        using admission: consuming DatabaseRetainedArrayAppendAdmission<VariableBinding>
    ) {
        storage.append(binding, using: admission)
    }

    consuming func finish() -> SPARQLRetainedBindings {
        footprintMeter.shutdown()
        guard !storage.isEmpty else {
            return .empty
        }
        return .unique(storage.finish())
    }
}
