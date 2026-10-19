import DatabaseKit
import DatabaseTypes

/// Selected SQL reduction shared by grouped and frame-based execution.
enum SQLAggregateEvaluator {
    typealias Cell = DatabaseContext.ProjectedQueryValue
    struct Input {
        let arguments: [Cell]
        let ordering: [Cell]
        let workspace: SQLAggregateWorkspace
    }

    static func evaluate(bound: CanonicalBoundExpression, rowCount: Int,
        evaluate: (Int, CanonicalBoundExpression) async throws -> Cell,
        direct: (CanonicalBoundExpression) async throws -> Cell,
        temporalEnvironment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter,
        limits: QueryValueLimits) async throws -> Cell {
        guard rowCount >= 0, let selected = bound.aggregate else { throw mismatch("selected aggregate descriptor") }
        try workMeter.checkpoint(at: .aggregateInput)
        let memory = try SQLAggregateWorkspace(workMeter)
        @AdmittedArray var directValues: [Cell] = []
        for argument in selected.directArguments {
            await Task.yield()
            try workMeter.checkpoint(at: .aggregateInput)
            try memory.append(memory.retain(try await direct(argument)), to: &$directValues)
        }
        let orderedSet = !selected.withinGroup.isEmpty
        @AdmittedArray var inputs: [Input] = []
        var seen = try CanonicalSQLIdentitySet(workMeter: workMeter, stage: .aggregateInput)
        for ordinal in 0..<rowCount {
            await Task.yield()
            try workMeter.consume(at: .aggregateInput)
            if let filter = selected.filter {
                let cell = try await evaluate(ordinal, filter)
                if cell.value.isNull { continue }
                guard case .primitive(.bool(let accepted)) = cell.value else { throw mismatch("aggregate FILTER boolean") }
                if !accepted { continue }
            }
            let rowMemory = try SQLAggregateWorkspace(workMeter)
            try rowMemory.reservation.reserveAdditional(rows: 1, at: .aggregateInput)
            @AdmittedArray var arguments: [Cell] = []
            @AdmittedArray var ordering: [Cell] = []
            if !orderedSet {
                for argument in selected.arguments {
                    try rowMemory.append(rowMemory.retain(try await evaluate(ordinal, argument)), to: &$arguments)
                }
            }
            for key in orderedSet ? selected.withinGroup : selected.orderBy {
                let cell: Cell
                if !orderedSet, let index = key.projectionIndex {
                    guard arguments.indices.contains(index) else { throw mismatch("aggregate cached ordering argument") }
                    cell = arguments[index]
                } else { cell = try rowMemory.retain(try await evaluate(ordinal, key.expression)) }
                try rowMemory.append(cell, to: &$ordering)
            }
            if orderedSet {
                guard let input = ordering.first else { throw mismatch("ordered-set input") }
                if input.value.isNull { continue }
                try rowMemory.append(input, to: &$arguments)
            }
            if selected.call.distinct {
                @AdmittedArray var identities: [CanonicalSQLValueIdentity] = []
                for index in arguments.indices {
                    try rowMemory.append(CanonicalSQLValueIdentity.prepare(arguments[index].value,
                        type: selected.arguments[index].type, environment: temporalEnvironment,
                        workMeter: workMeter, stage: .aggregateInput), to: &$identities)
                }
                if try !seen.insert(identities) { continue }
            }
            try memory.append(Input(arguments: arguments, ordering: ordering, workspace: rowMemory), to: &$inputs)
        }
        let keys = orderedSet ? selected.withinGroup : selected.orderBy
        if !keys.isEmpty { try await sort(&inputs, keys: keys, environment: temporalEnvironment, memory: memory) }
        let result = try await finalize(selected, bound: bound, inputs: inputs, direct: directValues,
            environment: temporalEnvironment, memory: memory, limits: limits)
        let normalized = try bound.normalize(result.value)
        guard !result.leases.isEmpty else { return scalar(normalized) }
        return try exporting(normalized, leases: result.leases, workMeter: workMeter)
    }

    /// Copy the small lease vector into an independently admitted immutable output.
    /// The result retains no mutable input workspace or authority callback.
    private static func exporting(_ value: QueryValue, leases: [DatabaseQueryPayloadLease],
        workMeter: DatabaseWorkMeter) throws -> Cell {
        let count = leases.count.addingReportingOverflow(1)
        guard !count.overflow else { throw QueryValueError.arithmeticOverflow }
        let layout = try DatabaseRetainedArrayLayout.forElement(DatabaseQueryPayloadLease.self)
        let growth = try layout.growth(from: 0, toFit: count.partialValue)
        let claim = try workMeter.reserveIntermediate(bytes: try DatabaseIntermediateFootprint(bytes: layout.containerByteCount)
            .adding(.init(bytes: growth.additionalByteCount)).bytes, at: .aggregateInput)
        var output: [DatabaseQueryPayloadLease] = []
        output.reserveCapacity(growth.capacity)
        for lease in leases { try workMeter.consume(at: .aggregateInput); output.append(lease) }
        output.append(DatabaseQueryPayloadLease(retaining: claim))
        return Cell(value: value, leases: output, payloadIsRetained: true)
    }

    static func retained(_ value: QueryValue, lease: DatabaseQueryPayloadLease?,
        memory: SQLAggregateWorkspace) throws -> Cell {
        @AdmittedArray var leases: [DatabaseQueryPayloadLease] = []
        if let lease { try memory.append(lease, to: &$leases) }
        return Cell(value: value, leases: leases, payloadIsRetained: true)
    }

    static func scalar(_ value: QueryValue) -> Cell { Cell(value: value, leases: [], payloadIsRetained: false) }
    static var null: Cell { scalar(.primitive(.null)) }
    static func mismatch(_ operation: String) -> DatabaseExpressionEvaluationError { .typeMismatch(operation: operation) }
    static func invalid(_ function: SQLAggregateFunction, _ reason: String) -> DatabaseExpressionEvaluationError {
        .invalidFunctionArgument(function: function.rawValue, reason: reason)
    }

    static func checkpoint(_ meter: DatabaseWorkMeter) async throws {
        await Task.yield()
        try meter.consume(at: .aggregateInput)
    }

    private static func sort(_ rows: inout [Input], keys: [CanonicalBoundKey],
        environment: SQLTemporalEnvironment?, memory: SQLAggregateWorkspace) async throws {
        guard rows.count > 1 else { return }
        @AdmittedArray var scratch: [Input?] = []
        try memory.prepare(&$scratch, adding: rows.count)
        for _ in rows.indices { try memory.append(Optional<Input>.none, to: &$scratch) }
        func less(_ left: Input, _ right: Input) throws -> Bool {
            for index in keys.indices {
                try memory.meter.consume(at: .sortComparison)
                let key = keys[index]
                guard let sort = key.sortKey else { throw mismatch("selected aggregate ordering") }
                let result = try CanonicalSQLValueSemantics.compareValues(left.ordering[index].value,
                    right.ordering[index].value, type: key.expression.type, using: sort,
                    environment: environment, workMeter: memory.meter)
                if result != .equal { return result == .lessThan }
            }
            return false
        }
        var width = 1
        while width < rows.count {
            await Task.yield()
            try memory.meter.checkpoint(at: .aggregateInput)
            var start = 0
            while start < rows.count {
                let middle = start + min(rows.count - start, width)
                let end = middle + min(rows.count - middle, width)
                var left = start, right = middle, destination = start
                while left < middle || right < end {
                    try memory.meter.consume(at: .sortComparison)
                    if try left < middle && (right >= end || !less(rows[right], rows[left])) {
                        scratch[destination] = rows[left]; left += 1
                    } else { scratch[destination] = rows[right]; right += 1 }
                    destination += 1
                }
                for index in start..<end {
                    guard let row = scratch[index] else { throw mismatch("aggregate merge scratch") }
                    rows[index] = row; scratch[index] = nil
                }
                start = end
            }
            if width > rows.count / 2 { break }
            width *= 2
        }
    }

    private static func finalize(_ selected: CanonicalBoundAggregate, bound: CanonicalBoundExpression,
        inputs: [Input], direct: [Cell], environment: SQLTemporalEnvironment?,
        memory: SQLAggregateWorkspace, limits: QueryValueLimits) async throws -> Cell {
        switch selected.call.function {
        case .count, .sum, .avg, .min, .max, .boolAnd, .boolOr, .every, .bitAnd, .bitOr, .bitXor:
            return try await general(selected, bound: bound, inputs: inputs, environment: environment, memory: memory)
        case .stringAgg, .arrayAgg, .jsonAgg, .jsonbAgg, .jsonObjectAgg, .jsonbObjectAgg:
            return try await collection(selected, bound: bound, inputs: inputs, environment: environment,
                memory: memory, limits: limits)
        case .varPop, .varSamp, .variance, .stddevPop, .stddevSamp, .stddev:
            return try await variance(selected, inputs: inputs, memory: memory)
        case .corr, .covarPop, .covarSamp, .regrCount, .regrAvgx, .regrAvgy, .regrSxx,
             .regrSxy, .regrSyy, .regrSlope, .regrIntercept, .regrR2:
            return try await regression(selected, inputs: inputs, memory: memory)
        case .percentileCont, .percentileDisc, .mode:
            return try await ordered(selected, bound: bound, inputs: inputs, direct: direct,
                environment: environment, memory: memory, limits: limits)
        }
    }
}
