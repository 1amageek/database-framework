import DatabaseKit
import DatabaseTypes

/// Executes selected windows over original indices without retaining callbacks.
enum SQLWindowEvaluator {
    static func evaluate(
        plan: CanonicalBoundWindow,
        rowCount: Int,
        evaluate: (Int, CanonicalBoundExpression) async throws -> DatabaseContext.ProjectedQueryValue,
        direct: (CanonicalBoundExpression) async throws -> DatabaseContext.ProjectedQueryValue,
        workMeter: DatabaseWorkMeter,
        temporalEnvironment: SQLTemporalEnvironment?,
        limits: QueryValueLimits
    ) async throws -> SQLWindowResults {
        guard rowCount >= 0 else { throw mismatch("negative window row count") }
        try workMeter.checkpoint(at: .expressionEvaluation)
        let offsets = try await SQLWindowFrameEvaluator.offsets(plan.frame, direct: direct, workMeter: workMeter)
        let scratch = try workMeter.reserveIntermediate(at: .expressionEvaluation)
        defer { scratch.release() }
        let output = try workMeter.reserveIntermediate(rows: UInt64(rowCount), at: .expressionEvaluation)
        var completed = false
        defer { if !completed { output.release() } }
        let keyCount = plan.partitionBy.count.addingReportingOverflow(plan.orderBy.count)
        let total = rowCount.multipliedReportingOverflow(by: keyCount.partialValue)
        guard !keyCount.overflow, !total.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
        var keys = try SQLWindowMemory.array(count: total.partialValue,
            of: DatabaseContext.ProjectedQueryValue.self, reservation: scratch)
        var indices = try SQLWindowMemory.array(count: rowCount, of: Int.self, reservation: scratch)
        var peerStarts = try SQLWindowMemory.array(count: rowCount, of: Int.self, reservation: scratch)
        var peerEnds = try SQLWindowMemory.array(count: rowCount, of: Int.self, reservation: scratch)
        var groups = try SQLWindowMemory.array(count: rowCount, of: Int.self, reservation: scratch)
        var members = try SQLWindowMemory.array(count: rowCount, of: Int.self, reservation: scratch)
        var values = try SQLWindowMemory.array(count: rowCount,
            of: DatabaseContext.ProjectedQueryValue.self, reservation: output)
        for row in 0..<rowCount {
            try workMeter.consume(at: .expressionEvaluation)
            indices.append(row); peerStarts.append(0); peerEnds.append(0); groups.append(0)
            values.append(null)
            for expression in plan.partitionBy {
                let cell = try await evaluate(row, expression)
                try SQLWindowMemory.retain(cell, reservation: scratch)
                keys.append(cell)
            }
            for key in plan.orderBy {
                let cell = try await evaluate(row, key.expression)
                try SQLWindowMemory.retain(cell, reservation: scratch)
                keys.append(cell)
            }
        }
        func partitionComparison(_ left: Int, _ right: Int) throws -> QueryComparison {
            try workMeter.consume(at: .sortComparison)
            for key in plan.partitionBy.indices {
                try workMeter.consume(at: .sortComparison)
                let expression = plan.partitionBy[key]
                let l = keys[left * keyCount.partialValue + key].value
                let r = keys[right * keyCount.partialValue + key].value
                if l.isNull || r.isNull {
                    if l.isNull && r.isNull { continue }
                    return l.isNull ? .lessThan : .greaterThan
                }
                let result = try CanonicalSQLValueSemantics.compareValues(l, r,
                    leftType: expression.type, rightType: expression.type,
                    environment: temporalEnvironment, workMeter: workMeter)
                if result != .equal { return result }
            }
            return .equal
        }
        func orderingComparison(_ left: Int, _ right: Int) throws -> QueryComparison {
            try workMeter.consume(at: .sortComparison)
            for key in plan.orderBy.indices {
                try workMeter.consume(at: .sortComparison)
                let bound = plan.orderBy[key]
                guard let sort = bound.sortKey else { throw mismatch("window ordering declaration") }
                let slot = plan.partitionBy.count + key
                let result = try CanonicalSQLValueSemantics.compareValues(
                    keys[left * keyCount.partialValue + slot].value,
                    keys[right * keyCount.partialValue + slot].value,
                    type: bound.expression.type, using: sort,
                    environment: temporalEnvironment, workMeter: workMeter)
                if result != .equal { return result }
            }
            return .equal
        }
        try SQLWindowMemory.sort(&indices) { left, right in
            let partition = try partitionComparison(left, right)
            if partition != .equal { return partition == .lessThan }
            let order = try orderingComparison(left, right)
            return order == .equal ? left < right : order == .lessThan
        }
        let aggregateInputs: SQLWindowAggregateInputs?
        if let aggregate = plan.aggregate?.aggregate,
           (plan.frame?.exclusion ?? .noOthers) == .noOthers {
            let slots = aggregate.arguments.count + (aggregate.filter == nil ? 0 : 1)
            aggregateInputs = slots == 0 ? nil : try SQLWindowAggregateInputs(
                rowCount: rowCount, slotCount: slots, workMeter: workMeter)
        } else { aggregateInputs = nil }
        var lower = 0
        while lower < rowCount {
            try workMeter.consume(at: .expressionEvaluation)
            var upper = lower + 1
            while upper < rowCount, try partitionComparison(indices[lower], indices[upper]) == .equal { upper += 1 }
            var peer = lower
            var group = 0
            while peer < upper {
                var end = peer + 1
                while end < upper, try orderingComparison(indices[peer], indices[end]) == .equal { end += 1 }
                for position in peer..<end {
                    try workMeter.consume(at: .expressionEvaluation)
                    peerStarts[position] = peer; peerEnds[position] = end; groups[position] = group
                }
                peer = end; group += 1
            }
            var buckets: Int64?
            var bucketStart = lower
            for position in lower..<upper {
                try workMeter.consume(at: .expressionEvaluation)
                let original = indices[position]
                let result: DatabaseContext.ProjectedQueryValue
                if let aggregate = plan.aggregate {
                    members.removeAll(keepingCapacity: true)
                    let bounds = try SQLWindowFrameEvaluator.bounds(plan: plan, position: position,
                        partition: lower..<upper, indices: indices, peerStarts: peerStarts,
                        peerEnds: peerEnds, groups: groups, keys: keys, keyCount: keyCount.partialValue,
                        offsets: offsets, environment: temporalEnvironment, workMeter: workMeter)
                    for candidate in bounds {
                        try workMeter.consume(at: .aggregateInput)
                        guard SQLWindowFrameEvaluator.includes(candidate,
                            current: position, peerStart: peerStarts[position], peerEnd: peerEnds[position],
                            exclusion: plan.frame?.exclusion ?? .noOthers) else { continue }
                        members.append(indices[candidate])
                    }
                    try aggregateInputs?.beginFrame(head: bounds.lowerBound)
                    result = try await SQLAggregateEvaluator.evaluate(bound: aggregate, rowCount: members.count,
                        evaluate: { index, expression in
                            let row = members[index]
                            guard let aggregateInputs else { return try await evaluate(row, expression) }
                            let slot = try aggregateInputs.next(row: row)
                            if let cached = slot.value { return cached }
                            let selected = try await evaluate(row, expression)
                            return try aggregateInputs.store(selected, at: slot.index)
                        },
                        direct: { expression in try await evaluate(original, expression) },
                        temporalEnvironment: temporalEnvironment, workMeter: workMeter, limits: limits)
                } else {
                    guard case .analytic(let function, _) = plan.call.function else { throw mismatch("window function declaration") }
                    switch function {
                    case .rowNumber: result = integer(position - lower + 1)
                    case .rank: result = integer(peerStarts[position] - lower + 1)
                    case .denseRank: result = integer(groups[position] + 1)
                    case .percentRank:
                        result = floating(upper - lower == 1 ? 0 : Double(peerStarts[position] - lower) / Double(upper - lower - 1))
                    case .cumeDist: result = floating(Double(peerEnds[position] - lower) / Double(upper - lower))
                    case .ntile:
                        if buckets == nil {
                            guard let argument = plan.arguments.first else { throw mismatch("NTILE argument") }
                            buckets = try SQLWindowMemory.integer(await evaluate(original, argument).value, function: "ntile")
                            if let buckets, buckets <= 0 { throw invalid("ntile", "bucket count must be positive") }
                            bucketStart = position
                        }
                        if let buckets {
                            let count = Int64(upper - lower), ordinal = Int64(position - bucketStart)
                            let size = count / buckets, remainder = count % buckets
                            let threshold = (size + 1) * remainder
                            let bucket = ordinal < threshold ? ordinal / (size + 1) + 1
                                : remainder + (ordinal - threshold) / max(1, size) + 1
                            result = .init(value: .primitive(.int32(Int32(bucket))), leases: [], payloadIsRetained: false)
                        } else { result = null }
                    case .lag, .lead:
                        guard let argument = plan.arguments.first else { throw mismatch("LAG/LEAD argument") }
                        let offset: Int64?
                        if plan.arguments.count > 1 {
                            offset = try SQLWindowMemory.integer(await evaluate(original, plan.arguments[1]).value, function: function.rawValue)
                        } else { offset = 1 }
                        if let offset {
                            let target = Int128(position) + (function == .lead ? Int128(offset) : -Int128(offset))
                            if target >= Int128(lower), target < Int128(upper) {
                                result = try await evaluate(indices[Int(target)], argument)
                            } else if plan.arguments.count > 2 {
                                result = try await evaluate(original, plan.arguments[2])
                            } else { result = null }
                        } else { result = null }
                    case .firstValue, .lastValue, .nthValue:
                        guard let argument = plan.arguments.first else { throw mismatch("frame value argument") }
                        let nth: Int64?
                        if function == .nthValue {
                            guard plan.arguments.count > 1 else { throw mismatch("NTH_VALUE count") }
                            nth = try SQLWindowMemory.integer(await evaluate(original, plan.arguments[1]).value, function: function.rawValue)
                            if let nth, nth <= 0 { throw invalid("nth_value", "position must be positive") }
                        } else { nth = 1 }
                        if let nth {
                            let bounds = try SQLWindowFrameEvaluator.bounds(plan: plan, position: position,
                                partition: lower..<upper, indices: indices, peerStarts: peerStarts,
                                peerEnds: peerEnds, groups: groups, keys: keys, keyCount: keyCount.partialValue,
                                offsets: offsets, environment: temporalEnvironment, workMeter: workMeter)
                            var selected: Int?
                            var ordinal: Int64 = 0
                            for candidate in bounds {
                                try workMeter.consume(at: .expressionEvaluation)
                                guard SQLWindowFrameEvaluator.includes(candidate, current: position,
                                    peerStart: peerStarts[position], peerEnd: peerEnds[position],
                                    exclusion: plan.frame?.exclusion ?? .noOthers) else { continue }
                                ordinal += 1
                                if function == .lastValue { selected = candidate }
                                else if ordinal == nth { selected = candidate; break }
                            }
                            if let selected { result = try await evaluate(indices[selected], argument) }
                            else { result = null }
                        } else { result = null }
                    }
                }
                try SQLWindowMemory.retain(result, reservation: output)
                values[original] = result
            }
            lower = upper
        }
        let leaseLayout = try DatabaseRetainedArrayLayout.forElement(DatabaseIntermediateReservation.self)
        let leaseGrowth = try leaseLayout.growth(from: 0, toFit: 1)
        try output.reserveAdditional(bytes: try DatabaseIntermediateFootprint(bytes: leaseLayout.containerByteCount)
            .adding(.init(bytes: leaseGrowth.additionalByteCount)).bytes, at: .expressionEvaluation)
        try workMeter.checkpoint(at: .expressionEvaluation)
        completed = true
        return SQLWindowResults(values: values, payloadLease: .init(retaining: output))
    }

    private static var null: DatabaseContext.ProjectedQueryValue {
        .init(value: .primitive(.null), leases: [], payloadIsRetained: false)
    }
    private static func integer(_ value: Int) -> DatabaseContext.ProjectedQueryValue {
        .init(value: .primitive(.int64(Int64(value))), leases: [], payloadIsRetained: false)
    }
    private static func floating(_ value: Double) -> DatabaseContext.ProjectedQueryValue {
        .init(value: .primitive(.float64(value)), leases: [], payloadIsRetained: false)
    }
    private static func mismatch(_ operation: String) -> DatabaseExpressionEvaluationError { .typeMismatch(operation: operation) }
    private static func invalid(_ function: String, _ reason: String) -> DatabaseExpressionEvaluationError {
        .invalidFunctionArgument(function: function, reason: reason)
    }
}
