import DatabaseKit

/// Selected positional record operations; relation presence is supplied by Rows.
enum CanonicalSQLCompositeEvaluator {
    static func construct(type: QueryCompositeType, fields: [QueryValue], workMeter: DatabaseWorkMeter,
        limits: QueryValueLimits) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        try CanonicalSQLValueConstruction.build(type: .sql(.composite(type)), values: fields,
            workMeter: workMeter, limits: limits)
    }

    static func evaluate(_ bound: CanonicalBoundExpression, operands: [QueryValue],
        temporalEnvironment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter,
        limits: QueryValueLimits) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        guard operands.count == bound.children.count else { throw mismatch("composite operand count") }
        try workMeter.consume(at: .expressionEvaluation)
        switch bound.expression {
        case .row:
            guard case .sql(.composite(let type)) = bound.type else { throw mismatch("ROW declaration") }
            return try construct(type: type, fields: operands, workMeter: workMeter, limits: limits)
        case .field:
            guard operands.count == 1, let index = bound.fieldOrdinal else { throw mismatch("selected composite field") }
            if operands[0].isNull { return (.primitive(.null), nil) }
            guard case .composite(let value) = operands[0], value.fields.indices.contains(index) else { throw mismatch("composite field ordinal") }
            return try retain(value.fields[index], workMeter: workMeter)
        case .fieldExpansion, .collate:
            guard let value = operands.first else { throw mismatch("selected wrapper operand") }
            return try retain(value, workMeter: workMeter)
        case .isNull, .isNotNull:
            guard let value = operands.first else { throw mismatch("row NULL predicate") }
            var allNull = value.isNull, allPresent = false
            if case .composite(let row) = value {
                allNull = true; allPresent = true
                for cell in row.fields {
                    try workMeter.consume(at: .expressionEvaluation)
                    allNull = allNull && cell.isNull
                    allPresent = allPresent && !cell.isNull
                }
            }
            if case .isNull = bound.expression { return (.primitive(.bool(allNull)), nil) }
            return (.primitive(.bool(allPresent)), nil)
        case .isDistinctFrom(_, _, let negated):
            guard operands.count == 2 else { throw mismatch("record DISTINCT arity") }
            let same: Bool
            if operands[0].isNull || operands[1].isNull { same = operands[0].isNull && operands[1].isNull }
            else { same = try compare(operands[0], operands[1], leftType: bound.children[0].type,
                rightType: bound.children[1].type, environment: temporalEnvironment, workMeter: workMeter) == .equal }
            return (.primitive(.bool(negated ? same : !same)), nil)
        case .equal, .notEqual, .lessThan, .lessThanOrEqual, .greaterThan, .greaterThanOrEqual:
            guard operands.count == 2 else { throw mismatch("record comparison arity") }
            if operands[0].isNull || operands[1].isNull { return (.primitive(.null), nil) }
            let result: QueryComparison?
            if bound.comparisonMode == .constructor {
                if case .equal = bound.expression {
                    return (try constructorEqual(operands[0], operands[1], environment: temporalEnvironment,
                        workMeter: workMeter).map { .primitive(.bool($0)) } ?? .primitive(.null), nil)
                }
                if case .notEqual = bound.expression {
                    return (try constructorEqual(operands[0], operands[1], environment: temporalEnvironment,
                        workMeter: workMeter).map { .primitive(.bool(!$0)) } ?? .primitive(.null), nil)
                }
                result = try constructorOrder(operands[0], operands[1], environment: temporalEnvironment, workMeter: workMeter)
            } else {
                result = try compare(operands[0], operands[1], leftType: bound.children[0].type,
                    rightType: bound.children[1].type, environment: temporalEnvironment, workMeter: workMeter)
            }
            guard let result else { return (.primitive(.null), nil) }
            let value: Bool
            switch bound.expression {
            case .equal: value = result == .equal
            case .notEqual: value = result != .equal
            case .lessThan: value = result == .lessThan
            case .lessThanOrEqual: value = result != .greaterThan
            case .greaterThan: value = result == .greaterThan
            case .greaterThanOrEqual: value = result != .lessThan
            default: throw mismatch("selected composite comparison")
            }
            return (.primitive(.bool(value)), nil)
        default: throw mismatch("selected composite expression")
        }
    }

    static func comparePredicate(_ left: QueryValue, _ right: QueryValue, leftType: QueryColumnType,
        rightType: QueryColumnType, comparison: DatabaseKit.ComparisonOperator, mode: CanonicalSQLCompositeComparisonMode,
        environment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter) throws -> Bool? {
        if left.isNull || right.isNull { return nil }
        if mode == .constructor, comparison == .equal || comparison == .notEqual {
            guard let equal = try constructorEqual(left, right, environment: environment, workMeter: workMeter) else { return nil }
            return comparison == .equal ? equal : !equal
        }
        let result: QueryComparison?
        if mode == .constructor { result = try constructorOrder(left, right, environment: environment, workMeter: workMeter) }
        else { result = try compare(left, right, leftType: leftType, rightType: rightType, environment: environment, workMeter: workMeter) }
        guard let result else { return nil }
        switch comparison {
        case .equal: return result == .equal
        case .notEqual: return result != .equal
        case .lessThan: return result == .lessThan
        case .lessThanOrEqual: return result != .greaterThan
        case .greaterThan: return result == .greaterThan
        case .greaterThanOrEqual: return result != .lessThan
        }
    }

    private static func constructorEqual(_ left: QueryValue, _ right: QueryValue,
        environment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter) throws -> Bool? {
        guard case .composite(let a) = left, case .composite(let b) = right,
              a.fields.count == b.fields.count else { throw mismatch("row comparison arity") }
        var unknown = false
        for index in a.fields.indices {
            try workMeter.consume(at: .expressionEvaluation)
            let x = a.fields[index], y = b.fields[index]
            if x.isNull || y.isNull { unknown = true; continue }
            if try compare(x, y, leftType: a.type.columns[index].type, rightType: b.type.columns[index].type,
                environment: environment, workMeter: workMeter) != .equal { return false }
        }
        return unknown ? nil : true
    }

    private static func constructorOrder(_ left: QueryValue, _ right: QueryValue,
        environment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter) throws -> QueryComparison? {
        guard case .composite(let a) = left, case .composite(let b) = right,
              a.fields.count == b.fields.count else { throw mismatch("row comparison arity") }
        for index in a.fields.indices {
            try workMeter.consume(at: .expressionEvaluation)
            let x = a.fields[index], y = b.fields[index]
            if x.isNull || y.isNull { return nil }
            let result = try compare(x, y, leftType: a.type.columns[index].type, rightType: b.type.columns[index].type,
                environment: environment, workMeter: workMeter)
            if result != .equal { return result }
        }
        return .equal
    }

    private enum Visit {
        case pair(QueryValue, QueryValue, QueryColumnType, QueryColumnType)
        case arrayTail(QueryArrayValue, QueryArrayValue)
    }

    static func compare(_ left: QueryValue, _ right: QueryValue, leftType: QueryColumnType,
        rightType: QueryColumnType, environment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter) throws -> QueryComparison {
        let layout = try DatabaseRetainedArrayLayout.forElement(Visit.self)
        let scratch = try workMeter.reserveIntermediate(bytes: layout.containerByteCount, at: .expressionEvaluation)
        defer { scratch.release() }
        var pending: [Visit] = []
        func push(_ visit: Visit) throws {
            let growth = try layout.growth(from: pending.capacity, toFit: pending.count + 1)
            if growth.capacity > pending.capacity {
                try scratch.reserveAdditional(bytes: growth.additionalByteCount, at: .expressionEvaluation)
                pending.reserveCapacity(growth.capacity)
                if pending.capacity > growth.capacity {
                    try scratch.reserveAdditional(bytes: try DatabaseIntermediateFootprint(bytes: layout.elementCapacitySlotByteCount)
                        .multiplied(by: UInt64(pending.capacity - growth.capacity)).bytes, at: .expressionEvaluation)
                }
            }
            pending.append(visit)
        }
        try push(.pair(left, right, leftType, rightType))
        while let visit = pending.popLast() {
            try workMeter.consume(at: .expressionEvaluation)
            switch visit {
            case .pair(let a, let b, let at, let bt):
                if a.isNull || b.isNull {
                    if a.isNull && b.isNull { continue }
                    return a.isNull ? .greaterThan : .lessThan
                }
                switch (a, b, at, bt) {
                case (.composite(let a), .composite(let b), .sql(.composite(let ad)), .sql(.composite(let bd))):
                    guard a.fields.count == b.fields.count, a.fields.count == ad.columns.count,
                          b.fields.count == bd.columns.count else { throw mismatch("record comparison arity") }
                    for index in a.fields.indices.reversed() {
                        try push(.pair(a.fields[index], b.fields[index], ad.columns[index].type, bd.columns[index].type))
                    }
                case (.array(let a), .array(let b), .sql(.array), .sql(.array)):
                    try push(.arrayTail(a, b))
                    for index in (0..<min(a.elements.count, b.elements.count)).reversed() {
                        try push(.pair(a.elements[index], b.elements[index], .sql(a.elementType), .sql(b.elementType)))
                    }
                default:
                    let result = try CanonicalSQLValueSemantics.compareValues(a, b, leftType: at, rightType: bt,
                        environment: environment, workMeter: workMeter)
                    if result != .equal { return result }
                }
            case .arrayTail(let a, let b):
                if a.elements.count != b.elements.count { return a.elements.count < b.elements.count ? .lessThan : .greaterThan }
                if a.dimensions.count != b.dimensions.count { return a.dimensions.count < b.dimensions.count ? .lessThan : .greaterThan }
                for index in a.dimensions.indices {
                    if a.dimensions[index].length != b.dimensions[index].length {
                        return a.dimensions[index].length < b.dimensions[index].length ? .lessThan : .greaterThan
                    }
                }
                for index in a.dimensions.indices {
                    if a.dimensions[index].lowerBound != b.dimensions[index].lowerBound {
                        return a.dimensions[index].lowerBound < b.dimensions[index].lowerBound ? .lessThan : .greaterThan
                    }
                }
            }
        }
        return .equal
    }

    static func cast(_ value: QueryValue, from source: QueryColumnType, to target: QueryColumnType,
        context: CanonicalQueryTypeBinder.CastContext, temporalEnvironment: SQLTemporalEnvironment?,
        workMeter: DatabaseWorkMeter, limits: QueryValueLimits) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        guard case .sql(.composite(let destination)) = target else { throw mismatch("composite cast target") }
        if value.isNull { return (.primitive(.null), nil) }
        guard case .composite(let record) = value, case .sql(.composite(let origin)) = source,
              origin.columns.count == destination.columns.count else { throw mismatch("composite cast arity") }
        if source == target { return try retain(value, workMeter: workMeter) }
        let count = record.fields.count
        let valueLayout = try DatabaseRetainedArrayLayout.forElement(QueryValue.self)
        let typeLayout = try DatabaseRetainedArrayLayout.forElement(QueryColumnType.self)
        let valuesGrowth = try valueLayout.growth(from: 0, toFit: count)
        let typesGrowth = try typeLayout.growth(from: 0, toFit: count)
        let scratch = try workMeter.reserveIntermediate(bytes: try DatabaseIntermediateFootprint(bytes: valueLayout.containerByteCount)
            .adding(.init(bytes: typeLayout.containerByteCount)).adding(.init(bytes: valuesGrowth.additionalByteCount))
            .adding(.init(bytes: typesGrowth.additionalByteCount)).bytes, at: .expressionEvaluation)
        defer { scratch.release() }
        var fields: [QueryValue] = [], sources: [QueryColumnType] = []
        fields.reserveCapacity(valuesGrowth.capacity); sources.reserveCapacity(typesGrowth.capacity)
        if fields.capacity > valuesGrowth.capacity {
            try scratch.reserveAdditional(bytes: try DatabaseIntermediateFootprint(bytes: valueLayout.elementCapacitySlotByteCount)
                .multiplied(by: UInt64(fields.capacity - valuesGrowth.capacity)).bytes, at: .expressionEvaluation)
        }
        if sources.capacity > typesGrowth.capacity {
            try scratch.reserveAdditional(bytes: try DatabaseIntermediateFootprint(bytes: typeLayout.elementCapacitySlotByteCount)
                .multiplied(by: UInt64(sources.capacity - typesGrowth.capacity)).bytes, at: .expressionEvaluation)
        }
        for index in record.fields.indices {
            try workMeter.consume(at: .expressionEvaluation)
            fields.append(record.fields[index]); sources.append(origin.columns[index].type)
        }
        return try CanonicalSQLValueConstruction.build(type: target, values: fields, sourceTypes: sources,
            context: context, temporalEnvironment: temporalEnvironment, workMeter: workMeter, limits: limits)
    }

    private static func retain(_ value: QueryValue, workMeter: DatabaseWorkMeter) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        if value.isNull { return (value, nil) }
        let footprint = try CanonicalRelationalFootprintMeter.valueFootprint(of: value,
            workMeter: workMeter, stage: .expressionEvaluation)
        let claim = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .expressionEvaluation)
        return (value, DatabaseQueryPayloadLease(retaining: claim))
    }

    private static func mismatch(_ operation: String) -> DatabaseExpressionEvaluationError { .typeMismatch(operation: operation) }
}
