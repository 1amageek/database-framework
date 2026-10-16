import DatabaseKit
import DatabaseTypes

/// Executes a signature already selected by the canonical binder.
enum CanonicalSQLArrayEvaluator {
    static func evaluate(_ bound: CanonicalBoundExpression, operands: [QueryValue],
        temporalEnvironment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter,
        limits: QueryValueLimits) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        guard operands.count == bound.children.count else { throw SQLArrayStorage.mismatch("selected array argument count") }
        for index in operands.indices {
            try SQLArrayStorage.validate(operands[index], type: bound.children[index].type, meter: workMeter)
        }
        switch bound.expression {
        case .array:
            return try construct(bound, operands: operands, meter: workMeter, limits: limits)
        case .arrayAccess(_, let subscripts):
            return try access(bound, operands: operands, subscripts: subscripts, meter: workMeter, limits: limits)
        case .concatenate:
            guard operands.count == 2 else { throw SQLArrayStorage.mismatch("array concatenation arity") }
            if case .sql(.array) = bound.children[0].type {
                if case .sql(.array) = bound.children[1].type { }
                else { return try append(operands[0], element: operands[1], prepend: false, type: bound.type, meter: workMeter, limits: limits) }
            } else {
                return try append(operands[1], element: operands[0], prepend: true, type: bound.type, meter: workMeter, limits: limits)
            }
            return try concatenate(operands[0], operands[1], type: bound.type, meter: workMeter, limits: limits)
        case .collectionOperation(let operation, _, _):
            guard operands.count == 2 else { throw SQLArrayStorage.mismatch("array collection arity") }
            if operands[0].isNull || operands[1].isNull { return (SQLArrayStorage.null, nil) }
            guard let left = try SQLArrayStorage.array(operands[0], type: bound.children[0].type),
                  let right = try SQLArrayStorage.array(operands[1], type: bound.children[1].type),
                  left.elementType == right.elementType else { throw SQLArrayStorage.mismatch("selected array collection") }
            let result: Bool
            switch operation {
            case .contains: result = try contains(left, right, overlap: false, environment: temporalEnvironment, meter: workMeter)
            case .containedBy: result = try contains(right, left, overlap: false, environment: temporalEnvironment, meter: workMeter)
            case .overlaps: result = try contains(left, right, overlap: true, environment: temporalEnvironment, meter: workMeter)
            default: throw SQLArrayStorage.mismatch("selected array collection operator")
            }
            return (.primitive(.bool(result)), nil)
        case .function(let call):
            return try function(call.name, bound: bound, operands: operands, environment: temporalEnvironment,
                meter: workMeter, limits: limits)
        default: throw SQLArrayStorage.mismatch("selected array expression")
        }
    }

    static func cast(_ value: QueryValue, from source: QueryColumnType, to target: QueryColumnType,
        context: CanonicalQueryTypeBinder.CastContext, temporalEnvironment: SQLTemporalEnvironment?,
        workMeter: DatabaseWorkMeter, limits: QueryValueLimits) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        try SQLArrayStorage.validate(value, type: source, meter: workMeter)
        guard QueryValueValidator.accepts(SQLArrayStorage.null, type: target) else { throw SQLArrayStorage.mismatch("selected array cast target") }
        if case .sql(.array(let element)) = target {
            _ = try SQLArrayStorage.elementType(target)
            if value.isNull { return (SQLArrayStorage.null, nil) }
            if case .primitive(.string(let text)) = value {
                return try SQLArrayText.parse(text, element: element, context: context, environment: temporalEnvironment,
                    meter: workMeter, limits: limits)
            }
            if case .array(let array) = value {
                let sourceElement = try SQLArrayStorage.elementType(source)
                return try SQLArrayStorage.makeArray(element: element, rank: array.dimensions.count,
                    count: array.elements.count, meter: workMeter, limits: limits,
                    dimension: { array.dimensions[$0] }, cell: { index, claim in
                        try SQLArrayStorage.scalarCast(array.elements[index], from: .sql(sourceElement), to: .sql(element),
                            context: context, environment: temporalEnvironment, meter: workMeter, limits: limits, destination: claim)
                    })
            }
            if case .primitive(.array(let values)) = value, case .primitive(let primitive, true) = source {
                return try SQLArrayStorage.makeArray(element: element, rank: values.isEmpty ? 0 : 1,
                    count: values.count, meter: workMeter, limits: limits,
                    dimension: { _ in try SQLArrayStorage.dimension(length: values.count) }, cell: { index, claim in
                        try SQLArrayStorage.scalarCast(.primitive(values[index]), from: .primitive(primitive), to: .sql(element),
                            context: context, environment: temporalEnvironment, meter: workMeter, limits: limits, destination: claim)
                    })
            }
            throw SQLArrayStorage.mismatch("selected array cast source")
        }
        guard target == .sql(.text) || target == .primitive(.string),
              let array = try SQLArrayStorage.array(value, type: source) else {
            if value.isNull, target == .sql(.text) || target == .primitive(.string) { return (SQLArrayStorage.null, nil) }
            throw SQLArrayStorage.mismatch("selected array text cast")
        }
        return try SQLArrayText.format(array, environment: temporalEnvironment, meter: workMeter)
    }

    static func compare(_ left: QueryValue, _ right: QueryValue, leftType: QueryColumnType,
        rightType: QueryColumnType, temporalEnvironment: SQLTemporalEnvironment?,
        workMeter: DatabaseWorkMeter) throws -> QueryComparison {
        try SQLArrayStorage.validate(left, type: leftType, meter: workMeter)
        try SQLArrayStorage.validate(right, type: rightType, meter: workMeter)
        guard let lhs = try SQLArrayStorage.array(left, type: leftType),
              let rhs = try SQLArrayStorage.array(right, type: rightType) else { throw SQLArrayStorage.mismatch("non-NULL array comparison") }
        for index in 0..<min(lhs.elements.count, rhs.elements.count) {
            try workMeter.consume(at: .expressionEvaluation)
            let a = lhs.elements[index], b = rhs.elements[index]
            if a.isNull || b.isNull {
                if a.isNull && b.isNull { continue }
                return a.isNull ? .greaterThan : .lessThan
            }
            let result = try CanonicalSQLValueSemantics.compareValues(a, b, leftType: .sql(lhs.elementType),
                rightType: .sql(rhs.elementType), environment: temporalEnvironment, workMeter: workMeter)
            if result != .equal { return result }
        }
        if lhs.elements.count != rhs.elements.count { return lhs.elements.count < rhs.elements.count ? .lessThan : .greaterThan }
        if lhs.dimensions.count != rhs.dimensions.count { return lhs.dimensions.count < rhs.dimensions.count ? .lessThan : .greaterThan }
        for index in lhs.dimensions.indices {
            try workMeter.consume(at: .expressionEvaluation)
            if lhs.dimensions[index].length != rhs.dimensions[index].length {
                return lhs.dimensions[index].length < rhs.dimensions[index].length ? .lessThan : .greaterThan
            }
        }
        for index in lhs.dimensions.indices {
            try workMeter.consume(at: .expressionEvaluation)
            if lhs.dimensions[index].lowerBound != rhs.dimensions[index].lowerBound {
                return lhs.dimensions[index].lowerBound < rhs.dimensions[index].lowerBound ? .lessThan : .greaterThan
            }
        }
        return .equal
    }

    static func makeRowCursor(_ bound: CanonicalBoundExpression, operands: [QueryValue],
        inputLeases: [DatabaseQueryPayloadLease], temporalEnvironment: SQLTemporalEnvironment?,
        workMeter: DatabaseWorkMeter, limits: QueryValueLimits) throws -> SQLArrayRowCursor {
        try SQLArrayRowCursor(bound: bound, operands: operands, inputLeases: inputLeases,
            temporalEnvironment: temporalEnvironment, workMeter: workMeter, limits: limits)
    }

    private static func construct(_ bound: CanonicalBoundExpression, operands: [QueryValue],
        meter: DatabaseWorkMeter, limits: QueryValueLimits) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        let element = try SQLArrayStorage.elementType(bound.type)
        guard !operands.isEmpty else { return try SQLArrayStorage.empty(element: element, meter: meter, limits: limits) }
        if case .sql(.array) = bound.children[0].type {
            var first: QueryArrayValue?
            var emptyCount = 0
            for index in operands.indices {
                try meter.consume(at: .expressionEvaluation)
                guard let array = try SQLArrayStorage.array(operands[index], type: bound.children[index].type), !array.dimensions.isEmpty else {
                    emptyCount += 1; continue
                }
                guard array.elementType == element else { throw SQLArrayStorage.mismatch("selected nested ARRAY element") }
                if let first, array.dimensions != first.dimensions {
                    throw SQLArrayStorage.failure(.invalidArrayShape)
                }
                first = array
            }
            guard let first else { return try SQLArrayStorage.empty(element: element, meter: meter, limits: limits) }
            guard emptyCount == 0, first.dimensions.count < 6 else { throw SQLArrayStorage.failure(.invalidArrayShape) }
            let count = try SQLArrayStorage.checkedMultiply(operands.count, first.elements.count)
            return try SQLArrayStorage.makeArray(element: element, rank: first.dimensions.count + 1,
                count: count, meter: meter, limits: limits, dimension: { index in
                    if index == 0 { return try SQLArrayStorage.dimension(length: operands.count) }
                    return first.dimensions[index - 1]
                }, cell: { index, _ in
                    guard case .array(let array) = operands[index / first.elements.count] else { throw SQLArrayStorage.failure(.invalidArrayShape) }
                    return array.elements[index % first.elements.count]
                })
        }
        for child in bound.children where child.type != .sql(element) { throw SQLArrayStorage.mismatch("selected ARRAY element") }
        return try SQLArrayStorage.makeArray(element: element, rank: 1, count: operands.count,
            meter: meter, limits: limits, dimension: { _ in try SQLArrayStorage.dimension(length: operands.count) },
            cell: { index, _ in operands[index] })
    }

    private static func access(_ bound: CanonicalBoundExpression, operands: [QueryValue], subscripts: [ArraySubscript],
        meter: DatabaseWorkMeter, limits: QueryValueLimits) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        guard !operands.isEmpty, !subscripts.isEmpty, subscripts.count <= 6 else { throw SQLArrayStorage.mismatch("selected array access") }
        let element = try SQLArrayStorage.elementType(bound.children[0].type)
        guard let array = try SQLArrayStorage.array(operands[0], type: bound.children[0].type) else { return (SQLArrayStorage.null, nil) }
        let slicing = subscripts.contains { if case .slice = $0 { return true }; return false }
        var lows = try SQLArrayBuffer<Int64>(count: array.dimensions.count, meter: meter)
        var lengths = try SQLArrayBuffer<Int>(count: array.dimensions.count, meter: meter)
        var argument = 1
        var empty = array.dimensions.isEmpty || subscripts.count > array.dimensions.count
        for index in subscripts.indices {
            try meter.consume(at: .expressionEvaluation)
            let actual = index < array.dimensions.count ? array.dimensions[index] : nil
            let lower = Int64(actual?.lowerBound ?? 1)
            let upper = lower + Int64(actual?.length ?? 0) - 1
            var from = lower, through = upper
            func read() throws -> Int64? {
                guard argument < operands.count else { throw SQLArrayStorage.mismatch("array access child count") }
                defer { argument += 1 }
                if operands[argument].isNull { return nil }
                return Int64(try SQLArrayStorage.integer(operands[argument]))
            }
            switch subscripts[index] {
            case .index:
                guard let value = try read() else { return (SQLArrayStorage.null, nil) }
                from = slicing ? 1 : value; through = value
            case .slice(let start, let end):
                if start != nil { guard let value = try read() else { return (SQLArrayStorage.null, nil) }; from = value }
                if end != nil { guard let value = try read() else { return (SQLArrayStorage.null, nil) }; through = value }
            }
            if slicing { from = max(from, lower); through = min(through, upper) }
            else if from < lower || through > upper { empty = true }
            if from > through { empty = true }
            if actual != nil {
                try lows.append(from)
                try lengths.append(from <= through ? Int(through - from + 1) : 0)
            }
        }
        guard argument == operands.count else { throw SQLArrayStorage.mismatch("array access child count") }
        if !slicing {
            if empty || subscripts.count != array.dimensions.count { return (SQLArrayStorage.null, nil) }
            var offset = 0
            for index in array.dimensions.indices {
                offset = try SQLArrayStorage.checkedAdd(SQLArrayStorage.checkedMultiply(offset, Int(array.dimensions[index].length)),
                    Int(lows.values[index] - Int64(array.dimensions[index].lowerBound)))
            }
            return (array.elements[offset], nil)
        }
        if empty { return try SQLArrayStorage.empty(element: element, meter: meter, limits: limits) }
        for index in subscripts.count..<array.dimensions.count {
            try lows.append(Int64(array.dimensions[index].lowerBound)); try lengths.append(Int(array.dimensions[index].length))
        }
        var count = 1
        for length in lengths.values { count = try SQLArrayStorage.checkedMultiply(count, length) }
        return try SQLArrayStorage.makeArray(element: element, rank: lengths.values.count, count: count,
            meter: meter, limits: limits, dimension: { try SQLArrayStorage.dimension(length: lengths.values[$0]) }, cell: { index, _ in
                var remaining = index, offset = 0, stride = 1
                for dimension in array.dimensions.indices.reversed() {
                    let coordinate = remaining % lengths.values[dimension]; remaining /= lengths.values[dimension]
                    let source = Int(lows.values[dimension] - Int64(array.dimensions[dimension].lowerBound)) + coordinate
                    offset = try SQLArrayStorage.checkedAdd(offset, SQLArrayStorage.checkedMultiply(source, stride))
                    stride = try SQLArrayStorage.checkedMultiply(stride, Int(array.dimensions[dimension].length))
                }
                return array.elements[offset]
            })
    }

    static func concatenate(_ left: QueryValue, _ right: QueryValue, type: QueryColumnType,
        meter: DatabaseWorkMeter, limits: QueryValueLimits) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        let element = try SQLArrayStorage.elementType(type)
        let lhs = try SQLArrayStorage.array(left, type: type), rhs = try SQLArrayStorage.array(right, type: type)
        guard let lhs else { return (right, nil) }; guard let rhs else { return (left, nil) }
        if lhs.elements.isEmpty { return (right, nil) }; if rhs.elements.isEmpty { return (left, nil) }
        let rank = max(lhs.dimensions.count, rhs.dimensions.count)
        let difference = lhs.dimensions.count - rhs.dimensions.count
        guard abs(difference) <= 1 else { throw SQLArrayStorage.failure(.invalidArrayShape) }
        let high = difference >= 0 ? lhs : rhs
        let low = difference >= 0 ? rhs : lhs
        for index in 1..<rank {
            try meter.consume(at: .expressionEvaluation)
            let other = difference == 0 ? low.dimensions[index] : low.dimensions[index - 1]
            guard high.dimensions[index] == other else { throw SQLArrayStorage.failure(.invalidArrayShape) }
        }
        let outerLength = try difference == 0
            ? SQLArrayStorage.checkedAdd(Int(lhs.dimensions[0].length), Int(rhs.dimensions[0].length))
            : SQLArrayStorage.checkedAdd(Int(high.dimensions[0].length), 1)
        let count = try SQLArrayStorage.checkedAdd(lhs.elements.count, rhs.elements.count)
        return try SQLArrayStorage.makeArray(element: element, rank: rank, count: count, meter: meter, limits: limits,
            dimension: { index in
                index == 0 ? try SQLArrayStorage.dimension(lower: high.dimensions[0].lowerBound, length: outerLength) : high.dimensions[index]
            }, cell: { index, _ in index < lhs.elements.count ? lhs.elements[index] : rhs.elements[index - lhs.elements.count] })
    }

    private static func contains(_ left: QueryArrayValue, _ right: QueryArrayValue, overlap: Bool,
        environment: SQLTemporalEnvironment?, meter: DatabaseWorkMeter) throws -> Bool {
        for needle in right.elements {
            try meter.consume(at: .expressionEvaluation)
            var found = false
            for candidate in left.elements {
                if try SQLArrayStorage.equal(candidate, needle, type: left.elementType, nullsMatch: false,
                    environment: environment, meter: meter) { found = true; break }
            }
            if overlap && found { return true }
            if !overlap && !found { return false }
        }
        return !overlap
    }
}
