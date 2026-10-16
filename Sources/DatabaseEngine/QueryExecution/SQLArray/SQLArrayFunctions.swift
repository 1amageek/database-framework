import DatabaseKit

extension CanonicalSQLArrayEvaluator {
    static func function(_ name: String, bound: CanonicalBoundExpression, operands: [QueryValue],
        environment: SQLTemporalEnvironment?, meter: DatabaseWorkMeter, limits: QueryValueLimits) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        func arity(_ range: ClosedRange<Int>) throws {
            guard range.contains(operands.count) else { throw SQLArrayStorage.mismatch("selected \(name) argument count") }
        }
        if name == "string_to_array" {
            try arity(2...3)
            return try SQLArrayText.split(operands, meter: meter, limits: limits)
        }
        if name == "array_prepend" || name == "array_append" {
            try arity(2...2)
            return try append(operands[name == "array_append" ? 0 : 1], element: operands[name == "array_append" ? 1 : 0],
                prepend: name == "array_prepend", type: bound.type, meter: meter, limits: limits)
        }
        if name == "array_cat" {
            try arity(2...2)
            return try concatenate(operands[0], operands[1], type: bound.type, meter: meter, limits: limits)
        }
        guard !operands.isEmpty else { throw SQLArrayStorage.mismatch("selected array function") }
        let element = try SQLArrayStorage.elementType(bound.children[0].type)
        // All remaining functions return NULL for a NULL array, even a NULL search start.
        guard let array = try SQLArrayStorage.array(operands[0], type: bound.children[0].type) else { return (SQLArrayStorage.null, nil) }
        switch name {
        case "array_length", "array_lower", "array_upper":
            try arity(2...2)
            if operands[1].isNull { return (SQLArrayStorage.null, nil) }
            let dimension = try SQLArrayStorage.integer(operands[1])
            guard dimension > 0, Int(dimension) <= array.dimensions.count else { return (SQLArrayStorage.null, nil) }
            let value = array.dimensions[Int(dimension) - 1]
            let number: Int32
            if name == "array_length" { number = value.length }
            else if name == "array_lower" { number = value.lowerBound }
            else { number = value.lowerBound + (value.length - 1) }
            return (.primitive(.int32(number)), nil)
        case "array_ndims":
            try arity(1...1)
            return (array.dimensions.isEmpty ? SQLArrayStorage.null : .primitive(.int32(Int32(array.dimensions.count))), nil)
        case "array_dims":
            try arity(1...1)
            if array.dimensions.isEmpty { return (SQLArrayStorage.null, nil) }
            return try SQLArrayText.dimensions(array, meter: meter)
        case "cardinality":
            try arity(1...1)
            return (.primitive(.int32(try SQLArrayStorage.length(array.elements.count))), nil)
        case "array_position", "array_positions":
            try arity(name == "array_position" ? 2...3 : 2...2)
            guard array.dimensions.count <= 1 else { throw SQLArrayStorage.failure(.invalidArrayArgument(function: name)) }
            if array.elements.isEmpty {
                return name == "array_position" ? (SQLArrayStorage.null, nil)
                    : try SQLArrayStorage.empty(element: .integer, meter: meter, limits: limits)
            }
            var start = Int64(array.dimensions[0].lowerBound)
            if operands.count == 3 {
                guard !operands[2].isNull else { throw SQLArrayStorage.failure(.nullArraySubscript(function: name)) }
                start = Int64(try SQLArrayStorage.integer(operands[2]))
            }
            let offset = max(0, start - Int64(array.dimensions[0].lowerBound))
            var matches = try SQLArrayBuffer<Int32>(meter: meter)
            if offset < Int64(array.elements.count) {
                for index in Int(offset)..<array.elements.count {
                    if try SQLArrayStorage.equal(array.elements[index], operands[1], type: element, nullsMatch: true,
                        environment: environment, meter: meter) {
                        let subscriptValue = Int32(Int64(array.dimensions[0].lowerBound) + Int64(index))
                        if name == "array_position" { return (.primitive(.int32(subscriptValue)), nil) }
                        try matches.append(subscriptValue)
                    }
                }
            }
            if name == "array_position" { return (SQLArrayStorage.null, nil) }
            return try SQLArrayStorage.makeArray(element: .integer, rank: matches.values.isEmpty ? 0 : 1,
                count: matches.values.count, meter: meter, limits: limits,
                dimension: { _ in try SQLArrayStorage.dimension(length: matches.values.count) },
                cell: { index, _ in .primitive(.int32(matches.values[index])) })
        case "array_remove", "array_replace":
            try arity(name == "array_remove" ? 2...2 : 3...3)
            if name == "array_remove", array.dimensions.count > 1 { throw SQLArrayStorage.failure(.invalidArrayArgument(function: name)) }
            if array.elements.isEmpty { return (operands[0], nil) }
            func matches(_ index: Int) throws -> Bool {
                try SQLArrayStorage.equal(array.elements[index], operands[1], type: element, nullsMatch: true,
                    environment: environment, meter: meter)
            }
            if name == "array_replace" {
                return try SQLArrayStorage.makeArray(element: element, rank: array.dimensions.count,
                    count: array.elements.count, meter: meter, limits: limits,
                    dimension: { array.dimensions[$0] }, cell: { index, _ in try matches(index) ? operands[2] : array.elements[index] })
            }
            var count = 0
            for index in array.elements.indices { if try !matches(index) { count += 1 } }
            if count == 0 { return try SQLArrayStorage.empty(element: element, meter: meter, limits: limits) }
            var source = 0
            return try SQLArrayStorage.makeArray(element: element, rank: 1, count: count, meter: meter, limits: limits,
                dimension: { _ in try SQLArrayStorage.dimension(lower: array.dimensions[0].lowerBound, length: count) },
                cell: { _, _ in
                    while try matches(source) { source += 1 }
                    defer { source += 1 }; return array.elements[source]
                })
        case "array_to_string":
            try arity(2...3)
            if operands[1].isNull { return (SQLArrayStorage.null, nil) }
            guard case .primitive(.string(let separator)) = operands[1] else { throw SQLArrayStorage.mismatch("array_to_string separator") }
            let substitute: String?
            if operands.count == 3, !operands[2].isNull {
                guard case .primitive(.string(let value)) = operands[2] else { throw SQLArrayStorage.mismatch("array_to_string nullString") }
                substitute = value
            } else { substitute = nil }
            return try SQLArrayText.join(array, separator: separator, nullString: substitute, environment: environment, meter: meter)
        default: throw DatabaseExpressionEvaluationError.unsupportedFunction(name)
        }
    }

    static func append(_ input: QueryValue, element value: QueryValue, prepend: Bool,
        type: QueryColumnType, meter: DatabaseWorkMeter, limits: QueryValueLimits) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        let element = try SQLArrayStorage.elementType(type)
        let array = try SQLArrayStorage.array(input, type: type)
        if let array, array.dimensions.count > 1 {
            throw SQLArrayStorage.failure(.invalidArrayArgument(function: prepend ? "array_prepend" : "array_append"))
        }
        let oldCount = array?.elements.count ?? 0
        let count = try SQLArrayStorage.checkedAdd(oldCount, 1)
        return try SQLArrayStorage.makeArray(element: element, rank: 1, count: count, meter: meter, limits: limits,
            dimension: { _ in try SQLArrayStorage.dimension(lower: array?.dimensions.first?.lowerBound ?? 1, length: count) },
            cell: { index, _ in
                if prepend && index == 0 || !prepend && index == oldCount { return value }
                guard let array else { throw SQLArrayStorage.failure(.invalidArrayShape) }
                return array.elements[index - (prepend ? 1 : 0)]
            })
    }
}
