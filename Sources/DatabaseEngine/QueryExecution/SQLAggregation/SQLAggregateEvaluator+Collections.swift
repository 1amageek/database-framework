import DatabaseKit
import DatabaseTypes

extension SQLAggregateEvaluator {
    static func collection(_ selected: CanonicalBoundAggregate, bound: CanonicalBoundExpression,
        inputs: [Input], environment: SQLTemporalEnvironment?, memory: SQLAggregateWorkspace,
        limits: QueryValueLimits) async throws -> Cell {
        guard !inputs.isEmpty else { return null }
        switch selected.call.function {
        case .arrayAgg:
            return try await array(selected, bound: bound, inputs: inputs, environment: environment,
                memory: memory, limits: limits)
        case .stringAgg: return try await string(selected, inputs: inputs, memory: memory, limits: limits)
        default: return try await json(selected, inputs: inputs, environment: environment, memory: memory, limits: limits)
        }
    }

    private static func array(_ selected: CanonicalBoundAggregate, bound: CanonicalBoundExpression,
        inputs: [Input], environment: SQLTemporalEnvironment?, memory: SQLAggregateWorkspace,
        limits: QueryValueLimits) async throws -> Cell {
        guard case .sql(.array(let element)) = bound.type,
              let inputType = selected.arguments.first?.type else { throw mismatch("ARRAY_AGG selected type") }
        var innerShape: QueryArrayDimensions?
        var innerCount = 1
        if case .sql(.array) = inputType {
            for row in inputs {
                try await checkpoint(memory.meter)
                guard case .array(let array) = row.arguments[0].value, !array.dimensions.isEmpty else {
                    throw invalid(.arrayAgg, "array inputs cannot be NULL or empty")
                }
                if let shape = innerShape {
                    guard shape.count == array.dimensions.count else { throw SQLArrayStorage.failure(.invalidArrayShape) }
                    for index in shape.indices {
                        try memory.meter.consume(at: .aggregateInput)
                        guard shape[index] == array.dimensions[index] else { throw SQLArrayStorage.failure(.invalidArrayShape) }
                    }
                } else { innerShape = array.dimensions; innerCount = array.elements.count }
            }
        }
        let rank = (innerShape?.count ?? 0) + 1
        let count = try SQLArrayStorage.checkedMultiply(inputs.count, innerCount)
        let output = try SQLArrayStorage.makeArray(element: element, rank: rank, count: count,
            meter: memory.meter, limits: limits, sourceElementType: .sql(element), environment: environment,
            dimension: { index in
                if index == 0 { return try SQLArrayStorage.dimension(length: inputs.count) }
                guard let innerShape else { throw SQLArrayStorage.failure(.invalidArrayShape) }
                return innerShape[index - 1]
            }, cell: { index, _ in
                let value = inputs[index / innerCount].arguments[0].value
                if case .array(let array) = value, innerShape != nil { return array.elements[index % innerCount] }
                return value
            })
        return try retained(output.value, lease: output.lease, memory: memory)
    }

    private static func string(_ selected: CanonicalBoundAggregate, inputs: [Input],
        memory: SQLAggregateWorkspace, limits: QueryValueLimits) async throws -> Cell {
        guard selected.arguments.count == 2 else { throw mismatch("STRING_AGG arguments") }
        let binary: Bool
        if case .sql(.blob) = selected.arguments[0].type { binary = true } else { binary = false }
        var count = 0, accepted = 0
        func length(_ value: QueryValue) throws -> Int {
            if value.isNull { return 0 }
            if binary, case .primitive(.bytes(let bytes)) = value { return bytes.count }
            if !binary, case .primitive(.string(let text)) = value { return text.utf8.count }
            throw mismatch("STRING_AGG selected value")
        }
        for row in inputs {
            try await checkpoint(memory.meter)
            if row.arguments[0].value.isNull { continue }
            if accepted > 0 { count = try SQLArrayStorage.checkedAdd(count, length(row.arguments[1].value)) }
            count = try SQLArrayStorage.checkedAdd(count, length(row.arguments[0].value))
            accepted += 1
        }
        guard accepted > 0 else { return null }
        guard UInt64(count) <= limits.maximumRetainedBytes else {
            throw QueryValueError.resourceLimit(resource: "retainedBytes", actual: UInt64(count), maximum: limits.maximumRetainedBytes)
        }
        let footprint = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint(retainedByteCount: UInt64(count))
        let claim = try memory.meter.reserveIntermediate(bytes: footprint.bytes, at: .aggregateInput)
        // UTF-8 assembly is one necessary output copy; text additionally owns its String storage.
        let scratch = try binary ? nil : memory.meter.reserveIntermediate(bytes: UInt64(count), at: .aggregateInput)
        defer { scratch?.release() }
        let bytes = try ByteString.copying(count: count) { output in
            var offset = 0, accepted = 0
            func emit<Bytes: Collection>(_ bytes: Bytes) throws where Bytes.Element == UInt8 {
                for byte in bytes {
                    try memory.meter.consume(at: .aggregateInput)
                    output[offset] = byte; offset += 1
                }
            }
            func emitValue(_ value: QueryValue) throws {
                if value.isNull { return }
                if binary, case .primitive(.bytes(let bytes)) = value { try emit(bytes) }
                else if !binary, case .primitive(.string(let text)) = value { try emit(text.utf8) }
                else { throw mismatch("STRING_AGG selected value") }
            }
            for row in inputs {
                if row.arguments[0].value.isNull { continue }
                if accepted > 0 { try emitValue(row.arguments[1].value) }
                try emitValue(row.arguments[0].value)
                accepted += 1
            }
        }
        let value: QueryValue = binary ? .primitive(.bytes(bytes)) : .primitive(.string(String(decoding: bytes, as: UTF8.self)))
        let actual = try CanonicalRelationalFootprintMeter.valueFootprint(of: value, workMeter: memory.meter, stage: .aggregateInput)
        if actual.bytes > footprint.bytes { try claim.reserveAdditional(bytes: actual.bytes - footprint.bytes, at: .aggregateInput) }
        return try retained(value, lease: DatabaseQueryPayloadLease(retaining: claim), memory: memory)
    }

    private static func json(_ selected: CanonicalBoundAggregate, inputs: [Input],
        environment: SQLTemporalEnvironment?, memory: SQLAggregateWorkspace, limits: QueryValueLimits) async throws -> Cell {
        let function = selected.call.function
        let binary = function == .jsonbAgg || function == .jsonbObjectAgg
        let object = function == .jsonObjectAgg || function == .jsonbObjectAgg
        let jsonMemory = try SQLJSONMemory(memory.meter)
        var plan = SQLJSONPlan(memory: jsonMemory, limits: limits, environment: environment)
        var elements: [Int] = [], members: [SQLJSONPlan.Member] = []
        for row in inputs {
            try await checkpoint(memory.meter)
            if object {
                guard row.arguments.count == 2, selected.arguments.count == 2 else { throw mismatch("JSON object aggregate arguments") }
                let keyValue = row.arguments[0].value
                if keyValue.isNull { throw SQLArrayStorage.failure(.nullJSONObjectKey(argument: 1)) }
                switch keyValue {
                case .array, .composite, .json, .jsonb, .primitive(.array):
                    throw SQLArrayStorage.failure(.invalidJSONObjectKey(argument: 1))
                default: break
                }
                let key = try CanonicalSQLScalarText.withText(of: keyValue, type: selected.arguments[0].type,
                    preservingCharacterPadding: true, temporalEnvironment: environment, workMeter: memory.meter) { text in
                        try jsonMemory.bytes(UInt64(text.utf8.count)); return String(text)
                    }
                let value = try plan.value(row.arguments[1].value, type: selected.arguments[1].type, jsonb: binary)
                let member = SQLJSONPlan.Member(key: key, value: value)
                if binary, let previous = try plan.memberIndex(key, in: members) { members[previous] = member }
                else { try jsonMemory.append(member, to: &members) }
            } else {
                guard row.arguments.count == 1, let type = selected.arguments.first?.type else { throw mismatch("JSON aggregate argument") }
                try jsonMemory.append(plan.value(row.arguments[0].value, type: type, jsonb: binary), to: &elements)
            }
        }
        if binary && object { try plan.sort(&members) }
        let root = try plan.add(object ? .object(members) : .array(elements))
        let value: QueryValue
        if binary { value = .jsonb(try plan.jsonb(root)) }
        else { value = .json(try plan.json(root, style: object ? .objectAggregate : .builder)) }
        let result = try SQLJSONMemory.retain(value, meter: memory.meter)
        return try retained(result.value, lease: result.lease, memory: memory)
    }
}
