import DatabaseKit
import DatabaseTypes

/// Selected JSON signatures and the sole facade into the child algorithms.
enum CanonicalSQLJSONEvaluator {
    static func evaluate(_ bound: CanonicalBoundExpression, operands: [QueryValue],
        temporalEnvironment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter,
        limits: QueryValueLimits) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        guard operands.count == bound.children.count else { throw SQLJSONMemory.failure("selected JSON arity") }
        for index in operands.indices {
            try SQLJSONMemory.validate(operands[index], type: bound.children[index].type, meter: workMeter)
        }
        let memory = try SQLJSONMemory(workMeter)
        var plan = SQLJSONPlan(memory: memory, limits: limits, environment: temporalEnvironment)
        let result: QueryValue
        switch bound.expression {
        case .collectionOperation(let operation, _, _):
            guard operands.count == 2 else { throw SQLJSONMemory.failure("JSON operator arity") }
            var types: [QueryColumnType] = []
            for child in bound.children { try memory.append(child.type, to: &types) }
            result = try collection(operation, operands: operands, types: types, plan: &plan)
        case .concatenate:
            if operands.contains(where: { $0.isNull }) { return (.primitive(.null), nil) }
            guard operands.count == 2 else { throw SQLJSONMemory.failure("JSON concatenate arity") }
            let left = try plan.source(jsonb(operands[0]).root), right = try plan.source(jsonb(operands[1]).root)
            let root = try plan.concatenate(left, right)
            result = .jsonb(try plan.jsonb(root))
        case .subtract:
            if operands.contains(where: { $0.isNull }) { return (.primitive(.null), nil) }
            guard operands.count == 2 else { throw SQLJSONMemory.failure("JSON subtract arity") }
            let root = try plan.source(jsonb(operands[0]).root)
            switch bound.children[1].type {
            case .sql(.text): try plan.delete(root, key: string(operands[1]))
            case .sql(.integer): try plan.delete(root, index: integer(operands[1]))
            case .sql(.array):
                let keys = try array(operands[1])
                guard keys.dimensions.count <= 1 else { throw failure(.multidimensionalJSONMutationPath(rank: keys.dimensions.count)) }
                for key in keys.elements where !key.isNull { try plan.delete(root, key: string(key)) }
            default: throw SQLJSONMemory.failure("JSON subtract signature")
            }
            result = .jsonb(try plan.jsonb(root))
        case .function(let call):
            result = try function(call, bound: bound, operands: operands, plan: &plan)
        default: throw SQLJSONMemory.failure("selected JSON expression")
        }
        return try SQLJSONMemory.retain(result, meter: workMeter)
    }

    static func cast(_ value: QueryValue, from source: QueryColumnType, to target: QueryColumnType,
        context: CanonicalQueryTypeBinder.CastContext, temporalEnvironment: SQLTemporalEnvironment?,
        workMeter: DatabaseWorkMeter, limits: QueryValueLimits) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        try SQLJSONMemory.validate(value, type: source, meter: workMeter)
        if value.isNull { return (.primitive(.null), nil) }
        let memory = try SQLJSONMemory(workMeter)
        var plan = SQLJSONPlan(memory: memory, limits: limits, environment: temporalEnvironment)
        let result: QueryValue
        switch target {
        case .sql(.json):
            switch value {
            case .json: result = value
            case .jsonb(let source):
                let root = try plan.source(source.root)
                result = .json(try plan.json(root, style: .jsonb))
            default:
                let lexical = try inputJSON(value, type: source, plan: &plan)
                result = .json(lexical)
            }
        case .sql(.jsonb):
            switch value {
            case .jsonb: result = value
            default:
                let lexical: QueryJSONValue
                if case .json(let json) = value { lexical = json }
                else { lexical = try inputJSON(value, type: source, plan: &plan) }
                let root = try plan.lexical(lexical)
                result = .jsonb(try plan.jsonb(root))
            }
        case .sql(.smallint), .sql(.integer), .sql(.bigint), .sql(.real), .sql(.doublePrecision), .sql(.decimal), .sql(.boolean):
            guard case .jsonb(let json) = value else { throw DatabaseExpressionEvaluationError.invalidCast("JSONB scalar") }
            switch (json.root, target) {
            case (.bool(let flag), .sql(.boolean)): result = .primitive(.bool(flag))
            case (.number(let numeric), _):
                guard target != .sql(.boolean) else { throw DatabaseExpressionEvaluationError.invalidCast("JSONB boolean") }
                result = try CanonicalSQLScalarCast.evaluateValue(.numeric(numeric),
                    from: .sql(.decimal(precision: nil, scale: nil)), to: target, context: context,
                    temporalEnvironment: temporalEnvironment, workMeter: workMeter)
            default: throw DatabaseExpressionEvaluationError.invalidCast("JSONB scalar")
            }
        case .sql(.text):
            switch value {
            case .json(let json):
                try memory.bytes(UInt64(json.bytes.count)); try memory.work(UInt64(json.bytes.count))
                result = .primitive(.string(String(decoding: json.bytes, as: UTF8.self)))
            case .jsonb(let source):
                let root = try plan.source(source.root)
                let lexical = try plan.json(root, style: .jsonb)
                try memory.bytes(UInt64(lexical.bytes.count))
                result = .primitive(.string(String(decoding: lexical.bytes, as: UTF8.self)))
            default: throw SQLJSONMemory.failure("JSON text output source")
            }
        default: throw SQLJSONMemory.failure("selected JSON cast target")
        }
        try QueryValueValidator.validate(result, type: target, limits: limits)
        return try SQLJSONMemory.retain(result, meter: workMeter)
    }

    static func convertRow(columns: [QueryColumn], values: [QueryValue], resultType: QueryColumnType,
        pretty: Bool, temporalEnvironment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter,
        limits: QueryValueLimits) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        guard columns.count == values.count else { throw SQLJSONMemory.failure("ROW conversion arity") }
        try QueryColumn.validate(columns)
        let memory = try SQLJSONMemory(workMeter)
        var plan = SQLJSONPlan(memory: memory, limits: limits, environment: temporalEnvironment)
        let binary = resultType == .sql(.jsonb)
        guard binary || resultType == .sql(.json) else { throw SQLJSONMemory.failure("ROW JSON result type") }
        var members: [SQLJSONPlan.Member] = []
        for index in values.indices {
            let column = columns[index]
            try SQLJSONMemory.validate(values[index], type: column.type, meter: workMeter)
            guard column.isNullable || !values[index].isNull else { throw SQLJSONMemory.failure("ROW JSON value type") }
            let id = try plan.value(values[index], type: column.type, jsonb: binary)
            let member = SQLJSONPlan.Member(key: column.name, value: id)
            if binary, let old = try plan.memberIndex(column.name, in: members) { members[old] = member }
            else { try memory.append(member, to: &members) }
        }
        if binary { try plan.sort(&members) }
        let root = try plan.add(.object(members))
        let value: QueryValue = binary ? .jsonb(try plan.jsonb(root)) : .json(try plan.json(root, style: .compact, pretty: pretty))
        return try SQLJSONMemory.retain(value, meter: workMeter)
    }

    static func compare(_ left: QueryJSONBValue, _ right: QueryJSONBValue,
        workMeter: DatabaseWorkMeter) throws -> QueryComparison {
        try SQLJSONComparison.compare(left.root, right.root, memory: SQLJSONMemory(workMeter))
    }

    static func makeRowCursor(_ bound: CanonicalBoundExpression, operands: [QueryValue],
        inputLeases: [DatabaseQueryPayloadLease], temporalEnvironment: SQLTemporalEnvironment?,
        workMeter: DatabaseWorkMeter, limits: QueryValueLimits) throws -> SQLJSONRowCursor {
        try SQLJSONRowCursor(bound: bound, operands: operands, inputLeases: inputLeases,
            workMeter: workMeter, limits: limits)
    }

    private static func inputJSON(_ value: QueryValue, type: QueryColumnType, plan: inout SQLJSONPlan) throws -> QueryJSONValue {
        let memory = plan.memory, limits = plan.limits, environment = plan.environment
        return try CanonicalSQLScalarText.withText(of: value, type: type, preservingCharacterPadding: false,
            temporalEnvironment: environment, workMeter: memory.meter) { text in
                let count = text.utf8.count
                try memory.bytes(UInt64(count)); try memory.work(UInt64(count))
                let bytes = ByteString.copying(count: count) { output in
                    var offset = 0
                    for byte in text.utf8 { output[offset] = byte; offset += 1 }
                }
                try memory.work(UInt64(count))
                return try QueryJSONValue(bytes: bytes, limits: limits)
            }
    }

    private static func function(_ call: FunctionCall, bound: CanonicalBoundExpression,
        operands: [QueryValue], plan: inout SQLJSONPlan) throws -> QueryValue {
        let name = call.name
        let buildObject = name == "json_build_object" || name == "jsonb_build_object"
        let buildArray = name == "json_build_array" || name == "jsonb_build_array"
        let binary = name.hasPrefix("jsonb_") || name == "to_jsonb"
        if buildObject || buildArray {
            var values: [QueryValue] = [], types: [QueryColumnType] = []
            for index in operands.indices {
                if call.finalArrayIsVariadic && index + 1 == operands.count {
                    if operands[index].isNull { return .primitive(.null) }
                    let expanded = try array(operands[index])
                    for value in expanded.elements {
                        try plan.memory.append(value, to: &values)
                        try plan.memory.append(.sql(expanded.elementType), to: &types)
                    }
                } else {
                    try plan.memory.append(operands[index], to: &values)
                    try plan.memory.append(bound.children[index].type, to: &types)
                }
            }
            if buildObject && !values.count.isMultiple(of: 2) {
                throw failure(.oddJSONObjectArgumentCount(actual: values.count))
            }
            let root: Int
            if buildObject {
                var members: [SQLJSONPlan.Member] = []
                for index in stride(from: 0, to: values.count, by: 2) {
                    guard !values[index].isNull else { throw failure(.nullJSONObjectKey(argument: index + 1)) }
                    switch values[index] {
                    case .array, .json, .jsonb: throw failure(.invalidJSONObjectKey(argument: index + 1))
                    default: break
                    }
                    let memory = plan.memory, environment = plan.environment
                    let key: String
                    if case .primitive(.bool(let flag)) = values[index] { key = flag ? "true" : "false" }
                    else {
                        key = try CanonicalSQLScalarText.withText(of: values[index], type: types[index],
                            preservingCharacterPadding: true, temporalEnvironment: environment, workMeter: memory.meter) { text in
                                try memory.bytes(UInt64(text.utf8.count)); return String(text)
                            }
                    }
                    let member = SQLJSONPlan.Member(key: key, value: try plan.value(values[index + 1], type: types[index + 1], jsonb: binary))
                    if binary, let old = try plan.memberIndex(key, in: members) { members[old] = member }
                    else { try plan.memory.append(member, to: &members) }
                }
                if binary { try plan.sort(&members) }
                root = try plan.add(.object(members))
            } else {
                var children: [Int] = []
                for index in values.indices { try plan.memory.append(try plan.value(values[index], type: types[index], jsonb: binary), to: &children) }
                root = try plan.add(.array(children))
            }
            return binary ? .jsonb(try plan.jsonb(root)) : .json(try plan.json(root, style: .builder))
        }
        if operands.contains(where: { $0.isNull }) { return .primitive(.null) }
        guard !operands.isEmpty else { throw SQLJSONMemory.failure("JSON function arity") }
        if name == "to_json" || name == "to_jsonb" || name == "array_to_json" {
            let root = try plan.value(operands[0], type: bound.children[0].type, jsonb: binary)
            let pretty = operands.count > 1 ? try boolean(operands[1]) : false
            return binary ? .jsonb(try plan.jsonb(root)) : .json(try plan.json(root, style: .compact, pretty: pretty))
        }
        if name == "json_typeof" || name == "jsonb_typeof" {
            return .primitive(.string(try kind(operands[0], plan: plan)))
        }
        if name == "json_array_length" || name == "jsonb_array_length" {
            let count: Int
            switch operands[0] {
            case .json(let json):
                let input = try SQLJSONLexicalInput(json, memory: plan.memory)
                guard input.nodes[0].kind == .array else { throw failure(.jsonArrayRootRequired(function: name)) }
                count = try input.childCount(0)
            case .jsonb(let json):
                guard case .array(let values) = json.root else { throw failure(.jsonArrayRootRequired(function: name)) }
                count = values.count
            default: throw SQLJSONMemory.failure("JSON array_length input")
            }
            guard let result = Int32(exactly: count) else { throw DatabaseExpressionEvaluationError.numericOverflow }
            return .primitive(.int32(result))
        }
        if name == "json_extract_path" || name == "jsonb_extract_path" || name == "json_extract_path_text" || name == "jsonb_extract_path_text" {
            var path: [QueryValue] = []
            for index in operands.indices.dropFirst() {
                if call.finalArrayIsVariadic && index + 1 == operands.count {
                    for cell in try array(operands[index]).elements { try plan.memory.append(cell, to: &path) }
                } else { try plan.memory.append(operands[index], to: &path) }
            }
            return try extract(operands[0], path: path, text: name.hasSuffix("_text"), plan: &plan)
        }
        let root = try plan.source(jsonb(operands[0]).root)
        if name == "jsonb_strip_nulls" {
            try plan.stripNulls(root, arrays: operands.count > 1 ? boolean(operands[1]) : false)
        } else if name == "jsonb_set" || name == "jsonb_insert" {
            guard operands.count == 3 || operands.count == 4 else { throw SQLJSONMemory.failure("JSON mutation arity") }
            let replacement = try plan.source(jsonb(operands[2]).root)
            let flag = operands.count == 4 ? try boolean(operands[3]) : name == "jsonb_set"
            try plan.mutate(root, path: array(operands[1]), replacement: replacement,
                operation: name == "jsonb_set" ? .set : .insert, flag: flag)
        } else { throw DatabaseExpressionEvaluationError.unsupportedFunction(name) }
        return .jsonb(try plan.jsonb(root))
    }

    static func string(_ value: QueryValue) throws -> String {
        guard case .primitive(.string(let text)) = value else { throw SQLJSONMemory.failure("JSON text operand") }; return text
    }
    static func integer(_ value: QueryValue) throws -> Int32 {
        guard case .primitive(.int32(let number)) = value else { throw SQLJSONMemory.failure("JSON integer operand") }; return number
    }
    static func boolean(_ value: QueryValue) throws -> Bool {
        guard case .primitive(.bool(let flag)) = value else { throw SQLJSONMemory.failure("JSON Boolean operand") }; return flag
    }
    static func array(_ value: QueryValue) throws -> QueryArrayValue {
        guard case .array(let array) = value else { throw SQLJSONMemory.failure("JSON array operand") }; return array
    }
    static func jsonb(_ value: QueryValue) throws -> QueryJSONBValue {
        guard case .jsonb(let json) = value else { throw SQLJSONMemory.failure("JSONB operand") }; return json
    }
    static func failure(_ cause: SQLCollectionError) -> DatabaseExpressionEvaluationError { .collection(cause) }
}
