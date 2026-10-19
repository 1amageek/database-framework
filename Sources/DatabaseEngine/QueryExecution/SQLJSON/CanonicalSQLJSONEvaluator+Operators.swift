import DatabaseKit
import DatabaseTypes

extension CanonicalSQLJSONEvaluator {
    static func collection(_ operation: CollectionOperator, operands: [QueryValue],
        types: [QueryColumnType], plan: inout SQLJSONPlan) throws -> QueryValue {
        if operands.contains(where: { $0.isNull }) { return .primitive(.null) }
        switch operation {
        case .jsonField, .jsonFieldText:
            let text = operation == .jsonFieldText
            if types[1] == .sql(.integer) {
                let index = try integer(operands[1])
                switch operands[0] {
                case .json(let json):
                    let input = try SQLJSONLexicalInput(json, memory: plan.memory)
                    guard let selected = try input.element(0, index: index) else { return .primitive(.null) }
                    return try input.value(selected, text: text, limits: plan.limits)
                case .jsonb(let json):
                    guard case .array(let children) = json.root else { return .primitive(.null) }
                    let offset = index < 0 ? Int64(children.count) + Int64(index) : Int64(index)
                    guard offset >= 0, offset < children.count else { return .primitive(.null) }
                    return try selected(children[Int(offset)], text: text, plan: &plan)
                default: throw SQLJSONMemory.failure("JSON extraction input")
                }
            }
            @AdmittedArray var path: [QueryValue] = []
            try plan.memory.append(operands[1], to: &$path)
            return try extract(operands[0], path: path, text: text, plan: &plan, objectOnly: true)
        case .jsonPath, .jsonPathText:
            return try extract(operands[0], path: array(operands[1]).elements, text: operation == .jsonPathText, plan: &plan)
        case .contains, .containedBy:
            let a = try jsonb(operands[0]), b = try jsonb(operands[1])
            return .primitive(.bool(try SQLJSONComparison.contains(operation == .contains ? a.root : b.root,
                operation == .contains ? b.root : a.root, memory: plan.memory)))
        case .jsonHasKey:
            return .primitive(.bool(try hasKey(jsonb(operands[0]).root, key: string(operands[1]), memory: plan.memory)))
        case .jsonHasAnyKey, .jsonHasAllKeys:
            let root = try jsonb(operands[0]).root
            let all = operation == .jsonHasAllKeys
            for cell in try array(operands[1]).elements where !cell.isNull {
                let found = try hasKey(root, key: string(cell), memory: plan.memory)
                if found != all { return .primitive(.bool(found)) }
            }
            return .primitive(.bool(all))
        case .jsonDeletePath:
            let root = try plan.source(jsonb(operands[0]).root)
            try plan.mutate(root, path: array(operands[1]), replacement: nil, operation: .delete, flag: false)
            return .jsonb(try plan.jsonb(root))
        case .overlaps: throw SQLJSONMemory.failure("JSONB overlap signature")
        }
    }

    static func extract<Path: Collection>(_ value: QueryValue, path: Path, text: Bool,
        plan: inout SQLJSONPlan, objectOnly: Bool = false) throws -> QueryValue where Path.Element == QueryValue {
        if path.contains(where: { $0.isNull }) { return .primitive(.null) }
        switch value {
        case .json(let json):
            let input = try SQLJSONLexicalInput(json, memory: plan.memory)
            var id = 0
            for cell in path {
                try plan.memory.work()
                if objectOnly && input.nodes[id].kind != .object { return .primitive(.null) }
                guard let next = try input.lookup(id, key: string(cell), limits: plan.limits) else { return .primitive(.null) }
                id = next
            }
            return try input.value(id, text: text, limits: plan.limits)
        case .jsonb(let json):
            var node = json.root
            for cell in path {
                try plan.memory.work()
                let key = try string(cell)
                switch node {
                case .object(let members):
                    var found: QueryJSONBNode?
                    for member in members {
                        if try SQLJSONComparison.strings(member.key, key, memory: plan.memory) == .equal { found = member.value; break }
                    }
                    guard let found else { return .primitive(.null) }; node = found
                case .array(let children):
                    guard !objectOnly, let index = SQLJSONLexicalInput.index(key) else { return .primitive(.null) }
                    let offset = index < 0 ? Int64(children.count) + Int64(index) : Int64(index)
                    guard offset >= 0, offset < children.count else { return .primitive(.null) }
                    node = children[Int(offset)]
                default: return .primitive(.null)
                }
            }
            return try selected(node, text: text, plan: &plan)
        default: throw SQLJSONMemory.failure("JSON extraction source")
        }
    }

    static func selected(_ node: QueryJSONBNode, text: Bool, plan: inout SQLJSONPlan) throws -> QueryValue {
        if !text { return .jsonb(try QueryJSONBValue(root: node, limits: plan.limits)) }
        let root = try plan.source(node)
        return try plan.text(root)
    }

    static func kind(_ value: QueryValue, plan: SQLJSONPlan) throws -> String {
        switch value {
        case .json(let json):
            let input = try SQLJSONLexicalInput(json, memory: plan.memory)
            switch input.nodes[0].kind {
            case .object: return "object"
            case .array: return "array"
            case .string: return "string"
            case .scalar:
                switch json.bytes[input.nodes[0].range].first {
                case 110: return "null"
                case 116, 102: return "boolean"
                default: return "number"
                }
            }
        case .jsonb(let json):
            switch json.root {
            case .null: return "null"
            case .bool: return "boolean"
            case .string: return "string"
            case .number: return "number"
            case .array: return "array"
            case .object: return "object"
            }
        default: throw SQLJSONMemory.failure("JSON typeof source")
        }
    }

    private static func hasKey(_ root: QueryJSONBNode, key: String, memory: SQLJSONMemory) throws -> Bool {
        switch root {
        case .object(let members):
            for member in members {
                if try SQLJSONComparison.strings(member.key, key, memory: memory) == .equal { return true }
            }
        case .array(let values):
            for value in values {
                try memory.work()
                if case .string(let string) = value,
                   try SQLJSONComparison.strings(string, key, memory: memory) == .equal { return true }
            }
        case .string(let string):
            // JSONB raw scalar strings participate in top-level existence too.
            return try SQLJSONComparison.strings(string, key, memory: memory) == .equal
        default: break
        }
        return false
    }
}
