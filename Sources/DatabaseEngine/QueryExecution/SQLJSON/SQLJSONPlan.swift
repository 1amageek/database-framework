import DatabaseKit
import DatabaseTypes

/// Flat, operation-owned references; no parent stores a recursively copied subtree.
struct SQLJSONPlan {
    struct Member { let key: String; let value: Int }
    enum Entry {
        case scalar(QueryJSONBNode)
        case lexical(QueryJSONValue)
        case array([Int])
        case object([Member])
    }
    private enum Visit {
        case node(QueryJSONBNode)
        case array(Int)
        case object(QueryJSONBMembers)
        case value(QueryValue, QueryColumnType)
        case dimension(QueryArrayValue, Int, Int)
    }
    let memory: SQLJSONMemory
    let limits: QueryValueLimits
    let environment: SQLTemporalEnvironment?
    var entries: [Entry] = []

    mutating func add(_ entry: Entry) throws -> Int {
        guard UInt64(entries.count) < limits.maximumTotalNodes else {
            throw QueryValueError.resourceLimit(resource: "nodes", actual: UInt64(entries.count) + 1,
                maximum: limits.maximumTotalNodes)
        }
        try memory.work()
        let id = entries.count
        try memory.append(entry, to: &entries)
        return id
    }

    mutating func source(_ root: QueryJSONBNode) throws -> Int {
        try append(.node(root), jsonb: true)
    }

    mutating func value(_ value: QueryValue, type: QueryColumnType, jsonb: Bool) throws -> Int {
        try append(.value(value, type), jsonb: jsonb)
    }

    private mutating func append(_ first: Visit, jsonb: Bool) throws -> Int {
        var visits: [Visit] = [], completed: [Int] = []
        try memory.append(first, to: &visits)
        while let visit = visits.popLast() {
            try memory.work()
            switch visit {
            case .node(let node):
                switch node {
                case .array(let values):
                    try memory.append(.array(values.count), to: &visits)
                    for index in values.indices.reversed() { try memory.append(.node(values[index]), to: &visits) }
                case .object(let members):
                    try memory.append(.object(members), to: &visits)
                    for index in members.indices.reversed() { try memory.append(.node(members[index].value), to: &visits) }
                default: try memory.append(try add(.scalar(node)), to: &completed)
                }
            case .value(let value, let type):
                switch value {
                case .jsonb(let source): try memory.append(.node(source.root), to: &visits)
                case .json(let source):
                    if jsonb { try memory.append(try lexical(source), to: &completed) }
                    else { try memory.append(try add(.lexical(source)), to: &completed) }
                case .primitive(.array(let cells)):
                    guard case .primitive(let kind, true) = type else { throw SQLJSONMemory.failure("JSON primitive array declaration") }
                    try memory.append(.array(cells.count), to: &visits)
                    for cell in cells.reversed() { try memory.append(.value(.primitive(cell), .primitive(kind)), to: &visits) }
                case .array(let array):
                    if array.dimensions.isEmpty { try memory.append(try add(.array([])), to: &completed) }
                    else { try memory.append(.dimension(array, 0, 0), to: &visits) }
                default:
                    let scalar: QueryJSONBNode
                    if value.isNull { scalar = .null }
                    else if case .primitive(.bool(let flag)) = value { scalar = .bool(flag) }
                    else if case .numeric(let numeric) = value { scalar = .number(numeric) }
                    else {
                        scalar = try CanonicalSQLScalarText.withText(of: value, type: type,
                            preservingCharacterPadding: true, temporalEnvironment: environment,
                            workMeter: memory.meter) { text in
                                try memory.bytes(UInt64(text.utf8.count))
                                if Self.numeric(type) {
                                    return .number(try QueryNumericValue(sqlLexicalForm: text,
                                        maximumUTF8Count: UInt64(text.utf8.count)))
                                }
                                return .string(String(text))
                            }
                    }
                    try memory.append(try add(.scalar(scalar)), to: &completed)
                }
            case .dimension(let array, let dimension, let offset):
                let count = Int(array.dimensions[dimension].length)
                try memory.append(.array(count), to: &visits)
                if dimension + 1 == array.dimensions.count {
                    for index in (0..<count).reversed() {
                        try memory.append(.value(array.elements[offset + index], .sql(array.elementType)), to: &visits)
                    }
                } else {
                    var stride = 1
                    for next in (dimension + 1)..<array.dimensions.count {
                        let product = stride.multipliedReportingOverflow(by: Int(array.dimensions[next].length))
                        guard !product.overflow else { throw QueryValueError.arithmeticOverflow }
                        stride = product.partialValue
                    }
                    for index in (0..<count).reversed() {
                        try memory.append(.dimension(array, dimension + 1, offset + index * stride), to: &visits)
                    }
                }
            case .array(let count):
                var children: [Int] = []
                try memory.prepare(&children, adding: count)
                children.append(contentsOf: completed.suffix(count))
                completed.removeLast(count)
                try memory.append(try add(.array(children)), to: &completed)
            case .object(let source):
                var members: [Member] = []
                try memory.prepare(&members, adding: source.count)
                let start = completed.count - source.count
                for index in source.indices {
                    members.append(Member(key: source[index].key, value: completed[start + index]))
                }
                completed.removeLast(source.count)
                try memory.append(try add(.object(members)), to: &completed)
            }
        }
        guard completed.count == 1 else { throw SQLJSONMemory.failure("JSON conversion stack") }
        return completed[0]
    }

    mutating func lexical(_ source: QueryJSONValue) throws -> Int {
        let input = try SQLJSONLexicalInput(source, memory: memory)
        var ids: [Int] = []
        try memory.prepare(&ids, adding: input.nodes.count)
        ids.append(contentsOf: repeatElement(-1, count: input.nodes.count))
        for index in input.nodes.indices.reversed() {
            let node = input.nodes[index]
            let entry: Entry
            switch node.kind {
            case .array:
                var children: [Int] = [], child = index + 1
                while child < node.end {
                    try memory.append(ids[child], to: &children); child = input.nodes[child].end
                }
                entry = .array(children)
            case .object:
                var members: [Member] = [], child = index + 1
                while child < node.end {
                    guard let key = input.nodes[child].key else { throw SQLJSONMemory.failure("JSON object key") }
                    let decoded = try input.decodedString(key, limits: limits)
                    if let old = try memberIndex(decoded, in: members) { members[old] = Member(key: decoded, value: ids[child]) }
                    else { try memory.append(Member(key: decoded, value: ids[child]), to: &members) }
                    child = input.nodes[child].end
                }
                try sort(&members)
                entry = .object(members)
            case .string: entry = .scalar(.string(try input.decodedString(node.range, limits: limits)))
            case .scalar:
                let bytes = source.bytes[node.range]
                if bytes.first == 110 { entry = .scalar(.null) }
                else if bytes.first == 116 { entry = .scalar(.bool(true)) }
                else if bytes.first == 102 { entry = .scalar(.bool(false)) }
                else {
                    try memory.bytes(UInt64(bytes.count))
                    let text = String(decoding: bytes, as: UTF8.self)
                    entry = .scalar(.number(try QueryNumericValue(sqlLexicalForm: text[...], maximumUTF8Count: UInt64(bytes.count))))
                }
            }
            ids[index] = try add(entry)
        }
        return ids[0]
    }

    func memberIndex(_ key: String, in members: [Member]) throws -> Int? {
        for index in members.indices {
            if try SQLJSONComparison.strings(members[index].key, key, memory: memory) == .equal { return index }
        }
        return nil
    }

    func sort(_ members: inout [Member]) throws {
        // In-place insertion avoids unadmitted sort scratch and supports throwing work checks.
        for index in members.indices.dropFirst() {
            var position = index
            while position > 0 {
                try memory.work(try SQLJSONMemory.add(UInt64(members[position].key.utf8.count), UInt64(members[position - 1].key.utf8.count)))
                guard Self.keyOrder(members[position].key, members[position - 1].key) else { break }
                members.swapAt(position, position - 1); position -= 1
            }
        }
    }

    static func keyOrder(_ left: String, _ right: String) -> Bool {
        if left.utf8.count != right.utf8.count { return left.utf8.count < right.utf8.count }
        return left.utf8.lexicographicallyPrecedes(right.utf8)
    }

    private static func numeric(_ type: QueryColumnType) -> Bool {
        switch type {
        case .sql(.smallint), .sql(.integer), .sql(.bigint), .sql(.real), .sql(.doublePrecision), .sql(.decimal): return true
        case .primitive(let kind, _):
            switch kind {
            case .int8, .uint8, .int16, .uint16, .int32, .uint32, .int64, .uint64, .float32, .float64, .decimal: return true
            default: return false
            }
        default: return false
        }
    }
}
