import DatabaseKit
import DatabaseTypes

extension SQLJSONPlan {
    /// `objectAggregate` is JSON_OBJECT_AGG's root form: builder members inside `{ ` and ` }`.
    enum Style: Equatable { case compact, builder, jsonb, objectAggregate }
    private enum BuildVisit { case enter(Int, UInt64), array(Int), object([Member]) }
    private enum RenderVisit { case entry(Int, Int, Style), text(String), key(String) }

    func jsonb(_ root: Int) throws -> QueryJSONBValue {
        var visits: [BuildVisit] = []
        try memory.append(.enter(root, 1), to: &visits)
        var nodes = 0, elements = 0, members = 0
        var utf8: UInt64 = 0
        while let visit = visits.popLast() {
            try memory.work()
            guard case .enter(let id, let depth) = visit else { throw SQLJSONMemory.failure("JSONB planning visit") }
            guard depth <= limits.maximumNestingDepth else {
                throw QueryValueError.resourceLimit(resource: "depth", actual: depth, maximum: limits.maximumNestingDepth)
            }
            let increment = nodes.addingReportingOverflow(1)
            guard !increment.overflow else { throw QueryValueError.arithmeticOverflow }; nodes = increment.partialValue
            switch entries[id] {
            case .scalar(.string(let string)): utf8 = try SQLJSONMemory.add(utf8, UInt64(string.utf8.count))
            case .scalar: break
            case .lexical: throw SQLJSONMemory.failure("unconverted lexical JSONB plan")
            case .array(let children):
                elements = try count(elements, adding: children.count)
                for child in children.reversed() { try memory.append(.enter(child, depth + 1), to: &visits) }
            case .object(let children, _):
                members = try count(members, adding: children.count)
                for child in children.reversed() {
                    utf8 = try SQLJSONMemory.add(utf8, UInt64(child.key.utf8.count))
                    try memory.append(.enter(child.value, depth + 1), to: &visits)
                }
            }
        }
        let planned = try QueryJSONBBuilder.constructionByteCount(nodeCapacity: nodes,
            elementIndexCapacity: elements, memberCapacity: members)
        try memory.bytes(try SQLJSONMemory.add(planned, utf8))
        var builder = try QueryJSONBBuilder(limits: limits, nodeCapacity: nodes,
            elementIndexCapacity: elements, memberCapacity: members)
        if builder.constructionRetainedByteCount > planned {
            try memory.bytes(builder.constructionRetainedByteCount - planned)
        }
        var completed: [Int] = []
        try memory.append(.enter(root, 1), to: &visits)
        while let visit = visits.popLast() {
            try memory.work()
            switch visit {
            case .enter(let id, _):
                switch entries[id] {
                case .scalar(let node): try memory.append(try builder.append(node), to: &completed)
                case .lexical: throw SQLJSONMemory.failure("unconverted lexical JSONB plan")
                case .array(let children):
                    try memory.append(.array(children.count), to: &visits)
                    for child in children.reversed() { try memory.append(.enter(child, 0), to: &visits) }
                case .object(let children, _):
                    try memory.append(.object(children), to: &visits)
                    for child in children.reversed() { try memory.append(.enter(child.value, 0), to: &visits) }
                }
            case .array(let count):
                var ids: [Int] = []
                try memory.prepare(&ids, adding: count)
                ids.append(contentsOf: completed.suffix(count)); completed.removeLast(count)
                try memory.append(try builder.appendArray(ids), to: &completed)
            case .object(let children):
                var ids: [QueryJSONBIndexedMember] = []
                try memory.prepare(&ids, adding: children.count)
                let start = completed.count - children.count
                for index in children.indices {
                    ids.append(QueryJSONBIndexedMember(key: children[index].key, value: completed[start + index]))
                }
                completed.removeLast(children.count)
                try memory.append(try builder.appendObject(ids), to: &completed)
            }
        }
        guard completed.count == 1 else { throw SQLJSONMemory.failure("JSONB completion stack") }
        try memory.work(UInt64(nodes))
        return try QueryJSONBValue(root: builder.finish(root: completed[0]), limits: limits)
    }

    func json(_ root: Int, style: Style, pretty: Bool = false) throws -> QueryJSONValue {
        var byteCount: UInt64 = 0
        try render(root, style: style, pretty: pretty) { _ in byteCount = try SQLJSONMemory.add(byteCount, 1) }
        guard let count = Int(exactly: byteCount) else { throw QueryValueError.arithmeticOverflow }
        guard byteCount <= limits.maximumRetainedBytes else {
            throw QueryValueError.resourceLimit(resource: "retainedBytes", actual: byteCount, maximum: limits.maximumRetainedBytes)
        }
        try memory.bytes(byteCount)
        let bytes = try ByteString.copying(count: count) { output in
            var offset = 0
            try render(root, style: style, pretty: pretty) { byte in
                output[offset] = byte; offset += 1
            }
        }
        try memory.work(byteCount)
        return try QueryJSONValue(bytes: bytes, limits: limits)
    }

    func text(_ root: Int) throws -> QueryValue {
        switch entries[root] {
        case .scalar(.null): return .primitive(.null)
        case .scalar(.string(let string)): return .primitive(.string(string))
        default:
            let lexical = try json(root, style: .jsonb)
            try memory.bytes(UInt64(lexical.bytes.count))
            return .primitive(.string(String(decoding: lexical.bytes, as: UTF8.self)))
        }
    }

    private func render(_ root: Int, style: Style, pretty: Bool,
        emit: (UInt8) throws -> Void) throws {
        var visits: [RenderVisit] = []
        var nodes: UInt64 = 0, elements: UInt64 = 0
        try memory.append(.entry(root, 0, style), to: &visits)
        func bytes<S: Sequence>(_ source: S) throws where S.Element == UInt8 {
            for byte in source { try memory.work(); try emit(byte) }
        }
        func quoted(_ string: String) throws {
            try emit(34)
            for byte in string.utf8 {
                try memory.work()
                switch byte {
                case 34, 92: try emit(92); try emit(byte)
                case 8: try bytes("\\b".utf8)
                case 9: try bytes("\\t".utf8)
                case 10: try bytes("\\n".utf8)
                case 12: try bytes("\\f".utf8)
                case 13: try bytes("\\r".utf8)
                case 0...31:
                    try bytes("\\u00".utf8)
                    try emit(CanonicalSQLScalarText.hexDigit(byte >> 4))
                    try emit(CanonicalSQLScalarText.hexDigit(byte & 15))
                default: try emit(byte)
                }
            }
            try emit(34)
        }
        while let visit = visits.popLast() {
            try memory.work()
            switch visit {
            case .text(let string): try bytes(string.utf8)
            case .key(let string): try quoted(string)
            case .entry(let id, let depth, let style):
                let childNodes: UInt64, childElements: UInt64, childDepth: UInt64
                switch entries[id] {
                case .lexical(let lexical):
                    let footprint = try lexical.measure()
                    childNodes = footprint.nodeCount
                    childElements = footprint.collectionElementCount
                    childDepth = footprint.maximumNestingDepth
                case .array(let children): childNodes = 1; childElements = UInt64(children.count); childDepth = 1
                case .object(let members, _): childNodes = 1; childElements = UInt64(members.count); childDepth = 1
                case .scalar: childNodes = 1; childElements = 0; childDepth = 1
                }
                nodes = try SQLJSONMemory.add(nodes, childNodes)
                elements = try SQLJSONMemory.add(elements, childElements)
                let deepest = try SQLJSONMemory.add(UInt64(depth), childDepth)
                guard nodes <= limits.maximumTotalNodes else {
                    throw QueryValueError.resourceLimit(resource: "nodes", actual: nodes, maximum: limits.maximumTotalNodes)
                }
                guard elements <= limits.maximumCollectionElements else {
                    throw QueryValueError.resourceLimit(resource: "collectionElements", actual: elements, maximum: limits.maximumCollectionElements)
                }
                guard deepest <= limits.maximumNestingDepth else {
                    throw QueryValueError.resourceLimit(resource: "depth", actual: deepest, maximum: limits.maximumNestingDepth)
                }
                switch entries[id] {
                case .scalar(let scalar):
                    switch scalar {
                    case .null: try bytes("null".utf8)
                    case .bool(let flag): try bytes((flag ? "true" : "false").utf8)
                    case .string(let string): try quoted(string)
                    case .number(let numeric):
                        try CanonicalSQLScalarText.withText(of: .numeric(numeric), type: .sql(.decimal(precision: nil, scale: nil)),
                            preservingCharacterPadding: false, workMeter: memory.meter) { try bytes($0.utf8) }
                    case .array, .object: throw SQLJSONMemory.failure("scalar JSON output")
                    }
                case .lexical(let lexical): try bytes(lexical.bytes)
                case .array(let children):
                    try emit(91)
                    try memory.append(.text("]"), to: &visits)
                    for index in children.indices.reversed() {
                        try memory.append(.entry(children[index], depth + 1, style), to: &visits)
                        if index > 0 {
                            let separator = pretty && depth == 0 ? ",\n " : (style == .compact ? "," : ", ")
                            try memory.append(.text(separator), to: &visits)
                        }
                    }
                case .object(let members, let compact):
                    let aggregateRoot = style == .objectAggregate && depth == 0
                    let objectStyle = compact ? Style.compact : (style == .objectAggregate ? .builder : style)
                    try emit(123)
                    if aggregateRoot { try emit(32) }
                    try memory.append(.text(aggregateRoot ? " }" : "}"), to: &visits)
                    for index in members.indices.reversed() {
                        try memory.append(.entry(members[index].value, depth + 1, objectStyle), to: &visits)
                        try memory.append(.text(objectStyle == .builder ? " : " : (objectStyle == .jsonb ? ": " : ":")), to: &visits)
                        try memory.append(.key(members[index].key), to: &visits)
                        if index > 0 {
                            let separator = pretty && depth == 0 ? ",\n " : (objectStyle == .compact ? "," : ", ")
                            try memory.append(.text(separator), to: &visits)
                        }
                    }
                }
            }
        }
    }

    private func count(_ initial: Int, adding value: Int) throws -> Int {
        let result = initial.addingReportingOverflow(value)
        guard !result.overflow else { throw QueryValueError.arithmeticOverflow }
        return result.partialValue
    }
}
