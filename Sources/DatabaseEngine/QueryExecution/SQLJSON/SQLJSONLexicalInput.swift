import DatabaseKit
import DatabaseTypes

/// A flat lexical index. Ranges retain the original complete byte owner.
struct SQLJSONLexicalInput {
    enum Kind: Equatable { case object, array, string, scalar }
    struct Node {
        let kind: Kind
        var range: Range<Int>
        var end: Int
        let key: Range<Int>?
    }
    private struct Frame {
        let node: Int
        var key: Range<Int>?
    }
    let source: QueryJSONValue
    let memory: SQLJSONMemory
    @AdmittedArray private(set) var nodes: [Node] = []

    init(_ source: QueryJSONValue, memory: SQLJSONMemory) throws {
        self.source = source
        self.memory = memory
        let retained = try source.measure().retainedByteCount
        try memory.bytes(retained)
        @AdmittedArray var frames: [Frame] = []
        var offset = 0
        let bytes = source.bytes
        while offset < bytes.count {
            try memory.work()
            let byte = bytes[offset]
            if byte == 32 || byte == 9 || byte == 10 || byte == 13 || byte == 44 || byte == 58 {
                offset += 1; continue
            }
            if byte == 93 || byte == 125 {
                guard let frame = frames.popLast() else { throw SQLJSONMemory.failure("validated JSON structure") }
                nodes[frame.node].range = nodes[frame.node].range.lowerBound..<(offset + 1)
                nodes[frame.node].end = nodes.count
                offset += 1; continue
            }
            let start = offset
            let kind: Kind
            if byte == 34 {
                offset += 1
                while offset < bytes.count {
                    try memory.work()
                    if bytes[offset] == 92 { offset += 2; continue }
                    if bytes[offset] == 34 { offset += 1; break }
                    offset += 1
                }
                var following = offset
                while following < bytes.count && Self.space(bytes[following]) { try memory.work(); following += 1 }
                if following < bytes.count && bytes[following] == 58 {
                    guard !frames.isEmpty else { throw SQLJSONMemory.failure("validated JSON key") }
                    frames[frames.count - 1].key = start..<offset
                    continue
                }
                kind = .string
            } else if byte == 91 || byte == 123 {
                offset += 1; kind = byte == 91 ? .array : .object
            } else {
                while offset < bytes.count && !Self.delimiter(bytes[offset]) { try memory.work(); offset += 1 }
                kind = .scalar
            }
            let key = frames.last?.key
            if !frames.isEmpty { frames[frames.count - 1].key = nil }
            let id = nodes.count
            try memory.append(Node(kind: kind, range: start..<offset, end: id + 1, key: key), to: &$nodes)
            if kind == .object || kind == .array {
                try memory.append(Frame(node: id, key: nil), to: &$frames)
            }
        }
        guard frames.isEmpty, !nodes.isEmpty else { throw SQLJSONMemory.failure("validated JSON root") }
    }

    func childCount(_ id: Int) throws -> Int {
        var child = id + 1, count = 0
        while child < nodes[id].end { try memory.work(); count += 1; child = nodes[child].end }
        return count
    }

    func lookup(_ id: Int, key: String, limits: QueryValueLimits) throws -> Int? {
        switch nodes[id].kind {
        case .object:
            var result: Int?, child = id + 1
            while child < nodes[id].end {
                try memory.work()
                if let range = nodes[child].key,
                   try decodedString(range, limits: limits).utf8.elementsEqual(key.utf8) { result = child }
                child = nodes[child].end
            }
            return result
        case .array:
            guard let index = Self.index(key) else { return nil }
            return try element(id, index: index)
        default: return nil
        }
    }

    func element(_ id: Int, index: Int32) throws -> Int? {
        guard nodes[id].kind == .array else { return nil }
        let count = try childCount(id)
        let position = index < 0 ? Int64(count) + Int64(index) : Int64(index)
        guard position >= 0, position < count else { return nil }
        var child = id + 1
        for _ in 0..<Int(position) { try memory.work(); child = nodes[child].end }
        return child
    }

    func value(_ id: Int, text: Bool, limits: QueryValueLimits, outputMemory: SQLJSONMemory? = nil) throws -> QueryValue {
        let memory = outputMemory ?? self.memory
        let range = nodes[id].range
        if text {
            if nodes[id].kind == .string { return .primitive(.string(try decodedString(range, limits: limits, outputMemory: memory))) }
            if range.count == 4, source.bytes[range.lowerBound] == 110 { return .primitive(.null) }
            try memory.bytes(UInt64(range.count))
            return .primitive(.string(String(decoding: source.bytes[range], as: UTF8.self)))
        }
        try memory.work(UInt64(range.count))
        return .json(try QueryJSONValue(bytes: source.bytes[range], limits: limits))
    }

    func decodedString(_ range: Range<Int>, limits: QueryValueLimits,
        outputMemory: SQLJSONMemory? = nil) throws -> String {
        let memory = outputMemory ?? self.memory
        var count: UInt64 = 0
        try decode(range, memory: memory) { _ in count = try SQLJSONMemory.add(count, 1) }
        guard count <= limits.maximumRetainedBytes, let size = Int(exactly: count) else {
            throw QueryValueError.resourceLimit(resource: "retainedBytes", actual: count, maximum: limits.maximumRetainedBytes)
        }
        // The exact temporary byte owner and final String overlap only at this output boundary.
        try memory.bytes(try SQLJSONMemory.add(count, count))
        let bytes = try ByteString.copying(count: size) { output in
            var offset = 0
            try decode(range, memory: memory) { byte in output[offset] = byte; offset += 1 }
        }
        return String(decoding: bytes, as: UTF8.self)
    }

    private func decode(_ range: Range<Int>, memory: SQLJSONMemory,
        emit: (UInt8) throws -> Void) throws {
        let bytes = source.bytes
        var offset = range.lowerBound + 1
        // The failing position is passed explicitly: capturing `offset` here
        // would overlap the exclusive access that `hex(&offset)` holds.
        func invalid(_ reason: String, at position: Int) -> QueryValueError {
            .invalidJSON(offset: UInt64(position), reason: reason)
        }
        func hex(_ position: inout Int) throws -> UInt32 {
            var result: UInt32 = 0
            for _ in 0..<4 {
                guard position < range.upperBound - 1 else { throw invalid("Incomplete Unicode escape", at: position) }
                let byte = bytes[position], digit: UInt32
                switch byte {
                case 48...57: digit = UInt32(byte - 48)
                case 65...70: digit = UInt32(byte - 55)
                case 97...102: digit = UInt32(byte - 87)
                default: throw invalid("Invalid Unicode escape", at: position)
                }
                result = result * 16 + digit; position += 1; try memory.work()
            }
            return result
        }
        while offset < range.upperBound - 1 {
            try memory.work()
            let byte = bytes[offset]; offset += 1
            if byte != 92 { try emit(byte); continue }
            guard offset < range.upperBound - 1 else { throw invalid("Incomplete string escape", at: offset) }
            let escaped = bytes[offset]; offset += 1
            switch escaped {
            case 34, 47, 92: try emit(escaped)
            case 98: try emit(8)
            case 102: try emit(12)
            case 110: try emit(10)
            case 114: try emit(13)
            case 116: try emit(9)
            case 117:
                var scalar = try hex(&offset)
                if (0xD800...0xDBFF).contains(scalar) {
                    guard offset + 1 < range.upperBound - 1, bytes[offset] == 92, bytes[offset + 1] == 117 else {
                        throw invalid("Unpaired high surrogate", at: offset)
                    }
                    offset += 2
                    let low = try hex(&offset)
                    guard (0xDC00...0xDFFF).contains(low) else { throw invalid("Invalid surrogate pair", at: offset) }
                    scalar = 0x10000 + ((scalar - 0xD800) << 10) + low - 0xDC00
                }
                guard scalar != 0, !(0xDC00...0xDFFF).contains(scalar), Unicode.Scalar(scalar) != nil else {
                    throw invalid("Selected text cannot contain NUL or an unpaired surrogate", at: offset)
                }
                if scalar < 0x80 { try emit(UInt8(scalar)) }
                else if scalar < 0x800 {
                    try emit(UInt8(0xC0 | (scalar >> 6))); try emit(UInt8(0x80 | (scalar & 63)))
                } else if scalar < 0x10000 {
                    try emit(UInt8(0xE0 | (scalar >> 12))); try emit(UInt8(0x80 | ((scalar >> 6) & 63))); try emit(UInt8(0x80 | (scalar & 63)))
                } else {
                    try emit(UInt8(0xF0 | (scalar >> 18))); try emit(UInt8(0x80 | ((scalar >> 12) & 63)))
                    try emit(UInt8(0x80 | ((scalar >> 6) & 63))); try emit(UInt8(0x80 | (scalar & 63)))
                }
            default: throw invalid("Invalid string escape", at: offset)
            }
        }
    }

    static func index(_ text: String) -> Int32? {
        // PostgreSQL array path indices accept surrounding ASCII whitespace and sign.
        var view = text[...]
        while let first = view.utf8.first, space(first) { view.removeFirst() }
        while let last = view.utf8.last, space(last) { view.removeLast() }
        return Int32(view)
    }
    private static func space(_ byte: UInt8) -> Bool { byte == 32 || (9...13).contains(byte) }
    private static func delimiter(_ byte: UInt8) -> Bool { space(byte) || byte == 44 || byte == 93 || byte == 125 }
}
