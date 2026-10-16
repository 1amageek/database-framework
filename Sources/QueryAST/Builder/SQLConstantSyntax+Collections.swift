import DatabaseKit
import DatabaseTypes

extension SQLConstantSyntax {
    private enum JSONVisit {
        case node(QueryJSONBNode, UInt64)
        case array(QueryJSONBElements, Int, UInt64)
        case object(QueryJSONBMembers, Int, UInt64)
        case text(String)
    }

    static func jsonbText(_ value: QueryJSONBValue, limits: QueryStructuralLimits) throws(SQLConstantError) -> String {
        var writer = SQLTextWriter(limits: limits)
        var visits: [JSONVisit] = [.node(value.root, 1)]
        var nodes: UInt64 = 0
        do {
            while let visit = visits.popLast() {
                switch visit {
                case .text(let text): try writer.append(text)
                case .node(let node, let depth):
                    guard depth <= limits.maximumNestingDepth, nodes < limits.maximumTotalNodes else {
                        throw SQLConstantError.resourceLimit("JSONB structural budget exceeded")
                    }
                    nodes += 1
                    switch node {
                    case .null: try writer.append("null")
                    case .bool(let value): try writer.append(value ? "true" : "false")
                    case .string(let value): try jsonString(value, writer: &writer)
                    case .number(let value):
                        try writer.append(try numericText(value, maximum: limits.maximumRetainedBytes - writer.byteCount))
                    case .array(let values):
                        try writer.collection(values.count)
                        try writer.append("[")
                        if values.isEmpty { try writer.append("]") }
                        else { visits.append(.array(values, 0, depth)) }
                    case .object(let values):
                        try writer.collection(values.count)
                        try writer.append("{")
                        if values.isEmpty { try writer.append("}") }
                        else { visits.append(.object(values, 0, depth)) }
                    }
                case .array(let values, let index, let depth):
                    if index > 0 { try writer.append(",") }
                    if index + 1 < values.count { visits.append(.array(values, index + 1, depth)) }
                    else { visits.append(.text("]")) }
                    visits.append(.node(values[index], depth + 1))
                case .object(let values, let index, let depth):
                    if index > 0 { try writer.append(",") }
                    let member = values[index]
                    try jsonString(member.key, writer: &writer)
                    try writer.append(":")
                    if index + 1 < values.count { visits.append(.object(values, index + 1, depth)) }
                    else { visits.append(.text("}")) }
                    visits.append(.node(member.value, depth + 1))
                }
            }
        } catch let error as SQLConstantError { throw error }
          catch let error as SQLSerializationError { throw SQLConstantError(serialization: error) }
          catch { throw .invalidConstant("SQL collection constant encoding failed") }
        return writer.text
    }

    private static func jsonString(_ text: String, writer: inout SQLTextWriter) throws(SQLSerializationError) {
        var bytes: UInt64 = 2
        for scalar in text.unicodeScalars {
            bytes += scalar.value < 32 ? 6 : scalar.value == 34 || scalar.value == 92 ? 2 : UInt64(scalar.utf8.count)
        }
        try writer.admit(bytes)
        try writer.append("\"")
        for scalar in text.unicodeScalars {
            switch scalar.value {
            case 34: try writer.append("\\\"")
            case 92: try writer.append("\\\\")
            case 0..<32:
                let hex = String(scalar.value, radix: 16)
                try writer.append("\\u" + String(repeating: "0", count: 4 - hex.count) + hex)
            default: try writer.append(String(scalar))
            }
        }
        try writer.append("\"")
    }

    static func arrayText(_ value: QueryArrayValue, limits: QueryStructuralLimits) throws(SQLConstantError) -> String {
        var writer = SQLTextWriter(limits: limits)
        do {
            guard value.dimensions.count <= 6 else { throw SQLConstantError.invalidConstant("Too many array dimensions") }
            try writer.collection(value.elements.count)
            if value.dimensions.isEmpty { try writer.append("{}"); return writer.text }
            var product: UInt64 = 1
            for dimension in value.dimensions {
                let next = product.multipliedReportingOverflow(by: UInt64(dimension.length))
                guard !next.overflow else { throw SQLConstantError.invalidConstant("Array dimension product overflow") }
                product = next.partialValue
                let upper = Int64(dimension.lowerBound) + Int64(dimension.length) - 1
                try writer.append("[\(dimension.lowerBound):\(upper)]")
            }
            guard product == UInt64(value.elements.count) else { throw SQLConstantError.invalidConstant("Array dimension product mismatch") }
            try writer.append("=")
            var index = 0
            try writeArray(value, dimension: 0, index: &index, writer: &writer)
        } catch let error as SQLConstantError { throw error }
          catch let error as SQLSerializationError { throw SQLConstantError(serialization: error) }
          catch { throw .invalidConstant("SQL collection constant encoding failed") }
        return writer.text
    }

    private static func writeArray(_ value: QueryArrayValue, dimension: Int, index: inout Int,
                                   writer: inout SQLTextWriter) throws(SQLConstantError) {
        do {
            try writer.append("{")
            for cell in 0..<Int(value.dimensions[dimension].length) {
                if cell > 0 { try writer.append(",") }
                if dimension + 1 < value.dimensions.count {
                    try writeArray(value, dimension: dimension + 1, index: &index, writer: &writer)
                } else {
                    let element = value.elements[index]
                    index += 1
                    if element.isNull { try writer.append("NULL"); continue }
                    let text = try elementText(element, limits: writer.limits)
                    switch value.elementType {
                    case .char(let length):
                        guard text.unicodeScalars.count == (length ?? 1) else { throw SQLConstantError.unsupportedConstant("CHAR array input would pad or truncate the cell") }
                    case .varchar(let length):
                        guard length == nil || text.unicodeScalars.count <= length! else { throw SQLConstantError.unsupportedConstant("VARCHAR array input would truncate the cell") }
                    default: break
                    }
                    var escaped: UInt64 = 2
                    for byte in text.utf8 { escaped += byte == 34 || byte == 92 ? 2 : 1 }
                    try writer.admit(escaped)
                    try writer.append("\"")
                    for scalar in text.unicodeScalars {
                        if scalar.value == 34 || scalar.value == 92 { try writer.append("\\") }
                        try writer.append(String(scalar))
                    }
                    try writer.append("\"")
                }
            }
            try writer.append("}")
        } catch let error as SQLConstantError { throw error }
          catch let error as SQLSerializationError { throw SQLConstantError(serialization: error) }
          catch { throw .invalidConstant("SQL collection constant encoding failed") }
    }

    static func elementText(_ value: QueryValue, limits: QueryStructuralLimits) throws(SQLConstantError) -> String {
        switch value {
        case .numeric(let number): return try numericText(number, maximum: limits.maximumRetainedBytes)
        case .json(let json): return json.bytes.withUnsafeBytes { String(decoding: $0, as: UTF8.self) }
        case .jsonb(let json): return try jsonbText(json, limits: limits)
        case .interval(let interval): return intervalText(interval)
        case .time(let time): return timeText(time)
        case .timeWithTimeZone(let time): return try offsetTimeText(time)
        case .dateInfinity(let value), .timestampInfinity(let value, _), .intervalInfinity(let value): return infinityText(value)
        case .primitive(let value):
            switch value {
            case .bool(let value): return value ? "true" : "false"
            case .int16(let value): return String(value)
            case .int32(let value): return String(value)
            case .int64(let value): return String(value)
            case .float32(let value):
                guard value.isFinite else { throw .unsupportedConstant("Nonfinite array element") }
                return String(value)
            case .float64(let value):
                guard value.isFinite else { throw .unsupportedConstant("Nonfinite array element") }
                return String(value)
            case .decimal(let value):
                do { return try numericText(QueryNumericValue(value: value, displayScale: max(0, value.scale)), maximum: limits.maximumRetainedBytes) }
                catch let error as SQLConstantError { throw error }
                catch { throw .invalidConstant("Invalid numeric array element") }
            case .string(let value): return value
            default: throw .unsupportedConstant("Array element has no intrinsic SQL input form")
            }
        case .array: throw .invalidConstant("Nested array values are not array cells")
        }
    }

    static func parseArray(_ text: String, element: DataType, limits: QueryStructuralLimits) throws(SQLConstantError) -> QueryArrayValue {
        guard UInt64(text.utf8.count) <= limits.maximumRetainedBytes else { throw .resourceLimit("Array input byte budget exceeded") }
        var parser = SQLArrayInput(text: text, element: element, limits: limits)
        return try parser.parse()
    }
}

/// PostgreSQL array input syntax; syntax operates on UTF-8 bytes, not graphemes.
private struct SQLArrayInput {
    let input: [UInt8]
    let element: DataType
    let limits: QueryStructuralLimits
    var index = 0
    var elements: [QueryValue] = []
    var nodeCount: UInt64 = 1
    var collectionCount: UInt64 = 0
    var retainedBytes: UInt64 = 0

    init(text: String, element: DataType, limits: QueryStructuralLimits) {
        input = Array(text.utf8)
        self.element = element
        self.limits = limits
    }

    var peek: UInt8? { index < input.count ? input[index] : nil }
    func isSpace(_ byte: UInt8) -> Bool { byte == 32 || (9...13).contains(byte) }
    mutating func spaces() { while let byte = peek, isSpace(byte) { index += 1 } }
    mutating func consume(_ byte: UInt8) -> Bool {
        guard peek == byte else { return false }
        index += 1
        return true
    }
    mutating func require(_ byte: UInt8) throws(SQLConstantError) {
        guard consume(byte) else { throw .invalidConstant("Malformed PostgreSQL array input") }
    }

    mutating func parse() throws(SQLConstantError) -> QueryArrayValue {
        guard limits.maximumTotalNodes >= 1, limits.maximumNestingDepth >= 1 else {
            throw .resourceLimit("Array root budget exceeded")
        }
        spaces()
        var declared: [(Int32, Int32)] = []
        while consume(91) {
            guard declared.count < 6 else { throw .invalidConstant("Too many array dimensions") }
            let lower = try bound(until: 58)
            let upper = try bound(until: 93)
            guard upper >= lower, let length = Int32(exactly: Int64(upper) - Int64(lower) + 1) else {
                throw .invalidConstant("Invalid array bounds")
            }
            declared.append((lower, length))
        }
        if !declared.isEmpty { try require(61) }
        let shape = try group(depth: 1)
        spaces()
        guard index == input.count else { throw .invalidConstant("Trailing array input") }
        guard declared.isEmpty || shape == declared.map({ Int($0.1) }) else { throw .invalidConstant("Array bounds disagree with shape") }
        var dimensions: [QueryArrayDimension] = []
        do {
            if !shape.isEmpty {
                for (offset, length) in shape.enumerated() {
                    dimensions.append(try QueryArrayDimension(lowerBound: declared.isEmpty ? 1 : declared[offset].0,
                                                              length: Int32(length)))
                }
            }
            return try QueryArrayValue(elementType: element, dimensions: dimensions, elements: elements,
                                       limits: SQLConstantSyntax.valueLimits(limits))
        } catch { throw .value(error) }
    }

    mutating func bound(until terminator: UInt8) throws(SQLConstantError) -> Int32 {
        let start = index
        while let byte = peek, byte != terminator { index += 1 }
        guard let value = Int32(String(decoding: input[start..<index], as: UTF8.self)) else { throw .invalidConstant("Invalid array bound") }
        try require(terminator)
        return value
    }

    mutating func group(depth: UInt64) throws(SQLConstantError) -> [Int] {
        guard depth <= 6, depth <= limits.maximumNestingDepth else { throw .resourceLimit("Array nesting budget exceeded") }
        spaces()
        try require(123)
        spaces()
        if consume(125) {
            guard depth == 1 else { throw .invalidConstant("Empty nested array dimension") }
            return []
        }
        let nested = peek == 123
        var childShape: [Int]?
        var count = 0
        repeat {
            spaces()
            guard (peek == 123) == nested else { throw .invalidConstant("Mixed scalar and nested array elements") }
            if nested {
                let shape = try group(depth: depth + 1)
                if let childShape, childShape != shape { throw .invalidConstant("Ragged array shape") }
                childShape = shape
            } else {
                guard collectionCount < limits.maximumCollectionElements,
                      nodeCount < limits.maximumTotalNodes, limits.maximumNestingDepth > 1 else {
                    throw .resourceLimit("Array cell budget exceeded")
                }
                // Reserve the array cell before giving its payload the remaining ledger.
                collectionCount += 1
                let childLimits = remainingChildLimits()
                let cell = try scalar(maximumByteCount: childLimits.maximumRetainedBytes)
                let value: QueryValue
                if let cell {
                    guard let literal = try SQLConstantSyntax.normalize(.literal(.string(cell)), as: element, limits: childLimits),
                          case .queryValue(let normalized, _) = literal else {
                        throw .unsupportedConstant("Array element conversion requires runtime policy")
                    }
                    value = normalized
                } else { value = .primitive(.null) }
                try admit(value, limits: childLimits)
                elements.append(value)
            }
            count += 1
            guard UInt64(count) <= limits.maximumCollectionElements else { throw .resourceLimit("Array dimension budget exceeded") }
            spaces()
            if !consume(44) { break }
        } while true
        try require(125)
        return [count] + (childShape ?? [])
    }

    private func remainingChildLimits() -> QueryStructuralLimits {
        QueryStructuralLimits(maximumNestingDepth: limits.maximumNestingDepth - 1,
                              maximumRetainedBytes: limits.maximumRetainedBytes - retainedBytes,
                              maximumTotalNodes: limits.maximumTotalNodes - nodeCount,
                              maximumCollectionElements: limits.maximumCollectionElements - collectionCount)
    }

    private mutating func admit(_ value: QueryValue, limits childLimits: QueryStructuralLimits) throws(SQLConstantError) {
        do {
            let footprint = try QueryValueValidator.measure(value)
            try footprint.validate(limits: SQLConstantSyntax.valueLimits(childLimits))
            nodeCount += footprint.nodeCount
            collectionCount += footprint.collectionElementCount
            retainedBytes += footprint.retainedByteCount
        } catch { throw .value(error) }
    }

    mutating func scalar(maximumByteCount: UInt64) throws(SQLConstantError) -> String? {
        spaces()
        let quoted = consume(34)
        let start = index
        var decodedCount: UInt64 = 0
        var escaped = false
        var closed = false
        var contentEnd = index
        while let byte = peek {
            if quoted, byte == 34 { contentEnd = index; index += 1; closed = true; break }
            if !quoted, byte == 44 || byte == 125 { contentEnd = index; break }
            index += 1
            if byte == 92 {
                guard peek != nil else { throw .invalidConstant("Unterminated array escape") }
                index += 1
                escaped = true
            } else {
                guard quoted || byte != 34 && byte != 123 else { throw .invalidConstant("Invalid unquoted array cell") }
            }
            decodedCount += 1
            contentEnd = index
        }
        if quoted {
            guard closed else { throw .invalidConstant("Unterminated quoted array cell") }
        } else if !escaped {
            while contentEnd > start, isSpace(input[contentEnd - 1]) { contentEnd -= 1; decodedCount -= 1 }
            guard contentEnd > start else { throw .invalidConstant("Empty unquoted array cell") }
            if decodedCount == 4,
               input[start] == 78 || input[start] == 110,
               input[start + 1] == 85 || input[start + 1] == 117,
               input[start + 2] == 76 || input[start + 2] == 108,
               input[start + 3] == 76 || input[start + 3] == 108 { return nil }
        }
        guard decodedCount <= maximumByteCount else { throw .resourceLimit("Array decoded cell byte budget exceeded") }
        if !escaped { return String(decoding: input[start..<contentEnd], as: UTF8.self) }
        // Only escaped input requires a materialized decode buffer, admitted above.
        var bytes: [UInt8] = []
        bytes.reserveCapacity(Int(decodedCount))
        var cursor = start
        while cursor < contentEnd {
            if input[cursor] == 92 { cursor += 1 }
            bytes.append(input[cursor])
            cursor += 1
        }
        if !quoted {
            while let last = bytes.last, isSpace(last) { bytes.removeLast() }
            guard !bytes.isEmpty else { throw .invalidConstant("Empty unquoted array cell") }
        }
        return String(decoding: bytes, as: UTF8.self)
    }
}
