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
            case .date(let value): return try sqlEraText(QueryLiteralEncoding.iso8601(value))
            case .dateTime(let value): return try civilDateTimeText(value)
            case .timestamp(let value):
                guard value.nanoseconds.isMultiple(of: 1000) else { throw .unsupportedConstant("SQL timestamps have microsecond resolution") }
                return try sqlEraText(QueryLiteralEncoding.iso8601(value))
            case .time(let value):
                guard value.nanoseconds.isMultiple(of: 1000) else { throw .unsupportedConstant("SQL times have microsecond resolution") }
                let micros = Int64(value.hour) * 3_600_000_000 + Int64(value.minute) * 60_000_000
                    + Int64(value.second) * 1_000_000 + Int64(value.nanoseconds / 1000)
                do { return timeText(try QueryTimeValue(microsecondsSinceMidnight: micros)) }
                catch { throw .value(error) }
            case .uuid(let value): return value.description
            case .bytes(let value):
                let doubled = UInt64(value.count).multipliedReportingOverflow(by: 2)
                let total = doubled.partialValue.addingReportingOverflow(2)
                guard !doubled.overflow, !total.overflow, total.partialValue <= limits.maximumRetainedBytes,
                      doubled.partialValue <= UInt64(Int.max) else { throw .resourceLimit("BYTEA array text budget exceeded") }
                return "\\x" + QueryLiteralEncoding.hex(value)
            default: throw .unsupportedConstant("Array element has no intrinsic SQL input form")
            }
        case .array: throw .invalidConstant("Nested array values are not array cells")
        }
    }

    static func parseArray(_ text: String, element: DataType, limits: QueryStructuralLimits) throws(SQLConstantError) -> QueryArrayValue? {
        guard UInt64(text.utf8.count) <= limits.maximumRetainedBytes else { throw .resourceLimit("Array input byte budget exceeded") }
        guard limits.maximumTotalNodes >= 1, limits.maximumNestingDepth >= 1 else { throw .resourceLimit("Array root budget exceeded") }
        let input: QueryArrayInput
        do { input = try QueryArrayInput(sqlLexicalForm: text, limits: valueLimits(limits)) }
        catch { throw .value(error) }
        if case .custom = element { return nil }
        guard QueryValueValidator.accepts(.primitive(.null), type: .sql(.array(element))) else {
            throw .invalidConstant("Invalid array element descriptor")
        }
        var elements: [QueryValue] = []
        var nodes: UInt64 = 1
        var collections: UInt64 = 0
        var retained: UInt64 = 0
        var requiresRuntime = false
        for index in 0..<input.count {
            guard collections < limits.maximumCollectionElements, nodes < limits.maximumTotalNodes,
                  limits.maximumNestingDepth > 1 else { throw .resourceLimit("Array cell budget exceeded") }
            collections += 1
            let childLimits = QueryStructuralLimits(maximumNestingDepth: limits.maximumNestingDepth - 1,
                maximumRetainedBytes: limits.maximumRetainedBytes - retained,
                maximumTotalNodes: limits.maximumTotalNodes - nodes,
                maximumCollectionElements: limits.maximumCollectionElements - collections)
            let cell: String?
            do { cell = try input.elementText(at: index) }
            catch { throw .value(error) }
            if let cell, UInt64(cell.utf8.count) > childLimits.maximumRetainedBytes {
                throw .resourceLimit("Array decoded cell byte budget exceeded")
            }
            let value: QueryValue
            if let cell {
                guard let literal = try normalize(.literal(.string(cell)), as: element, limits: childLimits),
                      case .queryValue(let normalized, _) = literal else {
                    requiresRuntime = true
                    continue
                }
                value = normalized
            } else { value = .primitive(.null) }
            do {
                let footprint = try QueryValueValidator.measure(value)
                try footprint.validate(limits: valueLimits(childLimits))
                nodes += footprint.nodeCount
                collections += footprint.collectionElementCount
                retained += footprint.retainedByteCount
            } catch { throw .value(error) }
            elements.append(value)
        }
        if requiresRuntime { return nil }
        do { return try QueryArrayValue(elementType: element, dimensions: input.dimensions, elements: elements, limits: valueLimits(limits)) }
        catch { throw .value(error) }
    }
}
