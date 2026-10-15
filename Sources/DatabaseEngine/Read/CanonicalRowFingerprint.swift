import DatabaseKit
import DatabaseTypes

public enum CanonicalRowFingerprint {
    public static func compute(
        _ row: QueryRow,
        columns: [QueryColumn],
        workMeter: DatabaseWorkMeter
    ) throws -> ByteString {
        try row.validate(columns: columns)
        var hasher = SHA256Accumulator()
        var domain = UInt32(0x0252_4244).littleEndian
        withUnsafeBytes(of: &domain) { hasher.update($0) }
        updateInteger(UInt64(columns.count), hasher: &hasher)
        for (column, value) in zip(columns, row.values) {
            try workMeter.consume(UInt64(column.name.utf8.count) + 1, at: .resultMaterialization)
            updateInteger(column.number, hasher: &hasher)
            updateString(column.name, hasher: &hasher)
            updateColumnType(column.type, hasher: &hasher)
            updateByte(column.isNullable ? 1 : 0, hasher: &hasher)
            try updateValue(value, hasher: &hasher, workMeter: workMeter)
        }
        try consume(row.annotations, workMeter: workMeter)
        hasher.update(try PersistableVersionTokenCodec.digest(fields: row.annotations))
        if let version = row.version {
            try workMeter.consume(UInt64(version.value.utf8.count), at: .resultMaterialization)
            updateByte(1, hasher: &hasher)
            updateString(version.value, hasher: &hasher)
        } else {
            updateByte(0, hasher: &hasher)
        }
        return hasher.finalize()
    }

    /// Preserves the existing name-keyed RDF binding fingerprint bytes.
    public static func compute(
        fields: [String: FieldValue],
        workMeter: DatabaseWorkMeter
    ) throws -> ByteString {
        try consume(fields, workMeter: workMeter)
        var hasher = SHA256Accumulator()
        var domain = UInt32(0x0152_4244).littleEndian
        withUnsafeBytes(of: &domain) { hasher.update($0) }
        hasher.update(try PersistableVersionTokenCodec.digest(fields: fields))
        hasher.update(try PersistableVersionTokenCodec.digest(fields: [String: FieldValue]()))
        return hasher.finalize()
    }

    private static func updateValue(
        _ value: QueryValue,
        hasher: inout SHA256Accumulator,
        workMeter: DatabaseWorkMeter
    ) throws {
        try workMeter.consume(at: .resultMaterialization)
        switch value {
        case .primitive(let primitive):
            updateByte(0, hasher: &hasher)
            try consume(primitive, workMeter: workMeter)
            try PersistableVersionTokenCodec.update(primitive, hasher: &hasher)
        case .numeric(let numeric):
            updateByte(1, hasher: &hasher)
            updateNumeric(numeric, hasher: &hasher)
        case .json(let json):
            updateByte(2, hasher: &hasher)
            try workMeter.consume(UInt64(json.bytes.count), at: .resultMaterialization)
            updateInteger(UInt64(json.bytes.count), hasher: &hasher)
            json.bytes.withUnsafeBytes { hasher.update($0) }
        case .jsonb(let json):
            updateByte(3, hasher: &hasher)
            try updateJSONB(json.root, hasher: &hasher, workMeter: workMeter)
        case .array(let array):
            updateByte(4, hasher: &hasher)
            updateSQLType(array.elementType, hasher: &hasher)
            updateInteger(UInt64(array.dimensions.count), hasher: &hasher)
            for dimension in array.dimensions {
                try workMeter.consume(at: .resultMaterialization)
                updateInteger(dimension.lowerBound, hasher: &hasher)
                updateInteger(dimension.length, hasher: &hasher)
            }
            updateInteger(UInt64(array.elements.count), hasher: &hasher)
            for element in array.elements {
                try updateValue(element, hasher: &hasher, workMeter: workMeter)
            }
        case .interval(let interval):
            updateByte(5, hasher: &hasher)
            updateInteger(interval.period.months, hasher: &hasher)
            updateInteger(interval.period.days, hasher: &hasher)
            updateInteger(interval.time.seconds, hasher: &hasher)
            updateInteger(interval.time.nanoseconds, hasher: &hasher)
        }
    }

    private struct JSONBTraversalFrame {
        let node: QueryJSONBNode
        let nextChild: Int?
    }

    private static func updateJSONB(
        _ node: QueryJSONBNode,
        hasher: inout SHA256Accumulator,
        workMeter: DatabaseWorkMeter
    ) throws {
        let layout = try DatabaseRetainedArrayLayout.forElement(JSONBTraversalFrame.self)
        let reservation = try workMeter.reserveIntermediate(at: .resultMaterialization)
        defer { reservation.release() }
        var capacity = 0
        var stack: [JSONBTraversalFrame] = []
        func append(_ node: QueryJSONBNode, nextChild: Int? = nil) throws {
            let (needed, overflow) = stack.count.addingReportingOverflow(1)
            guard !overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            let growth = try layout.growth(from: capacity, toFit: needed)
            if growth.additionalByteCount > 0 {
                try reservation.reserveAdditional(bytes: growth.additionalByteCount, at: .resultMaterialization)
                stack.reserveCapacity(growth.capacity)
                capacity = growth.capacity
            }
            stack.append(JSONBTraversalFrame(node: node, nextChild: nextChild))
        }
        try append(node)
        while let frame = stack.popLast() {
            if let index = frame.nextChild {
                switch frame.node {
                case .array(let values):
                    if index + 1 < values.count { try append(frame.node, nextChild: index + 1) }
                    try append(values[index])
                case .object(let members):
                    let member = members[index]
                    if index + 1 < members.count { try append(frame.node, nextChild: index + 1) }
                    try workMeter.consume(UInt64(member.key.utf8.count), at: .resultMaterialization)
                    updateString(member.key, hasher: &hasher)
                    try append(member.value)
                default:
                    preconditionFailure("Only JSONB containers have traversal cursors")
                }
                continue
            }
            try workMeter.consume(at: .resultMaterialization)
            switch frame.node {
            case .null: updateByte(0, hasher: &hasher)
            case .bool(let value):
                updateByte(1, hasher: &hasher)
                updateByte(value ? 1 : 0, hasher: &hasher)
            case .string(let value):
                updateByte(2, hasher: &hasher)
                try workMeter.consume(UInt64(value.utf8.count), at: .resultMaterialization)
                updateString(value, hasher: &hasher)
            case .number(let value):
                updateByte(3, hasher: &hasher)
                updateNumeric(value, hasher: &hasher)
            case .array(let values):
                updateByte(4, hasher: &hasher)
                updateInteger(UInt64(values.count), hasher: &hasher)
                if !values.isEmpty { try append(frame.node, nextChild: 0) }
            case .object(let members):
                updateByte(5, hasher: &hasher)
                updateInteger(UInt64(members.count), hasher: &hasher)
                if !members.isEmpty { try append(frame.node, nextChild: 0) }
            }
        }
    }

    private static func updateNumeric(_ value: QueryNumericValue, hasher: inout SHA256Accumulator) {
        updateInteger(value.value.coefficient, hasher: &hasher)
        updateInteger(value.value.scale, hasher: &hasher)
        updateInteger(value.displayScale, hasher: &hasher)
    }

    private static func updateColumnType(_ type: QueryColumnType, hasher: inout SHA256Accumulator) {
        switch type {
        case .sql(let type):
            updateByte(0, hasher: &hasher)
            updateSQLType(type, hasher: &hasher)
        case .primitive(let kind, let isArray):
            updateByte(1, hasher: &hasher)
            updateString(kind.rawValue, hasher: &hasher)
            updateByte(isArray ? 1 : 0, hasher: &hasher)
        }
    }

    private static func updateSQLType(_ type: DataType, hasher: inout SHA256Accumulator) {
        switch type {
        case .boolean: updateByte(0, hasher: &hasher)
        case .smallint: updateByte(1, hasher: &hasher)
        case .integer: updateByte(2, hasher: &hasher)
        case .bigint: updateByte(3, hasher: &hasher)
        case .real: updateByte(4, hasher: &hasher)
        case .doublePrecision: updateByte(5, hasher: &hasher)
        case .decimal(let precision, let scale):
            updateByte(6, hasher: &hasher)
            updateOptionalInteger(precision, hasher: &hasher)
            updateOptionalInteger(scale, hasher: &hasher)
        case .char(let length):
            updateByte(7, hasher: &hasher)
            updateOptionalInteger(length, hasher: &hasher)
        case .varchar(let length):
            updateByte(8, hasher: &hasher)
            updateOptionalInteger(length, hasher: &hasher)
        case .text: updateByte(9, hasher: &hasher)
        case .date: updateByte(10, hasher: &hasher)
        case .time(let withTimeZone, let precision):
            updateByte(precision == nil ? 11 : 22, hasher: &hasher)
            updateByte(withTimeZone ? 1 : 0, hasher: &hasher)
            if let precision { updateInteger(Int64(precision), hasher: &hasher) }
        case .timestamp(let withTimeZone, let precision):
            updateByte(precision == nil ? 12 : 23, hasher: &hasher)
            updateByte(withTimeZone ? 1 : 0, hasher: &hasher)
            if let precision { updateInteger(Int64(precision), hasher: &hasher) }
        case .interval: updateByte(13, hasher: &hasher)
        case .binary(let length):
            updateByte(14, hasher: &hasher)
            updateOptionalInteger(length, hasher: &hasher)
        case .varbinary(let length):
            updateByte(15, hasher: &hasher)
            updateOptionalInteger(length, hasher: &hasher)
        case .blob: updateByte(16, hasher: &hasher)
        case .json: updateByte(17, hasher: &hasher)
        case .jsonb: updateByte(18, hasher: &hasher)
        case .uuid: updateByte(19, hasher: &hasher)
        case .array(let element):
            updateByte(20, hasher: &hasher)
            updateSQLType(element, hasher: &hasher)
        case .custom(let name, let schema):
            updateByte(21, hasher: &hasher)
            updateString(name, hasher: &hasher)
            if let schema {
                updateByte(1, hasher: &hasher)
                updateString(schema, hasher: &hasher)
            } else {
                updateByte(0, hasher: &hasher)
            }
        }
    }

    private static func updateOptionalInteger(_ value: Int?, hasher: inout SHA256Accumulator) {
        if let value {
            updateByte(1, hasher: &hasher)
            updateInteger(Int64(value), hasher: &hasher)
        } else {
            updateByte(0, hasher: &hasher)
        }
    }

    private static func updateString(_ value: String, hasher: inout SHA256Accumulator) {
        updateInteger(UInt64(value.utf8.count), hasher: &hasher)
        updateUTF8(value, hasher: &hasher)
    }

    private static func updateInteger<Value: FixedWidthInteger>(_ value: Value, hasher: inout SHA256Accumulator) {
        var encoded = value.bigEndian
        withUnsafeBytes(of: &encoded) { hasher.update($0) }
    }

    private static func updateByte(_ value: UInt8, hasher: inout SHA256Accumulator) {
        var byte = value
        withUnsafeBytes(of: &byte) { hasher.update($0) }
    }

    private static func consume(
        _ fields: [String: FieldValue],
        workMeter: DatabaseWorkMeter
    ) throws {
        for key in fields.keys.sorted() {
            try workMeter.consume(
                UInt64(key.utf8.count),
                at: .resultMaterialization
            )
            guard let value = fields[key] else {
                throw PersistableVersionTokenCodecError.inconsistentFieldMap(key)
            }
            try consume(value, workMeter: workMeter)
        }
    }

    private static func consume(
        _ value: FieldValue,
        workMeter: DatabaseWorkMeter
    ) throws {
        try workMeter.consume(at: .resultMaterialization)
        switch value {
        case .string(let value):
            try workMeter.consume(
                UInt64(value.utf8.count),
                at: .resultMaterialization
            )
        case .bytes(let value):
            try workMeter.consume(
                UInt64(value.count),
                at: .resultMaterialization
            )
        case .rdfTerm(let value):
            try workMeter.consume(
                UInt64(try RDFTermStorageFormat.encodedByteCount(value)),
                at: .resultMaterialization
            )
        case .vector(let value):
            try workMeter.consume(
                UInt64(value.count) * vectorElementByteCount(value.elementType),
                at: .resultMaterialization
            )
        case .array(let values):
            for value in values {
                try consume(value, workMeter: workMeter)
            }
        case .object(let object):
            for field in object.fields {
                try workMeter.consume(
                    UInt64(field.key.utf8.count),
                    at: .resultMaterialization
                )
                try consume(field.value, workMeter: workMeter)
            }
        case .reference(let identity):
            try workMeter.consume(
                UInt64(identity.entity.utf8.count),
                at: .resultMaterialization
            )
            try consume(identity.id, workMeter: workMeter)
            for field in identity.partitions.fields {
                try workMeter.consume(
                    UInt64(field.key.utf8.count),
                    at: .resultMaterialization
                )
                try consume(field.value, workMeter: workMeter)
            }
        case .int8, .int16, .int32, .int64,
             .uint8, .uint16, .uint32, .uint64,
             .float32, .float64, .decimal, .bool,
             .date, .time, .dateTime, .timestamp,
             .timeSpan, .calendarPeriod, .geographicPoint,
             .geographicPosition, .uuid, .null:
            break
        }
    }

    private static func consume(
        _ identifier: ReferenceIdentifier,
        workMeter: DatabaseWorkMeter
    ) throws {
        try workMeter.consume(at: .resultMaterialization)
        switch identifier {
        case .string(let value):
            try workMeter.consume(
                UInt64(value.utf8.count),
                at: .resultMaterialization
            )
        case .bytes(let value):
            try workMeter.consume(
                UInt64(value.count),
                at: .resultMaterialization
            )
        case .composite(let components):
            for component in components {
                try consume(component, workMeter: workMeter)
            }
        case .bool, .int8, .int16, .int32, .int64,
             .uint8, .uint16, .uint32, .uint64, .uuid:
            break
        }
    }

    private static func vectorElementByteCount(
        _ type: VectorElementType
    ) -> UInt64 {
        switch type {
        case .int8, .uint8: 1
        case .int16, .uint16: 2
        case .int32, .uint32, .float32: 4
        case .int64, .uint64, .float64: 8
        }
    }

    private static func updateUTF8(
        _ value: String,
        hasher: inout SHA256Accumulator
    ) {
        let updated = value.utf8.withContiguousStorageIfAvailable { bytes in
            hasher.update(UnsafeRawBufferPointer(bytes))
            return true
        } ?? false
        if !updated {
            for byte in value.utf8 {
                var byte = byte
                withUnsafeBytes(of: &byte) { hasher.update($0) }
            }
        }
    }
}
