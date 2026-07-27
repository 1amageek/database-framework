import DatabaseKit
import DatabaseTypes

/// Reconstructs primitive field values from bounded engine storage frames.
package enum StorageValueDecoder {
    package static func read(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> FieldValue {
        switch try decoder.readUInt8() {
        case 0:
            return .null
        case 1:
            return .bool(try decoder.readBool())
        case 2:
            return .int8(try decoder.readInt8())
        case 3:
            return .int16(try decoder.readInt16())
        case 4:
            return .int32(try decoder.readInt32())
        case 5:
            return .int64(try decoder.readInt64())
        case 6:
            return .uint8(try decoder.readUInt8())
        case 7:
            return .uint16(try decoder.readUInt16())
        case 8:
            return .uint32(try decoder.readUInt32())
        case 9:
            return .uint64(try decoder.readUInt64())
        case 10:
            return .float32(try decoder.readFloat())
        case 11:
            return .float64(try decoder.readDouble())
        case 12:
            return .decimal(
                ExactDecimal(
                    coefficient: try decoder.readInt128(),
                    scale: try decoder.readInt32()
                )
            )
        case 13:
            return .string(try decoder.readString())
        case 14:
            return .bytes(try decoder.readBytes())
        case 15:
            return .date(
                try valid {
                    try CivilDate(
                        year: decoder.readInt32(),
                        month: decoder.readUInt8(),
                        day: decoder.readUInt8()
                    )
                }
            )
        case 16:
            return .time(try readCivilTime(from: &decoder))
        case 17:
            let date = try valid {
                try CivilDate(
                    year: decoder.readInt32(),
                    month: decoder.readUInt8(),
                    day: decoder.readUInt8()
                )
            }
            return .dateTime(
                CivilDateTime(
                    date: date,
                    time: try readCivilTime(from: &decoder)
                )
            )
        case 18:
            return .timestamp(
                try valid {
                    try Timestamp(
                        secondsSinceUnixEpoch: decoder.readInt64(),
                        nanoseconds: decoder.readUInt32()
                    )
                }
            )
        case 19:
            return .timeSpan(
                try valid {
                    try TimeSpan(
                        seconds: decoder.readInt64(),
                        nanoseconds: decoder.readUInt32()
                    )
                }
            )
        case 20:
            return .calendarPeriod(
                CalendarPeriod(
                    months: try decoder.readInt64(),
                    days: try decoder.readInt64()
                )
            )
        case 21:
            return .geographicPoint(
                try valid {
                    try GeographicPoint(
                        latitude: decoder.readDouble(),
                        longitude: decoder.readDouble()
                    )
                }
            )
        case 22:
            return .geographicPosition(
                try valid {
                    try GeographicPosition(
                        latitude: decoder.readDouble(),
                        longitude: decoder.readDouble(),
                        ellipsoidalHeightInMeters: decoder.readDouble()
                    )
                }
            )
        case 23:
            return .vector(try readVector(from: &decoder))
        case 24:
            return .uuid(
                DatabaseTypes.UUID(
                    high: try decoder.readUInt64(),
                    low: try decoder.readUInt64()
                )
            )
        case 25:
            let count = try decoder.readCount()
            var values: [FieldValue] = []
            values.reserveCapacity(count)
            for _ in 0..<count {
                values.append(
                    try decoder.readLengthPrefixed {
                        (child: inout StorageFrameDecoder) throws(StorageFrameError) in
                        try read(from: &child)
                    }
                )
            }
            return .array(values)
        case 26:
            return .object(try readFieldObject(from: &decoder))
        case 27:
            let entity = try decoder.readString()
            let identifier = try decoder.readLengthPrefixed {
                (child: inout StorageFrameDecoder) throws(StorageFrameError) in
                try readReferenceIdentifier(from: &child)
            }
            let partitions = try readFieldObject(from: &decoder)
            return .reference(
                try valid {
                    try EntityReference(
                        entity: entity,
                        id: identifier,
                        partitions: partitions
                    )
                }
            )
        case 28:
            let bytes = try decoder.readBytes()
            let limits: RDFTermStorageLimits
            do {
                limits = try RDFTermStorageLimits(
                    maximumBytes: decoder.limits.maximumByteStringBytes,
                    maximumDepth: decoder.limits.maximumNestingDepth,
                    maximumObjectCount: decoder.limits.maximumCollectionCount
                )
            } catch {
                throw .invalidRDFTermLimits(error)
            }
            do {
                return .rdfTerm(try RDFTermStorageFormat.decode(bytes, limits: limits))
            } catch {
                throw .invalidRDFTerm(error)
            }
        case let tag:
            throw .invalidValueTag(tag)
        }
    }

    private static func readCivilTime(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> CivilTime {
        try valid {
            try CivilTime(
                hour: decoder.readUInt8(),
                minute: decoder.readUInt8(),
                second: decoder.readUInt8(),
                nanoseconds: decoder.readUInt32()
            )
        }
    }

    private static func readFieldObject(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> FieldObject {
        let count = try decoder.readCount()
        var fields: [(key: String, value: FieldValue)] = []
        fields.reserveCapacity(count)
        for _ in 0..<count {
            let key = try decoder.readString()
            let value = try decoder.readLengthPrefixed {
                (child: inout StorageFrameDecoder) throws(StorageFrameError) in
                try read(from: &child)
            }
            fields.append((key: key, value: value))
        }
        return try valid {
            try FieldObject(fields)
        }
    }

    private static func readReferenceIdentifier(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> ReferenceIdentifier {
        switch try decoder.readUInt8() {
        case 0:
            return .bool(try decoder.readBool())
        case 1:
            return .int8(try decoder.readInt8())
        case 2:
            return .int16(try decoder.readInt16())
        case 3:
            return .int32(try decoder.readInt32())
        case 4:
            return .int64(try decoder.readInt64())
        case 5:
            return .uint8(try decoder.readUInt8())
        case 6:
            return .uint16(try decoder.readUInt16())
        case 7:
            return .uint32(try decoder.readUInt32())
        case 8:
            return .uint64(try decoder.readUInt64())
        case 9:
            return .string(try decoder.readString())
        case 10:
            return .bytes(try decoder.readBytes())
        case 11:
            return .uuid(
                DatabaseTypes.UUID(
                    high: try decoder.readUInt64(),
                    low: try decoder.readUInt64()
                )
            )
        case 12:
            let count = try decoder.readCount()
            var components: [ReferenceIdentifier] = []
            components.reserveCapacity(count)
            for _ in 0..<count {
                components.append(
                    try decoder.readLengthPrefixed {
                        (child: inout StorageFrameDecoder) throws(StorageFrameError) in
                        try readReferenceIdentifier(from: &child)
                    }
                )
            }
            guard !components.isEmpty else {
                throw .invalidValue
            }
            return .composite(components)
        case let tag:
            throw .invalidReferenceIdentifierTag(tag)
        }
    }

    private static func readVector(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> Vector {
        let tag = try decoder.readUInt8()
        let count = try decoder.readCount()
        switch tag {
        case 0:
            var values: [Int8] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readInt8()) }
            return Vector(int8: values)
        case 1:
            var values: [Int16] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readInt16()) }
            return Vector(int16: values)
        case 2:
            var values: [Int32] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readInt32()) }
            return Vector(int32: values)
        case 3:
            var values: [Int64] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readInt64()) }
            return Vector(int64: values)
        case 4:
            var values: [UInt8] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readUInt8()) }
            return Vector(uint8: values)
        case 5:
            var values: [UInt16] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readUInt16()) }
            return Vector(uint16: values)
        case 6:
            var values: [UInt32] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readUInt32()) }
            return Vector(uint32: values)
        case 7:
            var values: [UInt64] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readUInt64()) }
            return Vector(uint64: values)
        case 8:
            var values: [Float] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readFloat()) }
            return try valid { try Vector(float32: values) }
        case 9:
            var values: [Double] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readDouble()) }
            return try valid { try Vector(float64: values) }
        default:
            throw .invalidValueTag(tag)
        }
    }

    private static func valid<Value>(
        _ body: () throws -> Value
    ) throws(StorageFrameError) -> Value {
        do {
            return try body()
        } catch {
            throw .invalidValue
        }
    }
}
