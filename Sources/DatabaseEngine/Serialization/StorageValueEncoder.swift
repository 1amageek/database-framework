import DatabaseKit
import DatabaseTypes

/// Writes primitive field values into engine-owned storage frames.
///
/// Byte strings and vector storage are borrowed synchronously. RDF terms stream
/// their canonical representation into the enclosing frame without allocating
/// an intermediate payload.
package enum StorageValueEncoder {
    package static func write(
        _ value: FieldValue,
        into encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        switch value {
        case .null:
            encoder.writeUInt8(0)
        case .bool(let value):
            encoder.writeUInt8(1)
            encoder.writeBool(value)
        case .int8(let value):
            encoder.writeUInt8(2)
            encoder.writeInt8(value)
        case .int16(let value):
            encoder.writeUInt8(3)
            encoder.writeInt16(value)
        case .int32(let value):
            encoder.writeUInt8(4)
            encoder.writeInt32(value)
        case .int64(let value):
            encoder.writeUInt8(5)
            encoder.writeInt64(value)
        case .uint8(let value):
            encoder.writeUInt8(6)
            encoder.writeUInt8(value)
        case .uint16(let value):
            encoder.writeUInt8(7)
            encoder.writeUInt16(value)
        case .uint32(let value):
            encoder.writeUInt8(8)
            encoder.writeUInt32(value)
        case .uint64(let value):
            encoder.writeUInt8(9)
            encoder.writeUInt64(value)
        case .float32(let value):
            encoder.writeUInt8(10)
            encoder.writeFloat(value)
        case .float64(let value):
            encoder.writeUInt8(11)
            encoder.writeDouble(value)
        case .decimal(let value):
            encoder.writeUInt8(12)
            encoder.writeInt128(value.coefficient)
            encoder.writeInt32(value.scale)
        case .string(let value):
            encoder.writeUInt8(13)
            try encoder.writeString(value)
        case .bytes(let value):
            encoder.writeUInt8(14)
            try encoder.writeBytes(value)
        case .date(let value):
            encoder.writeUInt8(15)
            encoder.writeInt32(value.year)
            encoder.writeUInt8(value.month)
            encoder.writeUInt8(value.day)
        case .time(let value):
            encoder.writeUInt8(16)
            write(value, into: &encoder)
        case .dateTime(let value):
            encoder.writeUInt8(17)
            encoder.writeInt32(value.date.year)
            encoder.writeUInt8(value.date.month)
            encoder.writeUInt8(value.date.day)
            write(value.time, into: &encoder)
        case .timestamp(let value):
            encoder.writeUInt8(18)
            encoder.writeInt64(value.secondsSinceUnixEpoch)
            encoder.writeUInt32(value.nanoseconds)
        case .timeSpan(let value):
            encoder.writeUInt8(19)
            encoder.writeInt64(value.seconds)
            encoder.writeUInt32(value.nanoseconds)
        case .calendarPeriod(let value):
            encoder.writeUInt8(20)
            encoder.writeInt64(value.months)
            encoder.writeInt64(value.days)
        case .geographicPoint(let value):
            encoder.writeUInt8(21)
            encoder.writeDouble(value.latitude)
            encoder.writeDouble(value.longitude)
        case .geographicPosition(let value):
            encoder.writeUInt8(22)
            encoder.writeDouble(value.point.latitude)
            encoder.writeDouble(value.point.longitude)
            encoder.writeDouble(value.ellipsoidalHeightInMeters)
        case .vector(let value):
            encoder.writeUInt8(23)
            try write(value, into: &encoder)
        case .uuid(let value):
            encoder.writeUInt8(24)
            encoder.writeUInt64(value.high)
            encoder.writeUInt64(value.low)
        case .array(let values):
            encoder.writeUInt8(25)
            try encoder.writeCount(values.count)
            for value in values {
                try encoder.writeLengthPrefixed {
                    (child: inout StorageFrameEncoder) throws(StorageFrameError) in
                    try write(value, into: &child)
                }
            }
        case .object(let object):
            encoder.writeUInt8(26)
            try write(object, into: &encoder)
        case .reference(let reference):
            encoder.writeUInt8(27)
            try encoder.writeString(reference.entity)
            try encoder.writeLengthPrefixed {
                (child: inout StorageFrameEncoder) throws(StorageFrameError) in
                try write(reference.id, into: &child)
            }
            try write(reference.partitions, into: &encoder)
        case .rdfTerm(let term):
            encoder.writeUInt8(28)
            try encoder.writeRDFTerm(term)
        }
    }

    private static func write(
        _ value: CivilTime,
        into encoder: inout StorageFrameEncoder
    ) {
        encoder.writeUInt8(value.hour)
        encoder.writeUInt8(value.minute)
        encoder.writeUInt8(value.second)
        encoder.writeUInt32(value.nanoseconds)
    }

    private static func write(
        _ object: FieldObject,
        into encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeCount(object.count)
        for field in object.fields {
            try encoder.writeString(field.key)
            try encoder.writeLengthPrefixed {
                (child: inout StorageFrameEncoder) throws(StorageFrameError) in
                try write(field.value, into: &child)
            }
        }
    }

    private static func write(
        _ identifier: ReferenceIdentifier,
        into encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        switch identifier {
        case .bool(let value):
            encoder.writeUInt8(0)
            encoder.writeBool(value)
        case .int8(let value):
            encoder.writeUInt8(1)
            encoder.writeInt8(value)
        case .int16(let value):
            encoder.writeUInt8(2)
            encoder.writeInt16(value)
        case .int32(let value):
            encoder.writeUInt8(3)
            encoder.writeInt32(value)
        case .int64(let value):
            encoder.writeUInt8(4)
            encoder.writeInt64(value)
        case .uint8(let value):
            encoder.writeUInt8(5)
            encoder.writeUInt8(value)
        case .uint16(let value):
            encoder.writeUInt8(6)
            encoder.writeUInt16(value)
        case .uint32(let value):
            encoder.writeUInt8(7)
            encoder.writeUInt32(value)
        case .uint64(let value):
            encoder.writeUInt8(8)
            encoder.writeUInt64(value)
        case .string(let value):
            encoder.writeUInt8(9)
            try encoder.writeString(value)
        case .bytes(let value):
            encoder.writeUInt8(10)
            try encoder.writeBytes(value)
        case .uuid(let value):
            encoder.writeUInt8(11)
            encoder.writeUInt64(value.high)
            encoder.writeUInt64(value.low)
        case .composite(let components):
            encoder.writeUInt8(12)
            try encoder.writeCount(components.count)
            for component in components {
                try encoder.writeLengthPrefixed {
                    (child: inout StorageFrameEncoder) throws(StorageFrameError) in
                    try write(component, into: &child)
                }
            }
        }
    }

    private static func write(
        _ vector: Vector,
        into encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        encoder.writeUInt8(vectorElementTag(vector.elementType))
        try encoder.writeCount(vector.count)
        switch vector.elementType {
        case .int8:
            _ = vector.withInt8Elements { elements in
                for value in elements { encoder.writeInt8(value) }
            }
        case .int16:
            _ = vector.withInt16Elements { elements in
                for value in elements { encoder.writeInt16(value) }
            }
        case .int32:
            _ = vector.withInt32Elements { elements in
                for value in elements { encoder.writeInt32(value) }
            }
        case .int64:
            _ = vector.withInt64Elements { elements in
                for value in elements { encoder.writeInt64(value) }
            }
        case .uint8:
            _ = vector.withUInt8Elements { elements in
                for value in elements { encoder.writeUInt8(value) }
            }
        case .uint16:
            _ = vector.withUInt16Elements { elements in
                for value in elements { encoder.writeUInt16(value) }
            }
        case .uint32:
            _ = vector.withUInt32Elements { elements in
                for value in elements { encoder.writeUInt32(value) }
            }
        case .uint64:
            _ = vector.withUInt64Elements { elements in
                for value in elements { encoder.writeUInt64(value) }
            }
        case .float32:
            _ = vector.withFloat32Elements { elements in
                for value in elements { encoder.writeFloat(value) }
            }
        case .float64:
            _ = vector.withFloat64Elements { elements in
                for value in elements { encoder.writeDouble(value) }
            }
        }
    }

    private static func vectorElementTag(_ type: VectorElementType) -> UInt8 {
        switch type {
        case .int8: 0
        case .int16: 1
        case .int32: 2
        case .int64: 3
        case .uint8: 4
        case .uint16: 5
        case .uint32: 6
        case .uint64: 7
        case .float32: 8
        case .float64: 9
        }
    }
}
