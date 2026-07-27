import DatabaseKit
import DatabaseWire
import DatabaseTypes
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

public enum PersistableVersionTokenCodec {
    public static func token(
        for fields: [String: FieldValue]
    ) throws -> PersistableVersionToken {
        let digest = try digest(fields: fields)
        return PersistableVersionToken(Data(digest).base64EncodedString())
    }

    public static func digest(
        fields: [String: FieldValue]
    ) throws -> ByteString {
        var hasher = SHA256Accumulator()
        for key in fields.keys.sorted() {
            appendString(key, to: &hasher)
            guard let value = fields[key] else {
                throw PersistableVersionTokenCodecError.inconsistentFieldMap(key)
            }
            try appendValue(value, to: &hasher)
        }
        return hasher.finalize()
    }

    public static func digest(
        fields: [PersistableField]
    ) throws -> ByteString {
        var valuesByName: [String: FieldValue] = [:]
        valuesByName.reserveCapacity(fields.count)
        for field in fields {
            guard valuesByName.updateValue(field.value, forKey: field.name) == nil else {
                throw PersistableVersionTokenCodecError.duplicateField(field.name)
            }
        }
        return try digest(fields: valuesByName)
    }

    public static func digest(from token: PersistableVersionToken) throws -> ByteString {
        guard let data = Data(base64Encoded: token.value) else {
            throw PersistableVersionTokenCodecError.invalidToken
        }
        return ByteString(Array(data))
    }
}

public enum PersistableVersionTokenCodecError: Error, Sendable {
    case invalidToken
    case inconsistentFieldMap(String)
    case duplicateField(String)
}

private func appendString(
    _ value: String,
    to hasher: inout SHA256Accumulator
) {
    appendLength(value.utf8.count, to: &hasher)
    let updated = value.utf8.withContiguousStorageIfAvailable { bytes in
        hasher.update(UnsafeRawBufferPointer(bytes))
        return true
    } ?? false
    if !updated {
        for byte in value.utf8 {
            appendByte(byte, to: &hasher)
        }
    }
}

private func appendLength(
    _ value: Int,
    to hasher: inout SHA256Accumulator
) {
    var bigEndian = UInt64(value).bigEndian
    withUnsafeBytes(of: &bigEndian) { hasher.update($0) }
}

private func appendByte(
    _ value: UInt8,
    to hasher: inout SHA256Accumulator
) {
    var value = value
    withUnsafeBytes(of: &value) { hasher.update($0) }
}

private func appendInteger<T: FixedWidthInteger>(
    _ value: T,
    to hasher: inout SHA256Accumulator
) {
    var bigEndian = value.bigEndian
    withUnsafeBytes(of: &bigEndian) { hasher.update($0) }
}

private func appendValue(
    _ value: FieldValue,
    to hasher: inout SHA256Accumulator
) throws {
    switch value {
    case .null:
        appendByte(0x00, to: &hasher)
    case .bool(let value):
        appendByte(0x01, to: &hasher)
        appendByte(value ? 0x01 : 0x00, to: &hasher)
    case .int8(let value):
        appendByte(0x02, to: &hasher)
        appendInteger(value, to: &hasher)
    case .int16(let value):
        appendByte(0x03, to: &hasher)
        appendInteger(value, to: &hasher)
    case .int32(let value):
        appendByte(0x04, to: &hasher)
        appendInteger(value, to: &hasher)
    case .int64(let value):
        appendByte(0x05, to: &hasher)
        appendInteger(value, to: &hasher)
    case .uint8(let value):
        appendByte(0x06, to: &hasher)
        appendInteger(value, to: &hasher)
    case .uint16(let value):
        appendByte(0x07, to: &hasher)
        appendInteger(value, to: &hasher)
    case .uint32(let value):
        appendByte(0x08, to: &hasher)
        appendInteger(value, to: &hasher)
    case .uint64(let value):
        appendByte(0x09, to: &hasher)
        appendInteger(value, to: &hasher)
    case .float32(let value):
        appendByte(0x0A, to: &hasher)
        appendInteger(value.bitPattern, to: &hasher)
    case .float64(let value):
        appendByte(0x0B, to: &hasher)
        appendInteger(value.bitPattern, to: &hasher)
    case .decimal(let value):
        appendByte(0x0C, to: &hasher)
        appendInteger(value.coefficient, to: &hasher)
        appendInteger(value.scale, to: &hasher)
    case .string(let value):
        appendByte(0x0D, to: &hasher)
        appendString(value, to: &hasher)
    case .bytes(let value):
        appendByte(0x0E, to: &hasher)
        appendLength(value.count, to: &hasher)
        value.withUnsafeBytes { hasher.update($0) }
    case .date(let value):
        appendByte(0x0F, to: &hasher)
        appendInteger(value.year, to: &hasher)
        appendByte(value.month, to: &hasher)
        appendByte(value.day, to: &hasher)
    case .time(let value):
        appendByte(0x10, to: &hasher)
        appendCivilTime(value, to: &hasher)
    case .dateTime(let value):
        appendByte(0x11, to: &hasher)
        appendInteger(value.date.year, to: &hasher)
        appendByte(value.date.month, to: &hasher)
        appendByte(value.date.day, to: &hasher)
        appendCivilTime(value.time, to: &hasher)
    case .timestamp(let value):
        appendByte(0x12, to: &hasher)
        appendInteger(value.secondsSinceUnixEpoch, to: &hasher)
        appendInteger(value.nanoseconds, to: &hasher)
    case .timeSpan(let value):
        appendByte(0x13, to: &hasher)
        appendInteger(value.seconds, to: &hasher)
        appendInteger(value.nanoseconds, to: &hasher)
    case .calendarPeriod(let value):
        appendByte(0x14, to: &hasher)
        appendInteger(value.months, to: &hasher)
        appendInteger(value.days, to: &hasher)
    case .geographicPoint(let value):
        appendByte(0x15, to: &hasher)
        appendInteger(value.latitude.bitPattern, to: &hasher)
        appendInteger(value.longitude.bitPattern, to: &hasher)
    case .geographicPosition(let value):
        appendByte(0x16, to: &hasher)
        appendInteger(value.point.latitude.bitPattern, to: &hasher)
        appendInteger(value.point.longitude.bitPattern, to: &hasher)
        appendInteger(
            value.ellipsoidalHeightInMeters.bitPattern,
            to: &hasher
        )
    case .vector(let value):
        appendByte(0x17, to: &hasher)
        appendVector(value, to: &hasher)
    case .uuid(let value):
        appendByte(0x18, to: &hasher)
        appendInteger(value.high, to: &hasher)
        appendInteger(value.low, to: &hasher)
    case .array(let values):
        appendByte(0x19, to: &hasher)
        appendLength(values.count, to: &hasher)
        for value in values {
            try appendValue(value, to: &hasher)
        }
    case .object(let object):
        appendByte(0x1A, to: &hasher)
        appendLength(object.count, to: &hasher)
        for field in object.fields {
            appendString(field.key, to: &hasher)
            try appendValue(field.value, to: &hasher)
        }
    case .reference(let identity):
        appendByte(0x1B, to: &hasher)
        appendString(identity.entity, to: &hasher)
        appendPersistableIdentifier(identity.id, to: &hasher)
        appendLength(identity.partitions.count, to: &hasher)
        for field in identity.partitions.fields {
            appendString(field.key, to: &hasher)
            try appendValue(field.value, to: &hasher)
        }
    case .rdfTerm(let term):
        appendByte(0x1C, to: &hasher)
        let plan = try RDFTermStorageFormat.encodingPlan(term)
        appendLength(plan.byteCount, to: &hasher)
        var sink = SHA256RDFTermDigestSink(hasher: hasher)
        try RDFTermStorageFormat.encode(plan, into: &sink)
        hasher = sink.hasher
    }
}

private func appendPersistableIdentifier(
    _ value: ReferenceIdentifier,
    to hasher: inout SHA256Accumulator
) {
    switch value {
    case .bool(let value):
        appendByte(0x00, to: &hasher)
        appendByte(value ? 0x01 : 0x00, to: &hasher)
    case .int8(let value):
        appendByte(0x01, to: &hasher)
        appendInteger(value, to: &hasher)
    case .int16(let value):
        appendByte(0x02, to: &hasher)
        appendInteger(value, to: &hasher)
    case .int32(let value):
        appendByte(0x03, to: &hasher)
        appendInteger(value, to: &hasher)
    case .int64(let value):
        appendByte(0x04, to: &hasher)
        appendInteger(value, to: &hasher)
    case .uint8(let value):
        appendByte(0x05, to: &hasher)
        appendInteger(value, to: &hasher)
    case .uint16(let value):
        appendByte(0x06, to: &hasher)
        appendInteger(value, to: &hasher)
    case .uint32(let value):
        appendByte(0x07, to: &hasher)
        appendInteger(value, to: &hasher)
    case .uint64(let value):
        appendByte(0x08, to: &hasher)
        appendInteger(value, to: &hasher)
    case .string(let value):
        appendByte(0x09, to: &hasher)
        appendString(value, to: &hasher)
    case .bytes(let value):
        appendByte(0x0A, to: &hasher)
        appendLength(value.count, to: &hasher)
        value.withUnsafeBytes { hasher.update($0) }
    case .uuid(let value):
        appendByte(0x0B, to: &hasher)
        appendInteger(value.high, to: &hasher)
        appendInteger(value.low, to: &hasher)
    case .composite(let components):
        appendByte(0x0C, to: &hasher)
        appendLength(components.count, to: &hasher)
        for component in components {
            appendPersistableIdentifier(component, to: &hasher)
        }
    }
}

private func appendCivilTime(
    _ value: CivilTime,
    to hasher: inout SHA256Accumulator
) {
    appendByte(value.hour, to: &hasher)
    appendByte(value.minute, to: &hasher)
    appendByte(value.second, to: &hasher)
    appendInteger(value.nanoseconds, to: &hasher)
}

private func appendVector(
    _ vector: Vector,
    to hasher: inout SHA256Accumulator
) {
    appendLength(vector.count, to: &hasher)
    switch vector.elementType {
    case .int8:
        appendByte(0, to: &hasher)
        _ = vector.withInt8Elements {
            for value in $0 { appendInteger(value, to: &hasher) }
        }
    case .int16:
        appendByte(1, to: &hasher)
        _ = vector.withInt16Elements {
            for value in $0 { appendInteger(value, to: &hasher) }
        }
    case .int32:
        appendByte(2, to: &hasher)
        _ = vector.withInt32Elements {
            for value in $0 { appendInteger(value, to: &hasher) }
        }
    case .int64:
        appendByte(3, to: &hasher)
        _ = vector.withInt64Elements {
            for value in $0 { appendInteger(value, to: &hasher) }
        }
    case .uint8:
        appendByte(4, to: &hasher)
        _ = vector.withUInt8Elements {
            for value in $0 { appendInteger(value, to: &hasher) }
        }
    case .uint16:
        appendByte(5, to: &hasher)
        _ = vector.withUInt16Elements {
            for value in $0 { appendInteger(value, to: &hasher) }
        }
    case .uint32:
        appendByte(6, to: &hasher)
        _ = vector.withUInt32Elements {
            for value in $0 { appendInteger(value, to: &hasher) }
        }
    case .uint64:
        appendByte(7, to: &hasher)
        _ = vector.withUInt64Elements {
            for value in $0 { appendInteger(value, to: &hasher) }
        }
    case .float32:
        appendByte(8, to: &hasher)
        _ = vector.withFloat32Elements {
            for value in $0 {
                appendInteger(value.bitPattern, to: &hasher)
            }
        }
    case .float64:
        appendByte(9, to: &hasher)
        _ = vector.withFloat64Elements {
            for value in $0 {
                appendInteger(value.bitPattern, to: &hasher)
            }
        }
    }
}

private struct SHA256RDFTermDigestSink: RDFTermStorageSink {
    var hasher: SHA256Accumulator

    mutating func write(_ byte: UInt8) {
        var byte = byte
        withUnsafeBytes(of: &byte) {
            hasher.update($0)
        }
    }

    mutating func write(_ bytes: UnsafeRawBufferPointer) {
        hasher.update(bytes)
    }
}
