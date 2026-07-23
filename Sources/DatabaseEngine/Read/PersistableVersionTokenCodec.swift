import Core
import DatabaseDigest
import DatabaseValue
import DatabaseWire
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

public enum PersistableVersionTokenCodec {
    public static func token(
        for fields: [String: DatabaseValue]
    ) throws -> PersistableVersionToken {
        let digest = try digest(fields: fields)
        return PersistableVersionToken(Data(digest).base64EncodedString())
    }

    public static func digest(
        fields: [String: DatabaseValue]
    ) throws -> DatabaseBytes {
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
        fields: [DatabaseObjectField]
    ) throws -> DatabaseBytes {
        var valuesByName: [String: DatabaseValue] = [:]
        valuesByName.reserveCapacity(fields.count)
        for field in fields {
            guard valuesByName.updateValue(field.value, forKey: field.name) == nil else {
                throw PersistableVersionTokenCodecError.duplicateField(field.name)
            }
        }
        return try digest(fields: valuesByName)
    }

    public static func digest(from token: PersistableVersionToken) throws -> DatabaseBytes {
        guard let data = Data(base64Encoded: token.value) else {
            throw PersistableVersionTokenCodecError.invalidToken
        }
        return DatabaseBytes(Array(data))
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
    _ value: DatabaseValue,
    to hasher: inout SHA256Accumulator
) throws {
    switch value {
    case .null:
        appendByte(0x00, to: &hasher)
    case .bool(let value):
        appendByte(0x01, to: &hasher)
        appendByte(value ? 0x01 : 0x00, to: &hasher)
    case .int64(let value):
        appendByte(0x02, to: &hasher)
        appendInteger(value, to: &hasher)
    case .uint64(let value):
        appendByte(0x03, to: &hasher)
        appendInteger(value, to: &hasher)
    case .double(let value):
        appendByte(0x04, to: &hasher)
        appendInteger(value.bitPattern, to: &hasher)
    case .decimal(let coefficient, let scale):
        appendByte(0x05, to: &hasher)
        appendInteger(coefficient, to: &hasher)
        appendInteger(scale, to: &hasher)
    case .string(let value):
        appendByte(0x06, to: &hasher)
        appendString(value, to: &hasher)
    case .bytes(let value):
        appendByte(0x07, to: &hasher)
        appendLength(value.count, to: &hasher)
        value.withUnsafeBytes { hasher.update($0) }
    case .date(let value):
        appendByte(0x08, to: &hasher)
        appendInteger(value.year, to: &hasher)
        appendByte(value.month, to: &hasher)
        appendByte(value.day, to: &hasher)
    case .timestamp(let value):
        appendByte(0x09, to: &hasher)
        appendInteger(value.secondsSinceUnixEpoch, to: &hasher)
        appendInteger(value.nanoseconds, to: &hasher)
    case .array(let values):
        appendByte(0x0A, to: &hasher)
        appendLength(values.count, to: &hasher)
        for value in values {
            try appendValue(value, to: &hasher)
        }
    case .object(let fields):
        appendByte(0x0B, to: &hasher)
        appendLength(fields.count, to: &hasher)
        for field in fields {
            appendInteger(field.number, to: &hasher)
            appendString(field.name, to: &hasher)
            try appendValue(field.value, to: &hasher)
        }
    case .reference(let identity):
        appendByte(0x0C, to: &hasher)
        appendString(identity.entity, to: &hasher)
        appendPersistableIdentifier(identity.id, to: &hasher)
        appendLength(identity.partitions.count, to: &hasher)
        for field in identity.partitions {
            appendInteger(field.number, to: &hasher)
            appendString(field.name, to: &hasher)
            try appendValue(field.value, to: &hasher)
        }
    case .rdfTerm(let term):
        appendByte(0x0D, to: &hasher)
        let plan = try DatabaseRDFTermCodec.encodingPlan(term)
        appendLength(plan.byteCount, to: &hasher)
        var sink = SHA256RDFTermDigestSink(hasher: hasher)
        try DatabaseRDFTermCodec.encode(plan, into: &sink)
        hasher = sink.hasher
    case .uuid(let value):
        appendByte(0x0E, to: &hasher)
        appendInteger(value.high, to: &hasher)
        appendInteger(value.low, to: &hasher)
    }
}

private func appendPersistableIdentifier(
    _ value: PersistableIdentifierValue,
    to hasher: inout SHA256Accumulator
) {
    switch value {
    case .bool(let value):
        appendByte(0x00, to: &hasher)
        appendByte(value ? 0x01 : 0x00, to: &hasher)
    case .int64(let value):
        appendByte(0x01, to: &hasher)
        appendInteger(value, to: &hasher)
    case .uint64(let value):
        appendByte(0x02, to: &hasher)
        appendInteger(value, to: &hasher)
    case .string(let value):
        appendByte(0x03, to: &hasher)
        appendString(value, to: &hasher)
    case .bytes(let value):
        appendByte(0x04, to: &hasher)
        appendLength(value.count, to: &hasher)
        value.withUnsafeBytes { hasher.update($0) }
    case .uuid(let value):
        appendByte(0x05, to: &hasher)
        appendInteger(value.high, to: &hasher)
        appendInteger(value.low, to: &hasher)
    case .composite(let components):
        appendByte(0x06, to: &hasher)
        appendLength(components.count, to: &hasher)
        for component in components {
            appendPersistableIdentifier(component, to: &hasher)
        }
    }
}

private struct SHA256RDFTermDigestSink: DatabaseRDFTermEncodingSink {
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
