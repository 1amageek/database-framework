import DatabaseKit
import DatabaseWire
import DatabaseTypes
import DatabaseWire

public enum CanonicalRowFingerprint {
    public static func compute(
        _ row: QueryRow,
        workMeter: DatabaseWorkMeter
    ) throws -> ByteString {
        try consume(row.fields, workMeter: workMeter)
        try consume(row.annotations, workMeter: workMeter)
        if let version = row.version {
            try workMeter.consume(
                UInt64(version.value.utf8.count),
                at: .resultMaterialization
            )
        }

        var hasher = SHA256Accumulator()
        var domain = UInt32(0x0152_4244).littleEndian
        withUnsafeBytes(of: &domain) { hasher.update($0) }
        hasher.update(try PersistableVersionTokenCodec.digest(fields: row.fields))
        hasher.update(try PersistableVersionTokenCodec.digest(fields: row.annotations))
        if let version = row.version {
            updateUTF8(version.value, hasher: &hasher)
        }
        return hasher.finalize()
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
