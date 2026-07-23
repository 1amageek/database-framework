import Core
import DatabaseDigest
import DatabaseValue
import DatabaseWire

public enum CanonicalRowFingerprint {
    public static func compute(
        _ row: QueryRow,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseBytes {
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
        hasher.update(try RecordVersionTokenCodec.digest(fields: row.fields))
        hasher.update(try RecordVersionTokenCodec.digest(fields: row.annotations))
        if let version = row.version {
            updateUTF8(version.value, hasher: &hasher)
        }
        return hasher.finalize()
    }

    private static func consume(
        _ fields: [String: DatabaseValue],
        workMeter: DatabaseWorkMeter
    ) throws {
        for key in fields.keys.sorted() {
            try workMeter.consume(
                UInt64(key.utf8.count),
                at: .resultMaterialization
            )
            guard let value = fields[key] else {
                throw RecordVersionTokenCodecError.inconsistentFieldMap(key)
            }
            try consume(value, workMeter: workMeter)
        }
    }

    private static func consume(
        _ value: DatabaseValue,
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
                UInt64(try DatabaseRDFTermCodec.encodedByteCount(value)),
                at: .resultMaterialization
            )
        case .array(let values):
            for value in values {
                try consume(value, workMeter: workMeter)
            }
        case .object(let fields):
            for field in fields {
                try workMeter.consume(
                    UInt64(field.name.utf8.count),
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
            for field in identity.partitions {
                try workMeter.consume(
                    UInt64(field.name.utf8.count),
                    at: .resultMaterialization
                )
                try consume(field.value, workMeter: workMeter)
            }
        case .int64, .uint64, .double, .decimal, .bool, .date,
             .timestamp, .uuid, .null:
            break
        }
    }

    private static func consume(
        _ identifier: RecordIdentifierValue,
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
        case .bool, .int64, .uint64, .uuid:
            break
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
