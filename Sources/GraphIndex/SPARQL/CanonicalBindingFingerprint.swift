import Core
import DatabaseDigest
import DatabaseEngine
import DatabaseValue

/// Produces the canonical row fingerprint directly from a variable binding.
///
/// This encoder intentionally matches `CanonicalRowFingerprint` for a
/// `QueryRow` whose fields are the binding values and whose annotations and
/// version are empty. It streams owned binding storage into SHA-256 without a
/// converted row or intermediate digest buffers. The sorted variable-name
/// array is the sole intermediate collection required to make dictionary
/// iteration canonical; the final digest is the sole payload allocation.
enum CanonicalBindingFingerprint {
    static func compute(
        _ fields: [String: FieldValue],
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseBytes {
        let orderedKeys = fields.keys.sorted()
        var fieldHasher = SHA256Accumulator()

        for key in orderedKeys {
            try workMeter.consume(
                UInt64(key.utf8.count),
                at: .resultMaterialization
            )
            appendString(key, to: &fieldHasher)
            guard let value = fields[key] else {
                throw PersistableVersionTokenCodecError.inconsistentFieldMap(key)
            }
            try appendValue(
                value,
                to: &fieldHasher,
                workMeter: workMeter
            )
        }

        var fingerprintHasher = SHA256Accumulator()
        var domain = UInt32(0x0152_4244).littleEndian
        withUnsafeBytes(of: &domain) {
            fingerprintHasher.update($0)
        }
        fieldHasher.withUnsafeDigestBytes {
            fingerprintHasher.update($0)
        }
        SHA256Accumulator().withUnsafeDigestBytes {
            fingerprintHasher.update($0)
        }
        return fingerprintHasher.finalize()
    }

    private static func appendValue(
        _ value: FieldValue,
        to hasher: inout SHA256Accumulator,
        workMeter: DatabaseWorkMeter
    ) throws {
        try workMeter.consume(at: .resultMaterialization)
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
        case .string(let value):
            appendByte(0x06, to: &hasher)
            try workMeter.consume(
                UInt64(value.utf8.count),
                at: .resultMaterialization
            )
            appendString(value, to: &hasher)
        case .data(let value):
            appendByte(0x07, to: &hasher)
            try workMeter.consume(
                UInt64(value.count),
                at: .resultMaterialization
            )
            appendLength(value.count, to: &hasher)
            value.withUnsafeBytes {
                hasher.update($0)
            }
        case .array(let values):
            appendByte(0x0A, to: &hasher)
            appendLength(values.count, to: &hasher)
            for element in values {
                try appendValue(
                    element,
                    to: &hasher,
                    workMeter: workMeter
                )
            }
        case .rdfTerm(let term):
            appendByte(0x0D, to: &hasher)
            let plan = try DatabaseRDFTermCodec.encodingPlan(term)
            try workMeter.consume(
                UInt64(plan.byteCount),
                at: .resultMaterialization
            )
            appendLength(plan.byteCount, to: &hasher)
            var sink = SHA256RDFTermSink(hasher: hasher)
            try DatabaseRDFTermCodec.encode(plan, into: &sink)
            hasher = sink.hasher
        }
    }

    private static func appendString(
        _ value: String,
        to hasher: inout SHA256Accumulator
    ) {
        appendLength(value.utf8.count, to: &hasher)
        let usedContiguousStorage = value.utf8.withContiguousStorageIfAvailable {
            bytes in
            hasher.update(UnsafeRawBufferPointer(bytes))
            return true
        } ?? false
        guard !usedContiguousStorage else { return }
        for byte in value.utf8 {
            appendByte(byte, to: &hasher)
        }
    }

    private static func appendLength(
        _ value: Int,
        to hasher: inout SHA256Accumulator
    ) {
        appendInteger(UInt64(value), to: &hasher)
    }

    private static func appendByte(
        _ value: UInt8,
        to hasher: inout SHA256Accumulator
    ) {
        var value = value
        withUnsafeBytes(of: &value) {
            hasher.update($0)
        }
    }

    private static func appendInteger<Integer: FixedWidthInteger>(
        _ value: Integer,
        to hasher: inout SHA256Accumulator
    ) {
        var bigEndian = value.bigEndian
        withUnsafeBytes(of: &bigEndian) {
            hasher.update($0)
        }
    }
}

extension VariableBinding {
    /// Computes the canonical fingerprint without materializing a converted row.
    func canonicalFingerprint(
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseBytes {
        try withBindings { fields in
            try CanonicalBindingFingerprint.compute(
                fields,
                workMeter: workMeter
            )
        }
    }
}

private struct SHA256RDFTermSink: DatabaseRDFTermEncodingSink {
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
