#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseValue
import StorageKit

/// Canonical physical encoding for `FieldValue` instances stored in tuple keys.
///
/// Native scalar values keep their FoundationDB tuple representation. Composite
/// values use one versioned byte element whose payload never contains `0x00`, so
/// `TupleCursor` can retain it as a slice of the storage key. Arbitrary binary
/// data is nibble encoded because a decoded `DatabaseBytes` value must expose the
/// original contiguous bytes; that semantic boundary requires one final allocation.
public enum FieldValueTupleCodec {
    package indirect enum Plan: Sendable {
        case null
        case bool(Bool)
        case int64(Int64)
        case uint64(UInt64)
        case double(Double)
        case string(String)
        case data(DatabaseBytes)
        case rdf(DatabaseRDFTermEncodingPlan)
        case array([Plan])
    }

    package struct Prepared: Sendable {
        package let value: FieldValue
        package let plan: Plan
        package let encodedByteCount: Int
    }

    private static let magic0: UInt8 = 0x44
    private static let magic1: UInt8 = 0x56
    private static let magic2: UInt8 = 0x42
    private static let version: UInt8 = 0x01

    private static let end: UInt8 = 0x01
    private static let firstNibble: UInt8 = 0x02
    private static let lastNibble: UInt8 = 0x11

    private static let nullTag: UInt8 = 0x20
    private static let falseTag: UInt8 = 0x21
    private static let trueTag: UInt8 = 0x22
    private static let int64Tag: UInt8 = 0x23
    private static let doubleTag: UInt8 = 0x24
    private static let stringTag: UInt8 = 0x25
    private static let rdfTag: UInt8 = 0x26
    private static let dataTag: UInt8 = 0x27
    private static let arrayTag: UInt8 = 0x28
    private static let uint64Tag: UInt8 = 0x29

    public static func tupleElement(
        for value: FieldValue,
        limits: FieldValueTupleCodecLimits = .default
    ) throws(FieldValueTupleCodecError) -> any TupleElement {
        switch value {
        case .bool(let value):
            return value
        case .int64(let value):
            return value
        case .uint64(let value):
            return value
        case .double(let value):
            return value
        case .string(let value):
            return value
        case .null, .data, .rdfTerm, .array:
            return try CanonicalFieldValueTupleElement(
                prepared: prepareComposite(value, limits: limits)
            )
        }
    }

    public static func decode(
        _ element: any TupleElement,
        limits: FieldValueTupleCodecLimits = .default
    ) throws(FieldValueTupleCodecError) -> FieldValue {
        switch element {
        case let value as CanonicalFieldValueTupleElement:
            return value.prepared.value
        case let value as Bool:
            return .bool(value)
        case let value as Int64:
            return .int64(value)
        case let value as UInt64:
            return .uint64(value)
        case let value as Double:
            return .double(value)
        case let value as String:
            return .string(value)
        case let value as Bytes:
            var decoder = Decoder(bytes: value, limits: limits)
            return try decoder.decodeRoot()
        default:
            throw .unsupportedElementType(
                String(describing: type(of: element))
            )
        }
    }

    package static func prepareComposite(
        _ value: FieldValue,
        limits: FieldValueTupleCodecLimits
    ) throws(FieldValueTupleCodecError) -> Prepared {
        switch value {
        case .null, .data, .rdfTerm, .array:
            break
        case .bool, .int64, .uint64, .double, .string:
            preconditionFailure("Native scalar values do not use a composite plan")
        }

        var planner = Planner(limits: limits)
        let plan = try planner.prepare(value, depth: 0)
        let payloadCount = try encodedPayloadByteCount(plan, isRoot: true)
        let encodedCount = try checkedAdd(payloadCount, 2)
        guard encodedCount <= limits.maximumEncodedBytes else {
            throw .maximumEncodedBytesExceeded(
                actual: encodedCount,
                maximum: limits.maximumEncodedBytes
            )
        }
        return Prepared(
            value: value,
            plan: plan,
            encodedByteCount: encodedCount
        )
    }

    package static func write(
        _ prepared: Prepared,
        to sink: inout TupleEncodingSink
    ) {
        sink.writeByte(TupleTypeCode.bytes.rawValue)
        sink.writeByte(magic0)
        sink.writeByte(magic1)
        sink.writeByte(magic2)
        sink.writeByte(version)
        write(prepared.plan, isRoot: true, to: &sink)
        sink.writeByte(0x00)
    }

    private static func write(
        _ plan: Plan,
        isRoot: Bool,
        to sink: inout TupleEncodingSink
    ) {
        switch plan {
        case .null:
            sink.writeByte(nullTag)
        case .bool(let value):
            sink.writeByte(value ? trueTag : falseTag)
        case .int64(let value):
            sink.writeByte(int64Tag)
            writeFixedNibbles(
                UInt64(bitPattern: value) ^ 0x8000_0000_0000_0000,
                to: &sink
            )
        case .uint64(let value):
            sink.writeByte(uint64Tag)
            writeFixedNibbles(value, to: &sink)
        case .double(let value):
            sink.writeByte(doubleTag)
            let bits = value.bitPattern
            let ordered = (bits & 0x8000_0000_0000_0000) == 0
                ? bits ^ 0x8000_0000_0000_0000
                : ~bits
            writeFixedNibbles(ordered, to: &sink)
        case .string(let value):
            sink.writeByte(stringTag)
            writeNibbles(value.utf8, to: &sink)
            sink.writeByte(end)
        case .data(let value):
            sink.writeByte(dataTag)
            value.withUnsafeBytes { bytes in
                writeNibbles(bytes, to: &sink)
            }
            sink.writeByte(end)
        case .rdf(let plan):
            sink.writeByte(rdfTag)
            if !isRoot {
                writeZeroFreeVarint(plan.byteCount, to: &sink)
            }
            writeRDF(plan, to: &sink)
        case .array(let values):
            sink.writeByte(arrayTag)
            for value in values {
                write(value, isRoot: false, to: &sink)
            }
            sink.writeByte(end)
        }
    }

    private static func writeRDF(
        _ plan: DatabaseRDFTermEncodingPlan,
        to sink: inout TupleEncodingSink
    ) {
        do {
            try withUnsafeMutablePointer(to: &sink) { sinkPointer in
                var rdfSink = RDFSink(tupleSink: sinkPointer)
                try DatabaseRDFTermCodec.encode(plan, into: &rdfSink)
            }
        } catch let error as DatabaseRDFTermCodecError {
            preconditionFailure(
                "A validated RDF encoding plan failed during tuple emission: \(error)"
            )
        } catch {
            preconditionFailure("Unexpected RDF tuple encoding failure")
        }
    }

    private static func writeNibbles<Bytes: Sequence>(
        _ bytes: Bytes,
        to sink: inout TupleEncodingSink
    ) where Bytes.Element == UInt8 {
        for byte in bytes {
            sink.writeByte(firstNibble + (byte >> 4))
            sink.writeByte(firstNibble + (byte & 0x0F))
        }
    }

    private static func writeFixedNibbles(
        _ value: UInt64,
        to sink: inout TupleEncodingSink
    ) {
        for shift in stride(from: 60, through: 0, by: -4) {
            sink.writeByte(
                firstNibble + UInt8(
                    truncatingIfNeeded: (value >> UInt64(shift)) & 0x0F
                )
            )
        }
    }

    private static func writeZeroFreeVarint(
        _ value: Int,
        to sink: inout TupleEncodingSink
    ) {
        precondition(value >= 0)
        var remaining = value
        while remaining >= 127 {
            let digit = remaining % 127
            sink.writeByte(0x80 | UInt8(digit + 1))
            remaining /= 127
        }
        sink.writeByte(UInt8(remaining + 1))
    }

    private static func encodedPayloadByteCount(
        _ plan: Plan,
        isRoot: Bool
    ) throws(FieldValueTupleCodecError) -> Int {
        let bodyCount: Int
        switch plan {
        case .null, .bool:
            bodyCount = 1
        case .int64, .uint64, .double:
            bodyCount = 17
        case .string(let value):
            bodyCount = try checkedAdd(
                2,
                try checkedMultiply(value.utf8.count, by: 2)
            )
        case .data(let value):
            bodyCount = try checkedAdd(
                2,
                try checkedMultiply(value.count, by: 2)
            )
        case .rdf(let rdfPlan):
            let lengthCount = isRoot ? 0 : zeroFreeVarintByteCount(rdfPlan.byteCount)
            bodyCount = try checkedAdd(
                1,
                try checkedAdd(lengthCount, rdfPlan.byteCount)
            )
        case .array(let values):
            var count = 2
            for value in values {
                count = try checkedAdd(
                    count,
                    try encodedPayloadByteCount(value, isRoot: false)
                )
            }
            bodyCount = count
        }
        return isRoot ? try checkedAdd(4, bodyCount) : bodyCount
    }

    private static func zeroFreeVarintByteCount(_ value: Int) -> Int {
        precondition(value >= 0)
        var remaining = value
        var count = 1
        while remaining >= 127 {
            remaining /= 127
            count += 1
        }
        return count
    }

    private static func checkedAdd(
        _ lhs: Int,
        _ rhs: Int
    ) throws(FieldValueTupleCodecError) -> Int {
        let (value, overflow) = lhs.addingReportingOverflow(rhs)
        guard !overflow else {
            throw .integerOverflow
        }
        return value
    }

    private static func checkedMultiply(
        _ value: Int,
        by multiplier: Int
    ) throws(FieldValueTupleCodecError) -> Int {
        let (result, overflow) = value.multipliedReportingOverflow(
            by: multiplier
        )
        guard !overflow else {
            throw .integerOverflow
        }
        return result
    }

    private struct Planner {
        let limits: FieldValueTupleCodecLimits
        var objectCount = 0

        mutating func prepare(
            _ value: FieldValue,
            depth: Int
        ) throws(FieldValueTupleCodecError) -> Plan {
            guard depth <= limits.maximumDepth else {
                throw .maximumDepthExceeded(
                    actual: depth,
                    maximum: limits.maximumDepth
                )
            }
            try registerObject()

            switch value {
            case .null:
                return .null
            case .bool(let value):
                return .bool(value)
            case .int64(let value):
                return .int64(value)
            case .uint64(let value):
                return .uint64(value)
            case .double(let value):
                return .double(value)
            case .string(let value):
                return .string(value)
            case .data(let value):
                return .data(value)
            case .rdfTerm(let value):
                return .rdf(try prepareRDF(value, depth: depth))
            case .array(let values):
                guard values.count <= limits.maximumCollectionCount else {
                    throw .maximumCollectionCountExceeded(
                        actual: values.count,
                        maximum: limits.maximumCollectionCount
                    )
                }
                var plans: [Plan] = []
                plans.reserveCapacity(values.count)
                for value in values {
                    plans.append(try prepare(value, depth: depth + 1))
                }
                return .array(plans)
            }
        }

        private mutating func prepareRDF(
            _ term: DatabaseRDFTerm,
            depth: Int
        ) throws(FieldValueTupleCodecError) -> DatabaseRDFTermEncodingPlan {
            let remainingObjects = limits.maximumObjectCount - objectCount
            guard remainingObjects > 0 else {
                throw .maximumObjectCountExceeded(
                    actual: objectCount + 1,
                    maximum: limits.maximumObjectCount
                )
            }
            let rdfLimits = DatabaseRDFTermCodecLimits(
                maximumBytes: limits.maximumEncodedBytes,
                maximumDepth: limits.maximumDepth - depth,
                maximumObjectCount: remainingObjects
            )
            do {
                let plan = try DatabaseRDFTermCodec.encodingPlan(
                    term,
                    limits: rdfLimits
                )
                objectCount += plan.objectCount
                return plan
            } catch let error {
                throw mapRDFError(error, depth: depth)
            }
        }

        private mutating func registerObject() throws(FieldValueTupleCodecError) {
            let (next, overflow) = objectCount.addingReportingOverflow(1)
            guard !overflow else {
                throw .integerOverflow
            }
            guard next <= limits.maximumObjectCount else {
                throw .maximumObjectCountExceeded(
                    actual: next,
                    maximum: limits.maximumObjectCount
                )
            }
            objectCount = next
        }

        private func mapRDFError(
            _ error: DatabaseRDFTermCodecError,
            depth: Int
        ) -> FieldValueTupleCodecError {
            switch error {
            case .maximumBytesExceeded(let actual, _):
                return .maximumEncodedBytesExceeded(
                    actual: actual,
                    maximum: limits.maximumEncodedBytes
                )
            case .maximumDepthExceeded(let actual, _):
                return .maximumDepthExceeded(
                    actual: depth + actual,
                    maximum: limits.maximumDepth
                )
            case .maximumObjectCountExceeded(let actual, _):
                return .maximumObjectCountExceeded(
                    actual: objectCount + actual,
                    maximum: limits.maximumObjectCount
                )
            default:
                return .invalidRDFTerm(error)
            }
        }
    }

    private struct Decoder {
        let bytes: Bytes
        let limits: FieldValueTupleCodecLimits
        var offset = 0
        var objectCount = 0

        mutating func decodeRoot() throws(FieldValueTupleCodecError) -> FieldValue {
            let (encodedCount, overflow) = bytes.count.addingReportingOverflow(2)
            guard !overflow else {
                throw .integerOverflow
            }
            guard encodedCount <= limits.maximumEncodedBytes else {
                throw .maximumEncodedBytesExceeded(
                    actual: encodedCount,
                    maximum: limits.maximumEncodedBytes
                )
            }
            guard try readByte() == magic0,
                  try readByte() == magic1,
                  try readByte() == magic2 else {
                throw .invalidEnvelopeMagic
            }
            let actualVersion = try readByte()
            guard actualVersion == version else {
                throw .unsupportedEnvelopeVersion(actualVersion)
            }
            let tag = try readByte()
            try registerObject()

            let value: FieldValue
            switch tag {
            case nullTag:
                value = .null
            case dataTag:
                value = .data(try readNibbleBytes())
            case rdfTag:
                value = .rdfTerm(
                    try readRDF(byteCount: bytes.count - offset, depth: 0)
                )
            case arrayTag:
                value = .array(try readArray(depth: 0))
            default:
                throw .invalidRootTag(tag)
            }
            guard offset == bytes.count else {
                throw .trailingBytes
            }
            return value
        }

        private mutating func readValue(
            depth: Int
        ) throws(FieldValueTupleCodecError) -> FieldValue {
            guard depth <= limits.maximumDepth else {
                throw .maximumDepthExceeded(
                    actual: depth,
                    maximum: limits.maximumDepth
                )
            }
            let tag = try readByte()
            try registerObject()
            switch tag {
            case nullTag:
                return .null
            case falseTag:
                return .bool(false)
            case trueTag:
                return .bool(true)
            case int64Tag:
                let ordered = try readFixedNibbles()
                return .int64(
                    Int64(bitPattern: ordered ^ 0x8000_0000_0000_0000)
                )
            case uint64Tag:
                return .uint64(try readFixedNibbles())
            case doubleTag:
                let ordered = try readFixedNibbles()
                let bits = (ordered & 0x8000_0000_0000_0000) == 0
                    ? ~ordered
                    : ordered ^ 0x8000_0000_0000_0000
                return .double(Double(bitPattern: bits))
            case stringTag:
                let decoded = try readNibbleBytes()
                guard let value = String(bytes: decoded, encoding: .utf8) else {
                    throw .invalidUTF8
                }
                return .string(value)
            case rdfTag:
                let byteCount = try readZeroFreeVarint()
                return .rdfTerm(try readRDF(byteCount: byteCount, depth: depth))
            case dataTag:
                return .data(try readNibbleBytes())
            case arrayTag:
                return .array(try readArray(depth: depth))
            default:
                throw .unknownTag(tag)
            }
        }

        private mutating func readArray(
            depth: Int
        ) throws(FieldValueTupleCodecError) -> [FieldValue] {
            var values: [FieldValue] = []
            while true {
                guard offset < bytes.count else {
                    throw .truncated
                }
                if bytes[offset] == end {
                    offset += 1
                    return values
                }
                let nextCount = values.count + 1
                guard nextCount <= limits.maximumCollectionCount else {
                    throw .maximumCollectionCountExceeded(
                        actual: nextCount,
                        maximum: limits.maximumCollectionCount
                    )
                }
                do {
                    values.append(try readValue(depth: depth + 1))
                } catch let error {
                    switch error {
                    case .maximumEncodedBytesExceeded,
                         .maximumCollectionCountExceeded,
                         .maximumDepthExceeded,
                         .maximumObjectCountExceeded,
                         .integerOverflow:
                        throw error
                    default:
                        throw .invalidArrayElement(
                            index: values.count,
                            reason: error
                        )
                    }
                }
            }
        }

        private mutating func readRDF(
            byteCount: Int,
            depth: Int
        ) throws(FieldValueTupleCodecError) -> DatabaseRDFTerm {
            guard byteCount >= 0 else {
                throw .integerOverflow
            }
            let (endOffset, overflow) = offset.addingReportingOverflow(byteCount)
            guard !overflow else {
                throw .integerOverflow
            }
            guard endOffset <= bytes.count else {
                throw .truncated
            }
            let remainingObjects = limits.maximumObjectCount - objectCount
            guard remainingObjects > 0 else {
                throw .maximumObjectCountExceeded(
                    actual: objectCount + 1,
                    maximum: limits.maximumObjectCount
                )
            }
            let canonicalBytes = DatabaseBytes(
                retaining: bytes[offset..<endOffset]
            )
            let rdfLimits = DatabaseRDFTermCodecLimits(
                maximumBytes: byteCount,
                maximumDepth: limits.maximumDepth - depth,
                maximumObjectCount: remainingObjects
            )
            do {
                let result = try DatabaseRDFTermCodec.decodeWithMetrics(
                    canonicalBytes,
                    limits: rdfLimits
                )
                objectCount += result.objectCount
                offset = endOffset
                return result.term
            } catch let error {
                throw mapRDFError(error, depth: depth)
            }
        }

        private mutating func readNibbleBytes() throws(FieldValueTupleCodecError) -> DatabaseBytes {
            let start = offset
            var cursor = offset
            var digitCount = 0
            while cursor < bytes.count {
                let byte = bytes[cursor]
                if byte == end {
                    guard digitCount.isMultiple(of: 2) else {
                        throw .incompleteNibblePair
                    }
                    let decodedCount = digitCount / 2
                    let result = DatabaseBytes.copying(count: decodedCount) { output in
                        var source = start
                        var destination = 0
                        while source < cursor {
                            let high = bytes[source] - firstNibble
                            let low = bytes[source + 1] - firstNibble
                            output[destination] = (high << 4) | low
                            source += 2
                            destination += 1
                        }
                    }
                    offset = cursor + 1
                    return result
                }
                guard byte >= firstNibble && byte <= lastNibble else {
                    throw .invalidNibble(byte)
                }
                digitCount += 1
                cursor += 1
            }
            throw .truncated
        }

        private mutating func readFixedNibbles() throws(FieldValueTupleCodecError) -> UInt64 {
            var value: UInt64 = 0
            for _ in 0..<16 {
                let byte = try readByte()
                guard byte >= firstNibble && byte <= lastNibble else {
                    throw .invalidNibble(byte)
                }
                value = (value << 4) | UInt64(byte - firstNibble)
            }
            return value
        }

        private mutating func readZeroFreeVarint() throws(FieldValueTupleCodecError) -> Int {
            var value = 0
            var multiplier = 1
            var digitIndex = 0
            while true {
                let byte = try readByte()
                let encodedDigit = Int(byte & 0x7F)
                guard encodedDigit > 0 else {
                    throw .nonCanonicalVarint
                }
                let digit = encodedDigit - 1
                if digitIndex > 0 && (byte & 0x80) == 0 && digit == 0 {
                    throw .nonCanonicalVarint
                }
                let (component, componentOverflow) = digit.multipliedReportingOverflow(
                    by: multiplier
                )
                let (nextValue, additionOverflow) = value.addingReportingOverflow(
                    component
                )
                guard !componentOverflow && !additionOverflow else {
                    throw .integerOverflow
                }
                value = nextValue
                guard (byte & 0x80) != 0 else {
                    return value
                }
                let (nextMultiplier, multiplierOverflow) = multiplier
                    .multipliedReportingOverflow(by: 127)
                guard !multiplierOverflow else {
                    throw .integerOverflow
                }
                multiplier = nextMultiplier
                digitIndex += 1
            }
        }

        private mutating func readByte() throws(FieldValueTupleCodecError) -> UInt8 {
            guard offset < bytes.count else {
                throw .truncated
            }
            let value = bytes[offset]
            offset += 1
            return value
        }

        private mutating func registerObject() throws(FieldValueTupleCodecError) {
            let (next, overflow) = objectCount.addingReportingOverflow(1)
            guard !overflow else {
                throw .integerOverflow
            }
            guard next <= limits.maximumObjectCount else {
                throw .maximumObjectCountExceeded(
                    actual: next,
                    maximum: limits.maximumObjectCount
                )
            }
            objectCount = next
        }

        private func mapRDFError(
            _ error: DatabaseRDFTermCodecError,
            depth: Int
        ) -> FieldValueTupleCodecError {
            switch error {
            case .maximumBytesExceeded(let actual, _):
                return .maximumEncodedBytesExceeded(
                    actual: actual,
                    maximum: limits.maximumEncodedBytes
                )
            case .maximumDepthExceeded(let actual, _):
                return .maximumDepthExceeded(
                    actual: depth + actual,
                    maximum: limits.maximumDepth
                )
            case .maximumObjectCountExceeded(let actual, _):
                return .maximumObjectCountExceeded(
                    actual: objectCount + actual,
                    maximum: limits.maximumObjectCount
                )
            default:
                return .invalidRDFTerm(error)
            }
        }
    }

    private struct RDFSink: DatabaseRDFTermEncodingSink {
        let tupleSink: UnsafeMutablePointer<TupleEncodingSink>

        mutating func write(_ byte: UInt8) {
            precondition(byte != 0, "Canonical RDF tuple bytes must be zero-free")
            tupleSink.pointee.writeByte(byte)
        }

        mutating func write(_ bytes: UnsafeRawBufferPointer) {
            for byte in bytes {
                write(byte)
            }
        }
    }
}
