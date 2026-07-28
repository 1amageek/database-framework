#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Canonical physical encoding for `FieldValue` instances stored in tuple keys.
///
/// Native scalar values keep their FoundationDB tuple representation. Composite
/// values use one versioned byte element whose payload never contains `0x00`, so
/// `TupleCursor` can retain it as a slice of the storage key. Arbitrary binary
/// data is nibble encoded because a decoded `ByteString` value must expose the
/// original contiguous bytes; that semantic boundary requires one final allocation.
public enum FieldValueTupleCodec {
    package struct DecimalPlan: Sendable {
        package let value: ExactDecimal
        package let digitCount: Int
        package let leadingPower: Int64
    }

    package struct ObjectEntryPlan: Sendable {
        package let key: String
        package let value: Plan
    }

    package struct IdentityPlan: Sendable {
        package let entity: String
        package let identifier: IdentifierPlan
        package let partitions: [ObjectEntryPlan]
    }

    package indirect enum IdentifierPlan: Sendable {
        case bool(Bool)
        case int8(Int8)
        case int16(Int16)
        case int32(Int32)
        case int64(Int64)
        case uint8(UInt8)
        case uint16(UInt16)
        case uint32(UInt32)
        case uint64(UInt64)
        case string(String)
        case bytes(ByteString)
        case uuid(DatabaseTypes.UUID)
        case composite([IdentifierPlan])
    }

    package indirect enum Plan: Sendable {
        case null
        case bool(Bool)
        case int8(Int8)
        case int16(Int16)
        case int32(Int32)
        case int64(Int64)
        case uint8(UInt8)
        case uint16(UInt16)
        case uint32(UInt32)
        case uint64(UInt64)
        case float32(Float)
        case float64(Double)
        case decimal(DecimalPlan)
        case string(String)
        case bytes(ByteString)
        case date(CivilDate)
        case time(CivilTime)
        case dateTime(CivilDateTime)
        case timestamp(Timestamp)
        case timeSpan(TimeSpan)
        case calendarPeriod(CalendarPeriod)
        case geographicPoint(GeographicPoint)
        case geographicPosition(GeographicPosition)
        case vector(Vector)
        case uuid(DatabaseTypes.UUID)
        case array([Plan])
        case object([ObjectEntryPlan])
        case reference(IdentityPlan)
        case rdf(RDFTermStorageEncoding)
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
    private static let int8Tag: UInt8 = 0x23
    private static let int16Tag: UInt8 = 0x24
    private static let int32Tag: UInt8 = 0x25
    private static let int64Tag: UInt8 = 0x26
    private static let uint8Tag: UInt8 = 0x27
    private static let uint16Tag: UInt8 = 0x28
    private static let uint32Tag: UInt8 = 0x29
    private static let uint64Tag: UInt8 = 0x2A
    private static let float32Tag: UInt8 = 0x2B
    private static let float64Tag: UInt8 = 0x2C
    private static let decimalTag: UInt8 = 0x2D
    private static let stringTag: UInt8 = 0x2E
    private static let bytesTag: UInt8 = 0x2F
    private static let dateTag: UInt8 = 0x30
    private static let timeTag: UInt8 = 0x31
    private static let dateTimeTag: UInt8 = 0x32
    private static let timestampTag: UInt8 = 0x33
    private static let timeSpanTag: UInt8 = 0x34
    private static let calendarPeriodTag: UInt8 = 0x35
    private static let geographicPointTag: UInt8 = 0x36
    private static let geographicPositionTag: UInt8 = 0x37
    private static let vectorTag: UInt8 = 0x38
    private static let uuidTag: UInt8 = 0x39
    private static let arrayTag: UInt8 = 0x3A
    private static let objectTag: UInt8 = 0x3B
    private static let referenceTag: UInt8 = 0x3C
    private static let rdfTag: UInt8 = 0x3D

    private static let negativeDecimal: UInt8 = 0x20
    private static let zeroDecimal: UInt8 = 0x21
    private static let positiveDecimal: UInt8 = 0x22
    private static let negativeDecimalEnd: UInt8 = 0x12

    private static let identifierFalseTag: UInt8 = 0x30
    private static let identifierTrueTag: UInt8 = 0x31
    private static let identifierInt8Tag: UInt8 = 0x32
    private static let identifierInt16Tag: UInt8 = 0x33
    private static let identifierInt32Tag: UInt8 = 0x34
    private static let identifierInt64Tag: UInt8 = 0x35
    private static let identifierUInt8Tag: UInt8 = 0x36
    private static let identifierUInt16Tag: UInt8 = 0x37
    private static let identifierUInt32Tag: UInt8 = 0x38
    private static let identifierUInt64Tag: UInt8 = 0x39
    private static let identifierStringTag: UInt8 = 0x3A
    private static let identifierBytesTag: UInt8 = 0x3B
    private static let identifierUUIDTag: UInt8 = 0x3C
    private static let identifierCompositeTag: UInt8 = 0x3D

    public static func tupleElement(
        for value: FieldValue,
        limits: FieldValueTupleCodecLimits = .default
    ) throws(FieldValueTupleCodecError) -> any TupleElement {
        try CanonicalFieldValueTupleElement(
            prepared: prepareComposite(value, limits: limits)
        )
    }

    public static func decode(
        _ element: any TupleElement,
        limits: FieldValueTupleCodecLimits = .default
    ) throws(FieldValueTupleCodecError) -> FieldValue {
        switch element {
        case let value as CanonicalFieldValueTupleElement:
            return value.prepared.value
        case let value as ByteString:
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
        case .int8(let value):
            sink.writeByte(int8Tag)
            writeFixedNibbles(
                UInt64(UInt8(bitPattern: value) ^ 0x80),
                count: 2,
                to: &sink
            )
        case .int16(let value):
            sink.writeByte(int16Tag)
            writeFixedNibbles(
                UInt64(UInt16(bitPattern: value) ^ 0x8000),
                count: 4,
                to: &sink
            )
        case .int32(let value):
            sink.writeByte(int32Tag)
            writeFixedNibbles(
                UInt64(UInt32(bitPattern: value) ^ 0x8000_0000),
                count: 8,
                to: &sink
            )
        case .int64(let value):
            sink.writeByte(int64Tag)
            writeFixedNibbles(
                UInt64(bitPattern: value) ^ 0x8000_0000_0000_0000,
                to: &sink
            )
        case .uint8(let value):
            sink.writeByte(uint8Tag)
            writeFixedNibbles(UInt64(value), count: 2, to: &sink)
        case .uint16(let value):
            sink.writeByte(uint16Tag)
            writeFixedNibbles(UInt64(value), count: 4, to: &sink)
        case .uint32(let value):
            sink.writeByte(uint32Tag)
            writeFixedNibbles(UInt64(value), count: 8, to: &sink)
        case .uint64(let value):
            sink.writeByte(uint64Tag)
            writeFixedNibbles(value, to: &sink)
        case .float32(let value):
            sink.writeByte(float32Tag)
            let bits = value.bitPattern
            let ordered = (bits & 0x8000_0000) == 0
                ? bits ^ 0x8000_0000
                : ~bits
            writeFixedNibbles(UInt64(ordered), count: 8, to: &sink)
        case .float64(let value):
            sink.writeByte(float64Tag)
            let bits = value.bitPattern
            let ordered = (bits & 0x8000_0000_0000_0000) == 0
                ? bits ^ 0x8000_0000_0000_0000
                : ~bits
            writeFixedNibbles(ordered, to: &sink)
        case .decimal(let value):
            writeDecimal(value, to: &sink)
        case .string(let value):
            sink.writeByte(stringTag)
            writeNibbles(value.utf8, to: &sink)
            sink.writeByte(end)
        case .bytes(let value):
            sink.writeByte(bytesTag)
            value.withUnsafeBytes { bytes in
                writeNibbles(bytes, to: &sink)
            }
            sink.writeByte(end)
        case .date(let value):
            sink.writeByte(dateTag)
            writeFixedNibbles(
                UInt64(UInt32(bitPattern: value.year) ^ 0x8000_0000),
                count: 8,
                to: &sink
            )
            writeFixedNibbles(UInt64(value.month), count: 2, to: &sink)
            writeFixedNibbles(UInt64(value.day), count: 2, to: &sink)
        case .time(let value):
            sink.writeByte(timeTag)
            writeCivilTime(value, to: &sink)
        case .dateTime(let value):
            sink.writeByte(dateTimeTag)
            writeFixedNibbles(
                UInt64(UInt32(bitPattern: value.date.year) ^ 0x8000_0000),
                count: 8,
                to: &sink
            )
            writeFixedNibbles(UInt64(value.date.month), count: 2, to: &sink)
            writeFixedNibbles(UInt64(value.date.day), count: 2, to: &sink)
            writeCivilTime(value.time, to: &sink)
        case .timestamp(let value):
            sink.writeByte(timestampTag)
            writeFixedNibbles(
                UInt64(bitPattern: value.secondsSinceUnixEpoch)
                    ^ 0x8000_0000_0000_0000,
                to: &sink
            )
            writeFixedNibbles(UInt64(value.nanoseconds), count: 8, to: &sink)
        case .timeSpan(let value):
            sink.writeByte(timeSpanTag)
            writeFixedNibbles(
                UInt64(bitPattern: value.seconds)
                    ^ 0x8000_0000_0000_0000,
                to: &sink
            )
            writeFixedNibbles(UInt64(value.nanoseconds), count: 8, to: &sink)
        case .calendarPeriod(let value):
            sink.writeByte(calendarPeriodTag)
            writeFixedNibbles(
                UInt64(bitPattern: value.months)
                    ^ 0x8000_0000_0000_0000,
                to: &sink
            )
            writeFixedNibbles(
                UInt64(bitPattern: value.days)
                    ^ 0x8000_0000_0000_0000,
                to: &sink
            )
        case .geographicPoint(let value):
            sink.writeByte(geographicPointTag)
            writeOrderedDouble(value.latitude, to: &sink)
            writeOrderedDouble(value.longitude, to: &sink)
        case .geographicPosition(let value):
            sink.writeByte(geographicPositionTag)
            writeOrderedDouble(value.point.latitude, to: &sink)
            writeOrderedDouble(value.point.longitude, to: &sink)
            writeOrderedDouble(value.ellipsoidalHeightInMeters, to: &sink)
        case .vector(let value):
            sink.writeByte(vectorTag)
            writeVector(value, to: &sink)
        case .uuid(let value):
            sink.writeByte(uuidTag)
            writeFixedNibbles(value.high, to: &sink)
            writeFixedNibbles(value.low, to: &sink)
        case .rdf(let plan):
            sink.writeByte(rdfTag)
            writeRDF(plan, to: &sink)
            sink.writeByte(end)
        case .array(let values):
            sink.writeByte(arrayTag)
            for value in values {
                write(value, isRoot: false, to: &sink)
            }
            sink.writeByte(end)
        case .object(let fields):
            sink.writeByte(objectTag)
            writeObjectFields(fields, to: &sink)
        case .reference(let identity):
            sink.writeByte(referenceTag)
            writeNibbles(identity.entity.utf8, to: &sink)
            sink.writeByte(end)
            writeIdentifier(identity.identifier, to: &sink)
            writeObjectFields(identity.partitions, to: &sink)
        }
    }

    private static func writeDecimal(
        _ plan: DecimalPlan,
        to sink: inout TupleEncodingSink
    ) {
        sink.writeByte(decimalTag)
        if plan.value.coefficient == 0 {
            sink.writeByte(zeroDecimal)
        } else {
            let isNegative = plan.value.coefficient < 0
            sink.writeByte(isNegative ? negativeDecimal : positiveDecimal)
            let orderedPower = UInt64(bitPattern: plan.leadingPower)
                ^ 0x8000_0000_0000_0000
            writeFixedNibbles(
                isNegative ? ~orderedPower : orderedPower,
                to: &sink
            )
            var divisor = decimalPowerOfTen(plan.digitCount - 1)
            let magnitude = plan.value.coefficient.magnitude
            for _ in 0..<plan.digitCount {
                let digit = UInt8((magnitude / divisor) % 10)
                sink.writeByte(
                    isNegative ? 0x11 - digit : firstNibble + digit
                )
                if divisor > 1 {
                    divisor /= 10
                }
            }
            sink.writeByte(isNegative ? negativeDecimalEnd : end)
        }
    }

    private static func writeObjectFields(
        _ fields: [ObjectEntryPlan],
        to sink: inout TupleEncodingSink
    ) {
        for field in fields {
            writeNibbles(field.key.utf8, to: &sink)
            sink.writeByte(end)
            write(field.value, isRoot: false, to: &sink)
        }
        sink.writeByte(end)
    }

    private static func writeIdentifier(
        _ plan: IdentifierPlan,
        to sink: inout TupleEncodingSink
    ) {
        switch plan {
        case .bool(let value):
            sink.writeByte(value ? identifierTrueTag : identifierFalseTag)
        case .int8(let value):
            sink.writeByte(identifierInt8Tag)
            writeFixedNibbles(
                UInt64(UInt8(bitPattern: value) ^ 0x80),
                count: 2,
                to: &sink
            )
        case .int16(let value):
            sink.writeByte(identifierInt16Tag)
            writeFixedNibbles(
                UInt64(UInt16(bitPattern: value) ^ 0x8000),
                count: 4,
                to: &sink
            )
        case .int32(let value):
            sink.writeByte(identifierInt32Tag)
            writeFixedNibbles(
                UInt64(UInt32(bitPattern: value) ^ 0x8000_0000),
                count: 8,
                to: &sink
            )
        case .int64(let value):
            sink.writeByte(identifierInt64Tag)
            writeFixedNibbles(
                UInt64(bitPattern: value) ^ 0x8000_0000_0000_0000,
                to: &sink
            )
        case .uint8(let value):
            sink.writeByte(identifierUInt8Tag)
            writeFixedNibbles(UInt64(value), count: 2, to: &sink)
        case .uint16(let value):
            sink.writeByte(identifierUInt16Tag)
            writeFixedNibbles(UInt64(value), count: 4, to: &sink)
        case .uint32(let value):
            sink.writeByte(identifierUInt32Tag)
            writeFixedNibbles(UInt64(value), count: 8, to: &sink)
        case .uint64(let value):
            sink.writeByte(identifierUInt64Tag)
            writeFixedNibbles(value, to: &sink)
        case .string(let value):
            sink.writeByte(identifierStringTag)
            writeNibbles(value.utf8, to: &sink)
            sink.writeByte(end)
        case .bytes(let value):
            sink.writeByte(identifierBytesTag)
            value.withUnsafeBytes { bytes in
                writeNibbles(bytes, to: &sink)
            }
            sink.writeByte(end)
        case .uuid(let value):
            sink.writeByte(identifierUUIDTag)
            writeFixedNibbles(value.high, to: &sink)
            writeFixedNibbles(value.low, to: &sink)
        case .composite(let components):
            sink.writeByte(identifierCompositeTag)
            for component in components {
                writeIdentifier(component, to: &sink)
            }
            sink.writeByte(end)
        }
    }

    private static func writeRDF(
        _ plan: RDFTermStorageEncoding,
        to sink: inout TupleEncodingSink
    ) {
        do {
            try withUnsafeMutablePointer(to: &sink) { sinkPointer in
                var rdfSink = RDFSink(tupleSink: sinkPointer)
                try RDFTermStorageFormat.encode(plan, into: &rdfSink)
            }
        } catch let error as RDFTermStorageError {
            preconditionFailure(
                "A validated RDF encoding plan failed during tuple emission: \(error)"
            )
        } catch {
            preconditionFailure("Unexpected RDF tuple encoding failure")
        }
    }

    private static func writeNibbles<ByteSequence: Sequence>(
        _ bytes: ByteSequence,
        to sink: inout TupleEncodingSink
    ) where ByteSequence.Element == UInt8 {
        for byte in bytes {
            sink.writeByte(firstNibble + (byte >> 4))
            sink.writeByte(firstNibble + (byte & 0x0F))
        }
    }

    private static func writeFixedNibbles(
        _ value: UInt64,
        count: Int = 16,
        to sink: inout TupleEncodingSink
    ) {
        precondition((1...16).contains(count))
        for shift in stride(from: (count - 1) * 4, through: 0, by: -4) {
            sink.writeByte(
                firstNibble + UInt8(
                    truncatingIfNeeded: (value >> UInt64(shift)) & 0x0F
                )
            )
        }
    }

    private static func writeCivilTime(
        _ value: CivilTime,
        to sink: inout TupleEncodingSink
    ) {
        writeFixedNibbles(UInt64(value.hour), count: 2, to: &sink)
        writeFixedNibbles(UInt64(value.minute), count: 2, to: &sink)
        writeFixedNibbles(UInt64(value.second), count: 2, to: &sink)
        writeFixedNibbles(UInt64(value.nanoseconds), count: 8, to: &sink)
    }

    private static func writeOrderedDouble(
        _ value: Double,
        to sink: inout TupleEncodingSink
    ) {
        let bits = value.bitPattern
        let ordered = (bits & 0x8000_0000_0000_0000) == 0
            ? bits ^ 0x8000_0000_0000_0000
            : ~bits
        writeFixedNibbles(ordered, to: &sink)
    }

    private static func writeVector(
        _ vector: Vector,
        to sink: inout TupleEncodingSink
    ) {
        switch vector.elementType {
        case .int8:
            sink.writeByte(0x20)
            _ = vector.withInt8Elements {
                for value in $0 {
                    writeFixedNibbles(
                        UInt64(UInt8(bitPattern: value) ^ 0x80),
                        count: 2,
                        to: &sink
                    )
                }
            }
        case .int16:
            sink.writeByte(0x21)
            _ = vector.withInt16Elements {
                for value in $0 {
                    writeFixedNibbles(
                        UInt64(UInt16(bitPattern: value) ^ 0x8000),
                        count: 4,
                        to: &sink
                    )
                }
            }
        case .int32:
            sink.writeByte(0x22)
            _ = vector.withInt32Elements {
                for value in $0 {
                    writeFixedNibbles(
                        UInt64(UInt32(bitPattern: value) ^ 0x8000_0000),
                        count: 8,
                        to: &sink
                    )
                }
            }
        case .int64:
            sink.writeByte(0x23)
            _ = vector.withInt64Elements {
                for value in $0 {
                    writeFixedNibbles(
                        UInt64(bitPattern: value) ^ 0x8000_0000_0000_0000,
                        to: &sink
                    )
                }
            }
        case .uint8:
            sink.writeByte(0x24)
            _ = vector.withUInt8Elements {
                for value in $0 {
                    writeFixedNibbles(UInt64(value), count: 2, to: &sink)
                }
            }
        case .uint16:
            sink.writeByte(0x25)
            _ = vector.withUInt16Elements {
                for value in $0 {
                    writeFixedNibbles(UInt64(value), count: 4, to: &sink)
                }
            }
        case .uint32:
            sink.writeByte(0x26)
            _ = vector.withUInt32Elements {
                for value in $0 {
                    writeFixedNibbles(UInt64(value), count: 8, to: &sink)
                }
            }
        case .uint64:
            sink.writeByte(0x27)
            _ = vector.withUInt64Elements {
                for value in $0 {
                    writeFixedNibbles(value, to: &sink)
                }
            }
        case .float32:
            sink.writeByte(0x28)
            _ = vector.withFloat32Elements {
                for value in $0 {
                    let bits = value.bitPattern
                    let ordered = (bits & 0x8000_0000) == 0
                        ? bits ^ 0x8000_0000
                        : ~bits
                    writeFixedNibbles(
                        UInt64(ordered),
                        count: 8,
                        to: &sink
                    )
                }
            }
        case .float64:
            sink.writeByte(0x29)
            _ = vector.withFloat64Elements {
                for value in $0 {
                    writeOrderedDouble(value, to: &sink)
                }
            }
        }
        sink.writeByte(end)
    }

    private static func encodedPayloadByteCount(
        _ plan: Plan,
        isRoot: Bool
    ) throws(FieldValueTupleCodecError) -> Int {
        let bodyCount: Int
        switch plan {
        case .null, .bool:
            bodyCount = 1
        case .int8, .uint8:
            bodyCount = 3
        case .int16, .uint16:
            bodyCount = 5
        case .int32, .uint32, .float32:
            bodyCount = 9
        case .int64, .uint64, .float64:
            bodyCount = 17
        case .decimal(let value):
            bodyCount = value.value.coefficient == 0
                ? 2
                : try checkedAdd(19, value.digitCount)
        case .string(let value):
            bodyCount = try checkedAdd(
                2,
                try checkedMultiply(value.utf8.count, by: 2)
            )
        case .bytes(let value):
            bodyCount = try checkedAdd(
                2,
                try checkedMultiply(value.count, by: 2)
            )
        case .date:
            bodyCount = 13
        case .time:
            bodyCount = 15
        case .dateTime:
            bodyCount = 27
        case .timestamp, .timeSpan:
            bodyCount = 25
        case .calendarPeriod, .geographicPoint, .uuid:
            bodyCount = 33
        case .geographicPosition:
            bodyCount = 49
        case .vector(let vector):
            bodyCount = try checkedAdd(3, vectorEncodedByteCount(vector))
        case .rdf(let rdfPlan):
            bodyCount = try checkedAdd(
                2,
                try checkedMultiply(rdfPlan.byteCount, by: 2)
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
        case .object(let fields):
            bodyCount = try encodedObjectFieldsByteCount(fields)
        case .reference(let identity):
            let entityCount = try checkedAdd(
                1,
                try checkedMultiply(identity.entity.utf8.count, by: 2)
            )
            bodyCount = try checkedAdd(
                1,
                try checkedAdd(
                    entityCount,
                    try checkedAdd(
                        encodedIdentifierByteCount(identity.identifier),
                        encodedObjectFieldsByteCount(identity.partitions)
                    )
                )
            )
        }
        return isRoot ? try checkedAdd(4, bodyCount) : bodyCount
    }

    private static func encodedObjectFieldsByteCount(
        _ fields: [ObjectEntryPlan]
    ) throws(FieldValueTupleCodecError) -> Int {
        var count = 1
        for field in fields {
            count = try checkedAdd(
                count,
                try checkedAdd(
                    1,
                    try checkedMultiply(field.key.utf8.count, by: 2)
                )
            )
            count = try checkedAdd(
                count,
                try encodedPayloadByteCount(field.value, isRoot: false)
            )
        }
        return count
    }

    private static func encodedIdentifierByteCount(
        _ identifier: IdentifierPlan
    ) throws(FieldValueTupleCodecError) -> Int {
        switch identifier {
        case .bool:
            return 1
        case .int8, .uint8:
            return 3
        case .int16, .uint16:
            return 5
        case .int32, .uint32:
            return 9
        case .int64, .uint64:
            return 17
        case .string(let value):
            return try checkedAdd(
                2,
                try checkedMultiply(value.utf8.count, by: 2)
            )
        case .bytes(let value):
            return try checkedAdd(
                2,
                try checkedMultiply(value.count, by: 2)
            )
        case .uuid:
            return 33
        case .composite(let components):
            var count = 2
            for component in components {
                count = try checkedAdd(
                    count,
                    try encodedIdentifierByteCount(component)
                )
            }
            return count
        }
    }

    private static func vectorEncodedByteCount(
        _ vector: Vector
    ) throws(FieldValueTupleCodecError) -> Int {
        let elementByteCount: Int
        switch vector.elementType {
        case .int8, .uint8:
            elementByteCount = 2
        case .int16, .uint16:
            elementByteCount = 4
        case .int32, .uint32, .float32:
            elementByteCount = 8
        case .int64, .uint64, .float64:
            elementByteCount = 16
        }
        return try checkedMultiply(vector.count, by: elementByteCount)
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

    private static func decimalDigitCount(_ magnitude: UInt128) -> Int {
        var remaining = magnitude
        var count = 1
        while remaining >= 10 {
            remaining /= 10
            count += 1
        }
        return count
    }

    private static func decimalPowerOfTen(_ exponent: Int) -> UInt128 {
        precondition((0...38).contains(exponent))
        var value: UInt128 = 1
        for _ in 0..<exponent {
            value *= 10
        }
        return value
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
            case .int8(let value):
                return .int8(value)
            case .int16(let value):
                return .int16(value)
            case .int32(let value):
                return .int32(value)
            case .int64(let value):
                return .int64(value)
            case .uint8(let value):
                return .uint8(value)
            case .uint16(let value):
                return .uint16(value)
            case .uint32(let value):
                return .uint32(value)
            case .uint64(let value):
                return .uint64(value)
            case .float32(let value):
                return .float32(value)
            case .float64(let value):
                return .float64(value)
            case .decimal(let value):
                return .decimal(Self.decimalPlan(value))
            case .string(let value):
                return .string(value)
            case .bytes(let value):
                return .bytes(value)
            case .date(let value):
                return .date(value)
            case .time(let value):
                return .time(value)
            case .dateTime(let value):
                return .dateTime(value)
            case .timestamp(let value):
                return .timestamp(value)
            case .timeSpan(let value):
                return .timeSpan(value)
            case .calendarPeriod(let value):
                return .calendarPeriod(value)
            case .geographicPoint(let value):
                return .geographicPoint(value)
            case .geographicPosition(let value):
                return .geographicPosition(value)
            case .vector(let value):
                try validateCollectionCount(value.count)
                return .vector(value)
            case .uuid(let value):
                return .uuid(value)
            case .rdfTerm(let value):
                return .rdf(try prepareRDF(value, depth: depth))
            case .array(let values):
                try validateCollectionCount(values.count)
                var plans: [Plan] = []
                plans.reserveCapacity(values.count)
                for value in values {
                    plans.append(try prepare(value, depth: depth + 1))
                }
                return .array(plans)
            case .object(let fields):
                return .object(
                    try prepareObjectFields(fields, depth: depth + 1)
                )
            case .reference(let identity):
                guard depth < limits.maximumDepth else {
                    throw .maximumDepthExceeded(
                        actual: depth + 1,
                        maximum: limits.maximumDepth
                    )
                }
                return .reference(
                    IdentityPlan(
                        entity: identity.entity,
                        identifier: try prepareIdentifier(
                            identity.id,
                            depth: depth + 1
                        ),
                        partitions: try prepareObjectFields(
                            identity.partitions,
                            depth: depth + 1
                        )
                    )
                )
            }
        }

        fileprivate static func decimalPlan(_ value: ExactDecimal) -> DecimalPlan {
            let digitCount = FieldValueTupleCodec.decimalDigitCount(
                value.coefficient.magnitude
            )
            return DecimalPlan(
                value: value,
                digitCount: digitCount,
                leadingPower: Int64(digitCount - 1)
                    - Int64(value.scale)
            )
        }

        private mutating func prepareObjectFields(
            _ object: FieldObject,
            depth: Int
        ) throws(FieldValueTupleCodecError) -> [ObjectEntryPlan] {
            let fields = object.fields
            try validateCollectionCount(fields.count)
            var plans: [ObjectEntryPlan] = []
            plans.reserveCapacity(fields.count)
            for field in fields {
                plans.append(
                    ObjectEntryPlan(
                        key: field.key,
                        value: try prepare(field.value, depth: depth)
                    )
                )
            }
            return plans
        }

        private mutating func prepareIdentifier(
            _ identifier: ReferenceIdentifier,
            depth: Int
        ) throws(FieldValueTupleCodecError) -> IdentifierPlan {
            guard depth <= limits.maximumDepth else {
                throw .maximumDepthExceeded(
                    actual: depth,
                    maximum: limits.maximumDepth
                )
            }
            try registerObject()
            switch identifier {
            case .bool(let value):
                return .bool(value)
            case .int8(let value):
                return .int8(value)
            case .int16(let value):
                return .int16(value)
            case .int32(let value):
                return .int32(value)
            case .int64(let value):
                return .int64(value)
            case .uint8(let value):
                return .uint8(value)
            case .uint16(let value):
                return .uint16(value)
            case .uint32(let value):
                return .uint32(value)
            case .uint64(let value):
                return .uint64(value)
            case .string(let value):
                return .string(value)
            case .bytes(let value):
                return .bytes(value)
            case .uuid(let value):
                return .uuid(value)
            case .composite(let components):
                try validateCollectionCount(components.count)
                var plans: [IdentifierPlan] = []
                plans.reserveCapacity(components.count)
                for component in components {
                    plans.append(
                        try prepareIdentifier(component, depth: depth + 1)
                    )
                }
                return .composite(plans)
            }
        }

        private func validateCollectionCount(
            _ count: Int
        ) throws(FieldValueTupleCodecError) {
            guard count <= limits.maximumCollectionCount else {
                throw .maximumCollectionCountExceeded(
                    actual: count,
                    maximum: limits.maximumCollectionCount
                )
            }
        }

        private mutating func prepareRDF(
            _ term: RDFTerm,
            depth: Int
        ) throws(FieldValueTupleCodecError) -> RDFTermStorageEncoding {
            let remainingObjects = limits.maximumObjectCount - objectCount
            guard remainingObjects > 0 else {
                throw .maximumObjectCountExceeded(
                    actual: objectCount + 1,
                    maximum: limits.maximumObjectCount
                )
            }
            let rdfLimits: RDFTermStorageLimits
            do {
                rdfLimits = try RDFTermStorageLimits(
                    maximumBytes: limits.maximumEncodedBytes,
                    maximumDepth: limits.maximumDepth - depth,
                    maximumObjectCount: remainingObjects
                )
            } catch let error {
                throw .invalidRDFTermLimits(error)
            }
            do {
                let plan = try RDFTermStorageFormat.encodingPlan(
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
            _ error: RDFTermStorageError,
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
        private struct NibblePayload {
            let start: Int
            let end: Int
            let decodedCount: Int
        }

        let bytes: ByteString
        let limits: FieldValueTupleCodecLimits
        var offset: Int
        var objectCount = 0

        init(
            bytes: ByteString,
            limits: FieldValueTupleCodecLimits
        ) {
            self.bytes = bytes
            self.limits = limits
            self.offset = bytes.startIndex
        }

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
            let value = try readValue(depth: 0)
            guard offset == bytes.endIndex else {
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
            case int8Tag:
                let bits = try readFixedNibbles(count: 2)
                guard let encoded = UInt8(exactly: bits) else {
                    throw .integerOverflow
                }
                return .int8(Int8(bitPattern: encoded ^ 0x80))
            case int16Tag:
                let bits = try readFixedNibbles(count: 4)
                guard let encoded = UInt16(exactly: bits) else {
                    throw .integerOverflow
                }
                return .int16(Int16(bitPattern: encoded ^ 0x8000))
            case int32Tag:
                let bits = try readFixedNibbles(count: 8)
                guard let encoded = UInt32(exactly: bits) else {
                    throw .integerOverflow
                }
                return .int32(Int32(bitPattern: encoded ^ 0x8000_0000))
            case int64Tag:
                let ordered = try readFixedNibbles()
                return .int64(
                    Int64(bitPattern: ordered ^ 0x8000_0000_0000_0000)
                )
            case uint8Tag:
                guard let value = UInt8(
                    exactly: try readFixedNibbles(count: 2)
                ) else {
                    throw .integerOverflow
                }
                return .uint8(value)
            case uint16Tag:
                guard let value = UInt16(
                    exactly: try readFixedNibbles(count: 4)
                ) else {
                    throw .integerOverflow
                }
                return .uint16(value)
            case uint32Tag:
                guard let value = UInt32(
                    exactly: try readFixedNibbles(count: 8)
                ) else {
                    throw .integerOverflow
                }
                return .uint32(value)
            case uint64Tag:
                return .uint64(try readFixedNibbles())
            case float32Tag:
                let ordered = try readFixedNibbles(count: 8)
                guard let ordered = UInt32(exactly: ordered) else {
                    throw .integerOverflow
                }
                let bits = (ordered & 0x8000_0000) == 0
                    ? ~ordered
                    : ordered ^ 0x8000_0000
                return .float32(Float(bitPattern: bits))
            case float64Tag:
                let ordered = try readFixedNibbles()
                let bits = (ordered & 0x8000_0000_0000_0000) == 0
                    ? ~ordered
                    : ordered ^ 0x8000_0000_0000_0000
                return .float64(Double(bitPattern: bits))
            case decimalTag:
                return try readDecimal()
            case stringTag:
                return .string(try readNibbleString())
            case rdfTag:
                return .rdfTerm(try readRDF(depth: depth))
            case bytesTag:
                return .bytes(try readNibbleBytes())
            case dateTag:
                return try readDate()
            case timeTag:
                return .time(try readCivilTime())
            case dateTimeTag:
                return .dateTime(
                    CivilDateTime(
                        date: try readCivilDate(),
                        time: try readCivilTime()
                    )
                )
            case timestampTag:
                return try readTimestamp()
            case timeSpanTag:
                return try readTimeSpan()
            case calendarPeriodTag:
                return .calendarPeriod(
                    CalendarPeriod(
                        months: Int64(
                            bitPattern: try readFixedNibbles()
                                ^ 0x8000_0000_0000_0000
                        ),
                        days: Int64(
                            bitPattern: try readFixedNibbles()
                                ^ 0x8000_0000_0000_0000
                        )
                    )
                )
            case geographicPointTag:
                return .geographicPoint(try readGeographicPoint())
            case geographicPositionTag:
                return .geographicPosition(try readGeographicPosition())
            case vectorTag:
                return .vector(try readVector())
            case uuidTag:
                return .uuid(
                    DatabaseTypes.UUID(
                        high: try readFixedNibbles(),
                        low: try readFixedNibbles()
                    )
                )
            case arrayTag:
                return .array(try readArray(depth: depth))
            case objectTag:
                return .object(try readObjectFields(depth: depth))
            case referenceTag:
                return .reference(try readIdentity(depth: depth))
            default:
                throw .unknownTag(tag)
            }
        }

        private mutating func readDecimal() throws(
            FieldValueTupleCodecError
        ) -> FieldValue {
            let sign = try readByte()
            guard sign != zeroDecimal else {
                return .decimal(ExactDecimal(coefficient: 0, scale: 0))
            }
            guard sign == negativeDecimal || sign == positiveDecimal else {
                throw .nonCanonicalDecimal
            }

            let storedPower = try readFixedNibbles()
            let orderedPower = sign == negativeDecimal
                ? ~storedPower
                : storedPower
            let leadingPower = Int64(
                bitPattern: orderedPower ^ 0x8000_0000_0000_0000
            )
            let terminator = sign == negativeDecimal
                ? negativeDecimalEnd
                : end
            var magnitude: UInt128 = 0
            var encodedDigitCount = 0
            while true {
                let byte = try readByte()
                if byte == terminator { break }
                let digit: UInt8
                if sign == negativeDecimal {
                    guard (0x08...0x11).contains(byte) else {
                        throw .nonCanonicalDecimal
                    }
                    digit = 0x11 - byte
                } else {
                    guard (firstNibble...(firstNibble + 9)).contains(byte) else {
                        throw .nonCanonicalDecimal
                    }
                    digit = byte - firstNibble
                }
                guard encodedDigitCount < 39 else {
                    throw .nonCanonicalDecimal
                }
                let product = magnitude.multipliedReportingOverflow(by: 10)
                let sum = product.partialValue.addingReportingOverflow(
                    UInt128(digit)
                )
                guard !product.overflow, !sum.overflow else {
                    throw .integerOverflow
                }
                magnitude = sum.partialValue
                encodedDigitCount += 1
            }
            guard encodedDigitCount > 0,
                  magnitude != 0,
                  !magnitude.isMultiple(of: 10) else {
                throw .nonCanonicalDecimal
            }
            let scale64 = Int64(encodedDigitCount - 1)
                .subtractingReportingOverflow(leadingPower)
            guard !scale64.overflow,
                  let scale = Int32(exactly: scale64.partialValue) else {
                throw .integerOverflow
            }

            let coefficient: Int128
            if sign == negativeDecimal {
                if magnitude == UInt128(Int128.max) + 1 {
                    coefficient = Int128.min
                } else {
                    guard let positive = Int128(exactly: magnitude) else {
                        throw .integerOverflow
                    }
                    coefficient = -positive
                }
            } else {
                guard let positive = Int128(exactly: magnitude) else {
                    throw .integerOverflow
                }
                coefficient = positive
            }
            return .decimal(
                ExactDecimal(coefficient: coefficient, scale: scale)
            )
        }

        private mutating func readArray(
            depth: Int
        ) throws(FieldValueTupleCodecError) -> [FieldValue] {
            var values: [FieldValue] = []
            while true {
                guard offset < bytes.endIndex else {
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

        private mutating func readObjectFields(
            depth: Int
        ) throws(FieldValueTupleCodecError) -> FieldObject {
            var fields: [(key: String, value: FieldValue)] = []
            while true {
                guard offset < bytes.endIndex else {
                    throw .truncated
                }
                if bytes[offset] == end {
                    offset += 1
                    do {
                        return try FieldObject(fields)
                    } catch let error {
                        throw .invalidFieldObject(error)
                    }
                }
                let nextCount = fields.count + 1
                guard nextCount <= limits.maximumCollectionCount else {
                    throw .maximumCollectionCountExceeded(
                        actual: nextCount,
                        maximum: limits.maximumCollectionCount
                    )
                }
                do {
                    let key = try readNibbleString()
                    let value = try readValue(depth: depth + 1)
                    fields.append(
                        (key: key, value: value)
                    )
                } catch let error {
                    switch error {
                    case .maximumEncodedBytesExceeded,
                         .maximumCollectionCountExceeded,
                         .maximumDepthExceeded,
                         .maximumObjectCountExceeded,
                         .integerOverflow:
                        throw error
                    default:
                        throw .invalidObjectField(
                            index: fields.count,
                            reason: error
                        )
                    }
                }
            }
        }

        private mutating func readIdentity(
            depth: Int
        ) throws(FieldValueTupleCodecError) -> EntityReference {
            guard depth < limits.maximumDepth else {
                throw .maximumDepthExceeded(
                    actual: depth + 1,
                    maximum: limits.maximumDepth
                )
            }
            let entity = try readNibbleString()
            let identifier = try readIdentifier(depth: depth + 1)
            let partitions = try readObjectFields(depth: depth)
            do {
                return try EntityReference(
                    entity: entity,
                    id: identifier,
                    partitions: partitions
                )
            } catch let error {
                throw .invalidEntityReference(error)
            }
        }

        private mutating func readIdentifier(
            depth: Int
        ) throws(FieldValueTupleCodecError) -> ReferenceIdentifier {
            guard depth <= limits.maximumDepth else {
                throw .maximumDepthExceeded(
                    actual: depth,
                    maximum: limits.maximumDepth
                )
            }
            try registerObject()
            let tag = try readByte()
            switch tag {
            case identifierFalseTag:
                return .bool(false)
            case identifierTrueTag:
                return .bool(true)
            case identifierInt8Tag:
                let bits = try readFixedNibbles(count: 2)
                guard let value = UInt8(exactly: bits) else {
                    throw .integerOverflow
                }
                return .int8(Int8(bitPattern: value ^ 0x80))
            case identifierInt16Tag:
                let bits = try readFixedNibbles(count: 4)
                guard let value = UInt16(exactly: bits) else {
                    throw .integerOverflow
                }
                return .int16(Int16(bitPattern: value ^ 0x8000))
            case identifierInt32Tag:
                let bits = try readFixedNibbles(count: 8)
                guard let value = UInt32(exactly: bits) else {
                    throw .integerOverflow
                }
                return .int32(Int32(bitPattern: value ^ 0x8000_0000))
            case identifierInt64Tag:
                return .int64(
                    Int64(
                        bitPattern: try readFixedNibbles()
                            ^ 0x8000_0000_0000_0000
                    )
                )
            case identifierUInt8Tag:
                guard let value = UInt8(
                    exactly: try readFixedNibbles(count: 2)
                ) else {
                    throw .integerOverflow
                }
                return .uint8(value)
            case identifierUInt16Tag:
                guard let value = UInt16(
                    exactly: try readFixedNibbles(count: 4)
                ) else {
                    throw .integerOverflow
                }
                return .uint16(value)
            case identifierUInt32Tag:
                guard let value = UInt32(
                    exactly: try readFixedNibbles(count: 8)
                ) else {
                    throw .integerOverflow
                }
                return .uint32(value)
            case identifierUInt64Tag:
                return .uint64(try readFixedNibbles())
            case identifierStringTag:
                return .string(try readNibbleString())
            case identifierBytesTag:
                return .bytes(try readNibbleBytes())
            case identifierUUIDTag:
                return .uuid(
                    DatabaseTypes.UUID(
                        high: try readFixedNibbles(),
                        low: try readFixedNibbles()
                    )
                )
            case identifierCompositeTag:
                var components: [ReferenceIdentifier] = []
                while true {
                    guard offset < bytes.endIndex else {
                        throw .truncated
                    }
                    if bytes[offset] == end {
                        offset += 1
                        return .composite(components)
                    }
                    let nextCount = components.count + 1
                    guard nextCount <= limits.maximumCollectionCount else {
                        throw .maximumCollectionCountExceeded(
                            actual: nextCount,
                            maximum: limits.maximumCollectionCount
                        )
                    }
                    do {
                        components.append(
                            try readIdentifier(depth: depth + 1)
                        )
                    } catch let error {
                        switch error {
                        case .maximumEncodedBytesExceeded,
                             .maximumCollectionCountExceeded,
                             .maximumDepthExceeded,
                             .maximumObjectCountExceeded,
                             .integerOverflow:
                            throw error
                        default:
                            throw .invalidIdentifierComponent(
                                index: components.count,
                                reason: error
                            )
                        }
                    }
                }
            default:
                throw .unknownIdentifierTag(tag)
            }
        }

        private mutating func readCivilDate() throws(
            FieldValueTupleCodecError
        ) -> CivilDate {
            let yearBits = UInt32(
                truncatingIfNeeded: try readFixedNibbles(count: 8)
            )
                ^ 0x8000_0000
            let month = try readFixedNibbles(count: 2)
            let day = try readFixedNibbles(count: 2)
            guard let month = UInt8(exactly: month),
                  let day = UInt8(exactly: day) else {
                throw .integerOverflow
            }
            do {
                return try CivilDate(
                    year: Int32(bitPattern: yearBits),
                    month: month,
                    day: day
                )
            } catch let error {
                throw .invalidCivilDate(error)
            }
        }

        private mutating func readDate() throws(
            FieldValueTupleCodecError
        ) -> FieldValue {
            .date(try readCivilDate())
        }

        private mutating func readCivilTime() throws(
            FieldValueTupleCodecError
        ) -> CivilTime {
            guard let hour = UInt8(exactly: try readFixedNibbles(count: 2)),
                  let minute = UInt8(exactly: try readFixedNibbles(count: 2)),
                  let second = UInt8(exactly: try readFixedNibbles(count: 2)),
                  let nanoseconds = UInt32(
                    exactly: try readFixedNibbles(count: 8)
                  ) else {
                throw .integerOverflow
            }
            do {
                return try CivilTime(
                    hour: hour,
                    minute: minute,
                    second: second,
                    nanoseconds: nanoseconds
                )
            } catch let error {
                throw .invalidCivilTime(error)
            }
        }

        private mutating func readTimestamp() throws(
            FieldValueTupleCodecError
        ) -> FieldValue {
            let seconds = try readFixedNibbles()
                ^ 0x8000_0000_0000_0000
            let nanoseconds = try readFixedNibbles(count: 8)
            guard let nanoseconds = UInt32(exactly: nanoseconds) else {
                throw .integerOverflow
            }
            do {
                return .timestamp(
                    try Timestamp(
                    secondsSinceUnixEpoch: Int64(bitPattern: seconds),
                    nanoseconds: nanoseconds
                )
                )
            } catch let error {
                throw .invalidTimestamp(error)
            }
        }

        private mutating func readTimeSpan() throws(
            FieldValueTupleCodecError
        ) -> FieldValue {
            let seconds = Int64(
                bitPattern: try readFixedNibbles()
                    ^ 0x8000_0000_0000_0000
            )
            guard let nanoseconds = UInt32(
                exactly: try readFixedNibbles(count: 8)
            ) else {
                throw .integerOverflow
            }
            do {
                return .timeSpan(
                    try TimeSpan(
                        seconds: seconds,
                        nanoseconds: nanoseconds
                    )
                )
            } catch let error {
                throw .invalidTimeSpan(error)
            }
        }

        private mutating func readOrderedDouble() throws(
            FieldValueTupleCodecError
        ) -> Double {
            let ordered = try readFixedNibbles()
            let bits = (ordered & 0x8000_0000_0000_0000) == 0
                ? ~ordered
                : ordered ^ 0x8000_0000_0000_0000
            return Double(bitPattern: bits)
        }

        private mutating func readGeographicPoint() throws(
            FieldValueTupleCodecError
        ) -> GeographicPoint {
            let latitude = try readOrderedDouble()
            let longitude = try readOrderedDouble()
            do {
                return try GeographicPoint(
                    latitude: latitude,
                    longitude: longitude
                )
            } catch let error {
                throw .invalidGeographicPoint(error)
            }
        }

        private mutating func readGeographicPosition() throws(
            FieldValueTupleCodecError
        ) -> GeographicPosition {
            let latitude = try readOrderedDouble()
            let longitude = try readOrderedDouble()
            let height = try readOrderedDouble()
            do {
                return try GeographicPosition(
                    latitude: latitude,
                    longitude: longitude,
                    ellipsoidalHeightInMeters: height
                )
            } catch let error {
                throw .invalidGeographicPosition(error)
            }
        }

        private mutating func readVector() throws(
            FieldValueTupleCodecError
        ) -> Vector {
            let type = try readByte()
            switch type {
            case 0x20:
                var values: [Int8] = []
                while try !consumeEnd() {
                    let bits = try readFixedNibbles(count: 2)
                    guard let value = UInt8(exactly: bits) else {
                        throw .integerOverflow
                    }
                    try append(Int8(bitPattern: value ^ 0x80), to: &values)
                }
                return Vector(int8: values)
            case 0x21:
                var values: [Int16] = []
                while try !consumeEnd() {
                    let bits = try readFixedNibbles(count: 4)
                    guard let value = UInt16(exactly: bits) else {
                        throw .integerOverflow
                    }
                    try append(Int16(bitPattern: value ^ 0x8000), to: &values)
                }
                return Vector(int16: values)
            case 0x22:
                var values: [Int32] = []
                while try !consumeEnd() {
                    let bits = try readFixedNibbles(count: 8)
                    guard let value = UInt32(exactly: bits) else {
                        throw .integerOverflow
                    }
                    try append(
                        Int32(bitPattern: value ^ 0x8000_0000),
                        to: &values
                    )
                }
                return Vector(int32: values)
            case 0x23:
                var values: [Int64] = []
                while try !consumeEnd() {
                    try append(
                        Int64(
                            bitPattern: try readFixedNibbles()
                                ^ 0x8000_0000_0000_0000
                        ),
                        to: &values
                    )
                }
                return Vector(int64: values)
            case 0x24:
                var values: [UInt8] = []
                while try !consumeEnd() {
                    guard let value = UInt8(
                        exactly: try readFixedNibbles(count: 2)
                    ) else {
                        throw .integerOverflow
                    }
                    try append(value, to: &values)
                }
                return Vector(uint8: values)
            case 0x25:
                var values: [UInt16] = []
                while try !consumeEnd() {
                    guard let value = UInt16(
                        exactly: try readFixedNibbles(count: 4)
                    ) else {
                        throw .integerOverflow
                    }
                    try append(value, to: &values)
                }
                return Vector(uint16: values)
            case 0x26:
                var values: [UInt32] = []
                while try !consumeEnd() {
                    guard let value = UInt32(
                        exactly: try readFixedNibbles(count: 8)
                    ) else {
                        throw .integerOverflow
                    }
                    try append(value, to: &values)
                }
                return Vector(uint32: values)
            case 0x27:
                var values: [UInt64] = []
                while try !consumeEnd() {
                    try append(try readFixedNibbles(), to: &values)
                }
                return Vector(uint64: values)
            case 0x28:
                var values: [Float] = []
                while try !consumeEnd() {
                    let ordered = try readFixedNibbles(count: 8)
                    guard let ordered = UInt32(exactly: ordered) else {
                        throw .integerOverflow
                    }
                    let bits = (ordered & 0x8000_0000) == 0
                        ? ~ordered
                        : ordered ^ 0x8000_0000
                    try append(Float(bitPattern: bits), to: &values)
                }
                do {
                    return try Vector(float32: values)
                } catch let error {
                    throw .invalidVector(error)
                }
            case 0x29:
                var values: [Double] = []
                while try !consumeEnd() {
                    try append(readOrderedDouble(), to: &values)
                }
                do {
                    return try Vector(float64: values)
                } catch let error {
                    throw .invalidVector(error)
                }
            default:
                throw .unknownTag(type)
            }
        }

        private mutating func consumeEnd() throws(
            FieldValueTupleCodecError
        ) -> Bool {
            guard offset < bytes.endIndex else {
                throw .truncated
            }
            guard bytes[offset] == end else {
                return false
            }
            offset += 1
            return true
        }

        private func append<Element>(
            _ value: Element,
            to values: inout [Element]
        ) throws(FieldValueTupleCodecError) {
            guard values.count < limits.maximumCollectionCount else {
                throw .maximumCollectionCountExceeded(
                    actual: values.count + 1,
                    maximum: limits.maximumCollectionCount
                )
            }
            values.append(value)
        }

        private mutating func readRDF(
            depth: Int
        ) throws(FieldValueTupleCodecError) -> RDFTerm {
            let canonicalBytes = try readNibbleBytes()
            let byteCount = canonicalBytes.count
            let remainingObjects = limits.maximumObjectCount - objectCount
            guard remainingObjects > 0 else {
                throw .maximumObjectCountExceeded(
                    actual: objectCount + 1,
                    maximum: limits.maximumObjectCount
                )
            }
            let rdfLimits: RDFTermStorageLimits
            do {
                rdfLimits = try RDFTermStorageLimits(
                    maximumBytes: byteCount,
                    maximumDepth: limits.maximumDepth - depth,
                    maximumObjectCount: remainingObjects
                )
            } catch let error {
                throw .invalidRDFTermLimits(error)
            }
            do {
                let result = try RDFTermStorageFormat.decodeWithMetrics(
                    canonicalBytes,
                    limits: rdfLimits
                )
                objectCount += result.objectCount
                return result.term
            } catch let error {
                throw mapRDFError(error, depth: depth)
            }
        }

        private mutating func readNibblePayload() throws(
            FieldValueTupleCodecError
        ) -> NibblePayload {
            let start = offset
            var cursor = offset
            var digitCount = 0
            while cursor < bytes.endIndex {
                let byte = bytes[cursor]
                if byte == end {
                    guard digitCount.isMultiple(of: 2) else {
                        throw .incompleteNibblePair
                    }
                    offset = cursor + 1
                    return NibblePayload(
                        start: start,
                        end: cursor,
                        decodedCount: digitCount / 2
                    )
                }
                guard byte >= firstNibble && byte <= lastNibble else {
                    throw .invalidNibble(byte)
                }
                digitCount += 1
                cursor += 1
            }
            throw .truncated
        }

        private mutating func readNibbleBytes() throws(
            FieldValueTupleCodecError
        ) -> ByteString {
            let payload = try readNibblePayload()
            return ByteString.copying(count: payload.decodedCount) { output in
                for destination in 0..<payload.decodedCount {
                    output[destination] = decodedByte(
                        at: destination,
                        in: payload
                    )
                }
            }
        }

        private mutating func readNibbleString() throws(
            FieldValueTupleCodecError
        ) -> String {
            let payload = try readNibblePayload()
            guard isValidUTF8(payload) else {
                throw .invalidUTF8
            }
            return String(
                unsafeUninitializedCapacity: payload.decodedCount
            ) { output in
                for destination in 0..<payload.decodedCount {
                    output[destination] = decodedByte(
                        at: destination,
                        in: payload
                    )
                }
                return payload.decodedCount
            }
        }

        private func decodedByte(
            at index: Int,
            in payload: NibblePayload
        ) -> UInt8 {
            let source = payload.start + (index * 2)
            precondition(source + 1 < payload.end)
            let high = bytes[source] - firstNibble
            let low = bytes[source + 1] - firstNibble
            return (high << 4) | low
        }

        private func isValidUTF8(_ payload: NibblePayload) -> Bool {
            var index = 0
            while index < payload.decodedCount {
                let first = decodedByte(at: index, in: payload)
                switch first {
                case 0x00...0x7F:
                    index += 1
                case 0xC2...0xDF:
                    guard hasContinuationBytes(
                        count: 1,
                        after: index,
                        in: payload
                    ) else {
                        return false
                    }
                    index += 2
                case 0xE0:
                    guard index + 2 < payload.decodedCount else {
                        return false
                    }
                    let second = decodedByte(at: index + 1, in: payload)
                    guard (0xA0...0xBF).contains(second),
                          isContinuation(
                            decodedByte(at: index + 2, in: payload)
                          ) else {
                        return false
                    }
                    index += 3
                case 0xE1...0xEC, 0xEE...0xEF:
                    guard hasContinuationBytes(
                        count: 2,
                        after: index,
                        in: payload
                    ) else {
                        return false
                    }
                    index += 3
                case 0xED:
                    guard index + 2 < payload.decodedCount else {
                        return false
                    }
                    let second = decodedByte(at: index + 1, in: payload)
                    guard (0x80...0x9F).contains(second),
                          isContinuation(
                            decodedByte(at: index + 2, in: payload)
                          ) else {
                        return false
                    }
                    index += 3
                case 0xF0:
                    guard index + 3 < payload.decodedCount else {
                        return false
                    }
                    let second = decodedByte(at: index + 1, in: payload)
                    guard (0x90...0xBF).contains(second),
                          isContinuation(
                            decodedByte(at: index + 2, in: payload)
                          ),
                          isContinuation(
                            decodedByte(at: index + 3, in: payload)
                          ) else {
                        return false
                    }
                    index += 4
                case 0xF1...0xF3:
                    guard hasContinuationBytes(
                        count: 3,
                        after: index,
                        in: payload
                    ) else {
                        return false
                    }
                    index += 4
                case 0xF4:
                    guard index + 3 < payload.decodedCount else {
                        return false
                    }
                    let second = decodedByte(at: index + 1, in: payload)
                    guard (0x80...0x8F).contains(second),
                          isContinuation(
                            decodedByte(at: index + 2, in: payload)
                          ),
                          isContinuation(
                            decodedByte(at: index + 3, in: payload)
                          ) else {
                        return false
                    }
                    index += 4
                default:
                    return false
                }
            }
            return true
        }

        private func hasContinuationBytes(
            count: Int,
            after index: Int,
            in payload: NibblePayload
        ) -> Bool {
            guard count > 0,
                  index + count < payload.decodedCount else {
                return false
            }
            for continuationIndex in 1...count {
                guard isContinuation(
                    decodedByte(
                        at: index + continuationIndex,
                        in: payload
                    )
                ) else {
                    return false
                }
            }
            return true
        }

        private func isContinuation(_ byte: UInt8) -> Bool {
            (0x80...0xBF).contains(byte)
        }

        private mutating func readFixedNibbles(
            count: Int = 16
        ) throws(FieldValueTupleCodecError) -> UInt64 {
            guard (1...16).contains(count) else {
                throw .integerOverflow
            }
            var value: UInt64 = 0
            for _ in 0..<count {
                let byte = try readByte()
                guard byte >= firstNibble && byte <= lastNibble else {
                    throw .invalidNibble(byte)
                }
                value = (value << 4) | UInt64(byte - firstNibble)
            }
            return value
        }

        private mutating func readByte() throws(FieldValueTupleCodecError) -> UInt8 {
            guard offset < bytes.endIndex else {
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
            _ error: RDFTermStorageError,
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

    private struct RDFSink: RDFTermStorageSink {
        let tupleSink: UnsafeMutablePointer<TupleEncodingSink>

        mutating func write(_ byte: UInt8) {
            tupleSink.pointee.writeByte(firstNibble + (byte >> 4))
            tupleSink.pointee.writeByte(firstNibble + (byte & 0x0F))
        }

        mutating func write(_ bytes: UnsafeRawBufferPointer) {
            for byte in bytes {
                write(byte)
            }
        }
    }
}
