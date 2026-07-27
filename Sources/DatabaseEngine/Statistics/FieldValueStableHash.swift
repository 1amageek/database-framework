import DatabaseTypes

/// Stable, framed hashing for execution statistics and approximate cardinality indexes.
public enum FieldValueStableHash {
    public static func hash(
        _ value: FieldValue
    ) -> UInt64 {
        var stream = Stream()
        appendIteratively(value, to: &stream)
        return stream.finalize()
    }

    private enum HashStep {
        case value(FieldValue)
        case string(String)
        case identifier(ReferenceIdentifier)
        case rdfTerm(RDFTerm)
        case unsigned64(UInt64)
    }

    private static func appendIteratively(
        _ root: FieldValue,
        to stream: inout Stream
    ) {
        var pending: [HashStep] = [.value(root)]
        while let step = pending.popLast() {
            switch step {
            case .string(let value):
                append(value, to: &stream)
            case .unsigned64(let value):
                stream.updateLittleEndian(value)
            case .identifier(let value):
                append(value, to: &stream, pending: &pending)
            case .rdfTerm(let value):
                append(value, to: &stream, pending: &pending)
            case .value(let value):
                append(value, to: &stream, pending: &pending)
            }
        }
    }

    private static func append(
        _ value: FieldValue,
        to stream: inout Stream,
        pending: inout [HashStep]
    ) {
        switch value {
        case .null:
            stream.update(byte: 0x00)
        case .bool(let value):
            stream.update(byte: 0x01)
            stream.update(byte: value ? 1 : 0)
        case .int8(let value):
            stream.update(byte: 0x02)
            stream.update(byte: UInt8(bitPattern: value))
        case .int16(let value):
            stream.update(byte: 0x03)
            stream.updateLittleEndian(value)
        case .int32(let value):
            stream.update(byte: 0x04)
            stream.updateLittleEndian(value)
        case .int64(let value):
            stream.update(byte: 0x05)
            stream.updateLittleEndian(value)
        case .uint8(let value):
            stream.update(byte: 0x06)
            stream.update(byte: value)
        case .uint16(let value):
            stream.update(byte: 0x07)
            stream.updateLittleEndian(value)
        case .uint32(let value):
            stream.update(byte: 0x08)
            stream.updateLittleEndian(value)
        case .uint64(let value):
            stream.update(byte: 0x09)
            stream.updateLittleEndian(value)
        case .float32(let value):
            stream.update(byte: 0x0A)
            stream.updateLittleEndian(value.bitPattern)
        case .float64(let value):
            stream.update(byte: 0x0B)
            stream.updateLittleEndian(value.bitPattern)
        case .decimal(let value):
            stream.update(byte: 0x0C)
            stream.updateLittleEndian(value.coefficient)
            stream.updateLittleEndian(value.scale)
        case .string(let value):
            stream.update(byte: 0x0D)
            append(value, to: &stream)
        case .bytes(let value):
            stream.update(byte: 0x0E)
            stream.updateLittleEndian(UInt64(value.count))
            value.withUnsafeBytes { bytes in
                for byte in bytes {
                    stream.update(byte: byte)
                }
            }
        case .date(let value):
            stream.update(byte: 0x0F)
            stream.updateLittleEndian(value.year)
            stream.update(byte: value.month)
            stream.update(byte: value.day)
        case .time(let value):
            stream.update(byte: 0x10)
            append(value, to: &stream)
        case .dateTime(let value):
            stream.update(byte: 0x11)
            stream.updateLittleEndian(value.date.year)
            stream.update(byte: value.date.month)
            stream.update(byte: value.date.day)
            append(value.time, to: &stream)
        case .timestamp(let value):
            stream.update(byte: 0x12)
            stream.updateLittleEndian(value.secondsSinceUnixEpoch)
            stream.updateLittleEndian(value.nanoseconds)
        case .timeSpan(let value):
            stream.update(byte: 0x13)
            stream.updateLittleEndian(value.seconds)
            stream.updateLittleEndian(value.nanoseconds)
        case .calendarPeriod(let value):
            stream.update(byte: 0x14)
            stream.updateLittleEndian(value.months)
            stream.updateLittleEndian(value.days)
        case .geographicPoint(let value):
            stream.update(byte: 0x15)
            stream.updateLittleEndian(value.latitude.bitPattern)
            stream.updateLittleEndian(value.longitude.bitPattern)
        case .geographicPosition(let value):
            stream.update(byte: 0x16)
            stream.updateLittleEndian(value.point.latitude.bitPattern)
            stream.updateLittleEndian(value.point.longitude.bitPattern)
            stream.updateLittleEndian(
                value.ellipsoidalHeightInMeters.bitPattern
            )
        case .vector(let value):
            stream.update(byte: 0x17)
            append(value, to: &stream)
        case .uuid(let value):
            stream.update(byte: 0x18)
            stream.updateLittleEndian(value.high)
            stream.updateLittleEndian(value.low)
        case .array(let values):
            stream.update(byte: 0x19)
            stream.updateLittleEndian(UInt64(values.count))
            for value in values.reversed() {
                pending.append(.value(value))
            }
        case .object(let fields):
            stream.update(byte: 0x1A)
            stream.updateLittleEndian(UInt64(fields.count))
            for field in fields.fields.reversed() {
                pending.append(.value(field.value))
                pending.append(.string(field.key))
            }
        case .reference(let identity):
            stream.update(byte: 0x1B)
            for partition in identity.partitions.fields.reversed() {
                pending.append(.value(partition.value))
                pending.append(.string(partition.key))
            }
            pending.append(.unsigned64(UInt64(identity.partitions.count)))
            pending.append(.identifier(identity.id))
            pending.append(.string(identity.entity))
        case .rdfTerm(let term):
            stream.update(byte: 0x1C)
            pending.append(.rdfTerm(term))
        }
    }

    private static func append(
        _ term: RDFTerm,
        to stream: inout Stream,
        pending: inout [HashStep]
    ) {
        switch term {
        case .blankNode(let identifier):
            stream.update(byte: 0x00)
            pending.append(.string(identifier.rawValue))
        case .iri(let iri):
            stream.update(byte: 0x01)
            pending.append(.string(iri.rawValue))
        case .literal(let literal):
            stream.update(byte: 0x02)
            switch literal.annotation {
            case .typed(let datatype):
                stream.update(byte: 0x00)
                pending.append(.string(datatype.rawValue))
            case .languageTagged(let language):
                stream.update(byte: 0x01)
                pending.append(.string(language.rawValue))
            case .directionalLanguageTagged(let language, let direction):
                stream.update(byte: 0x02)
                stream.update(byte: direction == .leftToRight ? 0 : 1)
                pending.append(.string(language.rawValue))
            }
            pending.append(.string(literal.lexicalForm))
        case .tripleTerm(let subject, let predicate, let object):
            stream.update(byte: 0x03)
            pending.append(.rdfTerm(object))
            pending.append(.rdfTerm(predicate.term))
            pending.append(.rdfTerm(subject.term))
        }
    }

    private static func append(
        _ value: CivilTime,
        to stream: inout Stream
    ) {
        stream.update(byte: value.hour)
        stream.update(byte: value.minute)
        stream.update(byte: value.second)
        stream.updateLittleEndian(value.nanoseconds)
    }

    private static func append(
        _ value: Vector,
        to stream: inout Stream
    ) {
        stream.updateLittleEndian(UInt64(value.count))
        switch value.elementType {
        case .int8:
            stream.update(byte: 0)
            _ = value.withInt8Elements { elements in
                for element in elements {
                    stream.update(byte: UInt8(bitPattern: element))
                }
            }
        case .int16:
            stream.update(byte: 1)
            _ = value.withInt16Elements { elements in
                for element in elements {
                    stream.updateLittleEndian(element)
                }
            }
        case .int32:
            stream.update(byte: 2)
            _ = value.withInt32Elements { elements in
                for element in elements {
                    stream.updateLittleEndian(element)
                }
            }
        case .int64:
            stream.update(byte: 3)
            _ = value.withInt64Elements { elements in
                for element in elements {
                    stream.updateLittleEndian(element)
                }
            }
        case .uint8:
            stream.update(byte: 4)
            _ = value.withUInt8Elements { elements in
                stream.update(elements)
            }
        case .uint16:
            stream.update(byte: 5)
            _ = value.withUInt16Elements { elements in
                for element in elements {
                    stream.updateLittleEndian(element)
                }
            }
        case .uint32:
            stream.update(byte: 6)
            _ = value.withUInt32Elements { elements in
                for element in elements {
                    stream.updateLittleEndian(element)
                }
            }
        case .uint64:
            stream.update(byte: 7)
            _ = value.withUInt64Elements { elements in
                for element in elements {
                    stream.updateLittleEndian(element)
                }
            }
        case .float32:
            stream.update(byte: 8)
            _ = value.withFloat32Elements { elements in
                for element in elements {
                    stream.updateLittleEndian(element.bitPattern)
                }
            }
        case .float64:
            stream.update(byte: 9)
            _ = value.withFloat64Elements { elements in
                for element in elements {
                    stream.updateLittleEndian(element.bitPattern)
                }
            }
        }
    }

    private static func append(
        _ value: String,
        to stream: inout Stream
    ) {
        stream.updateLittleEndian(UInt64(value.utf8.count))
        stream.update(value.utf8)
    }

    private static func append(
        _ value: ReferenceIdentifier,
        to stream: inout Stream,
        pending: inout [HashStep]
    ) {
        switch value {
        case .bool(let value):
            stream.update(byte: 0x00)
            stream.update(byte: value ? 1 : 0)
        case .int8(let value):
            stream.update(byte: 0x01)
            stream.updateLittleEndian(value)
        case .int16(let value):
            stream.update(byte: 0x02)
            stream.updateLittleEndian(value)
        case .int32(let value):
            stream.update(byte: 0x03)
            stream.updateLittleEndian(value)
        case .int64(let value):
            stream.update(byte: 0x04)
            stream.updateLittleEndian(value)
        case .uint8(let value):
            stream.update(byte: 0x05)
            stream.updateLittleEndian(value)
        case .uint16(let value):
            stream.update(byte: 0x06)
            stream.updateLittleEndian(value)
        case .uint32(let value):
            stream.update(byte: 0x07)
            stream.updateLittleEndian(value)
        case .uint64(let value):
            stream.update(byte: 0x08)
            stream.updateLittleEndian(value)
        case .string(let value):
            stream.update(byte: 0x09)
            append(value, to: &stream)
        case .bytes(let value):
            stream.update(byte: 0x0a)
            stream.updateLittleEndian(UInt64(value.count))
            value.withUnsafeBytes { bytes in
                for byte in bytes {
                    stream.update(byte: byte)
                }
            }
        case .uuid(let value):
            stream.update(byte: 0x0b)
            stream.updateLittleEndian(value.high)
            stream.updateLittleEndian(value.low)
        case .composite(let values):
            stream.update(byte: 0x0c)
            stream.updateLittleEndian(UInt64(values.count))
            for value in values.reversed() {
                pending.append(.identifier(value))
            }
        }
    }

    private struct Stream {
        private var h1: UInt64 = 0
        private var h2: UInt64 = 0
        private var tailLow: UInt64 = 0
        private var tailHigh: UInt64 = 0
        private var tailCount = 0
        private var byteCount = 0

        private static let c1: UInt64 = 0x87c3_7b91_1142_53d5
        private static let c2: UInt64 = 0x4cf5_ad43_2745_937f

        mutating func update<Bytes: Sequence>(_ bytes: Bytes)
        where Bytes.Element == UInt8 {
            for byte in bytes {
                update(byte: byte)
            }
        }

        mutating func update(byte: UInt8) {
            if tailCount < 8 {
                tailLow |= UInt64(byte) << UInt64(tailCount * 8)
            } else {
                tailHigh |= UInt64(byte) << UInt64((tailCount - 8) * 8)
            }
            tailCount += 1
            byteCount += 1
            if tailCount == 16 {
                mixBlock(low: tailLow, high: tailHigh)
                tailLow = 0
                tailHigh = 0
                tailCount = 0
            }
        }

        mutating func updateLittleEndian<Integer: FixedWidthInteger>(
            _ value: Integer
        ) {
            var remaining = Integer.Magnitude(truncatingIfNeeded: value)
            for _ in 0..<MemoryLayout<Integer>.size {
                update(byte: UInt8(truncatingIfNeeded: remaining))
                remaining >>= 8
            }
        }

        func finalize() -> UInt64 {
            var finalizedH1 = h1
            var finalizedH2 = h2
            if tailCount > 8 {
                var k2 = tailHigh
                k2 = k2 &* Self.c2
                k2 = Self.rotateLeft(k2, by: 33)
                k2 = k2 &* Self.c1
                finalizedH2 ^= k2
            }
            if tailCount > 0 {
                var k1 = tailLow
                k1 = k1 &* Self.c1
                k1 = Self.rotateLeft(k1, by: 31)
                k1 = k1 &* Self.c2
                finalizedH1 ^= k1
            }
            finalizedH1 ^= UInt64(byteCount)
            finalizedH2 ^= UInt64(byteCount)
            finalizedH1 = finalizedH1 &+ finalizedH2
            finalizedH2 = finalizedH2 &+ finalizedH1
            finalizedH1 = Self.finalMix(finalizedH1)
            finalizedH2 = Self.finalMix(finalizedH2)
            return finalizedH1 &+ finalizedH2
        }

        private mutating func mixBlock(low: UInt64, high: UInt64) {
            var k1 = low
            var k2 = high
            k1 = k1 &* Self.c1
            k1 = Self.rotateLeft(k1, by: 31)
            k1 = k1 &* Self.c2
            h1 ^= k1
            h1 = Self.rotateLeft(h1, by: 27)
            h1 = h1 &+ h2
            h1 = h1 &* 5 &+ 0x52dc_e729
            k2 = k2 &* Self.c2
            k2 = Self.rotateLeft(k2, by: 33)
            k2 = k2 &* Self.c1
            h2 ^= k2
            h2 = Self.rotateLeft(h2, by: 31)
            h2 = h2 &+ h1
            h2 = h2 &* 5 &+ 0x3849_5ab5
        }

        private static func rotateLeft(_ value: UInt64, by count: Int) -> UInt64 {
            (value << count) | (value >> (64 - count))
        }

        private static func finalMix(_ value: UInt64) -> UInt64 {
            var result = value
            result ^= result >> 33
            result = result &* 0xff51_afd7_ed55_8ccd
            result ^= result >> 33
            result = result &* 0xc4ce_b9fe_1a85_ec53
            result ^= result >> 33
            return result
        }
    }
}

public extension FieldValue {
    func stableHash() -> UInt64 {
        FieldValueStableHash.hash(self)
    }
}
