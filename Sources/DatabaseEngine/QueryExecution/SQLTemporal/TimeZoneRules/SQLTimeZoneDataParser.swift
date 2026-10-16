import DatabaseTypes

/// Checked borrowed decoding of the admitted TZif representation.
enum SQLTimeZoneDataParser {
    struct Block: Sendable {
        let timeCount: Int
        let times: Int
        let indices: Int
        let types: Int
        let designations: Int
        let typeCount: Int
        let designationCount: Int
        let end: Int
    }

    private struct Header {
        let version: UInt8
        let utcCount: Int
        let standardCount: Int
        let timeCount: Int
        let typeCount: Int
        let charCount: Int
    }

    static func parse(_ bytes: ByteString) throws -> (Block, SQLTimeZoneRecurrence?) {
        try bytes.withUnsafeBytes { raw in
            try parse(raw)
        }
    }

    private static func parse(_ raw: UnsafeRawBufferPointer) throws(SQLTimeZoneError) -> (Block, SQLTimeZoneRecurrence?) {
        let first = try header(raw, at: 0)
        let legacy = try block(raw, header: first, start: 44, width: 4)
        let second = try header(raw, at: legacy.end)
        guard second.version == first.version else { throw .malformedData }
        let current = try block(raw, header: second, start: legacy.end + 44, width: 8)
        guard current.end < raw.count, raw[current.end] == 10,
              raw.count - current.end >= 2, raw[raw.count - 1] == 10 else { throw .malformedData }
        let footer = (current.end + 1)..<(raw.count - 1)
        let recurrence = footer.isEmpty ? nil : try SQLTimeZoneRecurrence(raw, range: footer, version: second.version)
        if let recurrence, current.timeCount > 0 {
            let last = signed64(raw, current.times + (current.timeCount - 1) * 8)
            let type = Int(raw[current.indices + current.timeCount - 1])
            let selected = recurrence.selection(at: last)
            guard selected.offset == signed32(raw, current.types + type * 6) else { throw .malformedData }
            var expected = current.designations + Int(raw[current.types + type * 6 + 5])
            for position in selected.abbreviation {
                guard raw[position] == raw[expected] else { throw .malformedData }
                expected += 1
            }
            guard raw[expected] == 0 else { throw .malformedData }
        }
        return (current, recurrence)
    }

    private static func header(_ bytes: UnsafeRawBufferPointer, at start: Int) throws(SQLTimeZoneError) -> Header {
        guard start >= 0, start <= bytes.count, bytes.count - start >= 44,
              bytes[start] == 84, bytes[start + 1] == 90, bytes[start + 2] == 105, bytes[start + 3] == 102 else {
            throw .malformedData
        }
        let version = bytes[start + 4]
        guard version == 50 || version == 51 || version == 52 else { throw .unsupportedData }
        for index in (start + 5)..<(start + 20) {
            guard bytes[index] == 0 else { throw .malformedData }
        }
        guard unsigned32(bytes, start + 28) == 0 else { throw .unsupportedData }
        guard let utc = Int(exactly: unsigned32(bytes, start + 20)),
              let standard = Int(exactly: unsigned32(bytes, start + 24)),
              let times = Int(exactly: unsigned32(bytes, start + 32)),
              let types = Int(exactly: unsigned32(bytes, start + 36)),
              let chars = Int(exactly: unsigned32(bytes, start + 40)),
              types > 0, types <= 256, chars > 0,
              (utc == 0 || utc == types), (standard == 0 || standard == types) else { throw .malformedData }
        return Header(version: version, utcCount: utc, standardCount: standard,
            timeCount: times, typeCount: types, charCount: chars)
    }

    private static func block(_ bytes: UnsafeRawBufferPointer, header: Header, start: Int, width: Int) throws(SQLTimeZoneError) -> Block {
        let endWide = Int128(start) + Int128(header.timeCount) * Int128(width + 1)
            + Int128(header.typeCount) * 6 + Int128(header.charCount)
            + Int128(header.standardCount) + Int128(header.utcCount)
        guard let end = Int(exactly: endWide), end <= bytes.count else { throw .malformedData }
        let indices = start + header.timeCount * width
        let types = indices + header.timeCount
        let chars = types + header.typeCount * 6
        let standard = chars + header.charCount
        let utc = standard + header.standardCount
        var previous: Int64?
        for index in 0..<header.timeCount {
            let instant = width == 8 ? signed64(bytes, start + index * width) : Int64(signed32(bytes, start + index * width))
            if let previous, instant <= previous { throw .malformedData }
            previous = instant
            guard Int(bytes[indices + index]) < header.typeCount else { throw .malformedData }
        }
        for index in 0..<header.typeCount {
            let base = types + index * 6
            guard signed32(bytes, base) != Int32.min, bytes[base + 4] <= 1 else { throw .malformedData }
            let designation = Int(bytes[base + 5])
            guard designation < header.charCount else { throw .malformedData }
            var position = chars + designation
            while position < standard, bytes[position] != 0 {
                guard bytes[position] >= 32, bytes[position] <= 126 else { throw .malformedData }
                position += 1
            }
            guard position < standard else { throw .malformedData }
            let standardFlag = header.standardCount == 0 ? 0 : bytes[standard + index]
            let utcFlag = header.utcCount == 0 ? 0 : bytes[utc + index]
            guard standardFlag <= 1, utcFlag <= 1, utcFlag == 0 || standardFlag == 1 else { throw .malformedData }
        }
        return Block(timeCount: header.timeCount, times: start, indices: indices,
            types: types, designations: chars, typeCount: header.typeCount,
            designationCount: header.charCount, end: end)
    }

    static func unsigned32(_ bytes: UnsafeRawBufferPointer, _ position: Int) -> UInt32 {
        var value: UInt32 = 0
        for offset in 0..<4 { value = (value << 8) | UInt32(bytes[position + offset]) }
        return value
    }

    static func signed32(_ bytes: UnsafeRawBufferPointer, _ position: Int) -> Int32 {
        Int32(bitPattern: unsigned32(bytes, position))
    }

    static func signed64(_ bytes: UnsafeRawBufferPointer, _ position: Int) -> Int64 {
        var value: UInt64 = 0
        for offset in 0..<8 { value = (value << 8) | UInt64(bytes[position + offset]) }
        return Int64(bitPattern: value)
    }
}
