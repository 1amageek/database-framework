import DatabaseTypes

/// Converts between a declared scalar field value and its canonical Directory
/// component (SPEC 12.2).
///
/// The mapping is total and injective per declared field kind, and injective
/// across kinds because the tag is part of the component. `decode` is the exact
/// inverse. Canonicality is enforced structurally rather than kind by kind:
/// `decode` re-encodes the value it built and rejects any input that is not
/// that exact string. One rule therefore rejects every non-canonical spelling —
/// a leading zero, a lowercase escape of a byte that needs none, and inputs the
/// value types normalize away such as a non-normalized `ExactDecimal` or a
/// negative-zero `GeographicPoint` latitude — instead of letting two components
/// resolve to two nodes for one value.
///
/// Only the scalar kinds SPEC 10.1 admits as a dynamic `#Directory` component
/// have a canonical image. Every other case is either absent, repeated, nested,
/// or unbounded, and is rejected here so that an entity declaring such a field
/// fails at declaration validation rather than producing a component this codec
/// cannot invert.
package enum DirectoryComponentCodec {

    // MARK: - Encoding

    /// The canonical component of `value`, or a typed failure when the value's
    /// kind is not admitted as a dynamic `#Directory` component.
    ///
    /// Directory APIs take an owned `String`, so the returned component is the
    /// single materialization on this path.
    package static func encode(
        _ value: FieldValue
    ) throws(DirectoryComponentCodecError) -> String {
        switch value {
        case .null:
            throw .unsupportedFieldKind("null")
        case .bool(let inner):
            return component("b", inner ? "1" : "0")
        case .int8(let inner):
            return component("i8", signedToken(inner))
        case .int16(let inner):
            return component("i16", signedToken(inner))
        case .int32(let inner):
            return component("i32", signedToken(inner))
        case .int64(let inner):
            return component("i64", signedToken(inner))
        case .uint8(let inner):
            return component("u8", unsignedToken(inner))
        case .uint16(let inner):
            return component("u16", unsignedToken(inner))
        case .uint32(let inner):
            return component("u32", unsignedToken(inner))
        case .uint64(let inner):
            return component("u64", unsignedToken(inner))
        case .float32(let inner):
            return component("f32", hexToken(inner.bitPattern, byteCount: 4))
        case .float64(let inner):
            return component("f64", hexToken(inner.bitPattern, byteCount: 8))
        case .decimal(let inner):
            return component(
                "dec",
                signedToken(inner.coefficient),
                signedToken(inner.scale)
            )
        case .string(let inner):
            return component("s", escapedToken(inner.utf8))
        case .bytes(let inner):
            return component("y", escapedToken(inner))
        case .date(let inner):
            return component(
                "d",
                signedToken(inner.year),
                unsignedToken(inner.month),
                unsignedToken(inner.day)
            )
        case .time(let inner):
            return component(
                "t",
                unsignedToken(inner.hour),
                unsignedToken(inner.minute),
                unsignedToken(inner.second),
                unsignedToken(inner.nanoseconds)
            )
        case .dateTime(let inner):
            return component(
                "dt",
                signedToken(inner.date.year),
                unsignedToken(inner.date.month),
                unsignedToken(inner.date.day),
                unsignedToken(inner.time.hour),
                unsignedToken(inner.time.minute),
                unsignedToken(inner.time.second),
                unsignedToken(inner.time.nanoseconds)
            )
        case .timestamp(let inner):
            return component(
                "ts",
                signedToken(inner.secondsSinceUnixEpoch),
                unsignedToken(inner.nanoseconds)
            )
        case .timeSpan(let inner):
            return component(
                "sp",
                signedToken(inner.seconds),
                unsignedToken(inner.nanoseconds)
            )
        case .calendarPeriod(let inner):
            return component(
                "cp",
                signedToken(inner.months),
                signedToken(inner.days)
            )
        case .geographicPoint(let inner):
            return component(
                "gp",
                hexToken(inner.latitude.bitPattern, byteCount: 8),
                hexToken(inner.longitude.bitPattern, byteCount: 8)
            )
        case .geographicPosition(let inner):
            return component(
                "gq",
                hexToken(inner.point.latitude.bitPattern, byteCount: 8),
                hexToken(inner.point.longitude.bitPattern, byteCount: 8),
                hexToken(
                    inner.ellipsoidalHeightInMeters.bitPattern,
                    byteCount: 8
                )
            )
        case .uuid(let inner):
            var token = hexToken(inner.high, byteCount: 8)
            token.append(hexToken(inner.low, byteCount: 8))
            return component("uu", token)
        case .vector:
            throw .unsupportedFieldKind("vector")
        case .array:
            throw .unsupportedFieldKind("array")
        case .object:
            throw .unsupportedFieldKind("object")
        case .reference:
            throw .unsupportedFieldKind("reference")
        case .rdfTerm:
            throw .unsupportedFieldKind("rdfTerm")
        }
    }

    // MARK: - Decoding

    /// The value whose canonical component is `component`.
    ///
    /// The parsers below are deliberately liberal about spelling; the re-encode
    /// gate at the end is the sole canonicality authority, so no non-canonical
    /// input can produce a value.
    package static func decode(
        _ component: String
    ) throws(DirectoryComponentCodecError) -> FieldValue {
        let bytes = Array(component.utf8)
        guard let separator = bytes.firstIndex(of: 0x2D) else {
            throw .malformedComponent(.missingTagSeparator)
        }
        guard separator > bytes.startIndex else {
            throw .malformedComponent(.emptyTag)
        }
        let tag = String(
            decoding: bytes[bytes.startIndex..<separator],
            as: UTF8.self
        )
        let tokens = try unescapedTokens(
            of: bytes[bytes.index(after: separator)...]
        )
        let value = try decodedValue(tag: tag, tokens: tokens)
        let canonical: String
        do {
            canonical = try encode(value)
        } catch {
            preconditionFailure(
                "Decoding produced a kind the canonical component codec rejects"
            )
        }
        guard canonical == component else {
            throw .nonCanonicalComponent(canonical: canonical)
        }
        return value
    }

    private static func decodedValue(
        tag: String,
        tokens: [[UInt8]]
    ) throws(DirectoryComponentCodecError) -> FieldValue {
        switch tag {
        case "b":
            let token = try only(tokens)
            guard token.count == 1 else {
                throw .malformedComponent(.invalidValue)
            }
            switch token[0] {
            case 0x30:
                return .bool(false)
            case 0x31:
                return .bool(true)
            default:
                throw .malformedComponent(.invalidValue)
            }
        case "i8":
            return .int8(try signedInteger(try only(tokens), as: Int8.self))
        case "i16":
            return .int16(try signedInteger(try only(tokens), as: Int16.self))
        case "i32":
            return .int32(try signedInteger(try only(tokens), as: Int32.self))
        case "i64":
            return .int64(try signedInteger(try only(tokens), as: Int64.self))
        case "u8":
            return .uint8(try unsignedInteger(try only(tokens), as: UInt8.self))
        case "u16":
            return .uint16(
                try unsignedInteger(try only(tokens), as: UInt16.self)
            )
        case "u32":
            return .uint32(
                try unsignedInteger(try only(tokens), as: UInt32.self)
            )
        case "u64":
            return .uint64(
                try unsignedInteger(try only(tokens), as: UInt64.self)
            )
        case "f32":
            let bits = try hexadecimal(
                try only(tokens),
                byteCount: 4,
                as: UInt32.self
            )
            return .float32(Float(bitPattern: bits))
        case "f64":
            let bits = try hexadecimal(
                try only(tokens),
                byteCount: 8,
                as: UInt64.self
            )
            return .float64(Double(bitPattern: bits))
        case "dec":
            try exactly(2, tokens)
            let coefficient = try signedInteger(tokens[0], as: Int128.self)
            let scale = try signedInteger(tokens[1], as: Int32.self)
            return .decimal(
                ExactDecimal(coefficient: coefficient, scale: scale)
            )
        case "s":
            let token = try only(tokens)
            guard let text = String(validating: token, as: UTF8.self) else {
                throw .malformedComponent(.invalidUTF8)
            }
            return .string(text)
        case "y":
            return .bytes(ByteString(try only(tokens)))
        case "d":
            try exactly(3, tokens)
            return .date(try civilDate(tokens[0], tokens[1], tokens[2]))
        case "t":
            try exactly(4, tokens)
            return .time(
                try civilTime(tokens[0], tokens[1], tokens[2], tokens[3])
            )
        case "dt":
            try exactly(7, tokens)
            let date = try civilDate(tokens[0], tokens[1], tokens[2])
            let time = try civilTime(
                tokens[3],
                tokens[4],
                tokens[5],
                tokens[6]
            )
            return .dateTime(CivilDateTime(date: date, time: time))
        case "ts":
            try exactly(2, tokens)
            let seconds = try signedInteger(tokens[0], as: Int64.self)
            let nanoseconds = try unsignedInteger(tokens[1], as: UInt32.self)
            do {
                return .timestamp(
                    try Timestamp(
                        secondsSinceUnixEpoch: seconds,
                        nanoseconds: nanoseconds
                    )
                )
            } catch {
                throw .malformedComponent(.invalidValue)
            }
        case "sp":
            try exactly(2, tokens)
            let seconds = try signedInteger(tokens[0], as: Int64.self)
            let nanoseconds = try unsignedInteger(tokens[1], as: UInt32.self)
            do {
                return .timeSpan(
                    try TimeSpan(seconds: seconds, nanoseconds: nanoseconds)
                )
            } catch {
                throw .malformedComponent(.invalidValue)
            }
        case "cp":
            try exactly(2, tokens)
            let months = try signedInteger(tokens[0], as: Int64.self)
            let days = try signedInteger(tokens[1], as: Int64.self)
            return .calendarPeriod(
                CalendarPeriod(months: months, days: days)
            )
        case "gp":
            try exactly(2, tokens)
            return .geographicPoint(
                try geographicPoint(tokens[0], tokens[1])
            )
        case "gq":
            try exactly(3, tokens)
            let point = try geographicPoint(tokens[0], tokens[1])
            let height = try hexadecimal(
                tokens[2],
                byteCount: 8,
                as: UInt64.self
            )
            do {
                return .geographicPosition(
                    try GeographicPosition(
                        point: point,
                        ellipsoidalHeightInMeters: Double(bitPattern: height)
                    )
                )
            } catch {
                throw .malformedComponent(.invalidValue)
            }
        case "uu":
            let token = try only(tokens)
            guard token.count == 32 else {
                throw .malformedComponent(.invalidHexadecimal)
            }
            let high = try hexadecimal(
                token.prefix(16),
                byteCount: 8,
                as: UInt64.self
            )
            let low = try hexadecimal(
                token.suffix(16),
                byteCount: 8,
                as: UInt64.self
            )
            return .uuid(UUID(high: high, low: low))
        default:
            throw .unknownTag(tag)
        }
    }

    // MARK: - Value construction

    private static func civilDate(
        _ year: [UInt8],
        _ month: [UInt8],
        _ day: [UInt8]
    ) throws(DirectoryComponentCodecError) -> CivilDate {
        let year = try signedInteger(year, as: Int32.self)
        let month = try unsignedInteger(month, as: UInt8.self)
        let day = try unsignedInteger(day, as: UInt8.self)
        do {
            return try CivilDate(year: year, month: month, day: day)
        } catch {
            throw .malformedComponent(.invalidValue)
        }
    }

    private static func civilTime(
        _ hour: [UInt8],
        _ minute: [UInt8],
        _ second: [UInt8],
        _ nanoseconds: [UInt8]
    ) throws(DirectoryComponentCodecError) -> CivilTime {
        let hour = try unsignedInteger(hour, as: UInt8.self)
        let minute = try unsignedInteger(minute, as: UInt8.self)
        let second = try unsignedInteger(second, as: UInt8.self)
        let nanoseconds = try unsignedInteger(nanoseconds, as: UInt32.self)
        do {
            return try CivilTime(
                hour: hour,
                minute: minute,
                second: second,
                nanoseconds: nanoseconds
            )
        } catch {
            throw .malformedComponent(.invalidValue)
        }
    }

    private static func geographicPoint(
        _ latitude: [UInt8],
        _ longitude: [UInt8]
    ) throws(DirectoryComponentCodecError) -> GeographicPoint {
        let latitude = try hexadecimal(latitude, byteCount: 8, as: UInt64.self)
        let longitude = try hexadecimal(
            longitude,
            byteCount: 8,
            as: UInt64.self
        )
        do {
            return try GeographicPoint(
                latitude: Double(bitPattern: latitude),
                longitude: Double(bitPattern: longitude)
            )
        } catch {
            throw .malformedComponent(.invalidValue)
        }
    }

    // MARK: - Token composition

    private static func component(
        _ tag: String,
        _ tokens: String...
    ) -> String {
        var capacity = tag.utf8.count + 1
        for token in tokens {
            capacity += token.utf8.count + 1
        }
        var text = String()
        text.reserveCapacity(capacity)
        text.append(tag)
        text.append("-")
        var isFirst = true
        for token in tokens {
            if isFirst {
                isFirst = false
            } else {
                text.append(".")
            }
            text.append(token)
        }
        return text
    }

    private static func unsignedToken(
        _ value: some UnsignedInteger & FixedWidthInteger
    ) -> String {
        if value == 0 {
            return "0"
        }
        var digits: [UInt8] = []
        digits.reserveCapacity(40)
        var remaining = value
        while remaining > 0 {
            digits.append(0x30 + UInt8(truncatingIfNeeded: remaining % 10))
            remaining /= 10
        }
        digits.reverse()
        return String(decoding: digits, as: UTF8.self)
    }

    private static func signedToken(
        _ value: some SignedInteger & FixedWidthInteger
    ) -> String {
        let magnitude = unsignedToken(value.magnitude)
        guard value < 0 else {
            return magnitude
        }
        var text = String()
        text.reserveCapacity(magnitude.utf8.count + 1)
        text.append("n")
        text.append(magnitude)
        return text
    }

    private static func hexToken(
        _ value: some UnsignedInteger & FixedWidthInteger,
        byteCount: Int
    ) -> String {
        var digits: [UInt8] = []
        digits.reserveCapacity(byteCount * 2)
        var shift = byteCount * 8
        while shift > 0 {
            shift -= 4
            digits.append(
                hexDigit(UInt8(truncatingIfNeeded: (value >> shift) & 0x0F))
            )
        }
        return String(decoding: digits, as: UTF8.self)
    }

    private static func escapedToken(
        _ bytes: some Sequence<UInt8>
    ) -> String {
        var output: [UInt8] = []
        output.reserveCapacity(bytes.underestimatedCount)
        for byte in bytes {
            if isUnreserved(byte) {
                output.append(byte)
            } else {
                output.append(0x25)
                output.append(hexDigit(byte >> 4))
                output.append(hexDigit(byte & 0x0F))
            }
        }
        return String(decoding: output, as: UTF8.self)
    }

    // MARK: - Token parsing

    private static func unescapedTokens(
        of body: ArraySlice<UInt8>
    ) throws(DirectoryComponentCodecError) -> [[UInt8]] {
        var tokens: [[UInt8]] = []
        var current: [UInt8] = []
        var index = body.startIndex
        while index < body.endIndex {
            let byte = body[index]
            switch byte {
            case 0x2E:
                tokens.append(current)
                current = []
                index = body.index(after: index)
            case 0x25:
                guard body.distance(from: index, to: body.endIndex) >= 3 else {
                    throw .malformedComponent(.invalidEscape)
                }
                let highIndex = body.index(after: index)
                let lowIndex = body.index(after: highIndex)
                guard
                    let high = nibble(body[highIndex]),
                    let low = nibble(body[lowIndex])
                else {
                    throw .malformedComponent(.invalidEscape)
                }
                current.append(high << 4 | low)
                index = body.index(after: lowIndex)
            default:
                guard isUnreserved(byte) else {
                    throw .malformedComponent(.invalidCharacter)
                }
                current.append(byte)
                index = body.index(after: index)
            }
        }
        tokens.append(current)
        return tokens
    }

    private static func only(
        _ tokens: [[UInt8]]
    ) throws(DirectoryComponentCodecError) -> [UInt8] {
        guard tokens.count == 1 else {
            throw .malformedComponent(
                .tokenCount(expected: 1, actual: tokens.count)
            )
        }
        return tokens[0]
    }

    private static func exactly(
        _ count: Int,
        _ tokens: [[UInt8]]
    ) throws(DirectoryComponentCodecError) {
        guard tokens.count == count else {
            throw .malformedComponent(
                .tokenCount(expected: count, actual: tokens.count)
            )
        }
    }

    private static func unsignedInteger<T: UnsignedInteger & FixedWidthInteger>(
        _ token: [UInt8],
        as type: T.Type
    ) throws(DirectoryComponentCodecError) -> T {
        try magnitude(token, as: T.self)
    }

    private static func signedInteger<T: SignedInteger & FixedWidthInteger>(
        _ token: [UInt8],
        as type: T.Type
    ) throws(DirectoryComponentCodecError) -> T {
        guard token.first == 0x6E else {
            let value = try magnitude(token, as: T.Magnitude.self)
            guard value <= T.Magnitude(T.max) else {
                throw .malformedComponent(.numberOutOfRange)
            }
            return T(value)
        }
        let value = try magnitude(token.dropFirst(), as: T.Magnitude.self)
        let limit = T.Magnitude(T.max) + 1
        guard value <= limit else {
            throw .malformedComponent(.numberOutOfRange)
        }
        guard value < limit else {
            return T.min
        }
        return 0 - T(value)
    }

    private static func magnitude<T: UnsignedInteger & FixedWidthInteger>(
        _ digits: some Collection<UInt8>,
        as type: T.Type
    ) throws(DirectoryComponentCodecError) -> T {
        guard !digits.isEmpty else {
            throw .malformedComponent(.invalidNumber)
        }
        var value: T = 0
        for byte in digits {
            guard byte >= 0x30, byte <= 0x39 else {
                throw .malformedComponent(.invalidNumber)
            }
            let scaled = value.multipliedReportingOverflow(by: 10)
            guard !scaled.overflow else {
                throw .malformedComponent(.numberOutOfRange)
            }
            let sum = scaled.partialValue.addingReportingOverflow(
                T(byte - 0x30)
            )
            guard !sum.overflow else {
                throw .malformedComponent(.numberOutOfRange)
            }
            value = sum.partialValue
        }
        return value
    }

    private static func hexadecimal<T: UnsignedInteger & FixedWidthInteger>(
        _ token: some Collection<UInt8>,
        byteCount: Int,
        as type: T.Type
    ) throws(DirectoryComponentCodecError) -> T {
        guard token.count == byteCount * 2 else {
            throw .malformedComponent(.invalidHexadecimal)
        }
        var value: T = 0
        for byte in token {
            guard let digit = nibble(byte) else {
                throw .malformedComponent(.invalidHexadecimal)
            }
            value = value << 4 | T(digit)
        }
        return value
    }

    // MARK: - Byte classification

    private static func isUnreserved(_ byte: UInt8) -> Bool {
        switch byte {
        case 0x41...0x5A, 0x61...0x7A, 0x30...0x39, 0x5F, 0x7E:
            return true
        default:
            return false
        }
    }

    private static func hexDigit(_ nibble: UInt8) -> UInt8 {
        nibble < 10 ? 0x30 + nibble : 0x37 + nibble
    }

    private static func nibble(_ byte: UInt8) -> UInt8? {
        switch byte {
        case 0x30...0x39:
            return byte - 0x30
        case 0x41...0x46:
            return byte - 0x37
        default:
            return nil
        }
    }
}
