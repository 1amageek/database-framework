import DatabaseTypes

/// Decodes engine-owned metadata while retaining the original frame owner.
///
/// Byte payload reads return constant-time `ByteString` slices. Only semantic
/// values such as `String` are materialized at their required output boundary.
package struct StorageFrameDecoder: Sendable {
    private let bytes: ByteString
    private var offset: Int
    private var nestingDepth: Int
    package let limits: StorageFrameLimits

    package init(
        _ bytes: ByteString,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) {
        guard bytes.count <= limits.maximumFrameBytes else {
            throw .frameTooLarge(
                actual: bytes.count,
                maximum: limits.maximumFrameBytes
            )
        }
        self.bytes = bytes
        self.offset = 0
        self.nestingDepth = 0
        self.limits = limits
    }

    package var remainingCount: Int {
        bytes.count - offset
    }

    package mutating func readUInt8() throws(StorageFrameError) -> UInt8 {
        guard offset < bytes.count else {
            throw .truncated
        }
        let value = bytes[bytes.startIndex + offset]
        offset += 1
        return value
    }

    package mutating func readBool() throws(StorageFrameError) -> Bool {
        let value = try readUInt8()
        switch value {
        case 0:
            return false
        case 1:
            return true
        default:
            throw .invalidBool(value)
        }
    }

    package mutating func readUInt16() throws(StorageFrameError) -> UInt16 {
        let payload = try readRawBytes(count: 2)
        return payload.withUnsafeBytes {
            UInt16($0[0]) | (UInt16($0[1]) << 8)
        }
    }

    package mutating func readInt8() throws(StorageFrameError) -> Int8 {
        Int8(bitPattern: try readUInt8())
    }

    package mutating func readInt16() throws(StorageFrameError) -> Int16 {
        Int16(bitPattern: try readUInt16())
    }

    package mutating func readUInt32() throws(StorageFrameError) -> UInt32 {
        let payload = try readRawBytes(count: 4)
        return payload.withUnsafeBytes {
            UInt32($0[0])
                | (UInt32($0[1]) << 8)
                | (UInt32($0[2]) << 16)
                | (UInt32($0[3]) << 24)
        }
    }

    package mutating func readInt32() throws(StorageFrameError) -> Int32 {
        Int32(bitPattern: try readUInt32())
    }

    package mutating func readUInt64() throws(StorageFrameError) -> UInt64 {
        let payload = try readRawBytes(count: 8)
        return payload.withUnsafeBytes {
            UInt64($0[0])
                | (UInt64($0[1]) << 8)
                | (UInt64($0[2]) << 16)
                | (UInt64($0[3]) << 24)
                | (UInt64($0[4]) << 32)
                | (UInt64($0[5]) << 40)
                | (UInt64($0[6]) << 48)
                | (UInt64($0[7]) << 56)
        }
    }

    package mutating func readInt64() throws(StorageFrameError) -> Int64 {
        Int64(bitPattern: try readUInt64())
    }

    package mutating func readInt128() throws(StorageFrameError) -> Int128 {
        let lower = UInt128(try readUInt64())
        let upper = UInt128(try readUInt64()) << 64
        return Int128(bitPattern: upper | lower)
    }

    package mutating func readFloat() throws(StorageFrameError) -> Float {
        Float(bitPattern: try readUInt32())
    }

    package mutating func readDouble() throws(StorageFrameError) -> Double {
        Double(bitPattern: try readUInt64())
    }

    package mutating func readCount() throws(StorageFrameError) -> Int {
        let count = try readLength()
        guard count <= limits.maximumCollectionCount else {
            throw .collectionTooLarge(
                actual: count,
                maximum: limits.maximumCollectionCount
            )
        }
        return count
    }

    package mutating func readBytes() throws(StorageFrameError) -> ByteString {
        let count = try readLength()
        guard count <= limits.maximumByteStringBytes else {
            throw .byteStringTooLarge(
                actual: count,
                maximum: limits.maximumByteStringBytes
            )
        }
        return try readRawBytes(count: count)
    }

    package mutating func readOptionalBytes() throws(
        StorageFrameError
    ) -> ByteString? {
        try readBool() ? try readBytes() : nil
    }

    package mutating func readString() throws(StorageFrameError) -> String {
        let count = try readLength()
        guard count <= limits.maximumStringBytes else {
            throw .stringTooLarge(
                actual: count,
                maximum: limits.maximumStringBytes
            )
        }
        let payload = try readRawBytes(count: count)
        guard let value = String(validating: payload, as: UTF8.self) else {
            throw .invalidUTF8
        }
        return value
    }

    /// Validates one encoded String while returning only a retained view of
    /// its UTF-8 bytes. Footprint scans use this path so admission can inspect
    /// the complete frame without materializing a String before its retained
    /// storage has been reserved.
    package mutating func readValidatedStringBytes() throws(
        StorageFrameError
    ) -> ByteString {
        let count = try readLength()
        guard count <= limits.maximumStringBytes else {
            throw .stringTooLarge(
                actual: count,
                maximum: limits.maximumStringBytes
            )
        }
        let payload = try readRawBytes(count: count)
        let isValid = payload.withUnsafeBytes { bytes in
            Self.isValidUTF8(bytes)
        }
        guard isValid else {
            throw .invalidUTF8
        }
        return payload
    }

    package mutating func readOptionalString() throws(
        StorageFrameError
    ) -> String? {
        try readBool() ? try readString() : nil
    }

    package mutating func readRDFTerm() throws(
        StorageFrameError
    ) -> RDFTerm {
        let byteCount = try readUInt32()
        guard let byteCount = Int(exactly: byteCount) else {
            throw .byteCountOverflow
        }
        guard byteCount <= limits.maximumByteStringBytes else {
            throw .byteStringTooLarge(
                actual: byteCount,
                maximum: limits.maximumByteStringBytes
            )
        }
        let bytes = try readRawBytes(count: byteCount)
        let termLimits: RDFTermStorageLimits
        do {
            termLimits = try RDFTermStorageLimits(
                maximumBytes: limits.maximumByteStringBytes,
                maximumDepth: limits.maximumNestingDepth,
                maximumObjectCount: limits.maximumCollectionCount
            )
        } catch {
            throw .invalidRDFTermLimits(error)
        }
        do {
            return try RDFTermStorageFormat.decode(
                bytes,
                limits: termLimits
            )
        } catch {
            throw .invalidRDFTerm(error)
        }
    }

    package mutating func readLengthPrefixed<Result>(
        _ body: (inout StorageFrameDecoder) throws(StorageFrameError) -> Result
    ) throws(StorageFrameError) -> Result {
        let payload = try readBytes()
        let nextDepth = nestingDepth.addingReportingOverflow(1)
        guard !nextDepth.overflow else {
            throw .byteCountOverflow
        }
        guard nextDepth.partialValue <= limits.maximumNestingDepth else {
            throw .nestingTooDeep(
                actual: nextDepth.partialValue,
                maximum: limits.maximumNestingDepth
            )
        }
        var child = try StorageFrameDecoder(payload, limits: limits)
        child.nestingDepth = nextDepth.partialValue
        let result = try body(&child)
        try child.ensureFullyRead()
        return result
    }

    package func ensureFullyRead() throws(StorageFrameError) {
        guard remainingCount == 0 else {
            throw .trailingBytes
        }
    }

    private mutating func readLength() throws(StorageFrameError) -> Int {
        let value = try readUInt32()
        guard let count = Int(exactly: value) else {
            throw .byteCountOverflow
        }
        return count
    }

    package mutating func readRawBytes(
        count: Int
    ) throws(StorageFrameError) -> ByteString {
        guard count >= 0, count <= bytes.count - offset else {
            throw .truncated
        }
        let lowerBound = bytes.startIndex + offset
        let upperBound = lowerBound + count
        offset += count
        return bytes[lowerBound..<upperBound]
    }

    private static func isValidUTF8(
        _ bytes: UnsafeRawBufferPointer
    ) -> Bool {
        var index = 0
        while index < bytes.count {
            let first = bytes[index]
            switch first {
            case 0x00...0x7F:
                index += 1
            case 0xC2...0xDF:
                guard hasContinuationBytes(
                    bytes,
                    startingAt: index + 1,
                    count: 1
                ) else {
                    return false
                }
                index += 2
            case 0xE0:
                guard index + 2 < bytes.count,
                      (0xA0...0xBF).contains(bytes[index + 1]),
                      isContinuation(bytes[index + 2]) else {
                    return false
                }
                index += 3
            case 0xE1...0xEC, 0xEE...0xEF:
                guard hasContinuationBytes(
                    bytes,
                    startingAt: index + 1,
                    count: 2
                ) else {
                    return false
                }
                index += 3
            case 0xED:
                guard index + 2 < bytes.count,
                      (0x80...0x9F).contains(bytes[index + 1]),
                      isContinuation(bytes[index + 2]) else {
                    return false
                }
                index += 3
            case 0xF0:
                guard index + 3 < bytes.count,
                      (0x90...0xBF).contains(bytes[index + 1]),
                      isContinuation(bytes[index + 2]),
                      isContinuation(bytes[index + 3]) else {
                    return false
                }
                index += 4
            case 0xF1...0xF3:
                guard hasContinuationBytes(
                    bytes,
                    startingAt: index + 1,
                    count: 3
                ) else {
                    return false
                }
                index += 4
            case 0xF4:
                guard index + 3 < bytes.count,
                      (0x80...0x8F).contains(bytes[index + 1]),
                      isContinuation(bytes[index + 2]),
                      isContinuation(bytes[index + 3]) else {
                    return false
                }
                index += 4
            default:
                return false
            }
        }
        return true
    }

    private static func hasContinuationBytes(
        _ bytes: UnsafeRawBufferPointer,
        startingAt start: Int,
        count: Int
    ) -> Bool {
        guard start <= bytes.count,
              count <= bytes.count - start else {
            return false
        }
        for index in start..<(start + count) where !isContinuation(bytes[index]) {
            return false
        }
        return true
    }

    private static func isContinuation(_ byte: UInt8) -> Bool {
        (byte & 0xC0) == 0x80
    }
}
