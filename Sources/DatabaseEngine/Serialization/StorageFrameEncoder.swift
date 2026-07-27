import DatabaseTypes
import DatabaseKit

/// Encodes engine-owned metadata directly into one final `ByteString`.
///
/// Encoding first measures the frame and then writes into an exact-size
/// allocation. Borrowed source buffers never escape a synchronous write.
package struct StorageFrameEncoder {
    private enum Destination {
        case measuring
        case fixed(UnsafeMutableRawBufferPointer)
    }

    package let limits: StorageFrameLimits
    private var destination: Destination
    private var offset: Int
    private var nestingDepth: Int
    private var deferredError: StorageFrameError?

    private init(
        destination: Destination,
        limits: StorageFrameLimits
    ) {
        self.destination = destination
        self.limits = limits
        self.offset = 0
        self.nestingDepth = 0
        self.deferredError = nil
    }

    package static func encode(
        limits: StorageFrameLimits = .default,
        _ body: (inout StorageFrameEncoder) throws(StorageFrameError) -> Void
    ) throws(StorageFrameError) -> ByteString {
        let byteCount = try measure(limits: limits, body)

        return try ByteString.copying(count: byteCount) {
            output throws(StorageFrameError) in
            var encoder = StorageFrameEncoder(
                destination: .fixed(output),
                limits: limits
            )
            try body(&encoder)
            try encoder.finish()
            guard encoder.offset == byteCount else {
                throw .byteCountOverflow
            }
        }
    }

    package static func measure(
        limits: StorageFrameLimits = .default,
        _ body: (inout StorageFrameEncoder) throws(StorageFrameError) -> Void
    ) throws(StorageFrameError) -> Int {
        var measurement = StorageFrameEncoder(
            destination: .measuring,
            limits: limits
        )
        try body(&measurement)
        try measurement.finish()
        let byteCount = measurement.offset
        guard byteCount <= limits.maximumFrameBytes else {
            throw .frameTooLarge(
                actual: byteCount,
                maximum: limits.maximumFrameBytes
            )
        }
        return byteCount
    }

    /// Encodes metadata whose semantic validation can raise a domain error in
    /// addition to frame errors. The same body is measured and written; no
    /// intermediate byte buffer is created.
    package static func encodeReportingFailure(
        limits: StorageFrameLimits = .default,
        _ body: (inout StorageFrameEncoder) throws -> Void
    ) throws -> ByteString {
        var measurement = StorageFrameEncoder(
            destination: .measuring,
            limits: limits
        )
        try body(&measurement)
        try measurement.finish()
        let byteCount = measurement.offset
        guard byteCount <= limits.maximumFrameBytes else {
            throw StorageFrameError.frameTooLarge(
                actual: byteCount,
                maximum: limits.maximumFrameBytes
            )
        }

        return try ByteString.copying(count: byteCount) { output in
            var encoder = StorageFrameEncoder(
                destination: .fixed(output),
                limits: limits
            )
            try body(&encoder)
            try encoder.finish()
            guard encoder.offset == byteCount else {
                throw StorageFrameError.byteCountOverflow
            }
        }
    }

    package mutating func writeUInt8(_ value: UInt8) {
        appendByte(value)
    }

    package mutating func writeInt8(_ value: Int8) {
        writeUInt8(UInt8(bitPattern: value))
    }

    package mutating func writeBool(_ value: Bool) {
        writeUInt8(value ? 1 : 0)
    }

    package mutating func writeUInt16(_ value: UInt16) {
        var value = value.littleEndian
        withUnsafeBytes(of: &value) { appendBytes($0) }
    }

    package mutating func writeInt16(_ value: Int16) {
        writeUInt16(UInt16(bitPattern: value))
    }

    package mutating func writeUInt32(_ value: UInt32) {
        var value = value.littleEndian
        withUnsafeBytes(of: &value) { appendBytes($0) }
    }

    package mutating func writeInt32(_ value: Int32) {
        writeUInt32(UInt32(bitPattern: value))
    }

    package mutating func writeUInt64(_ value: UInt64) {
        var value = value.littleEndian
        withUnsafeBytes(of: &value) { appendBytes($0) }
    }

    package mutating func writeInt64(_ value: Int64) {
        writeUInt64(UInt64(bitPattern: value))
    }

    package mutating func writeInt128(_ value: Int128) {
        let bits = UInt128(bitPattern: value)
        writeUInt64(UInt64(truncatingIfNeeded: bits))
        writeUInt64(UInt64(truncatingIfNeeded: bits >> 64))
    }

    package mutating func writeFloat(_ value: Float) {
        writeUInt32(value.bitPattern)
    }

    package mutating func writeDouble(_ value: Double) {
        writeUInt64(value.bitPattern)
    }

    package mutating func writeCount(
        _ count: Int
    ) throws(StorageFrameError) {
        guard count <= limits.maximumCollectionCount else {
            throw .collectionTooLarge(
                actual: count,
                maximum: limits.maximumCollectionCount
            )
        }
        try writeLength(count)
    }

    package mutating func writeBytes(
        _ value: ByteString
    ) throws(StorageFrameError) {
        guard value.count <= limits.maximumByteStringBytes else {
            throw .byteStringTooLarge(
                actual: value.count,
                maximum: limits.maximumByteStringBytes
            )
        }
        try writeLength(value.count)
        value.withUnsafeBytes { appendBytes($0) }
    }

    package mutating func writeOptionalBytes(
        _ value: ByteString?
    ) throws(StorageFrameError) {
        writeBool(value != nil)
        if let value {
            try writeBytes(value)
        }
    }

    package mutating func writeString(
        _ value: String
    ) throws(StorageFrameError) {
        let byteCount = value.utf8.count
        guard byteCount <= limits.maximumStringBytes else {
            throw .stringTooLarge(
                actual: byteCount,
                maximum: limits.maximumStringBytes
            )
        }
        try writeLength(byteCount)
        guard case .fixed = destination else {
            advance(by: byteCount)
            return
        }
        let emitted = value.utf8.withContiguousStorageIfAvailable {
            appendBytes(UnsafeRawBufferPointer($0))
            return true
        } ?? false
        if !emitted {
            for byte in value.utf8 {
                appendByte(byte)
            }
        }
    }

    package mutating func writeOptionalString(
        _ value: String?
    ) throws(StorageFrameError) {
        writeBool(value != nil)
        if let value {
            try writeString(value)
        }
    }

    package mutating func writeRDFTerm(
        _ term: RDFTerm
    ) throws(StorageFrameError) {
        let codecLimits: RDFTermStorageLimits
        do {
            codecLimits = try RDFTermStorageLimits(
                maximumBytes: limits.maximumByteStringBytes,
                maximumDepth: limits.maximumNestingDepth,
                maximumObjectCount: limits.maximumCollectionCount
            )
        } catch {
            throw .invalidRDFTermLimits(error)
        }

        let plan: RDFTermStorageEncoding
        do {
            plan = try RDFTermStorageFormat.encodingPlan(term, limits: codecLimits)
        } catch {
            throw .invalidRDFTerm(error)
        }
        guard let byteCount = UInt32(exactly: plan.byteCount) else {
            throw .byteCountOverflow
        }
        writeUInt32(byteCount)
        do {
            try RDFTermStorageFormat.encode(plan, into: &self)
        } catch {
            throw .invalidRDFTerm(error)
        }
    }

    package mutating func writeLengthPrefixed(
        _ body: (inout StorageFrameEncoder) throws(StorageFrameError) -> Void
    ) throws(StorageFrameError) {
        let lengthOffset = offset
        writeUInt32(0)
        let payloadOffset = offset
        let enclosingDepth = nestingDepth
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
        nestingDepth = nextDepth.partialValue
        do {
            try body(&self)
            nestingDepth = enclosingDepth
        } catch {
            nestingDepth = enclosingDepth
            throw error
        }

        let payloadByteCount = offset - payloadOffset
        guard payloadByteCount <= limits.maximumByteStringBytes,
              let encodedLength = UInt32(exactly: payloadByteCount) else {
            throw .byteStringTooLarge(
                actual: payloadByteCount,
                maximum: limits.maximumByteStringBytes
            )
        }
        if case .fixed(let output) = destination {
            output[lengthOffset] = UInt8(truncatingIfNeeded: encodedLength)
            output[lengthOffset + 1] = UInt8(
                truncatingIfNeeded: encodedLength >> 8
            )
            output[lengthOffset + 2] = UInt8(
                truncatingIfNeeded: encodedLength >> 16
            )
            output[lengthOffset + 3] = UInt8(
                truncatingIfNeeded: encodedLength >> 24
            )
        }
    }

    private mutating func writeLength(
        _ count: Int
    ) throws(StorageFrameError) {
        guard count >= 0, let count = UInt32(exactly: count) else {
            throw .byteCountOverflow
        }
        writeUInt32(count)
    }

    private mutating func appendByte(_ value: UInt8) {
        var value = value
        withUnsafeBytes(of: &value) { appendBytes($0) }
    }

    private mutating func appendBytes(_ source: UnsafeRawBufferPointer) {
        guard deferredError == nil else { return }
        let nextOffset = offset.addingReportingOverflow(source.count)
        guard !nextOffset.overflow else {
            deferredError = .byteCountOverflow
            return
        }
        switch destination {
        case .measuring:
            offset = nextOffset.partialValue
        case .fixed(let output):
            guard offset <= output.count,
                  source.count <= output.count - offset else {
                deferredError = .byteCountOverflow
                return
            }
            UnsafeMutableRawBufferPointer(
                rebasing: output[offset..<nextOffset.partialValue]
            ).copyMemory(from: source)
            offset = nextOffset.partialValue
        }
    }

    private mutating func advance(by count: Int) {
        guard deferredError == nil else { return }
        let nextOffset = offset.addingReportingOverflow(count)
        guard !nextOffset.overflow else {
            deferredError = .byteCountOverflow
            return
        }
        offset = nextOffset.partialValue
    }

    private func finish() throws(StorageFrameError) {
        if let deferredError {
            throw deferredError
        }
        guard nestingDepth == 0 else {
            throw .byteCountOverflow
        }
    }
}

extension StorageFrameEncoder: RDFTermStorageSink {
    package mutating func write(_ byte: UInt8) {
        appendByte(byte)
    }

    package mutating func write(_ bytes: UnsafeRawBufferPointer) {
        appendBytes(bytes)
    }
}
