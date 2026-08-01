import DatabaseTypes

/// Retains one validated persisted Float32 vector payload without materializing
/// an element array.
///
/// The payload is canonical little-endian storage. Its bytes are borrowed only
/// for the duration of a synchronous calculation, so an unaligned backend view
/// remains valid without rebinding or escaping its pointer.
struct PersistedVectorView: Sendable {
    let payload: ByteString
    let count: Int

    init(
        payload: ByteString,
        expectedCount: Int
    ) throws(VectorIndexError) {
        guard expectedCount >= 0 else {
            throw .invalidArgument(
                "Persisted vector dimensions must not be negative"
            )
        }
        let (expectedByteCount, overflow) = expectedCount
            .multipliedReportingOverflow(by: MemoryLayout<Float>.stride)
        guard !overflow else {
            throw .invalidArgument(
                "Persisted vector dimensions exceed the current platform limit"
            )
        }
        guard payload.count == expectedByteCount else {
            throw .invalidStructure(
                "Vector payload length \(payload.count) does not match expected dimension \(expectedCount)"
            )
        }
        self.payload = payload
        self.count = expectedCount
    }

    func withUnsafeBytes<Value>(
        _ body: (UnsafeRawBufferPointer) throws(VectorIndexError) -> Value
    ) throws(VectorIndexError) -> Value {
        var outcome: Result<Value, VectorIndexError>?
        payload.withUnsafeBytes { bytes in
            outcome = Result {
                () throws(VectorIndexError) -> Value in
                try body(bytes)
            }
        }
        guard let outcome else {
            preconditionFailure("Persisted vector borrow produced no result")
        }
        switch outcome {
        case .success(let value):
            return value
        case .failure(let failure):
            throw failure
        }
    }

    @inline(__always)
    static func element(
        at index: Int,
        in bytes: UnsafeRawBufferPointer
    ) throws(VectorIndexError) -> Float {
        precondition(index >= 0)
        precondition(MemoryLayout<Float>.size == MemoryLayout<UInt32>.size)
        let (byteOffset, overflow) = index.multipliedReportingOverflow(
            by: MemoryLayout<UInt32>.stride
        )
        precondition(!overflow)
        precondition(bytes.count >= MemoryLayout<UInt32>.size)
        precondition(byteOffset <= bytes.count - MemoryLayout<UInt32>.size)
        let bits = bytes.baseAddress!
            .advanced(by: byteOffset)
            .loadUnaligned(as: UInt32.self)
#if _endian(little)
        let value = Float(bitPattern: bits)
#else
        let value = Float(bitPattern: bits.byteSwapped)
#endif
        guard value.isFinite else {
            throw .invalidStructure(
                "Persisted vector contains a non-finite Float32 element at index \(index)"
            )
        }
        return value
    }
}
