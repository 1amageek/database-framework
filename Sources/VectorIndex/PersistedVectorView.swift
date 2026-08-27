import DatabaseTypes

/// A noncopyable scalar view over canonical persisted Float32 bytes.
///
/// The raw buffer is private so its pointer cannot escape the synchronous
/// borrow that keeps the payload owner alive.
struct PersistedVectorElements: ~Copyable {
    private let bytes: UnsafeRawBufferPointer
    let count: Int

    fileprivate init(bytes: UnsafeRawBufferPointer, count: Int) {
        self.bytes = bytes
        self.count = count
    }

    @inline(__always)
    borrowing func element(at index: Int) throws(VectorIndexError) -> Float {
        precondition(index >= 0 && index < count)
        precondition(MemoryLayout<Float>.size == MemoryLayout<UInt32>.size)
        let (byteOffset, overflow) = index.multipliedReportingOverflow(
            by: MemoryLayout<UInt32>.stride
        )
        precondition(!overflow)
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

/// Retains one validated persisted Float32 vector payload without materializing
/// an element array.
///
/// The payload is canonical little-endian storage. Callers receive only a
/// noncopyable scalar view, so an unaligned backend buffer remains valid
/// without rebinding or exposing an escapable pointer.
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

    func withElements<Value, Failure: Error>(
        _ body: (borrowing PersistedVectorElements) throws(Failure) -> Value
    ) throws(Failure) -> Value {
        var outcome: Result<Value, Failure>?
        payload.withUnsafeBytes { bytes in
            let elements = PersistedVectorElements(bytes: bytes, count: count)
            outcome = Result {
                () throws(Failure) -> Value in
                try body(elements)
            }
        }
        guard let outcome else {
            preconditionFailure("Persisted vector borrow produced no result")
        }
        return try outcome.get()
    }
}
