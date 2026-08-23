import DatabaseTypes

/// Mutable construction storage that becomes immutable before publication.
///
/// Unsafe invariants:
/// - the owner allocates exactly `count` raw bytes and deallocates the address
///   exactly once;
/// - `append` is used serially by one task before `finish` publishes the owner;
/// - every appended range is bounds-checked and initializes the next contiguous
///   byte range exactly once;
/// - published storage is immutable, making concurrent synchronous borrows safe;
/// - byte alignment is sufficient, no typed binding is retained, and the raw
///   pointer never escapes a borrow closure.
final class ExactByteAssemblyOwner: ByteStringOwner, @unchecked Sendable {
    let count: Int
    private let address: UnsafeMutableRawPointer
    private var initializedByteCount: Int

    init(count: Int) {
        precondition(count > 0)
        self.count = count
        self.address = UnsafeMutableRawPointer.allocate(
            byteCount: count,
            alignment: MemoryLayout<UInt8>.alignment
        )
        self.initializedByteCount = 0
    }

    deinit {
        address.deallocate()
    }

    var retainedByteCount: Int? { count }
    var isStorageSelfContained: Bool { true }

    func append(_ source: borrowing ByteString) {
        let (end, overflow) = initializedByteCount.addingReportingOverflow(
            source.count
        )
        precondition(!overflow && end <= count)
        source.withUnsafeBytes { sourceBytes in
            UnsafeMutableRawBufferPointer(
                start: address.advanced(by: initializedByteCount),
                count: sourceBytes.count
            ).copyMemory(from: sourceBytes)
        }
        initializedByteCount = end
    }

    func finish() -> ByteString {
        precondition(
            initializedByteCount == count,
            "Exact byte assembly was published before initialization completed"
        )
        return ByteString(retaining: self)
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        precondition(
            initializedByteCount == count,
            "Exact byte assembly was borrowed before publication"
        )
        try body(UnsafeRawBufferPointer(start: address, count: count))
    }
}
