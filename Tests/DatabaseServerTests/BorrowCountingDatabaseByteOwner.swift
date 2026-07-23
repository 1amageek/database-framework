import DatabaseValue
import Synchronization

final class BorrowCountingDatabaseByteOwner: DatabaseByteOwner, Sendable {
    private let bytes: [UInt8]
    private let countState = Mutex(0)

    init(_ bytes: [UInt8]) {
        self.bytes = bytes
    }

    var count: Int {
        bytes.count
    }

    var borrowCount: Int {
        countState.withLock { $0 }
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        countState.withLock { $0 += 1 }
        try bytes.withUnsafeBytes(body)
    }
}
