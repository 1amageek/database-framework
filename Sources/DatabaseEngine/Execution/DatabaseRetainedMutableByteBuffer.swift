import DatabaseTypes
import Synchronization

/// Exact-size storage admitted before asynchronous producers fill it.
///
/// The allocation has one owner and exactly-once deallocation. Its raw pointer
/// never escapes a synchronous `copy` or `borrowBytes` call. Initialization is
/// contiguous from offset zero, and immutable borrowing becomes legal only
/// after the complete byte range has been initialized and finalized. `count`
/// is a validated nonnegative Int, the allocation uses UInt8 alignment, and
/// memory is never bound or rebound. A Mutex serializes initialized-range and
/// finalization state; after finalization no mutation entry point is valid.
/// The retained reservation covers the exact allocation for its full lifetime.
package final class DatabaseRetainedMutableByteBuffer:
    ByteStringOwner,
    @unchecked Sendable
{
    private struct State: Sendable {
        var initializedByteCount = 0
        var isFinalized = false
    }

    package let count: Int
    package let retainedByteCount: Int?
    package let isStorageSelfContained = true

    private let allocation: UnsafeMutableRawPointer?
    private let reservation: DatabaseIntermediateReservation
    private let state = Mutex(State())

    package init(
        count: Int,
        reservation: DatabaseIntermediateReservation
    ) {
        precondition(count >= 0)
        self.count = count
        self.retainedByteCount = count
        self.reservation = reservation
        self.allocation = count == 0
            ? nil
            : UnsafeMutableRawPointer.allocate(
                byteCount: count,
                alignment: MemoryLayout<UInt8>.alignment
            )
    }

    deinit {
        allocation?.deallocate()
    }

    package func append(copying bytes: ByteString) {
        state.withLock { state in
            precondition(!state.isFinalized)
            precondition(bytes.count <= count - state.initializedByteCount)
            let offset = state.initializedByteCount
            bytes.withUnsafeBytes { source in
                UnsafeMutableRawBufferPointer(
                    start: allocation?.advanced(by: offset),
                    count: source.count
                ).copyMemory(from: source)
            }
            state.initializedByteCount += bytes.count
        }
    }

    package func finalize() -> ByteString {
        state.withLock { state in
            precondition(!state.isFinalized)
            precondition(state.initializedByteCount == count)
            state.isFinalized = true
        }
        return ByteString(retaining: self)
    }

    package func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        let isFinalized = state.withLock { $0.isFinalized }
        precondition(isFinalized)
        try body(UnsafeRawBufferPointer(start: allocation, count: count))
    }
}
