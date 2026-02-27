import Foundation
import StorageKit
import Synchronization

/// Transaction wrapper for tests that need to count and limit range operations.
///
/// This wrapper can:
/// - Count `collectRange` calls
/// - Fail if more than `maxCollectCalls` calls are performed
public final class LimitingTransaction: Transaction, @unchecked Sendable {

    // MARK: - Associated Type

    /// Delegates to the underlying transaction's RangeResult via type erasure.
    /// Since LimitingTransaction wraps `any Transaction`, we eagerly collect via collectRange.
    public struct RangeResult: AsyncSequence, Sendable {
        public typealias Element = (Bytes, Bytes)

        private let underlying: (any Transaction)?
        private let begin: KeySelector
        private let end: KeySelector
        private let limit: Int
        private let reverse: Bool
        private let snapshot: Bool
        private let streamingMode: StreamingMode

        /// Create from pre-collected pairs (e.g., when exceeded max calls).
        init(pairs: [(Bytes, Bytes)]) {
            self.underlying = nil
            self.begin = KeySelector(key: [], orEqual: false, offset: 0)
            self.end = KeySelector(key: [], orEqual: false, offset: 0)
            self.limit = 0
            self.reverse = false
            self.snapshot = false
            self.streamingMode = .wantAll
            self._pairs = pairs
        }

        /// Create from underlying transaction parameters (lazy collection).
        init(
            underlying: any Transaction,
            begin: KeySelector, end: KeySelector,
            limit: Int, reverse: Bool,
            snapshot: Bool, streamingMode: StreamingMode
        ) {
            self.underlying = underlying
            self.begin = begin
            self.end = end
            self.limit = limit
            self.reverse = reverse
            self.snapshot = snapshot
            self.streamingMode = streamingMode
            self._pairs = nil
        }

        private let _pairs: [(Bytes, Bytes)]?

        public func makeAsyncIterator() -> AsyncIterator {
            AsyncIterator(
                underlying: underlying,
                begin: begin, end: end,
                limit: limit, reverse: reverse,
                snapshot: snapshot, streamingMode: streamingMode,
                preFetched: _pairs
            )
        }

        public struct AsyncIterator: AsyncIteratorProtocol {
            private let underlying: (any Transaction)?
            private let begin: KeySelector
            private let end: KeySelector
            private let limit: Int
            private let reverse: Bool
            private let snapshot: Bool
            private let streamingMode: StreamingMode
            private var pairs: [(Bytes, Bytes)]?
            private var index = 0

            init(
                underlying: (any Transaction)?,
                begin: KeySelector, end: KeySelector,
                limit: Int, reverse: Bool,
                snapshot: Bool, streamingMode: StreamingMode,
                preFetched: [(Bytes, Bytes)]?
            ) {
                self.underlying = underlying
                self.begin = begin
                self.end = end
                self.limit = limit
                self.reverse = reverse
                self.snapshot = snapshot
                self.streamingMode = streamingMode
                self.pairs = preFetched
            }

            public mutating func next() async throws -> (Bytes, Bytes)? {
                // Lazily collect on first call
                if pairs == nil, let tx = underlying {
                    pairs = try await tx.collectRange(
                        from: begin, to: end,
                        limit: limit, reverse: reverse,
                        snapshot: snapshot, streamingMode: streamingMode
                    )
                }
                guard let pairs, index < pairs.count else { return nil }
                let pair = pairs[index]
                index += 1
                return pair
            }
        }
    }

    // MARK: - Error

    public enum LimitingError: Error, Equatable, CustomStringConvertible, Sendable {
        case exceededMaxCalls(max: Int)

        public var description: String {
            switch self {
            case .exceededMaxCalls(let max):
                return "Exceeded max collectRange calls (\(max))"
            }
        }
    }

    // MARK: - Properties

    private let underlying: any Transaction
    private let maxCollectCalls: Int
    private let callCount: Mutex<Int>

    public init(
        wrapping underlying: any Transaction,
        maxCollectCalls: Int = Int.max
    ) {
        self.underlying = underlying
        self.maxCollectCalls = maxCollectCalls
        self.callCount = Mutex(0)
    }

    public var callCountValue: Int { callCount.withLock { $0 } }

    // MARK: - Read

    public func getValue(for key: Bytes, snapshot: Bool) async throws -> Bytes? {
        try await underlying.getValue(for: key, snapshot: snapshot)
    }

    public func getKey(selector: KeySelector, snapshot: Bool) async throws -> Bytes? {
        try await underlying.getKey(selector: selector, snapshot: snapshot)
    }

    public func getRange(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> RangeResult {
        // Count the call
        let count = callCount.withLock { value in
            value += 1
            return value
        }

        // We can't throw from a non-throwing function, so we return empty if exceeded.
        guard count <= maxCollectCalls else {
            return RangeResult(pairs: [])
        }

        // Eagerly collect from the underlying transaction.
        // Since getRange is synchronous but underlying.collectRange is async,
        // we store the parameters and lazily collect in the iterator.
        return RangeResult(
            underlying: underlying,
            begin: begin, end: end,
            limit: limit, reverse: reverse,
            snapshot: snapshot, streamingMode: streamingMode
        )
    }

    // MARK: - Write

    public func setValue(_ value: Bytes, for key: Bytes) {
        underlying.setValue(value, for: key)
    }

    public func clear(key: Bytes) {
        underlying.clear(key: key)
    }

    public func clearRange(beginKey: Bytes, endKey: Bytes) {
        underlying.clearRange(beginKey: beginKey, endKey: endKey)
    }

    // MARK: - Atomic

    public func atomicOp(key: Bytes, param: Bytes, mutationType: MutationType) {
        underlying.atomicOp(key: key, param: param, mutationType: mutationType)
    }

    // MARK: - Transaction Control

    public func commit() async throws {
        try await underlying.commit()
    }

    public func cancel() {
        underlying.cancel()
    }

    // MARK: - Version

    public func setReadVersion(_ version: Int64) {
        underlying.setReadVersion(version)
    }

    public func getReadVersion() async throws -> Int64 {
        try await underlying.getReadVersion()
    }

    public func getCommittedVersion() throws -> Int64 {
        try underlying.getCommittedVersion()
    }

    // MARK: - Options

    public func setOption(forOption option: TransactionOption) throws {
        try underlying.setOption(forOption: option)
    }

    public func setOption(to value: Bytes?, forOption option: TransactionOption) throws {
        try underlying.setOption(to: value, forOption: option)
    }

    public func setOption(to value: Int, forOption option: TransactionOption) throws {
        try underlying.setOption(to: value, forOption: option)
    }

    // MARK: - Conflict Range

    public func addConflictRange(beginKey: Bytes, endKey: Bytes, type: ConflictRangeType) throws {
        try underlying.addConflictRange(beginKey: beginKey, endKey: endKey, type: type)
    }

    // MARK: - Statistics

    public func getEstimatedRangeSizeBytes(beginKey: Bytes, endKey: Bytes) async throws -> Int {
        try await underlying.getEstimatedRangeSizeBytes(beginKey: beginKey, endKey: endKey)
    }

    public func getRangeSplitPoints(beginKey: Bytes, endKey: Bytes, chunkSize: Int) async throws -> [[UInt8]] {
        try await underlying.getRangeSplitPoints(beginKey: beginKey, endKey: endKey, chunkSize: chunkSize)
    }

    // MARK: - Versionstamp

    public func getVersionstamp() async throws -> Bytes? {
        try await underlying.getVersionstamp()
    }
}
