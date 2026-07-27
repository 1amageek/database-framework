import Foundation
import DatabaseTypes
import StorageKit
import Synchronization

/// Transaction wrapper for tests that need to count and limit range operations.
///
/// This wrapper can:
/// - Count `collectRange` calls
/// - Fail if more than `maxCollectCalls` calls are performed
public final class LimitingTransaction: TransactionAccess, Sendable {

    public var capabilities: TransactionCapabilities {
        underlying.capabilities
    }

    // MARK: - Associated Type

    /// Delegates to the underlying transaction's RangeResult via type erasure.
    /// Since LimitingTransaction wraps type-erased storage access, range
    /// collection is deferred to the iterator.
    public struct RangeResult: TransactionRangeResult {
        public typealias Element = (ByteString, ByteString)

        private let underlying: (any TransactionAccess)?
        private let begin: KeySelector
        private let end: KeySelector
        private let limit: Int
        private let reverse: Bool
        private let snapshot: Bool
        private let streamingMode: StreamingMode
        private let error: LimitingError?

        /// Create from pre-collected pairs (e.g., when exceeded max calls).
        init(pairs: [(ByteString, ByteString)]) {
            self.underlying = nil
            self.begin = KeySelector(key: [], orEqual: false, offset: 0)
            self.end = KeySelector(key: [], orEqual: false, offset: 0)
            self.limit = 0
            self.reverse = false
            self.snapshot = false
            self.streamingMode = .wantAll
            self._pairs = pairs
            self.error = nil
        }

        init(error: LimitingError) {
            self.underlying = nil
            self.begin = KeySelector(key: [], orEqual: false, offset: 0)
            self.end = KeySelector(key: [], orEqual: false, offset: 0)
            self.limit = 0
            self.reverse = false
            self.snapshot = false
            self.streamingMode = .wantAll
            self._pairs = nil
            self.error = error
        }

        /// Create from underlying transaction parameters (lazy collection).
        init(
            underlying: any TransactionAccess,
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
            self.error = nil
        }

        private let _pairs: [(ByteString, ByteString)]?

        public func makeAsyncIterator() -> AsyncIterator {
            AsyncIterator(
                underlying: underlying,
                begin: begin, end: end,
                limit: limit, reverse: reverse,
                snapshot: snapshot, streamingMode: streamingMode,
                preFetched: _pairs,
                error: error
            )
        }

        public struct AsyncIterator: TransactionRangeIterator {
            private let underlying: (any TransactionAccess)?
            private let begin: KeySelector
            private let end: KeySelector
            private let limit: Int
            private let reverse: Bool
            private let snapshot: Bool
            private let streamingMode: StreamingMode
            private var pairs: [(ByteString, ByteString)]?
            private var index = 0
            private let error: LimitingError?

            init(
                underlying: (any TransactionAccess)?,
                begin: KeySelector, end: KeySelector,
                limit: Int, reverse: Bool,
                snapshot: Bool, streamingMode: StreamingMode,
                preFetched: [(ByteString, ByteString)]?,
                error: LimitingError?
            ) {
                self.underlying = underlying
                self.begin = begin
                self.end = end
                self.limit = limit
                self.reverse = reverse
                self.snapshot = snapshot
                self.streamingMode = streamingMode
                self.pairs = preFetched
                self.error = error
            }

            public mutating func next() async throws -> (ByteString, ByteString)? {
                if let error {
                    throw error
                }
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

            public mutating func finish(
                isolation actor: isolated (any Actor)?
            ) async throws {
                pairs = nil
                index = 0
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

    private let underlying: any TransactionAccess
    private let maxCollectCalls: Int
    private let callCount: Mutex<Int>

    public init(
        wrapping underlying: any TransactionAccess,
        maxCollectCalls: Int = Int.max
    ) {
        self.underlying = underlying
        self.maxCollectCalls = maxCollectCalls
        self.callCount = Mutex(0)
    }

    public var callCountValue: Int { callCount.withLock { $0 } }

    // MARK: - Read

    public func getValue(for key: ByteString, snapshot: Bool) async throws -> ByteString? {
        try await underlying.getValue(for: key, snapshot: snapshot)
    }

    public func getKey(selector: KeySelector, snapshot: Bool) async throws -> ByteString? {
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

        guard count <= maxCollectCalls else {
            return RangeResult(
                error: .exceededMaxCalls(max: maxCollectCalls)
            )
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

    public func setValue(_ value: ByteString, for key: ByteString) throws {
        try underlying.setValue(value, for: key)
    }

    public func clear(key: ByteString) throws {
        try underlying.clear(key: key)
    }

    public func clearRange(beginKey: ByteString, endKey: ByteString) throws {
        try underlying.clearRange(beginKey: beginKey, endKey: endKey)
    }

    // MARK: - Atomic

    public func atomicOp(
        key: ByteString,
        param: ByteString,
        mutationType: MutationType
    ) throws {
        try underlying.atomicOp(
            key: key,
            param: param,
            mutationType: mutationType
        )
    }

    // MARK: - Version

    public func setReadVersion(_ version: Int64) throws {
        try underlying.setReadVersion(version)
    }

    public func getReadVersion() async throws -> Int64 {
        try await underlying.getReadVersion()
    }

    // MARK: - Options

    public func setOption(forOption option: TransactionOption) throws {
        try underlying.setOption(forOption: option)
    }

    public func setOption(to value: ByteString?, forOption option: TransactionOption) throws {
        try underlying.setOption(to: value, forOption: option)
    }

    public func setOption(to value: Int, forOption option: TransactionOption) throws {
        try underlying.setOption(to: value, forOption: option)
    }

    // MARK: - Conflict Range

    public func addConflictRange(beginKey: ByteString, endKey: ByteString, type: ConflictRangeType) throws {
        try underlying.addConflictRange(beginKey: beginKey, endKey: endKey, type: type)
    }

    // MARK: - Statistics

    public func getEstimatedRangeSizeBytes(beginKey: ByteString, endKey: ByteString) async throws -> Int {
        try await underlying.getEstimatedRangeSizeBytes(beginKey: beginKey, endKey: endKey)
    }

    public func getRangeSplitPoints(beginKey: ByteString, endKey: ByteString, chunkSize: Int) async throws -> [ByteString] {
        try await underlying.getRangeSplitPoints(beginKey: beginKey, endKey: endKey, chunkSize: chunkSize)
    }

    // MARK: - Versionstamp

    public func requestVersionstamp() -> any PendingTransactionVersionstamp {
        underlying.requestVersionstamp()
    }
}
