import Foundation
import DatabaseTypes
import StorageKit
import Synchronization

/// Transaction wrapper for tests that need to count and limit range operations.
///
/// This wrapper can:
/// - Count opened range cursors
/// - Fail when the configured range cursor count is exceeded
public final class LimitingTransaction: TransactionAccess, Sendable {

    public var capabilities: TransactionCapabilities {
        underlying.capabilities
    }

    public var transactionDomain: StorageTransactionDomain {
        underlying.transactionDomain
    }

    // MARK: - Range Result

    /// Delegates range advancement without materializing backend-owned bytes.
    public struct RangeResult: TransactionRangeResult {
        private let source: KeyValueCursor?
        private let error: LimitingError?

        init(source: KeyValueCursor) {
            self.source = source
            self.error = nil
        }

        init(error: LimitingError) {
            self.source = nil
            self.error = error
        }

        public func makeCursor() -> Cursor {
            Cursor(
                source: source,
                error: error
            )
        }

        public struct Cursor: TransactionRangeCursor {
            private var source: KeyValueCursor?
            private var pendingError: LimitingError?

            init(
                source: KeyValueCursor?,
                error: LimitingError?
            ) {
                self.source = source
                self.pendingError = error
            }

            public mutating func next() async throws -> (ByteString, ByteString)? {
                if let error = pendingError {
                    pendingError = nil
                    throw error
                }
                guard var activeSource = source else { return nil }
                let row = try await activeSource.next()
                source = activeSource
                return row
            }

            public mutating func finish(
                isolation actor: isolated (any Actor)?
            ) async throws {
                pendingError = nil
                guard var activeSource = source else { return }
                source = nil
                try await activeSource.finish()
            }
        }
    }

    // MARK: - Error

    public enum LimitingError: Error, Equatable, CustomStringConvertible, Sendable {
        case exceededMaximumRangeCursorCount(maximum: Int)

        public var description: String {
            switch self {
            case .exceededMaximumRangeCursorCount(let maximum):
                return "Exceeded maximum range cursor count (\(maximum))"
            }
        }
    }

    // MARK: - Properties

    private let underlying: any TransactionAccess
    private let maximumRangeCursorCount: Int
    private let rangeCursorCount: Mutex<Int>

    public init(
        wrapping underlying: any TransactionAccess,
        maximumRangeCursorCount: Int = Int.max
    ) {
        self.underlying = underlying
        self.maximumRangeCursorCount = maximumRangeCursorCount
        self.rangeCursorCount = Mutex(0)
    }

    public var openedRangeCursorCount: Int {
        rangeCursorCount.withLock { $0 }
    }

    // MARK: - Read

    public func getValue(for key: ByteString, snapshot: Bool) async throws -> ByteString? {
        try await underlying.getValue(for: key, snapshot: snapshot)
    }

    public func getValue(for key: ByteString) async throws -> ByteString? {
        try await underlying.getValue(for: key)
    }

    public func getKey(selector: KeySelector, snapshot: Bool) async throws -> ByteString? {
        try await underlying.getKey(selector: selector, snapshot: snapshot)
    }

    public func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        let count = rangeCursorCount.withLock { value in
            value += 1
            return value
        }

        guard count <= maximumRangeCursorCount else {
            return KeyValueCursor(consuming: RangeResult(
                error: .exceededMaximumRangeCursorCount(
                    maximum: maximumRangeCursorCount
                )
            ))
        }

        return KeyValueCursor(consuming: RangeResult(source:
            underlying.rangeCursor(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            )
        ))
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
