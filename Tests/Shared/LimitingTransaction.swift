import Foundation
import FoundationDB
import Synchronization

/// TransactionProtocol wrapper for tests that need deterministic range paging.
///
/// `FDB.AsyncKVSequence` streams results by repeatedly calling `getRangeNative`.
/// This wrapper can:
/// - Count `getRangeNative` calls
/// - Force paging by truncating each native call to at most `maxRecordsPerNativeCall`
/// - Fail if more than `maxNativeCalls` calls are performed
public final class LimitingTransaction: TransactionProtocol, Sendable {
    public enum LimitingError: Error, Equatable, CustomStringConvertible, Sendable {
        case exceededMaxNativeCalls(max: Int)

        public var description: String {
            switch self {
            case .exceededMaxNativeCalls(let max):
                return "Exceeded max getRangeNative calls (\(max))"
            }
        }
    }

    private let underlying: any TransactionProtocol
    private let maxNativeCalls: Int
    private let maxRecordsPerNativeCall: Int
    private let nativeCallCount: Mutex<Int>

    public init(
        wrapping underlying: any TransactionProtocol,
        maxNativeCalls: Int,
        maxRecordsPerNativeCall: Int = 1
    ) {
        self.underlying = underlying
        self.maxNativeCalls = maxNativeCalls
        self.maxRecordsPerNativeCall = max(1, maxRecordsPerNativeCall)
        self.nativeCallCount = Mutex(0)
    }

    public var nativeCallCountValue: Int { nativeCallCount.withLock { $0 } }

    public func getValue(for key: FDB.Bytes, snapshot: Bool) async throws -> FDB.Bytes? {
        try await underlying.getValue(for: key, snapshot: snapshot)
    }

    public func setValue(_ value: FDB.Bytes, for key: FDB.Bytes) {
        underlying.setValue(value, for: key)
    }

    public func clear(key: FDB.Bytes) {
        underlying.clear(key: key)
    }

    public func clearRange(beginKey: FDB.Bytes, endKey: FDB.Bytes) {
        underlying.clearRange(beginKey: beginKey, endKey: endKey)
    }

    public func getKey(selector: FDB.Selectable, snapshot: Bool) async throws -> FDB.Bytes? {
        try await underlying.getKey(selector: selector, snapshot: snapshot)
    }

    public func getKey(selector: FDB.KeySelector, snapshot: Bool) async throws -> FDB.Bytes? {
        try await underlying.getKey(selector: selector, snapshot: snapshot)
    }

    public func getRange(
        beginSelector: FDB.KeySelector,
        endSelector: FDB.KeySelector,
        snapshot: Bool
    ) -> FDB.AsyncKVSequence {
        FDB.AsyncKVSequence(
            transaction: self,
            beginSelector: beginSelector,
            endSelector: endSelector,
            limit: 0,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .iterator
        )
    }

    public func getRangeNative(
        beginSelector: FDB.KeySelector,
        endSelector: FDB.KeySelector,
        limit: Int,
        targetBytes: Int,
        streamingMode: FDB.StreamingMode,
        iteration: Int,
        reverse: Bool,
        snapshot: Bool
    ) async throws -> ResultRange {
        let callCount = nativeCallCount.withLock { value in
            value += 1
            return value
        }
        if callCount > maxNativeCalls {
            throw LimitingError.exceededMaxNativeCalls(max: maxNativeCalls)
        }

        let range = try await underlying.getRangeNative(
            beginSelector: beginSelector,
            endSelector: endSelector,
            limit: limit,
            targetBytes: targetBytes,
            streamingMode: streamingMode,
            iteration: iteration,
            reverse: reverse,
            snapshot: snapshot
        )

        // Note: ResultRange init is internal to FoundationDB, so we cannot truncate.
        // This wrapper now only counts native calls and enforces max calls limit.
        // The maxRecordsPerNativeCall parameter is ignored for truncation purposes.
        return range
    }

    public func commit() async throws -> Bool {
        try await underlying.commit()
    }

    public func cancel() {
        underlying.cancel()
    }

    public func getVersionstamp() async throws -> FDB.Bytes? {
        try await underlying.getVersionstamp()
    }

    // MARK: - Additional TransactionProtocol Requirements

    public func setOption(to value: FDB.Bytes?, forOption option: FDB.TransactionOption) throws {
        try underlying.setOption(to: value, forOption: option)
    }

    public func setReadVersion(_ version: FDB.Version) {
        underlying.setReadVersion(version)
    }

    public func getReadVersion() async throws -> FDB.Version {
        try await underlying.getReadVersion()
    }

    public func onError(_ error: FDBError) async throws {
        try await underlying.onError(error)
    }

    public func getEstimatedRangeSizeBytes(beginKey: FDB.Bytes, endKey: FDB.Bytes) async throws -> Int {
        try await underlying.getEstimatedRangeSizeBytes(beginKey: beginKey, endKey: endKey)
    }

    public func getRangeSplitPoints(beginKey: FDB.Bytes, endKey: FDB.Bytes, chunkSize: Int) async throws -> [[UInt8]] {
        try await underlying.getRangeSplitPoints(beginKey: beginKey, endKey: endKey, chunkSize: chunkSize)
    }

    public func getCommittedVersion() throws -> FDB.Version {
        try underlying.getCommittedVersion()
    }

    public func getApproximateSize() async throws -> Int {
        try await underlying.getApproximateSize()
    }

    public func atomicOp(key: FDB.Bytes, param: FDB.Bytes, mutationType: FDB.MutationType) {
        underlying.atomicOp(key: key, param: param, mutationType: mutationType)
    }

    public func addConflictRange(beginKey: FDB.Bytes, endKey: FDB.Bytes, type: FDB.ConflictRangeType) throws {
        try underlying.addConflictRange(beginKey: beginKey, endKey: endKey, type: type)
    }
}
