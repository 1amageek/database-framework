#if FOUNDATION_DB
import FoundationDB

/// Test-only FoundationDB wrapper that forces system-priority transactions.
///
/// This keeps E2E test setup responsive even when the local cluster is
/// temporarily throttling default-priority work.
public final class FDBSystemPriorityDatabase: DatabaseProtocol, Sendable {
    public final class PriorityTransaction: TransactionProtocol, Sendable {
        private let underlying: any TransactionProtocol

        public init(wrapping underlying: any TransactionProtocol) throws {
            self.underlying = underlying
            try underlying.setOption(forOption: .prioritySystemImmediate)
            try underlying.setOption(forOption: .readPriorityHigh)
        }

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
            underlying.getRange(beginSelector: beginSelector, endSelector: endSelector, snapshot: snapshot)
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
            try await underlying.getRangeNative(
                beginSelector: beginSelector,
                endSelector: endSelector,
                limit: limit,
                targetBytes: targetBytes,
                streamingMode: streamingMode,
                iteration: iteration,
                reverse: reverse,
                snapshot: snapshot
            )
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

        public func getRangeSplitPoints(
            beginKey: FDB.Bytes,
            endKey: FDB.Bytes,
            chunkSize: Int
        ) async throws -> [[UInt8]] {
            try await underlying.getRangeSplitPoints(
                beginKey: beginKey,
                endKey: endKey,
                chunkSize: chunkSize
            )
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

        public func addConflictRange(
            beginKey: FDB.Bytes,
            endKey: FDB.Bytes,
            type: FDB.ConflictRangeType
        ) throws {
            try underlying.addConflictRange(beginKey: beginKey, endKey: endKey, type: type)
        }

        public func setOption(to value: FDB.Bytes?, forOption option: FDB.TransactionOption) throws {
            switch option {
            case .priorityBatch, .prioritySystemImmediate, .readPriorityLow, .readPriorityHigh:
                return
            default:
                try underlying.setOption(to: value, forOption: option)
            }
        }

        public func setOption(to value: String, forOption option: FDB.TransactionOption) throws {
            switch option {
            case .priorityBatch, .prioritySystemImmediate, .readPriorityLow, .readPriorityHigh:
                return
            default:
                try underlying.setOption(to: value, forOption: option)
            }
        }

        public func setOption(to value: Int, forOption option: FDB.TransactionOption) throws {
            switch option {
            case .priorityBatch, .prioritySystemImmediate, .readPriorityLow, .readPriorityHigh:
                return
            default:
                try underlying.setOption(to: value, forOption: option)
            }
        }
    }

    nonisolated(unsafe) private let underlying: any DatabaseProtocol

    public init(wrapping underlying: any DatabaseProtocol) {
        self.underlying = underlying
    }

    public func createTransaction() throws -> PriorityTransaction {
        try PriorityTransaction(wrapping: underlying.createTransaction())
    }
}
#endif
