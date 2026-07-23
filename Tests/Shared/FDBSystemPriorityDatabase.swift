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

        public func getValue<Key: FDB.ByteInput>(
            for key: Key,
            snapshot: Bool
        ) async throws -> FDB.ByteString? {
            try await underlying.getValue(for: key, snapshot: snapshot)
        }

        public func setValue<Value: FDB.ByteInput, Key: FDB.ByteInput>(
            _ value: Value,
            for key: Key
        ) throws {
            try underlying.setValue(value, for: key)
        }

        public func clear<Key: FDB.ByteInput>(key: Key) throws {
            try underlying.clear(key: key)
        }

        public func clearRange<
            Begin: FDB.ByteInput,
            End: FDB.ByteInput
        >(beginKey: Begin, endKey: End) throws {
            try underlying.clearRange(beginKey: beginKey, endKey: endKey)
        }

        public func getKey(
            selector: FDB.KeySelector,
            snapshot: Bool
        ) async throws -> FDB.ByteString {
            try await underlying.getKey(selector: selector, snapshot: snapshot)
        }

        public func readRangeBatch(
            from begin: FDB.KeySelector,
            to end: FDB.KeySelector,
            limit: Int,
            targetBytes: Int,
            streamingMode: FDB.StreamingMode,
            iteration: Int,
            reverse: Bool,
            snapshot: Bool
        ) async throws -> RangeBatch {
            try await underlying.readRangeBatch(
                from: begin,
                to: end,
                limit: limit,
                targetBytes: targetBytes,
                streamingMode: streamingMode,
                iteration: iteration,
                reverse: reverse,
                snapshot: snapshot
            )
        }

        public func commit() async throws {
            try await underlying.commit()
        }

        public func cancel() {
            underlying.cancel()
        }

        public func requestVersionstamp() -> any FDB.PendingTransactionVersionstamp {
            underlying.requestVersionstamp()
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

        public func getEstimatedRangeSizeBytes<
            Begin: FDB.ByteInput,
            End: FDB.ByteInput
        >(beginKey: Begin, endKey: End) async throws -> Int64 {
            try await underlying.getEstimatedRangeSizeBytes(beginKey: beginKey, endKey: endKey)
        }

        public func getRangeSplitPoints<
            Begin: FDB.ByteInput,
            End: FDB.ByteInput
        >(
            beginKey: Begin,
            endKey: End,
            chunkSize: Int64
        ) async throws -> [FDB.ByteString] {
            try await underlying.getRangeSplitPoints(
                beginKey: beginKey,
                endKey: endKey,
                chunkSize: chunkSize
            )
        }

        public func getCommittedVersion() throws -> FDB.Version {
            try underlying.getCommittedVersion()
        }

        public func approximateSize() async throws -> Int64 {
            try await underlying.approximateSize()
        }

        public func atomicOp<
            Key: FDB.ByteInput,
            Parameter: FDB.ByteInput
        >(
            key: Key,
            param: Parameter,
            mutationType: FDB.MutationType
        ) throws {
            try underlying.atomicOp(
                key: key,
                param: param,
                mutationType: mutationType
            )
        }

        public func addConflictRange<
            Begin: FDB.ByteInput,
            End: FDB.ByteInput
        >(
            beginKey: Begin,
            endKey: End,
            type: FDB.ConflictRangeType
        ) throws {
            try underlying.addConflictRange(beginKey: beginKey, endKey: endKey, type: type)
        }

        public func setOption<Value: FDB.ByteInput>(
            to value: Value,
            forOption option: FDB.TransactionOption
        ) throws {
            switch option {
            case .priorityBatch, .prioritySystemImmediate, .readPriorityLow, .readPriorityHigh:
                return
            default:
                try underlying.setOption(to: value, forOption: option)
            }
        }

        public func setOption(
            forOption option: FDB.TransactionOption
        ) throws {
            switch option {
            case .priorityBatch, .prioritySystemImmediate,
                 .readPriorityLow, .readPriorityHigh:
                return
            default:
                try underlying.setOption(forOption: option)
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

    private let underlying: any DatabaseProtocol

    public init(wrapping underlying: any DatabaseProtocol) {
        self.underlying = underlying
    }

    public func createTransaction() throws -> PriorityTransaction {
        try PriorityTransaction(wrapping: underlying.createTransaction())
    }
}
#endif
