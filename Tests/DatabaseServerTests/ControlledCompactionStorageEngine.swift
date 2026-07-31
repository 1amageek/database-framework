import DatabaseTypes
import StorageKit

final class ControlledCompactionStorageEngine: StorageEngine, Sendable {
    enum Behavior: Sendable {
        case twoSlices
        case oversizedContinuation(byteCount: Int)
    }

    struct Configuration: Sendable {
        let behavior: Behavior

        init(behavior: Behavior) {
            self.behavior = behavior
        }
    }

    typealias TransactionType = ControlledCompactionTransaction

    static let markerKey = ByteString(utf8: "maintenance-compaction-marker")

    private let underlying: InMemoryEngine
    private let behavior: Behavior

    init(configuration: Configuration) async throws {
        self.underlying = InMemoryEngine()
        self.behavior = configuration.behavior
    }

    convenience init(behavior: Behavior) async throws {
        try await self.init(configuration: Configuration(behavior: behavior))
    }

    func createTransaction() throws -> ControlledCompactionTransaction {
        ControlledCompactionTransaction(
            underlying: try underlying.createTransaction(),
            behavior: behavior
        )
    }

    func requestShutdown() {
        underlying.requestShutdown()
    }

    func waitUntilShutdown() async {
        await underlying.waitUntilShutdown()
    }

    final class ControlledCompactionTransaction:
        Transaction,
        StorageCompactionTransaction,
        Sendable {
        typealias RangeResult = KeyValueRangeResult

        let compactionLimits = StorageCompactionLimits(
            maximumWorkUnitsPerSlice: 1
        )

        var capabilities: TransactionCapabilities {
            underlying.capabilities
        }

        var mutationByteLimit: Int? {
            underlying.mutationByteLimit
        }
        var transactionDomain: StorageTransactionDomain {
            underlying.transactionDomain
        }
        var storageFailure: StorageError? { underlying.storageFailure }

        private let underlying: InMemoryTransaction
        private let behavior: Behavior

        init(
            underlying: InMemoryTransaction,
            behavior: Behavior
        ) {
            self.underlying = underlying
            self.behavior = behavior
        }

        func configureMutationByteLimit(maximumBytes: Int?) throws {
            try underlying.configureMutationByteLimit(maximumBytes: maximumBytes)
        }

        func getValue(
            for key: ByteString,
            snapshot: Bool
        ) async throws -> ByteString? {
            try await underlying.getValue(for: key, snapshot: snapshot)
        }

        func getValue(for key: ByteString) async throws -> ByteString? {
            try await underlying.getValue(for: key)
        }

        func getKey(
            selector: KeySelector,
            snapshot: Bool
        ) async throws -> ByteString? {
            try await underlying.getKey(selector: selector, snapshot: snapshot)
        }

        func rangeCursor(
            from begin: KeySelector,
            to end: KeySelector,
            limit: Int,
            reverse: Bool,
            snapshot: Bool,
            streamingMode: StreamingMode
        ) -> KeyValueCursor {
            underlying.rangeCursor(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            )
        }

        func setValue(_ value: ByteString, for key: ByteString) throws {
            try underlying.setValue(value, for: key)
        }

        func clear(key: ByteString) throws {
            try underlying.clear(key: key)
        }

        func clearRange(beginKey: ByteString, endKey: ByteString) throws {
            try underlying.clearRange(beginKey: beginKey, endKey: endKey)
        }

        func atomicOp(
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

        func commit() async throws {
            try await underlying.commit()
        }

        func cancel() async throws {
            try await underlying.cancel()
        }

        func setReadVersion(_ version: Int64) throws {
            try underlying.setReadVersion(version)
        }

        func getReadVersion() async throws -> Int64 {
            try await underlying.getReadVersion()
        }

        func getCommittedVersion() throws -> Int64 {
            try underlying.getCommittedVersion()
        }

        func setOption(forOption option: TransactionOption) throws {
            try underlying.setOption(forOption: option)
        }

        func setOption(
            to value: ByteString?,
            forOption option: TransactionOption
        ) throws {
            try underlying.setOption(to: value, forOption: option)
        }

        func setOption(
            to value: Int,
            forOption option: TransactionOption
        ) throws {
            try underlying.setOption(to: value, forOption: option)
        }

        func addConflictRange(
            beginKey: ByteString,
            endKey: ByteString,
            type: ConflictRangeType
        ) throws {
            try underlying.addConflictRange(
                beginKey: beginKey,
                endKey: endKey,
                type: type
            )
        }

        func getEstimatedRangeSizeBytes(
            beginKey: ByteString,
            endKey: ByteString
        ) async throws -> Int {
            try await underlying.getEstimatedRangeSizeBytes(
                beginKey: beginKey,
                endKey: endKey
            )
        }

        func getRangeSplitPoints(
            beginKey: ByteString,
            endKey: ByteString,
            chunkSize: Int
        ) async throws -> [ByteString] {
            try await underlying.getRangeSplitPoints(
                beginKey: beginKey,
                endKey: endKey,
                chunkSize: chunkSize
            )
        }

        func requestVersionstamp() -> any PendingTransactionVersionstamp {
            underlying.requestVersionstamp()
        }

        func stageCompactionSlice(
            maximumWorkUnits: UInt64,
            continuation: StorageCompactionContinuation?
        ) async throws(StorageCompactionError)
            -> StorageCompactionResult {
            guard maximumWorkUnits == 1 else {
                throw .invalidMaximumWorkUnits(
                    actual: maximumWorkUnits,
                    maximum: 1
                )
            }

            do {
                switch behavior {
                case .twoSlices:
                    if continuation == nil {
                        try underlying.setValue(
                            [1],
                            for: ControlledCompactionStorageEngine.markerKey
                        )
                        return StorageCompactionResult(
                            workUnitsConsumed: 1,
                            remainingWorkUnits: 1,
                            continuation: StorageCompactionContinuation(
                                bytes: [0xa1]
                            )
                        )
                    }
                    guard continuation?.bytes == [0xa1] else {
                        throw StorageCompactionError.invalidContinuation
                    }
                    try underlying.setValue(
                        [2],
                        for: ControlledCompactionStorageEngine.markerKey
                    )
                    return StorageCompactionResult(
                        workUnitsConsumed: 1,
                        remainingWorkUnits: 0,
                        continuation: nil
                    )

                case .oversizedContinuation(let byteCount):
                    guard continuation == nil else {
                        throw StorageCompactionError.invalidContinuation
                    }
                    try underlying.setValue(
                        [0xff],
                        for: ControlledCompactionStorageEngine.markerKey
                    )
                    return StorageCompactionResult(
                        workUnitsConsumed: 1,
                        remainingWorkUnits: 1,
                        continuation: StorageCompactionContinuation(
                            bytes: ByteString(repeating: 0xa2, count: byteCount)
                        )
                    )
                }
            } catch let error as StorageCompactionError {
                throw error
            } catch {
                throw .backendFailure(description: String(describing: error))
            }
        }
    }
}
