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

    static let markerKey = Bytes("maintenance-compaction-marker".utf8)

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

    final class ControlledCompactionTransaction:
        DatabaseStorageCompactionTransaction,
        Sendable {
        typealias RangeResult = KeyValueRangeResult

        let compactionLimits = DatabaseStorageCompactionLimits(
            maximumWorkUnitsPerSlice: 1
        )

        var capabilities: TransactionCapabilities {
            underlying.capabilities
        }

        var mutationByteLimit: Int? {
            underlying.mutationByteLimit
        }

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
            for key: Bytes,
            snapshot: Bool
        ) async throws -> Bytes? {
            try await underlying.getValue(for: key, snapshot: snapshot)
        }

        func getKey(
            selector: KeySelector,
            snapshot: Bool
        ) async throws -> Bytes? {
            try await underlying.getKey(selector: selector, snapshot: snapshot)
        }

        func getRange(
            from begin: KeySelector,
            to end: KeySelector,
            limit: Int,
            reverse: Bool,
            snapshot: Bool,
            streamingMode: StreamingMode
        ) -> KeyValueRangeResult {
            underlying.getRange(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            )
        }

        func setValue(_ value: Bytes, for key: Bytes) throws {
            try underlying.setValue(value, for: key)
        }

        func clear(key: Bytes) throws {
            try underlying.clear(key: key)
        }

        func clearRange(beginKey: Bytes, endKey: Bytes) throws {
            try underlying.clearRange(beginKey: beginKey, endKey: endKey)
        }

        func atomicOp(
            key: Bytes,
            param: Bytes,
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
            to value: Bytes?,
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
            beginKey: Bytes,
            endKey: Bytes,
            type: ConflictRangeType
        ) throws {
            try underlying.addConflictRange(
                beginKey: beginKey,
                endKey: endKey,
                type: type
            )
        }

        func getEstimatedRangeSizeBytes(
            beginKey: Bytes,
            endKey: Bytes
        ) async throws -> Int {
            try await underlying.getEstimatedRangeSizeBytes(
                beginKey: beginKey,
                endKey: endKey
            )
        }

        func getRangeSplitPoints(
            beginKey: Bytes,
            endKey: Bytes,
            chunkSize: Int
        ) async throws -> [Bytes] {
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
            continuation: DatabaseStorageCompactionContinuation?
        ) async throws(DatabaseStorageCompactionError)
            -> DatabaseStorageCompactionResult {
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
                        return DatabaseStorageCompactionResult(
                            workUnitsConsumed: 1,
                            remainingWorkUnits: 1,
                            continuation: DatabaseStorageCompactionContinuation(
                                bytes: [0xa1]
                            )
                        )
                    }
                    guard continuation?.bytes == [0xa1] else {
                        throw DatabaseStorageCompactionError.invalidContinuation
                    }
                    try underlying.setValue(
                        [2],
                        for: ControlledCompactionStorageEngine.markerKey
                    )
                    return DatabaseStorageCompactionResult(
                        workUnitsConsumed: 1,
                        remainingWorkUnits: 0,
                        continuation: nil
                    )

                case .oversizedContinuation(let byteCount):
                    guard continuation == nil else {
                        throw DatabaseStorageCompactionError.invalidContinuation
                    }
                    try underlying.setValue(
                        [0xff],
                        for: ControlledCompactionStorageEngine.markerKey
                    )
                    return DatabaseStorageCompactionResult(
                        workUnitsConsumed: 1,
                        remainingWorkUnits: 1,
                        continuation: DatabaseStorageCompactionContinuation(
                            bytes: Bytes(repeating: 0xa2, count: byteCount)
                        )
                    )
                }
            } catch let error as DatabaseStorageCompactionError {
                throw error
            } catch {
                throw .backendFailure(description: String(describing: error))
            }
        }
    }
}
