import DatabaseKit
import DatabaseTypes
import StorageKit

package enum IndexBuildTransactionAuthority: Sendable {
    case databaseContext(DatabaseContext)
    case requestAuthorization(AuthorizationContext)
}

public enum IndexBuildContextError: Error, Sendable, Equatable {
    case invalidPageLimit(Int)
    case invalidPageByteLimit(Int)
    case itemExceedsPageByteLimit(actual: Int, maximum: Int)
    case continuationMismatch
    case sessionClosed
}

private struct IndexBuildExecutionIdentity: Sendable {
    let container: DBContainer
    let schemaGeneration: UInt64
    let storage: DatabaseExecutionStorage

    init(
        authority: IndexBuildTransactionAuthority,
        container: DBContainer
    ) throws {
        self.container = container
        switch authority {
        case .databaseContext(let context):
            guard context.container === container else {
                throw DatabaseTransactionExecutionScopeError.containerMismatch
            }
            self.schemaGeneration = context.container.schemaGeneration
            self.storage = try context.executionStorage()
        case .requestAuthorization:
            self.schemaGeneration = container.schemaGeneration
            self.storage = try container.executionStorage()
        }
    }

    func validate(
        authority: IndexBuildTransactionAuthority
    ) throws {
        let currentContainer: DBContainer
        let currentSchemaGeneration: UInt64
        let currentStorage: DatabaseExecutionStorage
        switch authority {
        case .databaseContext(let context):
            currentContainer = context.container
            currentSchemaGeneration = context.container.schemaGeneration
            currentStorage = try context.executionStorage()
        case .requestAuthorization:
            currentContainer = container
            currentSchemaGeneration = container.schemaGeneration
            currentStorage = try container.executionStorage()
        }
        guard currentContainer === container else {
            throw DatabaseTransactionExecutionScopeError.containerMismatch
        }
        guard currentSchemaGeneration == schemaGeneration else {
            throw DatabaseTransactionExecutionScopeError.schemaGenerationMismatch
        }
        guard currentStorage.root == storage.root,
              currentStorage.generation == storage.generation else {
            throw DatabaseTransactionExecutionScopeError.dataRootMismatch
        }
        guard currentStorage.domainIdentifier == storage.domainIdentifier else {
            throw DatabaseTransactionExecutionScopeError.storageDomainMismatch
        }
    }
}

/// One canonical item payload supplied to a custom index build strategy.
public struct IndexBuildItem: Sendable {
    public let primaryKey: Tuple
    public let payload: ByteString

    package init(primaryKey: Tuple, payload: ByteString) {
        self.primaryKey = primaryKey
        self.payload = payload
    }
}

/// Opaque continuation for a bounded custom-build item scan.
public struct IndexBuildContinuation: Sendable {
    package let itemType: String
    package let key: ByteString

    package init(itemType: String, key: ByteString) {
        self.itemType = itemType
        self.key = key
    }
}

/// One bounded page of canonical item payloads.
public struct IndexBuildItemPage: Sendable {
    public let items: [IndexBuildItem]
    public let continuation: IndexBuildContinuation?
    /// Accounted payload, key, and element-storage bytes retained by `items`.
    public let materializedByteCount: Int

    package init(
        items: [IndexBuildItem],
        continuation: IndexBuildContinuation?,
        materializedByteCount: Int
    ) {
        self.items = items
        self.continuation = continuation
        self.materializedByteCount = materializedByteCount
    }
}

/// The bounded execution capability supplied to an online index build.
///
/// Every invocation opens a new root-scoped transaction and re-evaluates the
/// administrator Grant for that transaction attempt. A transaction retained
/// beyond the callback is rejected by its operation-scope gate. Copies passed
/// to a custom strategy share one revocable build session and cannot open new
/// transactions after the strategy callback returns.
public struct IndexBuildContext: Sendable {
    /// Hard upper bound for one retained page and its transient decode peak.
    public static let maximumPageBytes = 128 * 1_024 * 1_024

    package let itemSubspace: Subspace
    package let indexSubspace: Subspace
    public let itemType: String
    public let index: ResolvedIndex

    private let container: DBContainer
    private let authority: IndexBuildTransactionAuthority
    private let blobsSubspace: Subspace
    private let executionIdentity: IndexBuildExecutionIdentity
    private let sessionScope: DatabaseReadScopeGate

    package init(
        authority: IndexBuildTransactionAuthority,
        container: DBContainer,
        itemSubspace: Subspace,
        blobsSubspace: Subspace,
        indexSubspace: Subspace,
        itemType: String,
        index: ResolvedIndex
    ) throws {
        let executionIdentity = try IndexBuildExecutionIdentity(
            authority: authority,
            container: container
        )
        guard executionIdentity.storage.root.contains(itemSubspace.prefix),
              executionIdentity.storage.root.contains(blobsSubspace.prefix),
              executionIdentity.storage.root.contains(indexSubspace.prefix)
        else {
            throw DatabaseTransactionExecutionScopeError.dataRootMismatch
        }
        self.authority = authority
        self.container = container
        self.itemSubspace = itemSubspace
        self.blobsSubspace = blobsSubspace
        self.indexSubspace = indexSubspace
        self.itemType = itemType
        self.index = index
        self.executionIdentity = executionIdentity
        self.sessionScope = DatabaseReadScopeGate()
    }

    private init(
        authority: IndexBuildTransactionAuthority,
        container: DBContainer,
        itemSubspace: Subspace,
        blobsSubspace: Subspace,
        indexSubspace: Subspace,
        itemType: String,
        index: ResolvedIndex,
        executionIdentity: IndexBuildExecutionIdentity,
        sessionScope: DatabaseReadScopeGate
    ) {
        self.authority = authority
        self.container = container
        self.itemSubspace = itemSubspace
        self.blobsSubspace = blobsSubspace
        self.indexSubspace = indexSubspace
        self.itemType = itemType
        self.index = index
        self.executionIdentity = executionIdentity
        self.sessionScope = sessionScope
    }

    /// Reads a bounded page of canonical entity payloads.
    ///
    /// Envelope and external-blob handling remains owned by DatabaseEngine, so
    /// a custom strategy never receives authority for another entity or the
    /// shared blob namespace. `maximumBytes` bounds both the returned page and
    /// the source-plus-destination memory that temporarily coexists while each
    /// item is decoded.
    public func readItems(
        after continuation: IndexBuildContinuation? = nil,
        limit: Int,
        maximumBytes: Int = IndexBuildContext.maximumPageBytes
    ) async throws -> IndexBuildItemPage {
        guard (1...10_000).contains(limit) else {
            throw IndexBuildContextError.invalidPageLimit(limit)
        }
        guard (1...Self.maximumPageBytes).contains(maximumBytes) else {
            throw IndexBuildContextError.invalidPageByteLimit(maximumBytes)
        }
        if let continuation {
            guard continuation.itemType == itemType,
                  itemSubspace.contains(continuation.key) else {
                throw IndexBuildContextError.continuationMismatch
            }
        }
        return try await withTransaction { transaction in
            let storage = self.container.itemStorageFactory.makeReader(
                transaction: transaction,
                blobsSubspace: self.blobsSubspace
            )
            let range = self.itemSubspace.range()
            let pageMeter = DatabaseWorkMeter(
                budget: ExecutionBudget(
                    maximumRows: UInt32(limit),
                    maximumWorkUnits: UInt64.max,
                    maximumIntermediateRows: UInt32(limit),
                    maximumIntermediateBytes: UInt64(maximumBytes),
                    timeoutMilliseconds: UInt32.max
                ),
                monotonicClock: self.container.monotonicClock
            )
            var items: DatabaseRetainedArrayBuilder<IndexBuildItem>
            do {
                items = try DatabaseRetainedArrayBuilder(
                    workMeter: pageMeter,
                    stage: .storageRow,
                    layout: try CanonicalRelationalFootprintMeter
                        .retainedArrayLayout(for: IndexBuildItem.self)
                )
            } catch let error as DatabaseWorkLimitError {
                guard case .maximumIntermediateBytes(
                    _,
                    let consumed,
                    let requested,
                    _
                ) = error else {
                    throw error
                }
                throw IndexBuildContextError.itemExceedsPageByteLimit(
                    actual: Self.saturatedByteCount(consumed, requested),
                    maximum: maximumBytes
                )
            }
            var lastReturnedKey: ByteString?
            var cursor = transaction.rangeCursor(
                from: (continuation?.key).map(KeySelector.firstGreaterThan)
                    ?? .firstGreaterOrEqual(range.begin),
                to: .firstGreaterOrEqual(range.end),
                limit: limit + 1,
                reverse: false,
                snapshot: false,
                streamingMode: .small
            )
            do {
                while let (key, envelopeBytes) = try await cursor.next() {
                    if items.count == limit {
                        try await cursor.finish()
                        return Self.makePage(
                            items: consume items,
                            continuation: lastReturnedKey.map {
                                IndexBuildContinuation(
                                    itemType: self.itemType,
                                    key: $0
                                )
                            },
                            pageMeter: pageMeter
                        )
                    }
                    let admission = try storage.inspectEnvelopeForAdmission(
                        envelopeBytes
                    )
                    let retainedItemByteCount = try Self.addingByteCounts(
                        key.count,
                        admission.materializedPayloadByteCount
                    )
                    let decodeItemPeakByteCount = try Self.addingByteCounts(
                        key.count,
                        admission.decodePeakByteCount
                    )
                    do {
                        let appendAdmission = try items.prepareAppend(
                            footprint: DatabaseIntermediateFootprint(
                                rows: 1,
                                bytes: UInt64(retainedItemByteCount)
                            ),
                            at: .storageRow
                        )
                        let additionalDecodePeak = max(
                            0,
                            decodeItemPeakByteCount - retainedItemByteCount
                        )
                        let decodeReservation = try pageMeter.reserveIntermediate(
                            bytes: UInt64(additionalDecodePeak),
                            at: .storageRow
                        )
                        defer { decodeReservation.release() }
                        let ownedKey = key.detached()
                        let primaryKey = try self.itemSubspace.unpack(ownedKey)
                        let payload = try await storage.decodeAdmittedEnvelope(
                            envelopeBytes,
                            for: ownedKey,
                            snapshot: false
                        )
                        items.append(
                            IndexBuildItem(
                                primaryKey: primaryKey,
                                payload: payload
                            ),
                            using: appendAdmission
                        )
                        lastReturnedKey = ownedKey
                    } catch let error as DatabaseWorkLimitError {
                        guard case .maximumIntermediateBytes(
                            _,
                            let consumed,
                            let requested,
                            _
                        ) = error else {
                            throw error
                        }
                        guard !items.isEmpty else {
                            throw IndexBuildContextError
                                .itemExceedsPageByteLimit(
                                    actual: Self.saturatedByteCount(
                                        consumed,
                                        requested
                                    ),
                                    maximum: maximumBytes
                                )
                        }
                        try await cursor.finish()
                        return Self.makePage(
                            items: consume items,
                            continuation: lastReturnedKey.map {
                                IndexBuildContinuation(
                                    itemType: self.itemType,
                                    key: $0
                                )
                            },
                            pageMeter: pageMeter
                        )
                    }
                }
            } catch {
                let iterationError = error
                do {
                    try await cursor.finish()
                } catch {
                    throw StorageRangeCleanupError(
                        iterationError: iterationError,
                        cleanupError: error
                    )
                }
                throw iterationError
            }
            try await cursor.finish()
            return Self.makePage(
                items: consume items,
                continuation: nil,
                pageMeter: pageMeter
            )
        }
    }

    private static func makePage(
        items: consuming DatabaseRetainedArrayBuilder<IndexBuildItem>,
        continuation: IndexBuildContinuation?,
        pageMeter: DatabaseWorkMeter
    ) -> IndexBuildItemPage {
        let byteCount = Int(pageMeter.retainedIntermediateBytes)
        return IndexBuildItemPage(
            items: items.finish().promoteToOutput(),
            continuation: continuation,
            materializedByteCount: byteCount
        )
    }

    private static func saturatedByteCount(
        _ consumed: UInt64,
        _ requested: UInt64
    ) -> Int {
        let (total, overflow) = consumed.addingReportingOverflow(requested)
        guard !overflow, total <= UInt64(Int.max) else { return Int.max }
        return Int(total)
    }

    private static func addingByteCounts(
        _ lhs: Int,
        _ rhs: Int
    ) throws -> Int {
        let (total, overflow) = lhs.addingReportingOverflow(rhs)
        guard !overflow else {
            throw IndexBuildContextError.itemExceedsPageByteLimit(
                actual: Int.max,
                maximum: Self.maximumPageBytes
            )
        }
        return total
    }

    /// Executes one transaction whose authority is confined to this index.
    public func withIndexTransaction<Result: Sendable>(
        configuration: TransactionConfiguration = .batch,
        _ operation: @Sendable @escaping (
            any IndexMaintenanceTransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        try await withTransaction(configuration: configuration) {
            transaction in
            try await withIndexMaintenanceTransaction(
                transaction: transaction,
                indexSubspace: self.indexSubspace,
                operation
            )
        }
    }

    /// Executes one framework-owned administrator transaction.
    package func withTransaction<Result: Sendable>(
        configuration: TransactionConfiguration = .batch,
        _ operation: @Sendable @escaping (
            any TransactionAccess
        ) async throws -> Result
    ) async throws -> Result {
        let sessionLease: DatabaseReadScopeLease
        do {
            sessionLease = try sessionScope.beginRead()
        } catch DatabaseReadTransactionError.snapshotClosed {
            throw IndexBuildContextError.sessionClosed
        }
        defer { sessionLease.finish() }

        switch authority {
        case .databaseContext(let context):
            return try await context.withWriteStorageAccess(
                requiredAccess: .administer,
                configuration: configuration
            ) { transaction in
                try self.executionIdentity.validate(authority: self.authority)
                return try await operation(transaction)
            }
        case .requestAuthorization(let authorization):
            return try await RequestAuthorization.$context.withValue(
                authorization
            ) {
                try await container.withOperationSchemaLease { _ in
                    try self.executionIdentity.validate(
                        authority: self.authority
                    )
                    return try await container.withDatabaseTransaction(
                        requiredAccess: .administer,
                        configuration: configuration,
                        operation
                    )
                }
            }
        }
    }

    /// Runs one custom strategy with a separately revocable capability.
    ///
    /// The callback may copy the context, but every copy shares the same gate.
    /// Returning from the callback closes the gate, rejects late child tasks,
    /// and waits for operations admitted before close to reach their terminal
    /// transaction boundary.
    package func withCustomStrategySession<Result: Sendable>(
        _ operation: @Sendable (IndexBuildContext) async throws -> Result
    ) async throws -> Result {
        let session = IndexBuildContext(
            authority: authority,
            container: container,
            itemSubspace: itemSubspace,
            blobsSubspace: blobsSubspace,
            indexSubspace: indexSubspace,
            itemType: itemType,
            index: index,
            executionIdentity: executionIdentity,
            sessionScope: DatabaseReadScopeGate()
        )
        let result: Result
        do {
            result = try await operation(session)
        } catch {
            let operationError = error
            do {
                try await session.sessionScope.closeAndWait()
            } catch let cleanupError as DatabaseReadScopeCleanupError {
                throw cleanupError.preserving(operationError: operationError)
            }
            throw operationError
        }
        try await session.sessionScope.closeAndWait()
        return result
    }
}
