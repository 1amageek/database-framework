import DatabaseKit
import DatabaseTypes
import StorageKit

/// Enforces one admitted index subspace over a caller-owned read transaction.
package struct ScopedIndexReadAccess: IndexReadAccess {
    private let queryContext: IndexQueryContext?
    private let transaction: any TransactionReadAccess
    private let subspaces: [Subspace]

    package init(
        queryContext: IndexQueryContext,
        transaction: any TransactionReadAccess,
        subspace: Subspace?
    ) {
        self.queryContext = queryContext
        self.transaction = transaction
        self.subspaces = subspace.map { [$0] } ?? []
    }

    /// Constructs the physical-only capability used by confinement tests.
    /// Production execution always uses the context-bound initializer.
    package init(
        transaction: any TransactionReadAccess,
        subspace: Subspace?
    ) {
        self.queryContext = nil
        self.transaction = transaction
        self.subspaces = subspace.map { [$0] } ?? []
    }

    /// Constructs the physical-only multi-index capability used by
    /// confinement tests.
    package init(
        transaction: any TransactionReadAccess,
        subspaces: [Subspace]
    ) {
        self.queryContext = nil
        self.transaction = transaction
        self.subspaces = subspaces
    }

    package init(
        queryContext: IndexQueryContext,
        transaction: any TransactionReadAccess,
        index: ReadableIndex?
    ) {
        self.init(
            queryContext: queryContext,
            transaction: transaction,
            subspace: index?.subspace
        )
    }

    package init(
        queryContext: IndexQueryContext,
        transaction: any TransactionReadAccess,
        indexes: [ReadableIndex]
    ) {
        self.queryContext = queryContext
        self.transaction = transaction
        self.subspaces = indexes.map(\.subspace)
    }

    public var capabilities: TransactionCapabilities {
        transaction.capabilities
    }

    public func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        let admittedSubspace = try requireSubspace(containing: key)
        guard admittedSubspace.contains(key) else {
            throw IndexReadAccessError.keyOutsideReadableIndex
        }
        return try await transaction.getValue(for: key, snapshot: snapshot)
    }

    public func getValue(for key: ByteString) async throws -> ByteString? {
        try await getValue(for: key, snapshot: false)
    }

    public func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        let admittedSubspace = try requireSubspace(containing: selector.key)
        let upperBound = try subspaceUpperBound(admittedSubspace)
        guard selector.key >= admittedSubspace.prefix,
              selector.key <= upperBound else {
            throw IndexReadAccessError.keyOutsideReadableIndex
        }
        if selector.offset == 1, selector.key == upperBound {
            return nil
        }

        let bounds: (begin: KeySelector, end: KeySelector, reverse: Bool)
        switch (selector.orEqual, selector.offset) {
        case (false, 1), (true, 1):
            bounds = (
                begin: selector,
                end: .firstGreaterOrEqual(upperBound),
                reverse: false
            )
        case (false, 0):
            bounds = (
                begin: .firstGreaterOrEqual(admittedSubspace.prefix),
                end: .firstGreaterOrEqual(selector.key),
                reverse: true
            )
        case (true, 0):
            bounds = (
                begin: .firstGreaterOrEqual(admittedSubspace.prefix),
                end: selector.key >= upperBound
                    ? .firstGreaterOrEqual(upperBound)
                    : .firstGreaterThan(selector.key),
                reverse: true
            )
        default:
            throw IndexReadAccessError.unsupportedKeySelector
        }

        var cursor = transaction.rangeCursor(
            from: bounds.begin,
            to: bounds.end,
            limit: 1,
            reverse: bounds.reverse,
            snapshot: snapshot,
            streamingMode: .iterator
        )
        let key: ByteString?
        do {
            key = try await cursor.next()?.0
        } catch let cleanupError as StorageRangeCleanupError {
            throw cleanupError
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
        guard let key else { return nil }
        guard admittedSubspace.contains(key), key < upperBound else {
            throw IndexReadAccessError
                .backendReturnedKeyOutsideReadableIndex
        }
        return key
    }

    public func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        do {
            guard isForwardStartSelector(begin),
                  isExclusiveEndSelector(end) else {
                return failedCursor(
                    IndexReadAccessError.unsupportedKeySelector
                )
            }
            let admittedSubspace = try validateRange(
                beginKey: begin.key,
                endKey: end.key
            )
            let upperBound = try subspaceUpperBound(admittedSubspace)
            guard !begin.orEqual || begin.key < upperBound else {
                return failedCursor(
                    IndexReadAccessError.rangeOutsideReadableIndex
                )
            }
            let cursor = transaction.rangeCursor(
                from: begin,
                to: end,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            )
            return KeyValueCursor(
                consuming: ScopedIndexRangeResult(
                    cursor: cursor,
                    subspace: admittedSubspace,
                    upperBound: upperBound
                )
            )
        } catch let error as IndexReadAccessError {
            return failedCursor(error)
        } catch {
            return failedCursor(error)
        }
    }

    public func getEstimatedRangeSizeBytes(
        beginKey: ByteString,
        endKey: ByteString
    ) async throws -> Int {
        try validateRange(beginKey: beginKey, endKey: endKey)
        return try await transaction.getEstimatedRangeSizeBytes(
            beginKey: beginKey,
            endKey: endKey
        )
    }

    public func getRangeSplitPoints(
        beginKey: ByteString,
        endKey: ByteString,
        chunkSize: Int
    ) async throws -> [ByteString] {
        let admittedSubspace = try validateRange(
            beginKey: beginKey,
            endKey: endKey
        )
        let upperBound = try subspaceUpperBound(admittedSubspace)
        let points = try await transaction.getRangeSplitPoints(
            beginKey: beginKey,
            endKey: endKey,
            chunkSize: chunkSize
        )
        guard points.allSatisfy({ point in
            point >= admittedSubspace.prefix && point <= upperBound
        }) else {
            throw IndexReadAccessError.backendReturnedKeyOutsideReadableIndex
        }
        return points
    }

    private func requireSubspace(containing key: ByteString? = nil) throws -> Subspace {
        guard !subspaces.isEmpty else {
            throw IndexReadAccessError.indexPartitionAbsent
        }
        guard subspaces.allSatisfy({ !$0.prefix.isEmpty }) else {
            throw IndexReadAccessError.invalidReadableIndexSubspace
        }
        let subspace: Subspace
        if let key {
            var matching: Subspace?
            for candidate in subspaces {
                let upperBound = try subspaceUpperBound(candidate)
                if key >= candidate.prefix && key <= upperBound {
                    matching = candidate
                    break
                }
            }
            guard let matching else {
                throw IndexReadAccessError.keyOutsideReadableIndex
            }
            subspace = matching
        } else if subspaces.count == 1 {
            subspace = subspaces[0]
        } else {
            throw IndexReadAccessError.rangeOutsideReadableIndex
        }
        guard !subspace.prefix.isEmpty else {
            throw IndexReadAccessError.invalidReadableIndexSubspace
        }
        return subspace
    }

    @discardableResult
    private func validateRange(
        beginKey: ByteString,
        endKey: ByteString
    ) throws -> Subspace {
        guard !subspaces.isEmpty else {
            throw IndexReadAccessError.indexPartitionAbsent
        }
        guard subspaces.allSatisfy({ !$0.prefix.isEmpty }) else {
            throw IndexReadAccessError.invalidReadableIndexSubspace
        }
        guard beginKey <= endKey,
              let admittedSubspace = try subspaces.first(where: { candidate in
                  let upperBound = try subspaceUpperBound(candidate)
                  return beginKey >= candidate.prefix
                      && endKey <= upperBound
              }) else {
            throw IndexReadAccessError.rangeOutsideReadableIndex
        }
        return admittedSubspace
    }

    private func subspaceUpperBound(_ subspace: Subspace) throws -> ByteString {
        do {
            return try strinc(subspace.prefix)
        } catch {
            throw IndexReadAccessError.invalidReadableIndexSubspace
        }
    }

    private func isForwardStartSelector(_ selector: KeySelector) -> Bool {
        selector.offset == 1
    }

    private func isExclusiveEndSelector(_ selector: KeySelector) -> Bool {
        !selector.orEqual && selector.offset == 1
    }

    private func failedCursor(_ error: any Error) -> KeyValueCursor {
        KeyValueCursor(consuming: FailedIndexRangeResult(error: error))
    }
}

extension ScopedIndexReadAccess: IndexQueryReadAccess {
    package func withAuxiliaryReadStorage<Result: Sendable>(
        namespace: ByteString,
        _ operation: @Sendable @escaping (
            Subspace,
            any IndexReadAccess
        ) async throws -> Result
    ) async throws -> Result {
        guard let queryContext else {
            throw IndexReadAccessError.queryContextUnavailable
        }
        guard !namespace.isEmpty else {
            throw IndexReadAccessError.invalidReadableIndexSubspace
        }
        let subspace = try queryContext.context.operationDataRoot()
            .subspace("data")
            .subspace(namespace)
        return try await operation(
            subspace,
            ScopedIndexReadAccess(
                transaction: transaction,
                subspace: subspace
            )
        )
    }

    package func withAuxiliaryReadStorage<Result: Sendable>(
        path: [String],
        _ operation: @Sendable @escaping (
            Subspace,
            any IndexReadAccess
        ) async throws -> Result
    ) async throws -> Result {
        guard let queryContext else {
            throw IndexReadAccessError.queryContextUnavailable
        }
        guard !path.isEmpty, path.allSatisfy({ !$0.isEmpty }) else {
            throw IndexReadAccessError.invalidReadableIndexSubspace
        }
        var subspace = try queryContext.context.operationDataRoot()
            .subspace("data")
        for component in path {
            subspace = subspace.subspace(component)
        }
        return try await operation(
            subspace,
            ScopedIndexReadAccess(
                transaction: transaction,
                subspace: subspace
            )
        )
    }

    package func fetchPersistedModelsPreservingOrder<PrimaryKeys>(
        entity: Schema.Entity,
        primaryKeys: PrimaryKeys,
        partitions: FieldObject,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<PersistedModel?>
    where PrimaryKeys: RandomAccessCollection & Sendable,
          PrimaryKeys.Element == Tuple {
        guard let queryContext else {
            throw IndexReadAccessError.queryContextUnavailable
        }
        return try await queryContext.context
            .fetchPersistedModelsPreservingOrder(
            entity: entity,
            primaryKeys: primaryKeys,
            partitions: partitions,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    package func withReadableIndex<T: Persistable, Result: Sendable>(
        named indexName: String,
        indexType: IndexType,
        for type: T.Type,
        authorization: IndexReadAuthorization,
        _ operation: @Sendable @escaping (
            ReadableIndex?,
            any IndexQueryReadAccess
        ) async throws -> Result
    ) async throws -> Result {
        guard let queryContext else {
            throw IndexReadAccessError.queryContextUnavailable
        }
        let index = try await queryContext.readableIndex(
            named: indexName,
            indexType: indexType,
            for: type,
            authorization: authorization,
            transaction: transaction
        )
        return try await operation(
            index,
            ScopedIndexReadAccess(
                queryContext: queryContext,
                transaction: transaction,
                index: index
            )
        )
    }

    package func withReadableIndex<Result: Sendable>(
        named indexName: String,
        indexType: IndexType,
        forEntityName entityName: String,
        partitions: FieldObject,
        authorization: IndexReadAuthorization,
        _ operation: @Sendable @escaping (
            ReadableIndex?,
            any IndexQueryReadAccess
        ) async throws -> Result
    ) async throws -> Result {
        guard let queryContext else {
            throw IndexReadAccessError.queryContextUnavailable
        }
        let index = try await queryContext.readableIndex(
            named: indexName,
            indexType: indexType,
            forEntityName: entityName,
            partitions: partitions,
            authorization: authorization,
            transaction: transaction
        )
        return try await operation(
            index,
            ScopedIndexReadAccess(
                queryContext: queryContext,
                transaction: transaction,
                index: index
            )
        )
    }

    package func withReadableIndexes<Result: Sendable>(
        _ requests: [IndexReadRequest],
        _ operation: @Sendable @escaping (
            [ReadableIndex?],
            any IndexQueryReadAccess
        ) async throws -> Result
    ) async throws -> Result {
        guard let queryContext else {
            throw IndexReadAccessError.queryContextUnavailable
        }
        var indexes: [ReadableIndex?] = []
        indexes.reserveCapacity(requests.count)
        for request in requests {
            indexes.append(
                try await queryContext.readableIndex(
                    named: request.indexName,
                    indexType: request.indexType,
                    forEntityName: request.entityName,
                    partitions: request.partitions,
                    authorization: request.authorization,
                    transaction: transaction
                )
            )
        }
        return try await operation(
            indexes,
            ScopedIndexReadAccess(
                queryContext: queryContext,
                transaction: transaction,
                indexes: indexes.compactMap { $0 }
            )
        )
    }
}

private struct ScopedIndexRangeResult: TransactionRangeResult {
    let cursor: KeyValueCursor
    let subspace: Subspace
    let upperBound: ByteString

    func makeCursor() -> ScopedIndexRangeCursor {
        ScopedIndexRangeCursor(
            cursor: cursor,
            subspace: subspace,
            upperBound: upperBound
        )
    }
}

private struct ScopedIndexRangeCursor: TransactionRangeCursor {
    private var cursor: KeyValueCursor
    private let subspace: Subspace
    private let upperBound: ByteString

    init(
        cursor: KeyValueCursor,
        subspace: Subspace,
        upperBound: ByteString
    ) {
        self.cursor = cursor
        self.subspace = subspace
        self.upperBound = upperBound
    }

    mutating func next() async throws -> (ByteString, ByteString)? {
        guard let element = try await cursor.next() else { return nil }
        guard subspace.contains(element.0), element.0 < upperBound else {
            let boundaryError = IndexReadAccessError
                .backendReturnedKeyOutsideReadableIndex
            do {
                try await cursor.finish()
            } catch {
                throw StorageRangeCleanupError(
                    iterationError: boundaryError,
                    cleanupError: error
                )
            }
            throw boundaryError
        }
        return element
    }

    mutating func finish(
        isolation actor: isolated (any Actor)?
    ) async throws {
        try await cursor.finish()
    }
}

private struct FailedIndexRangeResult: TransactionRangeResult {
    let error: any Error

    func makeCursor() -> FailedIndexRangeCursor {
        FailedIndexRangeCursor(error: error)
    }
}

private struct FailedIndexRangeCursor: TransactionRangeCursor {
    let error: any Error

    mutating func next() async throws -> (ByteString, ByteString)? {
        throw error
    }

    mutating func finish(
        isolation actor: isolated (any Actor)?
    ) async throws {}
}
