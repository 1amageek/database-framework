import DatabaseKit
import StorageKit

public enum PolymorphicRowAnnotation {
    public static let typeName = "_typeName"
    public static let typeCode = "_typeCode"
}

public struct PolymorphicEntity: Sendable {
    public let item: PersistedModel
    public let typeName: String
    public let typeCode: Int64
    public let polymorphicIdentifier: Tuple

    public init(
        item: PersistedModel,
        typeName: String,
        typeCode: Int64,
        polymorphicIdentifier: Tuple
    ) {
        self.item = item
        self.typeName = typeName
        self.typeCode = typeCode
        self.polymorphicIdentifier = polymorphicIdentifier
    }
}

extension DatabaseContext {
    @_spi(DatabaseExecution)
    public func executeCanonicalRead<T: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable @escaping (
            any TransactionReadAccess
        ) async throws -> T
    ) async throws -> T {
        try await withReadStorageAccess(
            configuration: configuration,
            operation
        )
    }

    /// Executes a canonical read at an optional restorable storage position
    /// and returns the actual read point without exposing transaction-control
    /// methods to the callback.
    @_spi(DatabaseExecution)
    public func executeCanonicalRead<T: Sendable>(
        restoring requestedReadPoint: DatabaseExecutionReadPoint?,
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable @escaping (
            any TransactionReadAccess,
            DatabaseExecutionReadPoint
        ) async throws -> T
    ) async throws -> T {
        if requestedReadPoint != nil {
            guard ActiveDatabaseTransactionContext.binding == nil else {
                throw DatabaseExecutionReadPointError
                    .restoreRequiresIndependentTransaction
            }
        }
        return try await withAdmittedStorageAccess(
            requiredAccess: .read,
            mode: .readOnly,
            configuration: configuration,
            requestedReadPoint: requestedReadPoint
        ) { transaction in
            let identity = try self.executionStorage()
            if let requestedReadPoint,
               requestedReadPoint.domainIdentifier
                != identity.domainIdentifier {
                throw DatabaseExecutionReadPointError.domainMismatch
            }
            guard let rooted = transaction as? DataRootTransactionAccess else {
                throw DatabaseRuntimeError.internalError(
                    "Canonical reads require data-root-admitted storage access"
                )
            }
            let position: DatabaseExecutionReadPoint.Position
            if let version = try await rooted.captureReadVersion() {
                position = .version(version)
            } else {
                var generator = SystemRandomNumberGenerator()
                position = .opaque(ByteString((0..<32).map { _ in
                    UInt8.random(in: .min ... .max, using: &generator)
                }))
            }
            return try await operation(
                rooted.readProjection(),
                DatabaseExecutionReadPoint(
                    domainIdentifier: identity.domainIdentifier,
                    position: position
                )
            )
        }
    }

    func scanPolymorphicItems(
        group: PolymorphicGroup,
        configuration: TransactionConfiguration = .default,
        limit: Int? = nil,
        offset: Int? = nil,
        orderBy: [String]? = nil
    ) async throws -> [PolymorphicEntity] {
        let typeMap = try polymorphicTypeMap(for: group)

        try authorizePolymorphicListAccess(
            group: group,
            limit: limit,
            offset: offset,
            orderBy: orderBy
        )

        return try await withReadStorageAccess(
            configuration: configuration
        ) { transaction in
            guard let subspace = try await self.container
                .openPolymorphicDirectory(
                    for: group.identifier,
                    transaction: transaction
                ) else {
                return []
            }
            let itemSubspace = subspace.subspace(SubspaceKey.items)
            let blobsSubspace = subspace.subspace(SubspaceKey.blobs)
            let storage = self.container.itemStorageFactory.makeReader(
                transaction: transaction,
                blobsSubspace: blobsSubspace
            )
            let (begin, end) = itemSubspace.range()
            var entities: [PolymorphicEntity] = []

            var iterator = storage.scan(
                begin: begin,
                end: end,
                snapshot: true
            ).makeAsyncIterator()
            while let (key, data) = try await iterator.next() {
                let tuple = try itemSubspace.unpack(key)
                guard tuple.count > 0 else {
                    throw PolymorphicRuntimeError.invalidStoredIdentifier
                }
                let typeCodeValue = try tuple.value(at: 0)
                guard case .signedInteger(let typeCode) = typeCodeValue else {
                    throw PolymorphicRuntimeError.invalidStoredIdentifier
                }
                guard let runtime = typeMap[typeCode] else {
                    throw PolymorphicRuntimeError.unknownTypeCode(typeCode)
                }
                let persistedModel = try DataAccess.deserializePersistedModel(
                    data,
                    expectedEntity: runtime.entity.name
                )
                let item = try runtime.canonicalized(persistedModel).detached()
                try self.container.securityDelegate?.evaluateGet(
                    persistedModel,
                    fields: nil
                )
                entities.append(
                    PolymorphicEntity(
                        item: item,
                        typeName: runtime.entity.name,
                        typeCode: typeCode,
                        polymorphicIdentifier: tuple
                    )
                )
            }
            return entities
        }
    }

    public func fetchPolymorphicItems(
        group: PolymorphicGroup,
        ids: [Tuple],
        configuration: TransactionConfiguration = .default
    ) async throws -> [PolymorphicEntity] {
        try await withReadStorageAccess(
            configuration: configuration
        ) { transaction in
            try await self.fetchPolymorphicItems(
                group: group,
                ids: ids,
                transaction: transaction
            )
        }
    }

    /// Fetches polymorphic rows on the caller-owned transaction snapshot.
    package func fetchPolymorphicItems(
        group: PolymorphicGroup,
        ids: [Tuple],
        transaction: any TransactionReadAccess
    ) async throws -> [PolymorphicEntity] {
        let execution = ReadExecutionContext(
            monotonicClock: container.monotonicClock
        )
        return try await fetchPolymorphicItemsPreservingOrder(
            group: group,
            ids: ids,
            transaction: transaction,
            workMeter: execution.workMeter
        ).compactMap { $0 }
    }

    /// Fetches polymorphic rows without discarding unresolved identifiers.
    /// The returned array has exactly one slot for every requested identifier.
    package func fetchPolymorphicItemsPreservingOrder<Identifiers>(
        group: PolymorphicGroup,
        ids: Identifiers,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<PolymorphicEntity?>
    where Identifiers: RandomAccessCollection & Sendable,
          Identifiers.Element == Tuple {
        try await withReadStorageAccess { transaction in
            try await self.fetchPolymorphicItemsPreservingOrder(
                group: group,
                ids: ids,
                transaction: transaction,
                workMeter: workMeter
            )
        }
    }

    /// Fetches polymorphic rows on an explicit trusted transaction capability.
    package func fetchPolymorphicItemsPreservingOrder<Identifiers>(
        group: PolymorphicGroup,
        ids: Identifiers,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSharedRetainedArray<PolymorphicEntity?>
    where Identifiers: RandomAccessCollection & Sendable,
          Identifiers.Element == Tuple {
        let reservation = try workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: ids.count,
                element: PolymorphicEntity?.self
            ).bytes,
            at: .storageRow
        )
        var transferredReservation = false
        defer {
            if !transferredReservation { reservation.release() }
        }
        guard let subspace = try await container.openPolymorphicDirectory(
            for: group.identifier,
            transaction: transaction
        ) else {
            let nilFootprint = try DatabaseIntermediateFootprint(
                rows: 1,
                bytes: 16
            ).multiplied(by: UInt64(ids.count))
            try reservation.reserveAdditional(
                rows: nilFootprint.rows,
                bytes: nilFootprint.bytes,
                at: .storageRow
            )
            let retainedItems = try DatabaseSharedRetainedArray.adopting(
                [PolymorphicEntity?](repeating: nil, count: ids.count),
                reservation: reservation,
                workMeter: workMeter,
                stage: .storageRow
            )
            transferredReservation = true
            return retainedItems
        }
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        let typeMap = try polymorphicTypeMap(for: group)
        let storage = container.itemStorageFactory.makeReader(
            transaction: transaction,
            blobsSubspace: blobsSubspace
        )
        func load(
            _ id: Tuple
        ) async throws -> (entity: PolymorphicEntity?, admitted: Bool) {
            guard id.count > 0 else {
                throw PolymorphicRuntimeError.invalidRequestedIdentifier
            }
            let typeCodeValue = try id.value(at: 0)
            guard case .signedInteger(let typeCode) = typeCodeValue else {
                throw PolymorphicRuntimeError.invalidRequestedIdentifier
            }
            let key = itemSubspace.pack(id)
            guard let retainedValue = try await storage.readRetained(
                for: key,
                workMeter: workMeter,
                stage: .storageRow
            ) else {
                return (nil, false)
            }
            guard let runtime = typeMap[typeCode] else {
                throw PolymorphicRuntimeError.unknownTypeCode(typeCode)
            }
            let fieldOverhead = try DatabaseIntermediateFootprint(
                bytes: 96
            ).multiplied(by: UInt64(runtime.entity.fields.count))
            let provisionalFootprint = try DatabaseIntermediateFootprint(
                rows: 1,
                bytes: UInt64(retainedValue.count)
            ).adding(fieldOverhead).adding(
                DatabaseIntermediateFootprint(
                    bytes: UInt64(runtime.entity.name.utf8.count)
                        + UInt64(id.count) * 64
                        + 320
                )
            )
            try reservation.reserveAdditional(
                rows: provisionalFootprint.rows,
                bytes: provisionalFootprint.bytes,
                at: .storageRow
            )
            let persistedModel = try retainedValue.withValue { data in
                try DataAccess.deserializePersistedModel(
                    data,
                    expectedEntity: runtime.entity.name
                )
            }
            let item = try runtime.canonicalized(persistedModel).detached()
            var actualFootprint = try CanonicalRelationalFootprintMeter
                .footprint(of: item, workMeter: workMeter)
            actualFootprint = try actualFootprint.adding(
                DatabaseIntermediateFootprint(
                    bytes: UInt64(runtime.entity.name.utf8.count)
                        + UInt64(id.count) * 64
                        + 320
                )
            )
            if actualFootprint.bytes > provisionalFootprint.bytes {
                try reservation.reserveAdditional(
                    bytes: actualFootprint.bytes - provisionalFootprint.bytes,
                    at: .storageRow
                )
            } else if actualFootprint.bytes < provisionalFootprint.bytes {
                try reservation.releasePartial(
                    bytes: provisionalFootprint.bytes - actualFootprint.bytes
                )
            }
            try container.securityDelegate?.evaluateGet(
                item,
                fields: nil
            )
            return (
                PolymorphicEntity(
                    item: item,
                    typeName: runtime.entity.name,
                    typeCode: typeCode,
                    polymorphicIdentifier: id
                ),
                true
            )
        }
        var items = [PolymorphicEntity?](
            repeating: nil,
            count: ids.count
        )
        for (index, id) in ids.enumerated() {
            // TransactionAccess is a serial operation boundary. A true batch
            // read requires an explicit StorageKit contract; child tasks must
            // not issue overlapping operations against one transaction.
            try workMeter.consume(at: .storageRow)
            let loaded = try await load(id)
            if !loaded.admitted {
                try reservation.reserveAdditional(
                    rows: 1,
                    bytes: 16,
                    at: .storageRow
                )
            }
            items[index] = loaded.entity
        }
        let retainedItems = try DatabaseSharedRetainedArray.adopting(
            items,
            reservation: reservation,
            workMeter: workMeter,
            stage: .storageRow
        )
        transferredReservation = true
        return retainedItems
    }

    package func polymorphicTypeMap(
        for group: PolymorphicGroup
    ) throws -> [Int64: EntityRuntimeRegistration] {
        var result: [Int64: EntityRuntimeRegistration] = [:]
        for typeName in group.memberTypeNames {
            guard let runtime = container.runtimeConfiguration.entityRuntimes.registration(
                named: typeName
            ) else {
                throw DatabaseRuntimeConfigurationError
                    .missingCompiledPolymorphicMemberType(
                        groupIdentifier: group.identifier,
                        memberTypeName: typeName
                    )
            }
            guard runtime.entity.polymorphicMembership?.identifier == group.identifier else {
                throw PolymorphicRuntimeError.nonPolymorphableMember(
                    groupIdentifier: group.identifier,
                    memberTypeName: typeName
                )
            }
            result[PolymorphicTypeCode.value(for: runtime.entity.name)] = runtime
        }
        return result
    }

    public func authorizePolymorphicListAccess(
        group: PolymorphicGroup,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) throws {
        let authorization = IndexReadAuthorization(
            limit: limit,
            offset: offset,
            orderBy: orderBy
        )
        for typeName in group.memberTypeNames {
            guard let runtime = container.runtimeConfiguration.entityRuntimes.registration(
                named: typeName
            ) else {
                throw DatabaseRuntimeConfigurationError
                    .missingCompiledPolymorphicMemberType(
                        groupIdentifier: group.identifier,
                        memberTypeName: typeName
                    )
            }
            try indexQueryContext.authorizeListAccess(
                entityName: runtime.entity.name,
                authorization: authorization
            )
        }
    }

}
