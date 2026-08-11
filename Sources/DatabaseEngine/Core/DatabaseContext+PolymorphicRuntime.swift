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
    public func executeCanonicalRead<T: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable @escaping (any TransactionAccess) async throws -> T
    ) async throws -> T {
        try await withStorageAccess(
            requiredAccess: .read,
            configuration: configuration,
            operation
        )
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

        return try await withStorageAccess(
            requiredAccess: .read,
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
            let storage = self.container.itemStorageFactory.make(transaction: transaction, blobsSubspace: blobsSubspace)
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
                let item = try runtime.canonicalized(persistedModel)
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
        try await withStorageAccess(
            requiredAccess: .read,
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
        transaction: any TransactionAccess
    ) async throws -> [PolymorphicEntity] {
        try await fetchPolymorphicItemsPreservingOrder(
            group: group,
            ids: ids,
            transaction: transaction
        ).compactMap { $0 }
    }

    /// Fetches polymorphic rows without discarding unresolved identifiers.
    /// The returned array has exactly one slot for every requested identifier.
    package func fetchPolymorphicItemsPreservingOrder(
        group: PolymorphicGroup,
        ids: [Tuple],
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> [PolymorphicEntity?] {
        let reservation = try workMeter?.reserveIntermediate(
            rows: UInt64(ids.count),
            bytes: try DatabaseIntermediateFootprint(
                bytes: UInt64(
                    max(1, MemoryLayout<PolymorphicEntity?>.stride + 16)
                )
            ).multiplied(by: UInt64(ids.count)).bytes,
            at: .storageRow
        )
        defer { reservation?.release() }
        guard let subspace = try await container.openPolymorphicDirectory(
            for: group.identifier,
            transaction: transaction
        ) else {
            return [PolymorphicEntity?](repeating: nil, count: ids.count)
        }
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        let typeMap = try polymorphicTypeMap(for: group)
        let storage = container.itemStorageFactory.make(
            transaction: transaction,
            blobsSubspace: blobsSubspace
        )
        func load(
            _ id: Tuple
        ) async throws -> PolymorphicEntity? {
            guard id.count > 0 else {
                throw PolymorphicRuntimeError.invalidRequestedIdentifier
            }
            let typeCodeValue = try id.value(at: 0)
            guard case .signedInteger(let typeCode) = typeCodeValue else {
                throw PolymorphicRuntimeError.invalidRequestedIdentifier
            }
            let key = itemSubspace.pack(id)
            guard let data = try await storage.read(for: key) else {
                return nil
            }
            guard let runtime = typeMap[typeCode] else {
                throw PolymorphicRuntimeError.unknownTypeCode(typeCode)
            }
            let persistedModel = try DataAccess.deserializePersistedModel(
                data,
                expectedEntity: runtime.entity.name
            )
            let item = try runtime.canonicalized(persistedModel)
            try container.securityDelegate?.evaluateGet(
                persistedModel,
                fields: nil
            )
            return PolymorphicEntity(
                item: item,
                typeName: runtime.entity.name,
                typeCode: typeCode,
                polymorphicIdentifier: id
            )
        }
        var items = [PolymorphicEntity?](
            repeating: nil,
            count: ids.count
        )
        for index in ids.indices {
            // TransactionAccess is a serial operation boundary. A true batch
            // read requires an explicit StorageKit contract; child tasks must
            // not issue overlapping operations against one transaction.
            try workMeter?.consume(at: .storageRow)
            items[index] = try await load(ids[index])
        }
        return items
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
            try evaluatePolymorphicListAccess(
                for: runtime.entity,
                limit: limit,
                offset: offset,
                orderBy: orderBy
            )
        }
    }

    private func evaluatePolymorphicListAccess(
        for entity: Schema.Entity,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) throws {
        try container.securityDelegate?.evaluateList(
            entity: entity.name,
            limit: limit,
            offset: offset,
            orderBy: orderBy
        )
    }

}
