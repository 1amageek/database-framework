import Foundation
import Core
import StorageKit

public enum PolymorphicRowAnnotation {
    public static let typeName = "_typeName"
    public static let typeCode = "_typeCode"
}

public struct PolymorphicRecord: Sendable {
    public let item: any Persistable
    public let typeName: String
    public let typeCode: Int64

    public init(item: any Persistable, typeName: String, typeCode: Int64) {
        self.item = item
        self.typeName = typeName
        self.typeCode = typeCode
    }
}

extension FDBContext {
    public func executeCanonicalRead<T: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable @escaping (any Transaction) async throws -> T
    ) async throws -> T {
        try await withRawTransaction(
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
    ) async throws -> [PolymorphicRecord] {
        let subspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        let typeMap = polymorphicTypeMap(for: group)

        try authorizePolymorphicListAccess(
            group: group,
            limit: limit,
            offset: offset,
            orderBy: orderBy
        )

        return try await withRawTransaction(configuration: configuration) { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            let (begin, end) = itemSubspace.range()
            var records: [PolymorphicRecord] = []

            for try await (key, data) in storage.scan(begin: begin, end: end, snapshot: true) {
                let tuple = try itemSubspace.unpack(key)
                guard let typeCode = tuple[0] as? Int64,
                      let runtimeType = typeMap[typeCode] else {
                    continue
                }
                let item = try DataAccess.deserializeAny(data, as: runtimeType)
                guard self.isPolymorphicGetAllowed(item) else {
                    continue
                }
                records.append(
                    PolymorphicRecord(
                        item: item,
                        typeName: runtimeType.persistableType,
                        typeCode: typeCode
                    )
                )
            }
            return records
        }
    }

    public func fetchPolymorphicItems(
        group: PolymorphicGroup,
        ids: [Tuple],
        configuration: TransactionConfiguration = .default,
        cachePolicy: CachePolicy = .server
    ) async throws -> [PolymorphicRecord] {
        let subspace = try await container.resolvePolymorphicDirectory(for: group.identifier)
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        let typeMap = polymorphicTypeMap(for: group)

        return try await withRawTransaction(configuration: configuration) { transaction in
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            var items: [PolymorphicRecord] = []

            for id in ids {
                guard let typeCode = id[0] as? Int64 else {
                    continue
                }
                let key = itemSubspace.pack(id)
                guard let data = try await storage.read(for: key),
                      let runtimeType = typeMap[typeCode] else {
                    continue
                }
                let item = try DataAccess.deserializeAny(data, as: runtimeType)
                guard self.isPolymorphicGetAllowed(item) else {
                    continue
                }
                items.append(
                    PolymorphicRecord(
                        item: item,
                        typeName: runtimeType.persistableType,
                        typeCode: typeCode
                    )
                )
            }
            return items
        }
    }

    func polymorphicTypeMap(
        for group: PolymorphicGroup
    ) -> [Int64: any Persistable.Type] {
        Dictionary(
            uniqueKeysWithValues: group.memberTypeNames.compactMap { typeName in
                guard let type = container.schema.entity(named: typeName)?.persistableType,
                      let polymorphicType = type as? any Polymorphable.Type else {
                    return nil
                }
                return (polymorphicType.typeCode(for: type.persistableType), type)
            }
        )
    }

    public func authorizePolymorphicListAccess(
        group: PolymorphicGroup,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) throws {
        for typeName in group.memberTypeNames {
            guard let type = container.schema.entity(named: typeName)?.persistableType else {
                continue
            }
            try evaluatePolymorphicListAccess(
                for: type,
                limit: limit,
                offset: offset,
                orderBy: orderBy
            )
        }
    }

    private func evaluatePolymorphicListAccess(
        for type: any Persistable.Type,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) throws {
        func helper<T: Persistable>(_ concreteType: T.Type) throws {
            try container.securityDelegate?.evaluateList(
                type: concreteType,
                limit: limit,
                offset: offset,
                orderBy: orderBy
            )
        }

        try _openExistential(type, do: helper)
    }

    private func isPolymorphicGetAllowed(_ item: any Persistable) -> Bool {
        do {
            try container.securityDelegate?.evaluateGet(item)
            return true
        } catch {
            return false
        }
    }
}
