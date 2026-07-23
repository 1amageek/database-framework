import Core
import DatabaseValue
import DatabaseEngine
import StorageKit
import Testing
@testable import ScalarIndex

@Suite("Scalar UInt64 index runtime")
struct ScalarUInt64IndexRuntimeTests {
    @Test("Provider emits exact ordered keys across the Int64 boundary")
    func providerBuildsFullWidthKeys() async throws {
        let values: [UInt64] = [
            UInt64(Int64.max),
            UInt64(Int64.max) + 1,
            UInt64.max,
        ]
        let kind = ScalarIndexKind<UnsignedScalarEntity>(fields: [\.value])
        let index = Index(
            name: "UnsignedScalarEntity_value",
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "value"),
            subspaceKey: "UnsignedScalarEntity_value",
            itemTypes: [UnsignedScalarEntity.persistableType]
        )
        let indexSubspace = Subspace(
            prefix: Tuple("scalar-uint64-runtime").pack()
        )
        let maintainer: any IndexMaintainer<UnsignedScalarEntity> = try ScalarIndexMaintainerProvider()
            .makeIndexMaintainer(
                index: index,
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id"),
                configurations: []
            )

        var keys: [Bytes] = []
        keys.reserveCapacity(values.count)
        for (offset, value) in values.enumerated() {
            let entity = UnsignedScalarEntity(
                id: "entity-\(offset)",
                value: value
            )
            let computed = try await maintainer.computeIndexKeys(
                for: entity,
                id: Tuple(entity.id)
            )
            #expect(computed.count == 1)
            let key = try #require(computed.first)
            let unpacked = try indexSubspace.unpack(key)
            let decoded = try TupleDecoder.decode(
                unpacked.element(at: 0),
                as: UInt64.self
            )

            #expect(decoded == value)
            keys.append(key)
        }

        #expect(keys[0].lexicographicallyPrecedes(keys[1]))
        #expect(keys[1].lexicographicallyPrecedes(keys[2]))
    }
}

private struct UnsignedScalarEntity: Persistable {
    typealias ID = String

    let id: String
    let value: UInt64

    static var persistableType: String { "UnsignedScalarEntity" }
    static var allFields: [String] { ["id", "value"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": id
        case "value": value
        default: nil
        }
    }

    static func fieldName<Value>(
        for keyPath: KeyPath<UnsignedScalarEntity, Value>
    ) -> String {
        switch keyPath {
        case \UnsignedScalarEntity.id: "id"
        case \UnsignedScalarEntity.value: "value"
        default: "\(keyPath)"
        }
    }

    static func fieldName(
        for keyPath: PartialKeyPath<UnsignedScalarEntity>
    ) -> String {
        switch keyPath {
        case \UnsignedScalarEntity.id: "id"
        case \UnsignedScalarEntity.value: "value"
        default: "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        guard let keyPath = keyPath as? PartialKeyPath<UnsignedScalarEntity> else {
            return "\(keyPath)"
        }
        return fieldName(for: keyPath)
    }
}
