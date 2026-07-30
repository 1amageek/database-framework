import DatabaseKit
import DatabaseTypes
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
        let index = Index(
            name: "UnsignedScalarEntity_value",
            kind: scalarIndexMetadata(
                fields: [FieldIdentity(name: "value", number: 2)]
            ),
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
                configurations: [],
                wallClock: FixedScalarIndexWallClock(
                    now: Timestamp(secondsSinceUnixEpoch: 0)
                )
            )

        var keys: [ByteString] = []
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
            let decodedField = try FieldValue(
                tupleElement: unpacked.element(at: 0)
            )
            guard case .uint64(let decoded) = decodedField else {
                Issue.record("Expected a UInt64 field value")
                continue
            }

            #expect(decoded == value)
            keys.append(key)
        }

        #expect(keys[0].lexicographicallyPrecedes(keys[1]))
        #expect(keys[1].lexicographicallyPrecedes(keys[2]))
    }

    @Test("Provider exposes uniqueness without runtime capability casts")
    func providerBuildsUniquenessMaintainer() async throws {
        let index = Index(
            name: "UnsignedScalarEntity_value",
            kind: scalarIndexMetadata(
                fields: [FieldIdentity(name: "value", number: 2)]
            ),
            rootExpression: FieldKeyExpression(fieldName: "value"),
            subspaceKey: "UnsignedScalarEntity_value",
            itemTypes: [UnsignedScalarEntity.persistableType],
            isUnique: true
        )
        let provider = ScalarIndexMaintainerProvider()

        let maintainer: any IndexUniquenessMaintainer<UnsignedScalarEntity> =
            try provider.makeIndexUniquenessMaintainer(
                index: index,
                subspace: Subspace(prefix: Tuple("scalar-unique-runtime").pack()),
                idExpression: FieldKeyExpression(fieldName: "id"),
                configurations: []
            )

        let keys = try await maintainer.computeIndexKeys(
            for: UnsignedScalarEntity(id: "entity", value: UInt64.max),
            id: Tuple("entity")
        )
        #expect(keys.count == 1)
    }
}

private struct FixedScalarIndexWallClock: WallClock {
    let now: Timestamp
}

@Persistable
private struct UnsignedScalarEntity {
    var id: String
    var value: UInt64
}
